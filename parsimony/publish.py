"""Publish catalog snapshots from plugin modules.

A plugin declares its catalogs by exporting one of two shapes on the
module:

* ``CATALOGS: list[tuple[str, Callable[[], Awaitable[Result]]]]`` — static
  list of ``(namespace, async_fn)`` pairs. Use when the namespace set is
  known at import time (e.g. FRED publishes one catalog called ``fred``).
* ``async def CATALOGS() -> AsyncIterator[tuple[str, Callable[[], Awaitable[Result]]]]``
  — async generator. Use when namespaces are fetched at build time (e.g.
  SDMX discovers agencies/dataflows on the wire).

An optional ``RESOLVE_CATALOG(namespace: str) -> Callable | None`` function
produces a single catalog on demand — used by ``--only`` when the caller
knows the namespace without iterating the full generator.

The publisher runs each enumerator once per namespace, ingests rows into a
fresh :class:`~parsimony.Catalog`, and pushes to
``target_template.format(namespace=...)``. No resume, no manifest, no
content hashing — if a publish fails, re-run it.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from parsimony.catalog import Catalog
from parsimony.discovery import DiscoveredProvider, discovered_providers
from parsimony.result import Result

logger = logging.getLogger(__name__)

__all__ = [
    "PublishReport",
    "collect_catalogs",
    "publish",
    "publish_provider",
]


CatalogFn = Callable[[], Awaitable[Result]]
CatalogEntry = tuple[str, CatalogFn]


@dataclass(frozen=True)
class PublishReport:
    """Outcome of a publish run for one provider."""

    provider: str
    target_template: str
    published: list[str]  # namespaces successfully published
    skipped: list[str]  # namespaces that had no rows
    failed: list[tuple[str, str]]  # (namespace, error message)

    @property
    def ok(self) -> bool:
        return not self.failed


# ---------------------------------------------------------------------------
# Collect catalogs from a plugin module
# ---------------------------------------------------------------------------


async def collect_catalogs(
    module: ModuleType,
    *,
    only: Iterable[str] | None = None,
) -> list[CatalogEntry]:
    """Resolve a plugin module's declared catalogs.

    Accepts the following shapes on *module*:

    * ``CATALOGS = [("ns", fn), ...]`` — static list.
    * ``async def CATALOGS(): yield "ns", fn`` — async generator function.
    * ``RESOLVE_CATALOG(namespace) -> Callable | None`` — on-demand lookup,
      only consulted when *only* names a namespace not yielded by
      ``CATALOGS``.

    When *only* is supplied, the result is filtered to just those namespaces;
    for each name not already yielded, ``RESOLVE_CATALOG`` is consulted.
    """
    wanted = None if only is None else {n for n in only}
    catalogs: list[CatalogEntry] = []
    seen: set[str] = set()

    raw = getattr(module, "CATALOGS", None)
    if raw is not None:
        async for ns, fn in _iter_catalogs(raw):
            if wanted is not None and ns not in wanted:
                continue
            catalogs.append((ns, fn))
            seen.add(ns)

    if wanted is not None:
        resolve = getattr(module, "RESOLVE_CATALOG", None)
        if resolve is not None:
            for ns in wanted - seen:
                fn = resolve(ns)
                if fn is not None:
                    catalogs.append((ns, fn))

    return catalogs


async def _iter_catalogs(raw: Any) -> AsyncIterator[CatalogEntry]:
    """Yield (namespace, fn) pairs from whatever shape *raw* takes."""
    if inspect.isasyncgenfunction(raw):
        async for item in raw():
            yield _validate_entry(item)
        return

    if inspect.isasyncgen(raw):
        async for item in raw:
            yield _validate_entry(item)
        return

    if callable(raw):
        result = raw()
        if inspect.isasyncgen(result):
            async for item in result:
                yield _validate_entry(item)
            return
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, Iterable):
            for item in result:
                yield _validate_entry(item)
            return
        raise TypeError(
            f"CATALOGS callable must return an async iterator or iterable of (ns, fn); got {type(result).__name__}"
        )

    if isinstance(raw, Iterable):
        for item in raw:
            yield _validate_entry(item)
        return

    raise TypeError(
        f"CATALOGS must be an iterable of (ns, fn) or an async generator function; got {type(raw).__name__}"
    )


def _validate_entry(item: Any) -> CatalogEntry:
    if not isinstance(item, tuple) or len(item) != 2:
        raise TypeError(f"CATALOGS entry must be a (namespace, fn) tuple; got {item!r}")
    ns, fn = item
    if not isinstance(ns, str) or not ns:
        raise ValueError(f"CATALOGS namespace must be a non-empty string; got {ns!r}")
    if not callable(fn):
        raise TypeError(f"CATALOGS fn for namespace {ns!r} must be callable; got {type(fn).__name__}")
    return ns, fn


# ---------------------------------------------------------------------------
# Bind connector-style dependencies from environment variables
# ---------------------------------------------------------------------------


def _bind_fn(fn: CatalogFn, env_vars: dict[str, str], env: dict[str, str]) -> CatalogFn:
    """Bind environment-sourced deps to *fn* if it's a :class:`~parsimony.Connector`.

    Plain async functions pass through unchanged — the plugin author is
    expected to have captured credentials via closure or module state.
    """
    from parsimony.connector import Connector

    if not isinstance(fn, Connector):
        return fn
    if not env_vars:
        return fn
    deps: dict[str, Any] = {}
    required = set(fn.dep_names)
    for dep_name, env_var in env_vars.items():
        value = env.get(env_var, "")
        if not value:
            if dep_name in required:
                raise RuntimeError(f"required env var {env_var!r} (→ dep {dep_name!r}) is not set for {fn.name!r}")
            continue
        deps[dep_name] = value
    return fn.bind_deps(**deps) if deps else fn


# ---------------------------------------------------------------------------
# Core publish primitives
# ---------------------------------------------------------------------------


async def publish(
    module: ModuleType,
    *,
    target: str,
    only: Iterable[str] | None = None,
    dry_run: bool = False,
    env_vars: dict[str, str] | None = None,
    env: dict[str, str] | None = None,
    provider_name: str | None = None,
) -> PublishReport:
    """Publish every declared catalog in *module* to ``target.format(namespace=...)``.

    Parameters
    ----------
    module:
        The plugin module exporting ``CATALOGS`` (and optionally
        ``RESOLVE_CATALOG``).
    target:
        URL template — must contain ``{namespace}``. Examples:
        ``"file:///tmp/catalogs/{namespace}"``, ``"hf://ockham/catalog-{namespace}"``.
    only:
        When set, limit the publish to these namespaces.
    dry_run:
        Collect catalogs and log targets, but skip enumeration + push.
    env_vars:
        ``{dep_name: env_var_name}`` mapping applied via
        ``Connector.bind_deps`` for entries that are Connectors.
    env:
        Override for ``os.environ``.
    provider_name:
        Label used in the report; defaults to ``module.__name__``.
    """
    if "{namespace}" not in target:
        raise ValueError(f"target {target!r} must contain '{{namespace}}'")

    resolved_env = env if env is not None else dict(os.environ)
    resolved_env_vars = env_vars if env_vars is not None else getattr(module, "ENV_VARS", {}) or {}
    report_name = provider_name or module.__name__

    catalogs = await collect_catalogs(module, only=only)
    published: list[str] = []
    skipped: list[str] = []
    failed: list[tuple[str, str]] = []

    for namespace, fn in catalogs:
        url = target.format(namespace=namespace)
        logger.info("publishing %s/%s → %s", report_name, namespace, url)
        if dry_run:
            published.append(namespace)
            continue
        try:
            bound = _bind_fn(fn, resolved_env_vars, resolved_env)
            result = await _invoke(bound)
            catalog = Catalog(namespace)
            index = await catalog.add_from_result(result)
            if index.indexed == 0 and index.total == 0:
                skipped.append(namespace)
                continue
            await catalog.push(url)
            published.append(namespace)
        except Exception as exc:  # broad — one catalog failure shouldn't abort the batch
            logger.exception("publish failed for %s/%s", report_name, namespace)
            failed.append((namespace, str(exc)))

    return PublishReport(
        provider=report_name,
        target_template=target,
        published=published,
        skipped=skipped,
        failed=failed,
    )


async def _invoke(fn: CatalogFn) -> Result:
    """Call *fn* with no arguments; if the callable is a Connector, use its empty params."""
    from parsimony.connector import Connector

    if isinstance(fn, Connector):
        return await fn(fn.param_type())
    ret = fn()
    if inspect.isawaitable(ret):
        ret = await ret
    if not isinstance(ret, Result):
        raise TypeError(f"catalog callable must return Result; got {type(ret).__name__}")
    return ret


# ---------------------------------------------------------------------------
# Discovery-integrated entry point (used by the CLI)
# ---------------------------------------------------------------------------


async def publish_provider(
    provider_name: str,
    *,
    target: str,
    only: Iterable[str] | None = None,
    dry_run: bool = False,
    env: dict[str, str] | None = None,
) -> PublishReport:
    """Publish *provider_name*'s catalogs — resolves the module via discovery."""
    provider = _find_provider(provider_name)
    module = provider.module
    if module is None:
        import importlib

        module = importlib.import_module(provider.module_path)
    return await publish(
        module,
        target=target,
        only=only,
        dry_run=dry_run,
        env_vars=dict(provider.env_vars),
        env=env,
        provider_name=provider.name,
    )


def _find_provider(name: str) -> DiscoveredProvider:
    for p in discovered_providers():
        if p.name == name:
            return p
    available = sorted(p.name for p in discovered_providers())
    raise ValueError(f"no parsimony provider named {name!r}. Available: {available}")


# ---------------------------------------------------------------------------
# Sync CLI entry point
# ---------------------------------------------------------------------------


def run_cli(
    *,
    provider: str,
    target: str,
    only: Iterable[str] | None = None,
    dry_run: bool = False,
) -> int:
    """CLI-friendly wrapper: returns process exit code."""
    try:
        report = asyncio.run(publish_provider(provider, target=target, only=only, dry_run=dry_run))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    for ns in report.published:
        print(f"  published: {ns}")
    for ns in report.skipped:
        print(f"  skipped (no rows): {ns}")
    for ns, err in report.failed:
        print(f"  FAILED: {ns}: {err}", file=sys.stderr)
    return 0 if report.ok else 1
