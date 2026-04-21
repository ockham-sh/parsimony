"""Plugin discovery and connector composition.

External packages expose connectors via the ``parsimony.providers``
:mod:`importlib.metadata` entry-point group. This module is the only
mechanism the kernel uses to learn about installed connectors. See
``docs/contract.md`` for the author-facing spec.

Public API
----------

* :class:`DiscoveredProvider` — immutable record of one loaded entry point.
* :func:`discovered_providers` — enumerate every valid, loaded plugin (cached).
* :func:`iter_entry_points` — low-level iteration over the entry-point group.
* :func:`load_provider` — load and validate a single entry point.
* :func:`build_connectors_from_env` — compose every provider into a single
  :class:`~parsimony.connector.Connectors` collection, binding deps from env.
* :class:`PluginError` / :class:`PluginImportError` / :class:`PluginContractError`.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import logging
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

from parsimony.connector import Connectors

__all__ = [
    "DiscoveredProvider",
    "ENTRY_POINT_GROUP",
    "PluginContractError",
    "PluginError",
    "PluginImportError",
    "build_connectors_from_env",
    "discovered_providers",
    "iter_entry_points",
    "load_provider",
]

ENTRY_POINT_GROUP = "parsimony.providers"

_logger = logging.getLogger("parsimony.discovery")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class PluginError(Exception):
    """Base class for plugin discovery/loading errors.

    Subclasses set :attr:`check` so downstream renderers can group errors by
    contract aspect; :attr:`next_action` is surfaced next to the error in
    operator output.
    """

    check: str = "plugin"

    def __init__(
        self,
        message: str,
        *,
        module_path: str | None = None,
        reason: str | None = None,
        next_action: str | None = None,
    ) -> None:
        super().__init__(message)
        self.module_path = module_path
        self.reason = reason or message
        self.next_action = next_action

    def to_report_dict(self) -> dict[str, Any]:
        """Structured fields for JSON-report consumers (CLI, CI)."""
        return {
            "check": self.check,
            "module_path": self.module_path,
            "reason": self.reason,
            "next_action": self.next_action,
        }


class PluginImportError(PluginError):
    """Raised when a plugin entry-point target cannot be imported."""

    check = "import"

    def __init__(self, module_path: str, original: BaseException) -> None:
        self.original = original
        super().__init__(
            f"Failed to import plugin module {module_path!r}: {original}",
            module_path=module_path,
            reason=str(original),
            next_action=f"run `python -c 'import {module_path}'` locally to see the full traceback",
        )


class PluginContractError(PluginError):
    """Raised when a plugin module does not satisfy the export contract."""

    check = "contract"

    def __init__(
        self,
        module_path: str,
        reason: str,
        *,
        next_action: str | None = None,
    ) -> None:
        super().__init__(
            f"Plugin {module_path!r} violates contract: {reason}",
            module_path=module_path,
            reason=reason,
            next_action=next_action,
        )


# ---------------------------------------------------------------------------
# DiscoveredProvider
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DiscoveredProvider:
    """Outcome of loading one ``parsimony.providers`` entry point."""

    name: str
    module_path: str
    connectors: Connectors
    env_vars: dict[str, str] = field(default_factory=dict)
    provider_metadata: dict[str, Any] = field(default_factory=dict)
    distribution_name: str | None = None
    version: str | None = None
    module: ModuleType | None = None


# ---------------------------------------------------------------------------
# Indirection seams (patched in tests)
# ---------------------------------------------------------------------------


def _entry_points(*, group: str) -> list[importlib.metadata.EntryPoint]:
    """Return the entry points declared under *group*."""
    eps = importlib.metadata.entry_points(group=group)
    return list(eps)


def _import_module(module_path: str) -> Any:
    return importlib.import_module(module_path)


def _distribution_for_entry_point(
    ep: importlib.metadata.EntryPoint,
) -> tuple[str | None, str | None]:
    dist = getattr(ep, "dist", None)
    if dist is None:
        return (None, None)
    meta = getattr(dist, "metadata", None)
    name = meta["Name"] if meta is not None else None
    version = getattr(dist, "version", None)
    return (name, version)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


_cache: tuple[DiscoveredProvider, ...] | None = None


def _clear_cache() -> None:
    """Reset the per-process discovery cache."""
    global _cache
    _cache = None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def iter_entry_points() -> Iterator[importlib.metadata.EntryPoint]:
    """Yield the entry points declared in the ``parsimony.providers`` group."""
    yield from _entry_points(group=ENTRY_POINT_GROUP)


def load_provider(ep: importlib.metadata.EntryPoint) -> DiscoveredProvider:
    """Load a single entry point and validate the plugin contract.

    Raises:
        PluginImportError: the target module cannot be imported.
        PluginContractError: the module imports but violates the export contract.
    """
    try:
        module = _import_module(ep.value)
    except BaseException as exc:  # plugin owns arbitrary init code
        raise PluginImportError(ep.value, exc) from exc

    if not hasattr(module, "CONNECTORS"):
        raise PluginContractError(ep.value, "module must export CONNECTORS: Connectors")

    connectors = module.CONNECTORS
    if not isinstance(connectors, Connectors):
        raise PluginContractError(
            ep.value,
            f"CONNECTORS must be a Connectors instance; got {type(connectors).__name__}",
        )

    raw_env = getattr(module, "ENV_VARS", {})
    if not isinstance(raw_env, dict):
        raise PluginContractError(
            ep.value,
            f"ENV_VARS must be a dict[str, str]; got {type(raw_env).__name__}",
        )
    env_vars = {str(k): str(v) for k, v in raw_env.items()}

    raw_meta = getattr(module, "PROVIDER_METADATA", {})
    if not isinstance(raw_meta, dict):
        raise PluginContractError(
            ep.value,
            f"PROVIDER_METADATA must be a dict; got {type(raw_meta).__name__}",
        )
    provider_metadata = dict(raw_meta)

    dist_name, version = _distribution_for_entry_point(ep)

    return DiscoveredProvider(
        name=ep.name,
        module_path=ep.value,
        connectors=connectors,
        env_vars=env_vars,
        provider_metadata=provider_metadata,
        distribution_name=dist_name,
        version=version,
        module=module,
    )


def discovered_providers() -> tuple[DiscoveredProvider, ...]:
    """Return every discovered & validated provider. Cached per-process."""
    global _cache
    if _cache is not None:
        return _cache

    out = [load_provider(ep) for ep in iter_entry_points()]
    providers = tuple(out)
    _cache = providers
    return providers


# ---------------------------------------------------------------------------
# Compose discovered providers into a Connectors collection
# ---------------------------------------------------------------------------


def _resolve_env_deps(
    connectors: Connectors,
    env_vars: dict[str, str],
    env: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Resolve env vars to bind_deps kwargs.

    Returns the dict of resolved deps, or ``None`` if the provider should be
    skipped (a required dep's env var is absent).
    """
    if not env_vars:
        return {}

    sample = next(iter(connectors))
    required_deps = sample.dep_names
    deps: dict[str, Any] = {}

    for dep_name, env_var in env_vars.items():
        value = env.get(env_var, "")
        if not value:
            if dep_name in required_deps:
                return None
            continue
        deps[dep_name] = value

    return deps


def build_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
) -> Connectors:
    """Build the full connector surface from environment variables.

    Composes every provider discovered via :func:`discovered_providers`.
    Providers whose declared env vars are absent are silently skipped.
    """
    _env = env if env is not None else dict(os.environ)
    result = Connectors([])

    for provider in discovered_providers():
        deps = _resolve_env_deps(provider.connectors, provider.env_vars, _env)
        if deps is None:
            continue
        connectors = provider.connectors
        if deps:
            connectors = connectors.bind_deps(**deps)
        result = result + connectors

    return result
