"""Entry-point based discovery for ``parsimony`` plugins.

Plugins declare themselves by adding an entry point in the
``parsimony.providers`` group that points at a module satisfying the plugin
contract (see ``docs/plugin-contract.md``).

At discovery time we:

1. Enumerate entry points in the ``parsimony.providers`` group.
2. Import each target module.
3. Validate it exports a ``Connectors`` instance.
4. Record optional ``ENV_VARS`` and ``PROVIDER_METADATA``.

Results are cached per-process. Call :func:`_clear_cache` (e.g. from tests,
or after ``importlib.invalidate_caches()``) to force rediscovery.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from collections.abc import Iterator
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

from parsimony.connector import Connectors
from parsimony.plugins.errors import PluginContractError, PluginImportError

__all__ = [
    "DiscoveredProvider",
    "discovered_providers",
    "iter_entry_points",
    "load_provider",
]


ENTRY_POINT_GROUP = "parsimony.providers"


# ---------------------------------------------------------------------------
# DiscoveredProvider record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DiscoveredProvider:
    """Outcome of loading one ``parsimony.providers`` entry point.

    Immutable snapshot — safe to cache and share across the process.
    """

    name: str
    """Provider key (left-hand side of the entry-point declaration)."""

    module_path: str
    """Dotted module path the entry point points at (e.g. ``parsimony_fred``)."""

    connectors: Connectors
    """Immutable collection exported as ``CONNECTORS`` by the plugin module."""

    env_vars: dict[str, str] = field(default_factory=dict)
    """Mapping of connector dep name → environment variable name. Empty if not declared."""

    provider_metadata: dict[str, Any] = field(default_factory=dict)
    """Free-form metadata exposed by ``PROVIDER_METADATA``. Empty if not declared."""

    distribution_name: str | None = None
    """PyPI distribution name owning this entry point (e.g. ``parsimony-fred``). None if unresolved."""

    version: str | None = None
    """Installed distribution version. None if unresolved."""

    module: ModuleType | None = None
    """The imported plugin module. Cached so conformance checks and CLI
    introspection don't need to re-import (important in tests that
    monkeypatch ``_import_module``)."""


# ---------------------------------------------------------------------------
# Indirection seams (patched in tests)
# ---------------------------------------------------------------------------


def _entry_points(*, group: str) -> list[importlib.metadata.EntryPoint]:
    """Return the entry points declared under *group*.

    Thin wrapper so tests can monkeypatch without touching importlib.metadata.
    """
    eps = importlib.metadata.entry_points(group=group)
    return list(eps)


def _import_module(module_path: str) -> Any:
    return importlib.import_module(module_path)


def _distribution_for_entry_point(
    ep: importlib.metadata.EntryPoint,
) -> tuple[str | None, str | None]:
    """Best-effort resolution of ``(distribution_name, version)`` for *ep*.

    ``EntryPoint.dist`` is populated when the entry point comes from the
    ``Distribution`` scan; for synthetic/monkeypatched entry points this may
    be ``None``.
    """
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
    """Reset the per-process discovery cache. Intended for tests and reload hooks."""
    global _cache
    _cache = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def iter_entry_points() -> Iterator[importlib.metadata.EntryPoint]:
    """Yield the entry points declared in the ``parsimony.providers`` group.

    Order follows ``importlib.metadata`` — not guaranteed stable across
    Python versions. Downstream code should not rely on order.
    """
    yield from _entry_points(group=ENTRY_POINT_GROUP)


def load_provider(ep: importlib.metadata.EntryPoint) -> DiscoveredProvider:
    """Load a single entry point and validate the plugin contract.

    Raises:
        PluginImportError: the target module cannot be imported.
        PluginContractError: the module imports but violates the export contract.
    """
    try:
        module = _import_module(ep.value)
    except BaseException as exc:  # keep broad — the plugin owns arbitrary init code
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
    """Return every discovered & validated provider. Cached per-process.

    Entry points that fail the contract raise at discovery time — we do not
    silently drop them. If you need best-effort behavior, iterate
    :func:`iter_entry_points` + :func:`load_provider` yourself and catch
    :class:`~parsimony.plugins.errors.PluginError`.
    """
    global _cache
    if _cache is not None:
        return _cache
    providers = tuple(load_provider(ep) for ep in iter_entry_points())
    _cache = providers
    return providers
