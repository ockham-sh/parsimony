"""Data source connectors and env-var-based factory.

Each connector module exports a ``CONNECTORS`` constant (the full surface:
search, discovery, fetch, enumerators).  :func:`build_connectors_from_env`
composes every discovered & every bundled provider into a single
:class:`~parsimony.connector.Connectors` collection.

Discovery paths (in order of precedence):

1. **Plugins discovered via** ``parsimony.providers`` **entry points** —
   the long-term path. External packages (``parsimony-fred``, ``parsimony-sdmx``,
   third-party ``parsimony-*``) declare themselves here.
   See :mod:`parsimony.plugins` and ``docs/plugin-contract.md``.
2. **Bundled legacy** :data:`PROVIDERS` **tuple** — transitional path for
   connector modules still living inside this package. Deduplicated against
   the discovered set by module path (discovered wins). This tuple is
   removed in Phase 7 of ``PLAN-plugin-migration.md``.

Consumers that want a subset (e.g. only ``"tool"``-tagged connectors for MCP,
or excluding enumerators) filter the returned collection themselves::

    all_connectors = build_connectors_from_env()
    tool_connectors = all_connectors.filter(tags=["tool"])
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from parsimony.connector import Connectors
from parsimony.plugins import discovery as _plugin_discovery
from parsimony.plugins.errors import PluginError

# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProviderSpec:
    """Declarative description of a data-source provider module.

    Each module must export:
    * ``CONNECTORS`` — a :class:`~parsimony.connector.Connectors` collection.
    * ``ENV_VARS``   — ``dict[str, str]`` mapping dep name → env var
      (empty or absent for public/no-auth providers).
    """

    module: str
    """Fully-qualified module path, e.g. ``"parsimony.connectors.treasury"``."""

    required: bool = False
    """If ``True``, :func:`build_connectors_from_env` raises when the
    provider's env vars are missing.  If ``False``, the provider is silently
    skipped."""


#: Legacy fallback registry — empty after Phase 1.6.
#:
#: Every bundled connector is now discovered via the ``parsimony.providers``
#: entry-point group in :file:`pyproject.toml`. This tuple is retained as a
#: type-stable fallback for out-of-tree consumers who may still import it;
#: it is removed entirely in Phase 7 of ``PLAN-plugin-migration.md``.
PROVIDERS: tuple[ProviderSpec, ...] = ()


# Raw module listing kept for documentation / reference only. NOT iterated
# by :func:`build_connectors_from_env` — discovery drives the real surface.
# Delete as connectors move out to their own packages.
_BUNDLED_MODULES: tuple[str, ...] = (
    # fred — extracted to parsimony-fred in Phase 2
    "parsimony.connectors.fmp",
    "parsimony.connectors.fmp_screener",
    "parsimony.connectors.eodhd",
    "parsimony.connectors.coingecko",
    "parsimony.connectors.finnhub",
    "parsimony.connectors.tiingo",
    "parsimony.connectors.financial_reports",
    "parsimony.connectors.eia",
    "parsimony.connectors.bdf",
    "parsimony.connectors.alpha_vantage",
    "parsimony.connectors.riksbank",
    "parsimony.connectors.destatis",
    "parsimony.connectors.bls",
    # sdmx — extracted to parsimony-sdmx in Phase 5
    "parsimony.connectors.polymarket",
    "parsimony.connectors.sec_edgar",
    "parsimony.connectors.treasury",
    "parsimony.connectors.snb",
    "parsimony.connectors.rba",
    "parsimony.connectors.bde",
    "parsimony.connectors.boc",
    "parsimony.connectors.boj",
    "parsimony.connectors.bdp",
)


# ---------------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------------


def _resolve_env_deps(
    connectors: Connectors,
    env_vars: dict[str, str],
    env: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Resolve env vars to bind_deps kwargs using the Connector's own dep info.

    Returns a dict of resolved deps, or ``None`` if the provider should be
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
                return None  # can't construct — caller decides raise vs skip
            continue  # optional dep: skip binding, function default applies
        deps[dep_name] = value

    return deps


def _bind_required_deps(
    result: Connectors,
    connectors: Connectors,
    env_vars: dict[str, str],
    env: Mapping[str, Any],
) -> Connectors:
    """Bind env-var deps and add a provider. Raises if any required dep is missing."""
    deps = _resolve_env_deps(connectors, env_vars, env)
    if deps is None:
        # Find which env var is missing for the error message
        for _dep_name, env_var in env_vars.items():
            if not env.get(env_var, ""):
                raise ValueError(f"{env_var} is not configured")
        raise ValueError("Required provider dependency missing")  # unreachable
    if deps:
        connectors = connectors.bind_deps(**deps)
    return result + connectors


def _bind_optional_deps(
    result: Connectors,
    connectors: Connectors,
    env_vars: dict[str, str],
    env: Mapping[str, Any],
) -> Connectors:
    """Bind env-var deps and add a provider. Skips silently if a required dep is missing."""
    deps = _resolve_env_deps(connectors, env_vars, env)
    if deps is None:
        return result  # skip — required dep absent
    if deps:
        connectors = connectors.bind_deps(**deps)
    return result + connectors


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
    lenient: bool = False,
) -> Connectors:
    """Build the full connector surface from environment variables.

    Composes providers from two sources:

    1. Entry-point plugins discovered via :mod:`parsimony.plugins.discovery`
       (silently skipped when their env vars are absent — the design default
       for installed plugins).
    2. The legacy :data:`PROVIDERS` tuple (bundled modules still living in
       this package; deduplicated against the discovered set by module path).

    Pass *env* to override ``os.environ`` (useful for testing).

    Set *lenient* to ``True`` to skip bundled providers whose env vars are
    missing even when they are marked ``required`` — useful for partial
    environments such as catalog builds that lack some API keys. The flag
    has no effect on discovered plugins (which are always optional).
    """
    _env = env if env is not None else dict(os.environ)
    result = Connectors([])

    discovered_module_paths: set[str] = set()
    for provider in _discovered_providers_safe():
        discovered_module_paths.add(provider.module_path)
        result = _bind_optional_deps(result, provider.connectors, provider.env_vars, _env)

    for spec in PROVIDERS:
        if spec.module in discovered_module_paths:
            # Already composed via entry-point discovery; avoid double-registration.
            continue
        module = importlib.import_module(spec.module)
        connectors: Connectors = module.CONNECTORS
        env_vars: dict[str, str] = getattr(module, "ENV_VARS", {})

        if not env_vars:
            result = result + connectors
        elif spec.required and not lenient:
            result = _bind_required_deps(result, connectors, env_vars, _env)
        else:
            result = _bind_optional_deps(result, connectors, env_vars, _env)

    return result


def _discovered_providers_safe() -> tuple[Any, ...]:
    """Return discovered providers; surface contract errors clearly.

    A plugin that violates the contract is a hard error — silencing it would
    mask bugs. Import errors are also re-raised so installs that partially
    failed don't produce a mysteriously-smaller connector set.
    """
    try:
        return _plugin_discovery.discovered_providers()
    except PluginError:
        raise
