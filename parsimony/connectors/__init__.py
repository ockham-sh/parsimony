"""Data source connectors and env-var-based factory.

Each connector module exports a ``CONNECTORS`` constant (the full surface:
search, discovery, fetch, enumerators).  The unified :data:`PROVIDERS` registry
lists every module once; :func:`build_connectors_from_env` drives off it.

Consumers that want a subset (e.g. only ``"tool"``-tagged connectors for MCP,
or excluding enumerators) filter the returned :class:`~parsimony.connector.Connectors`
collection themselves::

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
    """Fully-qualified module path, e.g. ``"parsimony.connectors.fred"``."""

    required: bool = False
    """If ``True``, :func:`build_connectors_from_env` raises when the
    provider's env vars are missing.  If ``False``, the provider is silently
    skipped."""


PROVIDERS: tuple[ProviderSpec, ...] = (
    # --- Required providers (raise if env vars absent, unless lenient=True) ---
    ProviderSpec("parsimony.connectors.fred", required=True),
    # --- Optional providers with credentials ---
    ProviderSpec("parsimony.connectors.fmp"),
    ProviderSpec("parsimony.connectors.fmp_screener"),
    ProviderSpec("parsimony.connectors.eodhd"),
    ProviderSpec("parsimony.connectors.coingecko"),
    ProviderSpec("parsimony.connectors.finnhub"),
    ProviderSpec("parsimony.connectors.tiingo"),
    ProviderSpec("parsimony.connectors.financial_reports"),
    ProviderSpec("parsimony.connectors.eia"),
    ProviderSpec("parsimony.connectors.bdf"),
    ProviderSpec("parsimony.connectors.alpha_vantage"),
    ProviderSpec("parsimony.connectors.riksbank"),
    ProviderSpec("parsimony.connectors.destatis"),
    ProviderSpec("parsimony.connectors.bls"),
    # --- Public providers (no credentials) ---
    ProviderSpec("parsimony.connectors.sdmx"),
    ProviderSpec("parsimony.connectors.polymarket"),
    ProviderSpec("parsimony.connectors.sec_edgar"),
    ProviderSpec("parsimony.connectors.treasury"),
    ProviderSpec("parsimony.connectors.snb"),
    ProviderSpec("parsimony.connectors.rba"),
    ProviderSpec("parsimony.connectors.bde"),
    ProviderSpec("parsimony.connectors.boc"),
    ProviderSpec("parsimony.connectors.boj"),
    ProviderSpec("parsimony.connectors.bdp"),
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
        for dep_name, env_var in env_vars.items():
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

    Iterates the :data:`PROVIDERS` registry.  Each module's ``CONNECTORS``
    are composed after binding dependencies resolved from *env* (or
    ``os.environ`` when *env* is ``None``).

    Set *lenient* to ``True`` to skip providers whose env vars are missing
    even when they are marked ``required``.  This is useful for partial
    environments such as catalog builds that may lack all API keys.

    Pass *env* to override ``os.environ`` (useful for testing).
    """
    _env = env if env is not None else dict(os.environ)
    result = Connectors([])

    for spec in PROVIDERS:
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
