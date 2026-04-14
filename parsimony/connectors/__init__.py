"""Data source connectors and env-var-based factory.

Each connector module exports a ``CONNECTORS`` constant (full surface: search,
discovery, and fetch).  Fetch-only bundles (e.g. ``FETCH_CONNECTORS``,
``SDMX_FETCH_CONNECTORS``) are composed by :func:`build_fetch_connectors_from_env`
for the application agent: discovery via the series catalog + fetch
connectors only.

The :func:`build_connectors_from_env` factory composes the full set from
environment variables — used for indexing, examples, and integration tests.
"""

from __future__ import annotations

import os
from typing import Any

from parsimony.connector import Connectors


# ---------------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------------


def _resolve_env_deps(
    connectors: Connectors,
    env_vars: dict[str, str],
    env: dict[str, Any],
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
    env: dict[str, Any],
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
    env: dict[str, Any],
) -> Connectors:
    """Bind env-var deps and add a provider. Skips silently if a required dep is missing."""
    deps = _resolve_env_deps(connectors, env_vars, env)
    if deps is None:
        return result  # skip — required dep absent
    if deps:
        connectors = connectors.bind_deps(**deps)
    return result + connectors


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------


def build_fetch_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
) -> Connectors:
    """Build **fetch-only** connectors from environment variables.

    Excludes source-level search/discovery (e.g. ``fred_search``, ``fmp_search``,
    SDMX DSD/codelist/list helpers, FMP screener). The app layers
    :func:`parsimony.connectors.catalog.catalog_search` on top for discovery.
    """
    from parsimony.connectors import alpha_vantage, bde, bdf, bdp, bls, boc, boj
    from parsimony.connectors import coingecko, destatis, eia, eodhd, financial_reports
    from parsimony.connectors import finnhub, fmp, fred, polymarket, rba, riksbank
    from parsimony.connectors import sdmx, sec_edgar, snb, tiingo, treasury

    _env = env if env is not None else os.environ

    # Required providers (raise if key missing)
    result = _bind_required_deps(Connectors([]), fred.FETCH_CONNECTORS, fred.ENV_VARS, _env)
    result = result + sdmx.SDMX_FETCH_CONNECTORS
    result = _bind_required_deps(result, fmp.FMP_FETCH_CONNECTORS, fmp.ENV_VARS, _env)

    # Optional providers (skipped if key absent)
    result = _bind_optional_deps(result, eodhd.EODHD_FETCH_CONNECTORS, eodhd.ENV_VARS, _env)
    result = _bind_optional_deps(result, coingecko.FETCH_CONNECTORS, coingecko.ENV_VARS, _env)
    result = _bind_optional_deps(result, finnhub.FETCH_CONNECTORS, finnhub.ENV_VARS, _env)
    result = _bind_optional_deps(result, tiingo.FETCH_CONNECTORS, tiingo.ENV_VARS, _env)
    result = _bind_optional_deps(result, financial_reports.FETCH_CONNECTORS, financial_reports.ENV_VARS, _env)
    result = _bind_optional_deps(result, eia.FETCH_CONNECTORS, eia.ENV_VARS, _env)
    result = _bind_optional_deps(result, bdf.FETCH_CONNECTORS, bdf.ENV_VARS, _env)
    result = _bind_optional_deps(result, alpha_vantage.FETCH_CONNECTORS, alpha_vantage.ENV_VARS, _env)

    # Public data providers (no credentials needed)
    result = (
        result
        + polymarket.CONNECTORS
        + sec_edgar.CONNECTORS
        + treasury.FETCH_CONNECTORS
        + snb.FETCH_CONNECTORS
        + rba.FETCH_CONNECTORS
        + bde.FETCH_CONNECTORS
        + boc.FETCH_CONNECTORS
        + boj.FETCH_CONNECTORS
        + bdp.FETCH_CONNECTORS
    )

    # Optional providers with credentials (skipped if key absent)
    result = _bind_optional_deps(result, riksbank.FETCH_CONNECTORS, riksbank.ENV_VARS, _env)
    result = _bind_optional_deps(result, destatis.FETCH_CONNECTORS, destatis.ENV_VARS, _env)
    result = _bind_optional_deps(result, bls.FETCH_CONNECTORS, bls.ENV_VARS, _env)

    return result


def build_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
) -> Connectors:
    """Build the full connector surface from environment variables.

    Includes search, discovery, screener, and fetch operations. Use
    :func:`build_fetch_connectors_from_env` for the agent runtime when
    ``catalog_search`` is the sole discovery path.

    Pass *env* to override ``os.environ`` (useful for testing).
    """
    from parsimony.connectors import alpha_vantage, bde, bdf, bdp, bls, boc, boj
    from parsimony.connectors import coingecko, destatis, eia, eodhd, financial_reports
    from parsimony.connectors import finnhub, fmp, fmp_screener, fred, polymarket
    from parsimony.connectors import rba, riksbank, sdmx, sec_edgar, snb, tiingo, treasury

    _env = env if env is not None else os.environ

    # Required providers (raise if key missing)
    result = _bind_required_deps(Connectors([]), fred.CONNECTORS, fred.ENV_VARS, _env)
    result = result + sdmx.CONNECTORS
    result = _bind_required_deps(result, fmp.CONNECTORS, fmp.ENV_VARS, _env)
    result = _bind_required_deps(result, fmp_screener.CONNECTORS, fmp_screener.ENV_VARS, _env)

    # Optional providers with credentials (skipped if key absent)
    result = _bind_optional_deps(result, eodhd.CONNECTORS, eodhd.ENV_VARS, _env)
    result = _bind_optional_deps(result, coingecko.CONNECTORS, coingecko.ENV_VARS, _env)
    result = _bind_optional_deps(result, finnhub.CONNECTORS, finnhub.ENV_VARS, _env)
    result = _bind_optional_deps(result, tiingo.CONNECTORS, tiingo.ENV_VARS, _env)
    result = _bind_optional_deps(result, financial_reports.CONNECTORS, financial_reports.ENV_VARS, _env)
    result = _bind_optional_deps(result, eia.CONNECTORS, eia.ENV_VARS, _env)
    result = _bind_optional_deps(result, bdf.CONNECTORS, bdf.ENV_VARS, _env)
    result = _bind_optional_deps(result, alpha_vantage.CONNECTORS, alpha_vantage.ENV_VARS, _env)

    # Public data providers (no credentials needed)
    result = (
        result
        + polymarket.CONNECTORS
        + sec_edgar.CONNECTORS
        + treasury.CONNECTORS
        + snb.CONNECTORS
        + rba.CONNECTORS
        + bde.CONNECTORS
        + boc.CONNECTORS
        + boj.CONNECTORS
        + bdp.CONNECTORS
    )

    # Optional providers with credentials (skipped if key absent)
    result = _bind_optional_deps(result, riksbank.CONNECTORS, riksbank.ENV_VARS, _env)
    result = _bind_optional_deps(result, destatis.CONNECTORS, destatis.ENV_VARS, _env)
    result = _bind_optional_deps(result, bls.CONNECTORS, bls.ENV_VARS, _env)

    return result
