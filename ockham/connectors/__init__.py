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

from ockham.connector import Connectors


def build_fetch_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
) -> Connectors:
    """Build **fetch-only** connectors from environment variables.

    Excludes source-level search/discovery (e.g. ``fred_search``, ``fmp_search``,
    SDMX DSD/codelist/list helpers, FMP screener). The app layers
    :func:`ockham.connectors.catalog.catalog_search` on top for discovery.
    """
    from ockham.connectors.fmp import FMP_FETCH_CONNECTORS as FMP_FETCH
    from ockham.connectors.fred import FETCH_CONNECTORS as FRED_FETCH
    from ockham.connectors.polymarket import CONNECTORS as POLYMARKET
    from ockham.connectors.sdmx import SDMX_FETCH_CONNECTORS as SDMX_FETCH
    from ockham.connectors.sec_edgar import CONNECTORS as SEC_EDGAR

    _env = env if env is not None else os.environ

    fred_key = _env.get("FRED_API_KEY")
    if not fred_key:
        raise ValueError("FRED_API_KEY is not configured")
    result = FRED_FETCH.bind_deps(api_key=fred_key) + SDMX_FETCH

    fmp_key = _env.get("FMP_API_KEY")
    if not fmp_key:
        raise ValueError("FMP_API_KEY is not configured")
    result = result + FMP_FETCH.bind_deps(api_key=fmp_key)

    eod_key = _env.get("EODHD_API_KEY")
    if eod_key:
        from ockham.connectors.eodhd import CONNECTORS as EODHD

        result = result + EODHD.bind_deps(api_key=eod_key)

    result = result + POLYMARKET

    result = result + SEC_EDGAR

    fr_key = _env.get("FINANCIAL_REPORTS_API_KEY")
    if fr_key:
        from ockham.connectors.financial_reports import FETCH_CONNECTORS as FR_FETCH

        result = result + FR_FETCH.bind_deps(api_key=fr_key)

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
    from ockham.connectors.fmp import CONNECTORS as FMP
    from ockham.connectors.fmp_screener import CONNECTORS as FMP_SCREENER
    from ockham.connectors.fred import CONNECTORS as FRED
    from ockham.connectors.polymarket import CONNECTORS as POLYMARKET
    from ockham.connectors.sdmx import CONNECTORS as SDMX
    from ockham.connectors.sec_edgar import CONNECTORS as SEC_EDGAR

    _env = env if env is not None else os.environ

    fred_key = _env.get("FRED_API_KEY")
    if not fred_key:
        raise ValueError("FRED_API_KEY is not configured")
    result = FRED.bind_deps(api_key=fred_key) + SDMX

    fmp_key = _env.get("FMP_API_KEY")
    if not fmp_key:
        raise ValueError("FMP_API_KEY is not configured")
    result = result + FMP.bind_deps(api_key=fmp_key) + FMP_SCREENER.bind_deps(api_key=fmp_key)

    eod_key = _env.get("EODHD_API_KEY")
    if eod_key:
        from ockham.connectors.eodhd import CONNECTORS as EODHD
        result = result + EODHD.bind_deps(api_key=eod_key)

    result = result + POLYMARKET

    result = result + SEC_EDGAR

    fr_key = _env.get("FINANCIAL_REPORTS_API_KEY")
    if fr_key:
        from ockham.connectors.financial_reports import CONNECTORS as FR
        result = result + FR.bind_deps(api_key=fr_key)

    return result
