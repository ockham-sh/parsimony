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


def build_fetch_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
) -> Connectors:
    """Build **fetch-only** connectors from environment variables.

    Excludes source-level search/discovery (e.g. ``fred_search``, ``fmp_search``,
    SDMX DSD/codelist/list helpers, FMP screener). The app layers
    :func:`parsimony.connectors.catalog.catalog_search` on top for discovery.
    """
    from parsimony.connectors.fmp import FMP_FETCH_CONNECTORS as FMP_FETCH
    from parsimony.connectors.fred import FETCH_CONNECTORS as FRED_FETCH
    from parsimony.connectors.polymarket import CONNECTORS as POLYMARKET
    from parsimony.connectors.sdmx import SDMX_FETCH_CONNECTORS as SDMX_FETCH
    from parsimony.connectors.sec_edgar import CONNECTORS as SEC_EDGAR

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
        from parsimony.connectors.eodhd import CONNECTORS as EODHD

        result = result + EODHD.bind_deps(api_key=eod_key)

    result = result + POLYMARKET

    result = result + SEC_EDGAR

    fr_key = _env.get("FINANCIAL_REPORTS_API_KEY")
    if fr_key:
        from parsimony.connectors.financial_reports import FETCH_CONNECTORS as FR_FETCH

        result = result + FR_FETCH.bind_deps(api_key=fr_key)

    # --- Public data connectors (no or optional API key) ---
    from parsimony.connectors.boe import FETCH_CONNECTORS as BOE_FETCH
    from parsimony.connectors.rba import FETCH_CONNECTORS as RBA_FETCH
    from parsimony.connectors.snb import FETCH_CONNECTORS as SNB_FETCH
    from parsimony.connectors.treasury import FETCH_CONNECTORS as TREASURY_FETCH

    result = result + TREASURY_FETCH + SNB_FETCH + BOE_FETCH + RBA_FETCH

    from parsimony.connectors.riksbank import FETCH_CONNECTORS as RIKSBANK_FETCH

    result = result + RIKSBANK_FETCH.bind_deps(api_key=_env.get("RIKSBANK_API_KEY", ""))

    from parsimony.connectors.destatis import FETCH_CONNECTORS as DESTATIS_FETCH

    result = result + DESTATIS_FETCH.bind_deps(
        username=_env.get("DESTATIS_USERNAME", "GAST"),
        password=_env.get("DESTATIS_PASSWORD", "GAST"),
    )

    eia_key = _env.get("EIA_API_KEY")
    if eia_key:
        from parsimony.connectors.eia import FETCH_CONNECTORS as EIA_FETCH

        result = result + EIA_FETCH.bind_deps(api_key=eia_key)

    # BLS (optional API key for higher rate limits)
    from parsimony.connectors.bls import FETCH_CONNECTORS as BLS_FETCH

    result = result + BLS_FETCH.bind_deps(api_key=_env.get("BLS_API_KEY", ""))

    # --- Central bank connectors (no or optional API key) ---
    from parsimony.connectors.bde import FETCH_CONNECTORS as BDE_FETCH
    from parsimony.connectors.boc import FETCH_CONNECTORS as BOC_FETCH
    from parsimony.connectors.boj import FETCH_CONNECTORS as BOJ_FETCH
    from parsimony.connectors.bdp import FETCH_CONNECTORS as BDP_FETCH

    result = result + BDE_FETCH + BOC_FETCH + BOJ_FETCH + BDP_FETCH

    bdf_key = _env.get("BANQUEDEFRANCE_KEY")
    if bdf_key:
        from parsimony.connectors.bdf import FETCH_CONNECTORS as BDF_FETCH

        result = result + BDF_FETCH.bind_deps(api_key=bdf_key)

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
    from parsimony.connectors.fmp import CONNECTORS as FMP
    from parsimony.connectors.fmp_screener import CONNECTORS as FMP_SCREENER
    from parsimony.connectors.fred import CONNECTORS as FRED
    from parsimony.connectors.polymarket import CONNECTORS as POLYMARKET
    from parsimony.connectors.sdmx import CONNECTORS as SDMX
    from parsimony.connectors.sec_edgar import CONNECTORS as SEC_EDGAR

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
        from parsimony.connectors.eodhd import CONNECTORS as EODHD
        result = result + EODHD.bind_deps(api_key=eod_key)

    result = result + POLYMARKET

    result = result + SEC_EDGAR

    fr_key = _env.get("FINANCIAL_REPORTS_API_KEY")
    if fr_key:
        from parsimony.connectors.financial_reports import CONNECTORS as FR
        result = result + FR.bind_deps(api_key=fr_key)

    # --- Public data connectors (no or optional API key) ---
    from parsimony.connectors.boe import CONNECTORS as BOE
    from parsimony.connectors.rba import CONNECTORS as RBA
    from parsimony.connectors.snb import CONNECTORS as SNB
    from parsimony.connectors.treasury import CONNECTORS as TREASURY

    result = result + TREASURY + SNB + BOE + RBA

    from parsimony.connectors.riksbank import CONNECTORS as RIKSBANK

    result = result + RIKSBANK.bind_deps(api_key=_env.get("RIKSBANK_API_KEY", ""))

    from parsimony.connectors.destatis import CONNECTORS as DESTATIS

    result = result + DESTATIS.bind_deps(
        username=_env.get("DESTATIS_USERNAME", "GAST"),
        password=_env.get("DESTATIS_PASSWORD", "GAST"),
    )

    eia_key = _env.get("EIA_API_KEY")
    if eia_key:
        from parsimony.connectors.eia import CONNECTORS as EIA

        result = result + EIA.bind_deps(api_key=eia_key)

    # BLS (optional API key for higher rate limits)
    from parsimony.connectors.bls import CONNECTORS as BLS

    result = result + BLS.bind_deps(api_key=_env.get("BLS_API_KEY", ""))

    # --- Central bank connectors (no or optional API key) ---
    from parsimony.connectors.bde import CONNECTORS as BDE
    from parsimony.connectors.boc import CONNECTORS as BOC
    from parsimony.connectors.boj import CONNECTORS as BOJ
    from parsimony.connectors.bdp import CONNECTORS as BDP

    result = result + BDE + BOC + BOJ + BDP

    bdf_key = _env.get("BANQUEDEFRANCE_KEY")
    if bdf_key:
        from parsimony.connectors.bdf import CONNECTORS as BDF

        result = result + BDF.bind_deps(api_key=bdf_key)

    return result
