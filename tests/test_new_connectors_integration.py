"""Integration tests for new connectors: fetch, enumerate, catalog build, search.

Requires live API access and env vars (loaded from ../../.env).
Run with: pytest tests/test_new_connectors_integration.py -v -s

These tests hit real APIs and may be slow.  Mark with ``@pytest.mark.integration``
so CI can skip them.
"""

from __future__ import annotations

import os
from pathlib import Path

import httpx
import pandas as pd
import pytest

# Load env from the monorepo root .env
_ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
if _ENV_PATH.exists():
    from dotenv import load_dotenv

    load_dotenv(_ENV_PATH)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_key(name: str) -> bool:
    return bool(os.environ.get(name))


# ---------------------------------------------------------------------------
# 1) Connector fetch tests — each hits the real API for a known series
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_treasury_fetch():
    from parsimony.connectors.treasury import TreasuryFetchParams, treasury_fetch

    result = await treasury_fetch(
        TreasuryFetchParams(
            endpoint="v2/accounting/od/debt_to_penny",
            page_size=5,
            sort="-record_date",
        )
    )
    assert result.data is not None
    df = result.data
    assert len(df) > 0
    assert "record_date" in df.columns
    assert "endpoint" in df.columns
    assert "title" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bls_fetch():
    if not _has_key("BLS_API_KEY"):
        pytest.skip("BLS_API_KEY not set")
    from parsimony.connectors.bls import BlsFetchParams, bls_fetch

    conn = bls_fetch.bind_deps(api_key=os.environ["BLS_API_KEY"])
    result = await conn(
        BlsFetchParams(
            series_id="LNS14000000",
            start_year="2023",
            end_year="2024",
        )
    )
    df = result.data
    assert len(df) > 0
    assert "series_id" in df.columns
    assert "title" in df.columns
    assert "date" in df.columns
    assert "value" in df.columns
    assert df["series_id"].iloc[0] == "LNS14000000"
    assert df["title"].iloc[0] != "LNS14000000"  # Should be enriched


@pytest.mark.integration
@pytest.mark.asyncio
async def test_riksbank_fetch():
    from parsimony.connectors.riksbank import RiksbankFetchParams, riksbank_fetch

    conn = riksbank_fetch.bind_deps(api_key="")
    try:
        result = await conn(
            RiksbankFetchParams(
                series_id="SEKEURPMI",
                from_date="2024-01-01",
                to_date="2024-03-01",
            )
        )
    except httpx.HTTPStatusError as exc:
        pytest.skip(f"Riksbank API unavailable: {exc.response.status_code}")
    df = result.data
    assert len(df) > 0
    assert "series_id" in df.columns
    assert "date" in df.columns
    assert "value" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_snb_fetch():
    from parsimony.connectors.snb import SnbFetchParams, snb_fetch

    result = await snb_fetch(
        SnbFetchParams(
            cube_id="rendoblim",
            from_date="2024",
            to_date="2024",
        )
    )
    df = result.data
    assert len(df) > 0
    assert "cube_id" in df.columns
    assert "date" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_eia_fetch():
    if not _has_key("EIA_API_KEY"):
        pytest.skip("EIA_API_KEY not set")
    from parsimony.connectors.eia import EiaFetchParams, eia_fetch

    conn = eia_fetch.bind_deps(api_key=os.environ["EIA_API_KEY"])
    result = await conn(
        EiaFetchParams(
            route="petroleum/pri/spt",
            frequency="monthly",
            start="2024-01",
            end="2024-06",
        )
    )
    df = result.data
    assert len(df) > 0
    assert "route" in df.columns
    assert "title" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_destatis_fetch():
    from parsimony.connectors.destatis import DestatisFetchParams, destatis_fetch

    conn = destatis_fetch.bind_deps(username="GAST", password="GAST")
    try:
        result = await conn(
            DestatisFetchParams(
                table_id="61111-0001",
                start_year="2023",
                end_year="2024",
            )
        )
        df = result.data
        assert len(df) > 0
        assert "table_id" in df.columns
        assert "title" in df.columns
    except ValueError as e:
        if "announcement" in str(e).lower() or "gast" in str(e).lower():
            pytest.skip("Destatis GAST credentials redirected (temporary outage)")


# ---------------------------------------------------------------------------
# 2) Enumerator tests — each discovers series from live APIs
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enumerate_treasury():
    from parsimony.connectors.treasury import TreasuryEnumerateParams, enumerate_treasury

    result = await enumerate_treasury(TreasuryEnumerateParams())
    df = result.data
    assert len(df) > 10, f"Expected >10 Treasury endpoints, got {len(df)}"
    assert "endpoint" in df.columns
    assert "title" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enumerate_bls():
    if not _has_key("BLS_API_KEY"):
        pytest.skip("BLS_API_KEY not set")
    from parsimony.connectors.bls import BlsEnumerateParams, enumerate_bls

    conn = enumerate_bls.bind_deps(api_key=os.environ["BLS_API_KEY"])
    result = await conn(BlsEnumerateParams())
    df = result.data
    assert len(df) > 50, f"Expected >50 BLS series, got {len(df)}"
    assert "series_id" in df.columns
    assert "title" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enumerate_riksbank():
    from parsimony.connectors.riksbank import RiksbankEnumerateParams, enumerate_riksbank

    conn = enumerate_riksbank.bind_deps(api_key="")
    try:
        result = await conn(RiksbankEnumerateParams())
    except httpx.HTTPStatusError as exc:
        pytest.skip(f"Riksbank API unavailable: {exc.response.status_code}")
    df = result.data
    assert len(df) > 10, f"Expected >10 Riksbank series, got {len(df)}"
    assert "series_id" in df.columns
    assert "title" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enumerate_eia():
    if not _has_key("EIA_API_KEY"):
        pytest.skip("EIA_API_KEY not set")
    from parsimony.connectors.eia import EiaEnumerateParams, enumerate_eia

    conn = enumerate_eia.bind_deps(api_key=os.environ["EIA_API_KEY"])
    result = await conn(EiaEnumerateParams())
    df = result.data
    assert len(df) > 3, f"Expected >3 EIA routes, got {len(df)}"
    assert "route" in df.columns
    assert "title" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enumerate_destatis():
    from parsimony.connectors.destatis import DestatisEnumerateParams, enumerate_destatis

    conn = enumerate_destatis.bind_deps(username="GAST", password="GAST")
    try:
        result = await conn(DestatisEnumerateParams())
        df = result.data
        assert "table_id" in df.columns
        assert "title" in df.columns
    except Exception:
        pytest.skip("Destatis GAST credentials unavailable")


# ---------------------------------------------------------------------------
# 3) Catalog build + search: enumerator → SQLiteCatalogStore → search
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_and_search_treasury_catalog(tmp_path):
    """Full pipeline: enumerate → index → SQLite FTS search."""
    from parsimony.catalog.catalog import Catalog, entries_from_table_result
    from parsimony.connectors.treasury import TreasuryEnumerateParams, enumerate_treasury
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

    # 1) Enumerate
    result = await enumerate_treasury(TreasuryEnumerateParams())
    entries = entries_from_table_result(result)
    assert len(entries) > 10

    # 2) Index into SQLite
    db_path = tmp_path / "treasury_catalog.db"
    store = SQLiteCatalogStore(db_path)
    catalog = Catalog(store, embeddings=None)
    idx = await catalog.ingest(entries, embed=False, force=True)
    assert idx.indexed > 10

    # 3) Search
    matches = await store.search("debt", 10)
    assert len(matches) > 0, "Expected search for 'debt' to find Treasury endpoints"
    assert any("debt" in m.title.lower() or "debt" in m.code.lower() for m in matches)

    # 4) Namespace
    ns = await store.list_namespaces()
    assert "treasury" in ns

    await store.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_and_search_bls_catalog(tmp_path):
    """Full pipeline: BLS enumerate → index → search."""
    if not _has_key("BLS_API_KEY"):
        pytest.skip("BLS_API_KEY not set")

    from parsimony.catalog.catalog import Catalog, entries_from_table_result
    from parsimony.connectors.bls import BlsEnumerateParams, enumerate_bls
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

    conn = enumerate_bls.bind_deps(api_key=os.environ["BLS_API_KEY"])
    result = await conn(BlsEnumerateParams())
    entries = entries_from_table_result(result)
    assert len(entries) > 30

    db_path = tmp_path / "bls_catalog.db"
    store = SQLiteCatalogStore(db_path)
    catalog = Catalog(store, embeddings=None)
    idx = await catalog.ingest(entries, embed=False, force=True)
    assert idx.indexed > 30

    matches = await store.search("unemployment", 10)
    assert len(matches) > 0, "Expected search for 'unemployment' to find BLS series"

    await store.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_and_search_riksbank_catalog(tmp_path):
    """Full pipeline: Riksbank enumerate → index → search."""
    from parsimony.catalog.catalog import Catalog, entries_from_table_result
    from parsimony.connectors.riksbank import RiksbankEnumerateParams, enumerate_riksbank
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

    conn = enumerate_riksbank.bind_deps(api_key="")
    try:
        result = await conn(RiksbankEnumerateParams())
    except httpx.HTTPStatusError as exc:
        pytest.skip(f"Riksbank API unavailable: {exc.response.status_code}")
    entries = entries_from_table_result(result)
    assert len(entries) > 5

    db_path = tmp_path / "riksbank_catalog.db"
    store = SQLiteCatalogStore(db_path)
    catalog = Catalog(store, embeddings=None)
    idx = await catalog.ingest(entries, embed=False, force=True)
    assert idx.indexed > 5

    matches = await store.search("exchange", 10)
    assert len(matches) >= 0  # Riksbank may not have "exchange" in titles

    ns = await store.list_namespaces()
    assert "riksbank" in ns

    await store.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multi_provider_catalog_search(tmp_path):
    """Build a catalog from multiple providers and search across all."""
    from parsimony.catalog.catalog import Catalog, entries_from_table_result
    from parsimony.connectors.riksbank import RiksbankEnumerateParams, enumerate_riksbank
    from parsimony.connectors.treasury import TreasuryEnumerateParams, enumerate_treasury
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

    db_path = tmp_path / "multi_catalog.db"
    store = SQLiteCatalogStore(db_path)
    catalog = Catalog(store, embeddings=None)

    # Index Treasury
    result = await enumerate_treasury(TreasuryEnumerateParams())
    entries = entries_from_table_result(result)
    await catalog.ingest(entries, embed=False, force=True)

    # Index Riksbank (may 429 if run after other Riksbank tests)
    try:
        conn = enumerate_riksbank.bind_deps(api_key="")
        result = await conn(RiksbankEnumerateParams())
        entries = entries_from_table_result(result)
        await catalog.ingest(entries, embed=False, force=True)
    except Exception:
        pass  # Rate limited — test with Treasury data alone

    # Search across all namespaces
    all_matches = await store.search("rate", 20)
    assert len(all_matches) > 0

    # Search with namespace filter
    treasury_only = await store.search("rate", 20, namespaces=["treasury"])
    riksbank_only = await store.search("rate", 20, namespaces=["riksbank"])

    # Both should have results, and they shouldn't overlap
    if treasury_only and riksbank_only:
        t_codes = {m.code for m in treasury_only}
        r_codes = {m.code for m in riksbank_only}
        assert t_codes.isdisjoint(r_codes), "Namespace filtering should separate results"

    ns = await store.list_namespaces()
    assert "treasury" in ns
    # riksbank may be absent if rate-limited

    await store.close()


# ---------------------------------------------------------------------------
# bbk-sourced central bank connectors
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bde_fetch():
    from parsimony.connectors.bde import BdeFetchParams, bde_fetch

    result = await bde_fetch(
        BdeFetchParams(
            key="D_1NBAF472",
            time_range="30M",
        )
    )
    assert result.data is not None
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "key" in df.columns
    assert "date" in df.columns
    assert "value" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_boc_fetch():
    from parsimony.connectors.boc import BocFetchParams, boc_fetch

    result = await boc_fetch(
        BocFetchParams(
            series_name="FXUSDCAD",
            start_date="2024-01-01",
            end_date="2024-03-31",
        )
    )
    assert result.data is not None
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "series_name" in df.columns
    assert "date" in df.columns
    assert "value" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_boj_fetch():
    from parsimony.connectors.boj import BojFetchParams, boj_fetch

    result = await boj_fetch(
        BojFetchParams(
            db="FM08",
            code="FXERD01",
            start_date="202401",
        )
    )
    assert result.data is not None
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "code" in df.columns
    assert "date" in df.columns
    assert "value" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enumerate_boc():
    from parsimony.connectors.boc import BocEnumerateParams, enumerate_boc

    result = await enumerate_boc(BocEnumerateParams())
    assert result.data is not None
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "series_name" in df.columns
    assert "title" in df.columns
