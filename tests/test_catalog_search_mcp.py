"""Tests for the unified catalog_search connector, namespace helpers, and warm()."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from parsimony.catalog.catalog import Catalog, WarmResult
from parsimony.catalog.models import SeriesEntry, SeriesMatch
from parsimony.connectors import build_connectors_from_env, list_enumerator_namespaces
from parsimony.connectors.catalog_search import (
    CONNECTORS as CATALOG_CONNECTORS,
    CatalogListNamespacesParams,
    CatalogSearchParams,
    resolve_namespaces,
)
from parsimony.connectors.sdmx import CONNECTORS as SDMX_CONNECTORS
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore, sanitize_fts5_query

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def catalog() -> Catalog:
    """In-memory catalog with a few test entries."""
    store = SQLiteCatalogStore(db_path=":memory:")
    cat = Catalog(store=store)
    entries = [
        SeriesEntry(namespace="sdmx_ecb_datasets", code="YC", title="Yield curve"),
        SeriesEntry(namespace="sdmx_ecb_datasets", code="EXR", title="Exchange rates"),
        SeriesEntry(namespace="sdmx_ecb_datasets", code="MIR", title="MFI interest rates"),
        SeriesEntry(namespace="bls", code="CUUR0000SA0", title="Consumer Price Index"),
        SeriesEntry(namespace="treasury", code="DGS10", title="10-Year Treasury Constant Maturity Rate"),
    ]
    await store.upsert(entries)
    return cat


@pytest.fixture()
async def bound_catalog_search(catalog: Catalog):
    """catalog_search connector with catalog bound."""
    return CATALOG_CONNECTORS.bind_deps(catalog=catalog).get("catalog_search")


@pytest.fixture()
async def bound_catalog_list_namespaces(catalog: Catalog):
    """catalog_list_namespaces connector with catalog bound."""
    return CATALOG_CONNECTORS.bind_deps(catalog=catalog).get("catalog_list_namespaces")


# ---------------------------------------------------------------------------
# Namespace registry
# ---------------------------------------------------------------------------


def test_list_enumerator_namespaces_returns_sdmx():
    """Namespace extraction includes SDMX agency namespaces (no network)."""
    ns = list_enumerator_namespaces()
    assert "sdmx_ecb_datasets" in ns
    assert "sdmx_estat_datasets" in ns
    assert "sdmx_imf_data_datasets" in ns
    assert "sdmx_wb_wdi_datasets" in ns
    assert "sdmx_oecd_datasets" in ns


def test_list_enumerator_namespaces_includes_existing():
    """Namespace extraction includes pre-existing enumerator namespaces."""
    ns = list_enumerator_namespaces()
    for expected in ("bde", "treasury", "snb", "rba", "boc", "boj"):
        assert expected in ns, f"Missing namespace: {expected}"


def test_list_enumerator_namespaces_with_explicit_connectors():
    """Passing connectors explicitly works the same as the default."""
    conns = build_connectors_from_env(lenient=True)
    ns = list_enumerator_namespaces(conns)
    assert "sdmx_ecb_datasets" in ns
    assert len(ns) >= 10


# ---------------------------------------------------------------------------
# SDMX enumerators
# ---------------------------------------------------------------------------


def test_sdmx_enumerators_exist():
    """Five curated SDMX enumerators are registered in CONNECTORS."""
    expected = [
        "sdmx_enumerate_ecb",
        "sdmx_enumerate_estat",
        "sdmx_enumerate_imf_data",
        "sdmx_enumerate_wb_wdi",
        "sdmx_enumerate_oecd",
    ]
    names = [c.name for c in SDMX_CONNECTORS]
    for name in expected:
        assert name in names, f"Missing enumerator: {name}"


def test_sdmx_enumerators_have_correct_tags():
    """Enumerators are tagged 'enumerator' + 'sdmx', NOT 'tool'."""
    for c in SDMX_CONNECTORS:
        if "enumerate" in c.name:
            assert "enumerator" in c.tags
            assert "sdmx" in c.tags
            assert "tool" not in c.tags


def test_sdmx_enumerators_have_static_namespaces():
    """Each enumerator declares a unique, static namespace on its KEY column."""
    from parsimony.result import ColumnRole

    namespaces = set()
    for c in SDMX_CONNECTORS:
        if "enumerate" not in c.name:
            continue
        assert c.output_config is not None
        key_cols = [col for col in c.output_config.columns if col.role == ColumnRole.KEY]
        assert len(key_cols) == 1
        ns = key_cols[0].namespace
        assert ns is not None
        assert ns.startswith("sdmx_")
        assert ns.endswith("_datasets")
        namespaces.add(ns)
    assert len(namespaces) == 5


def test_sdmx_list_datasets_not_tool():
    """sdmx_list_datasets should no longer be tagged 'tool'."""
    conn = SDMX_CONNECTORS.get("sdmx_list_datasets")
    assert conn is not None
    assert "tool" not in conn.tags


def test_sdmx_list_datasets_not_in_tool_filter():
    """sdmx_list_datasets should not appear in filter(tags=['tool'])."""
    all_conns = build_connectors_from_env(lenient=True)
    tools = all_conns.filter(tags=["tool"])
    assert "sdmx_list_datasets" not in tools


# ---------------------------------------------------------------------------
# FTS5 sanitization (now in store)
# ---------------------------------------------------------------------------


def test_sanitize_fts5_preserves_words():
    """Normal words pass through."""
    assert sanitize_fts5_query("unemployment rate") == "unemployment rate"


def test_sanitize_fts5_strips_special_chars():
    """FTS5 special characters are removed."""
    result = sanitize_fts5_query('test*"injection NEAR(foo)')
    assert "*" not in result
    assert '"' not in result
    assert "(" not in result
    assert "test" in result
    assert "foo" in result


def test_sanitize_fts5_strips_operators():
    """FTS5 operator characters are stripped; words are preserved."""
    result = sanitize_fts5_query("GDP AND NOT quarterly")
    assert result == "GDP AND NOT quarterly"


def test_sanitize_fts5_empty_after_strip():
    """All-special input produces empty string."""
    assert sanitize_fts5_query("***") == ""


# ---------------------------------------------------------------------------
# resolve_namespaces helper
# ---------------------------------------------------------------------------


def test_resolve_namespaces_explicit_wins():
    """Explicit namespaces override defaults."""
    assert resolve_namespaces(["a"], ["b"]) == ["a"]


def test_resolve_namespaces_default_fallback():
    """Default namespaces used when explicit is None."""
    assert resolve_namespaces(None, ["b"]) == ["b"]


def test_resolve_namespaces_all_when_both_none():
    """None returned when both are None (search all)."""
    assert resolve_namespaces(None, None) is None


# ---------------------------------------------------------------------------
# catalog_search connector — namespaces optional
# ---------------------------------------------------------------------------


def test_catalog_search_params_accepts_no_namespaces():
    """namespaces is optional — omitting it is valid."""
    params = CatalogSearchParams(query="test")
    assert params.namespaces is None


def test_catalog_search_params_accepts_namespaces_list():
    """namespaces accepts a list of strings."""
    params = CatalogSearchParams(query="test", namespaces=["fred", "treasury"])
    assert params.namespaces == ["fred", "treasury"]


@pytest.mark.asyncio()
async def test_catalog_search_returns_results(bound_catalog_search):
    """Search returns matching entries from the catalog."""
    result = await bound_catalog_search(
        params=CatalogSearchParams(query="yield curve", namespaces=["sdmx_ecb_datasets"])
    )
    df = result.df
    assert len(df) > 0
    assert "code" in df.columns
    assert "namespace" in df.columns
    assert "title" in df.columns


@pytest.mark.asyncio()
async def test_catalog_search_or_recall(bound_catalog_search):
    """OR matching returns results when any token matches."""
    result = await bound_catalog_search(
        params=CatalogSearchParams(query="interest rates", namespaces=["sdmx_ecb_datasets"])
    )
    df = result.df
    assert len(df) >= 2


@pytest.mark.asyncio()
async def test_catalog_search_empty_returns_empty_df(bound_catalog_search):
    """Search with no matches returns empty DataFrame (not exception)."""
    result = await bound_catalog_search(
        params=CatalogSearchParams(query="xyznonexistent12345", namespaces=["sdmx_ecb_datasets"])
    )
    df = result.df
    assert len(df) == 0
    assert list(df.columns) == ["namespace", "code", "title", "similarity"]


@pytest.mark.asyncio()
async def test_catalog_search_limit_capped(bound_catalog_search):
    """Limit is respected."""
    result = await bound_catalog_search(
        params=CatalogSearchParams(query="rate", namespaces=["sdmx_ecb_datasets"], limit=1)
    )
    df = result.df
    assert len(df) <= 1


@pytest.mark.asyncio()
async def test_catalog_search_no_namespace_searches_all(bound_catalog_search):
    """Omitting namespaces searches across all."""
    result = await bound_catalog_search(
        params=CatalogSearchParams(query="rate")
    )
    df = result.df
    # Should find results from multiple namespaces (ecb, bls, treasury)
    assert len(df) > 0


def test_catalog_search_params_query_validation():
    """Empty query is rejected by Pydantic validation."""
    with pytest.raises(ValueError):
        CatalogSearchParams(query="  ")


def test_catalog_search_params_limit_bounds():
    """Limit outside bounds is rejected."""
    with pytest.raises(ValueError):
        CatalogSearchParams(query="test", limit=0)
    with pytest.raises(ValueError):
        CatalogSearchParams(query="test", limit=100)


def test_catalog_search_has_catalog_dependency():
    """catalog_search requires 'catalog' dep before calling."""
    conn = CATALOG_CONNECTORS.get("catalog_search")
    assert conn is not None
    assert "catalog" in conn.dep_names


# ---------------------------------------------------------------------------
# catalog_list_namespaces connector
# ---------------------------------------------------------------------------


@pytest.mark.asyncio()
async def test_catalog_list_namespaces_returns_populated(bound_catalog_list_namespaces):
    """catalog_list_namespaces returns namespaces from the store."""
    result = await bound_catalog_list_namespaces(params=CatalogListNamespacesParams())
    df = result.df
    assert "namespace" in df.columns
    ns_list = list(df["namespace"])
    assert "sdmx_ecb_datasets" in ns_list
    assert "treasury" in ns_list


def test_catalog_list_namespaces_has_catalog_dependency():
    """catalog_list_namespaces requires 'catalog' dep."""
    conn = CATALOG_CONNECTORS.get("catalog_list_namespaces")
    assert conn is not None
    assert "catalog" in conn.dep_names


# ---------------------------------------------------------------------------
# Three-tier namespace resolution via connector
# ---------------------------------------------------------------------------


@pytest.mark.asyncio()
async def test_catalog_search_uses_default_namespaces():
    """When namespaces omitted, default_namespaces from dep injection is used."""
    from parsimony.connectors.catalog_search import catalog_search as cs_connector

    mock_catalog = AsyncMock()
    mock_catalog.search = AsyncMock(return_value=[
        SeriesMatch(namespace="fred", code="gdp", title="GDP", similarity=0.95)
    ])
    conn = cs_connector.bind_deps(catalog=mock_catalog, default_namespaces=["fred"])
    await conn(params=CatalogSearchParams(query="growth"))
    mock_catalog.search.assert_awaited_once()
    call_kw = mock_catalog.search.await_args
    assert call_kw.kwargs["namespaces"] == ["fred"]


@pytest.mark.asyncio()
async def test_catalog_search_explicit_overrides_default():
    """Explicit namespaces override default_namespaces."""
    from parsimony.connectors.catalog_search import catalog_search as cs_connector

    mock_catalog = AsyncMock()
    mock_catalog.search = AsyncMock(return_value=[
        SeriesMatch(namespace="x", code="c", title="t", similarity=1.0)
    ])
    conn = cs_connector.bind_deps(catalog=mock_catalog, default_namespaces=["fred"])
    await conn(params=CatalogSearchParams(query="q", namespaces=["custom"], limit=3))
    assert mock_catalog.search.await_args.kwargs["namespaces"] == ["custom"]


# ---------------------------------------------------------------------------
# WarmResult dataclass
# ---------------------------------------------------------------------------


def test_warm_result_defaults():
    """WarmResult has sensible defaults."""
    r = WarmResult()
    assert r.downloaded == 0
    assert r.skipped == 0
    assert r.failed == 0
    assert r.elapsed_s == 0.0
    assert r.errors == []
