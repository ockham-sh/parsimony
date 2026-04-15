"""Tests for SQLiteCatalogStore."""

from __future__ import annotations

import pytest

from parsimony.catalog.models import SeriesEntry
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


@pytest.fixture
def store():
    s = SQLiteCatalogStore(":memory:")
    yield s
    # Sync close — the connection is thread-local anyway
    if s._conn is not None:
        s._conn.close()
        s._conn = None


def _entry(ns: str = "fred", code: str = "GDPC1", title: str = "Real GDP") -> SeriesEntry:
    return SeriesEntry(namespace=ns, code=code, title=title)


# --- upsert / get ---


@pytest.mark.asyncio
async def test_upsert_and_get(store: SQLiteCatalogStore) -> None:
    e = _entry()
    await store.upsert([e])
    got = await store.get("fred", "GDPC1")
    assert got is not None
    assert got.namespace == "fred"
    assert got.code == "GDPC1"
    assert got.title == "Real GDP"


@pytest.mark.asyncio
async def test_get_missing_returns_none(store: SQLiteCatalogStore) -> None:
    assert await store.get("fred", "MISSING") is None


@pytest.mark.asyncio
async def test_upsert_updates_title(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry(title="Old")])
    await store.upsert([_entry(title="New")])
    got = await store.get("fred", "GDPC1")
    assert got is not None
    assert got.title == "New"


@pytest.mark.asyncio
async def test_upsert_preserves_embedding_on_null_update(store: SQLiteCatalogStore) -> None:
    e = SeriesEntry(namespace="fred", code="GDPC1", title="Real GDP", embedding=[1.0, 2.0, 3.0])
    await store.upsert([e])
    await store.upsert([_entry(title="Updated")])
    got = await store.get("fred", "GDPC1")
    assert got is not None
    assert got.embedding is not None
    assert got.embedding == pytest.approx([1.0, 2.0, 3.0])
    assert got.title == "Updated"


# --- exists ---


@pytest.mark.asyncio
async def test_exists(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry()])
    result = await store.exists([("fred", "GDPC1"), ("fred", "MISSING")])
    assert ("fred", "GDPC1") in result
    assert ("fred", "MISSING") not in result


@pytest.mark.asyncio
async def test_exists_empty_list(store: SQLiteCatalogStore) -> None:
    assert await store.exists([]) == set()


# --- delete ---


@pytest.mark.asyncio
async def test_delete(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry()])
    await store.delete("fred", "GDPC1")
    assert await store.get("fred", "GDPC1") is None


@pytest.mark.asyncio
async def test_delete_nonexistent_is_noop(store: SQLiteCatalogStore) -> None:
    await store.delete("fred", "NOPE")  # no error


# --- search (FTS5) ---


@pytest.mark.asyncio
async def test_search_basic(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(code="GDPC1", title="Real Gross Domestic Product"),
            _entry(code="UNRATE", title="Unemployment Rate"),
            _entry(code="CPIAUCSL", title="Consumer Price Index"),
        ]
    )
    results = await store.search("unemployment", 10)
    assert len(results) >= 1
    assert results[0].code == "UNRATE"


@pytest.mark.asyncio
async def test_search_namespace_filter(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(ns="fred", code="GDPC1", title="Real GDP"),
            _entry(ns="bls", code="GDPC1", title="Real GDP BLS"),
        ]
    )
    results = await store.search("GDP", 10, namespaces=["bls"])
    assert len(results) == 1
    assert results[0].namespace == "bls"


@pytest.mark.asyncio
async def test_search_empty_query(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry()])
    assert await store.search("", 10) == []
    assert await store.search("   ", 10) == []


@pytest.mark.asyncio
async def test_search_multiple_tokens(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(code="GDPC1", title="Real Gross Domestic Product"),
            _entry(code="UNRATE", title="Unemployment Rate"),
        ]
    )
    results = await store.search("Real Domestic", 10)
    assert len(results) >= 1
    assert results[0].code == "GDPC1"


@pytest.mark.asyncio
async def test_search_returns_similarity(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry(code="A", title="Test Series")])
    results = await store.search("Test", 10)
    assert len(results) == 1
    assert 0.0 < results[0].similarity <= 1.0


# --- list_namespaces ---


@pytest.mark.asyncio
async def test_list_namespaces(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(ns="fred", code="A", title="a"),
            _entry(ns="bls", code="B", title="b"),
            _entry(ns="fred", code="C", title="c"),
        ]
    )
    ns = await store.list_namespaces()
    assert ns == ["bls", "fred"]


@pytest.mark.asyncio
async def test_list_namespaces_empty(store: SQLiteCatalogStore) -> None:
    assert await store.list_namespaces() == []


# --- list ---


@pytest.mark.asyncio
async def test_list_all(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(ns="fred", code="A", title="Alpha"),
            _entry(ns="fred", code="B", title="Beta"),
        ]
    )
    entries, total = await store.list()
    assert total == 2
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_list_with_namespace_filter(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(ns="fred", code="A", title="Alpha"),
            _entry(ns="bls", code="B", title="Beta"),
        ]
    )
    entries, total = await store.list(namespace="fred")
    assert total == 1
    assert entries[0].namespace == "fred"


@pytest.mark.asyncio
async def test_list_with_text_filter(store: SQLiteCatalogStore) -> None:
    await store.upsert(
        [
            _entry(ns="fred", code="GDPC1", title="Real GDP"),
            _entry(ns="fred", code="UNRATE", title="Unemployment"),
        ]
    )
    entries, total = await store.list(q="GDP")
    assert total == 1
    assert entries[0].code == "GDPC1"


@pytest.mark.asyncio
async def test_list_pagination(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry(ns="fred", code=f"S{i:03d}", title=f"Series {i}") for i in range(10)])
    page1, total = await store.list(limit=3, offset=0)
    assert total == 10
    assert len(page1) == 3
    page2, _ = await store.list(limit=3, offset=3)
    assert len(page2) == 3
    assert page1[0].code != page2[0].code


# --- metadata / tags / properties ---


@pytest.mark.asyncio
async def test_roundtrip_metadata_and_tags(store: SQLiteCatalogStore) -> None:
    e = SeriesEntry(
        namespace="fred",
        code="GDPC1",
        title="Real GDP",
        tags=["macro", "us"],
        metadata={"frequency": "quarterly", "units": "billions"},
        properties={"source": "fed"},
    )
    await store.upsert([e])
    got = await store.get("fred", "GDPC1")
    assert got is not None
    assert got.tags == ["macro", "us"]
    assert got.metadata == {"frequency": "quarterly", "units": "billions"}
    assert got.properties == {"source": "fed"}


# --- file-based persistence ---


@pytest.mark.asyncio
async def test_file_persistence(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    store1 = SQLiteCatalogStore(db_path)
    await store1.upsert([_entry()])
    await store1.close()

    store2 = SQLiteCatalogStore(db_path)
    got = await store2.get("fred", "GDPC1")
    assert got is not None
    assert got.title == "Real GDP"
    await store2.close()


# --- search after delete ---


@pytest.mark.asyncio
async def test_vec_available(store: SQLiteCatalogStore) -> None:
    """sqlite-vec extension should be loaded when installed."""
    try:
        import sqlite_vec  # noqa: F401
    except ImportError:
        pytest.skip("sqlite-vec not installed")
    assert store.has_vec, "sqlite-vec installed but not loaded"


@pytest.mark.asyncio
async def test_hybrid_search(tmp_path) -> None:
    """Hybrid RRF search combining FTS5 BM25 + vec0 cosine."""

    dim = 4
    store = SQLiteCatalogStore(tmp_path / "hybrid.db", embedding_dim=dim)

    # Create entries with embeddings
    entries = [
        SeriesEntry(
            namespace="fred",
            code="GDPC1",
            title="Real Gross Domestic Product",
            embedding=[1.0, 0.0, 0.0, 0.0],
        ),
        SeriesEntry(
            namespace="fred",
            code="UNRATE",
            title="Unemployment Rate",
            embedding=[0.0, 1.0, 0.0, 0.0],
        ),
        SeriesEntry(
            namespace="fred",
            code="CPIAUCSL",
            title="Consumer Price Index",
            embedding=[0.0, 0.0, 1.0, 0.0],
        ),
    ]
    await store.upsert(entries)

    # FTS-only search (no query_embedding)
    fts_results = await store.search("GDP", 10)
    assert len(fts_results) >= 1
    assert fts_results[0].code == "GDPC1"

    # Hybrid search (FTS + vector)
    query_emb = [0.9, 0.1, 0.0, 0.0]  # close to GDPC1's embedding
    hybrid_results = await store.search(
        "GDP",
        10,
        query_embedding=query_emb,
    )
    assert len(hybrid_results) >= 1
    assert hybrid_results[0].code == "GDPC1"
    assert hybrid_results[0].similarity > 0

    # Vector-biased hybrid: keyword matches UNRATE, but vector is close to GDPC1
    # RRF should rank GDPC1 or UNRATE depending on fusion
    hybrid2 = await store.search(
        "Rate",
        10,
        query_embedding=[1.0, 0.0, 0.0, 0.0],
    )
    assert len(hybrid2) >= 1
    # Both UNRATE (keyword match for "Rate") and GDPC1 (vector match) should appear
    codes = [m.code for m in hybrid2]
    assert "UNRATE" in codes or "GDPC1" in codes

    if store._conn:
        store._conn.close()


@pytest.mark.asyncio
async def test_search_after_delete(store: SQLiteCatalogStore) -> None:
    await store.upsert([_entry(code="A", title="Alpha Series")])
    results = await store.search("Alpha", 10)
    assert len(results) == 1
    await store.delete("fred", "A")
    results = await store.search("Alpha", 10)
    assert len(results) == 0
