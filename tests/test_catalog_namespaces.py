"""Tests for namespace-scoped catalog search and list_namespaces."""

from __future__ import annotations

import pytest

from parsimony.catalog.catalog import Catalog
from parsimony.catalog.models import SeriesEntry
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


@pytest.mark.asyncio
async def test_list_namespaces_sorted_distinct() -> None:
    store = SQLiteCatalogStore(":memory:")
    await store.upsert(
        [
            SeriesEntry(namespace="b_ns", code="1", title="t"),
            SeriesEntry(namespace="a_ns", code="2", title="t"),
            SeriesEntry(namespace="b_ns", code="3", title="t"),
        ]
    )
    assert await store.list_namespaces() == ["a_ns", "b_ns"]


@pytest.mark.asyncio
async def test_search_filters_by_namespaces() -> None:
    store = SQLiteCatalogStore(":memory:")
    await store.upsert(
        [
            SeriesEntry(namespace="ns_a", code="X1", title="alpha match"),
            SeriesEntry(namespace="ns_b", code="X2", title="alpha match"),
        ]
    )
    all_hits = await store.search("alpha", 10, namespaces=None)
    assert {h.namespace for h in all_hits} == {"ns_a", "ns_b"}

    only_a = await store.search("alpha", 10, namespaces=["ns_a"])
    assert len(only_a) == 1 and only_a[0].namespace == "ns_a"

    empty = await store.search("alpha", 10, namespaces=[])
    assert empty == []


@pytest.mark.asyncio
async def test_catalog_search_passes_namespaces() -> None:
    store = SQLiteCatalogStore(":memory:")
    await store.upsert(
        [
            SeriesEntry(namespace="z", code="c", title="findme"),
        ]
    )
    cat = Catalog(store, embeddings=None)
    hits = await cat.search("findme", limit=5, namespaces=["z"])
    assert len(hits) == 1
    assert hits[0].code == "c"

    none_hits = await cat.search("findme", limit=5, namespaces=["other"])
    assert none_hits == []


@pytest.mark.asyncio
async def test_catalog_list_namespaces() -> None:
    store = SQLiteCatalogStore(":memory:")
    await store.upsert([SeriesEntry(namespace="m", code="x", title="t")])
    cat = Catalog(store, embeddings=None)
    assert await cat.store.list_namespaces() == ["m"]
