"""Tests for structured query search."""

from __future__ import annotations

import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    CatalogEntry,
    CatalogMatches,
    StructuredQuery,
    _parse_query,
)


def test_parse_query_broad() -> None:
    assert _parse_query("inflation germany", {"REF_AREA", "ICP_ITEM"}) is None
    assert _parse_query("REF_AREA", {"REF_AREA", "ICP_ITEM"}) is None


def test_parse_query_structured() -> None:
    q1 = _parse_query("REF_AREA: Germany", {"REF_AREA", "ICP_ITEM"})
    assert isinstance(q1, StructuredQuery)
    assert q1.clauses == [("REF_AREA", ["Germany"])]

    q2 = _parse_query("REF_AREA: Germany, Italy && ICP_ITEM: energy", {"REF_AREA", "ICP_ITEM"})
    assert isinstance(q2, StructuredQuery)
    assert q2.clauses == [
        ("REF_AREA", ["Germany", "Italy"]),
        ("ICP_ITEM", ["energy"]),
    ]


def test_parse_query_unknown_field_fallback() -> None:
    assert _parse_query("UNKNOWN_FIELD: Germany", {"REF_AREA"}) is None


def test_parse_query_malformed() -> None:
    with pytest.raises(ValueError, match="Malformed clause"):
        _parse_query("REF_AREA: Germany && malformed_part", {"REF_AREA", "ICP_ITEM"})

    with pytest.raises(ValueError, match="No values"):
        _parse_query("REF_AREA: ", {"REF_AREA", "ICP_ITEM"})


@pytest.mark.asyncio
async def test_structured_search_execution() -> None:
    entries = [
        CatalogEntry(namespace="ns", code="A", title="Title A", metadata={"REF_AREA": "Germany", "ICP_ITEM": "energy"}),
        CatalogEntry(namespace="ns", code="B", title="Title B", metadata={"REF_AREA": "Italy", "ICP_ITEM": "energy"}),
        CatalogEntry(namespace="ns", code="C", title="Title C", metadata={"REF_AREA": "Germany", "ICP_ITEM": "food"}),
        CatalogEntry(namespace="ns", code="D", title="Title D", metadata={"REF_AREA": "France", "ICP_ITEM": "food"}),
        # Add dummy entries to prevent IDF from going to 0 for 50% frequency terms in small corpus
        CatalogEntry(namespace="ns", code="E", title="Title E", metadata={"REF_AREA": "Spain", "ICP_ITEM": "housing"}),
        CatalogEntry(
            namespace="ns", code="F", title="Title F", metadata={"REF_AREA": "Portugal", "ICP_ITEM": "health"}
        ),
        CatalogEntry(
            namespace="ns", code="G", title="Title G", metadata={"REF_AREA": "Greece", "ICP_ITEM": "education"}
        ),
    ]

    cat = Catalog("test_cat")
    cat.set_indexes(
        [
            BM25Index("title_bm25", field="title"),
            BM25Index("ref_area_bm25", field="REF_AREA"),
            BM25Index("icp_item_bm25", field="ICP_ITEM"),
        ]
    )
    cat.set_entries(entries)
    await cat.build()

    # Unknown field falls back to broad search gracefully without raising ValueError
    res_fallback = await cat.search("UNKNOWN_FIELD: Germany", limit=5)
    assert isinstance(res_fallback, CatalogMatches)

    res = await cat.search("REF_AREA: Germany", limit=5)
    assert {m.code for m in res} == {"A", "C"}

    res_or = await cat.search("REF_AREA: Germany, Italy", limit=5)
    assert {m.code for m in res_or} == {"A", "B", "C"}

    res_and = await cat.search("REF_AREA: Germany && ICP_ITEM: energy", limit=5)
    assert {m.code for m in res_and} == {"A"}

    res_comb = await cat.search("REF_AREA: Germany, France && ICP_ITEM: food", limit=5)
    assert {m.code for m in res_comb} == {"C", "D"}
