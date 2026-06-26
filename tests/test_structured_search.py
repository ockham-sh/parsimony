"""Tests for structured query search."""

from __future__ import annotations

import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    Entity,
    UnknownIndexedFieldError,
)
from parsimony.catalog.query import StructuredQuery, parse_query


def test_parse_query_broad() -> None:
    assert parse_query("inflation germany", {"REF_AREA", "ICP_ITEM"}) is None
    assert parse_query("REF_AREA", {"REF_AREA", "ICP_ITEM"}) is None


def test_parse_query_structured() -> None:
    q1 = parse_query("REF_AREA: Germany", {"REF_AREA", "ICP_ITEM"})
    assert isinstance(q1, StructuredQuery)
    assert q1.clauses == [("REF_AREA", ["Germany"])]

    q2 = parse_query("REF_AREA: Germany, Italy && ICP_ITEM: energy", {"REF_AREA", "ICP_ITEM"})
    assert isinstance(q2, StructuredQuery)
    assert q2.clauses == [
        ("REF_AREA", ["Germany", "Italy"]),
        ("ICP_ITEM", ["energy"]),
    ]


def test_parse_query_unknown_field_raises() -> None:
    with pytest.raises(UnknownIndexedFieldError, match="UNKNOWN_FIELD"):
        parse_query("UNKNOWN_FIELD: Germany", {"REF_AREA"})


def test_parse_query_malformed() -> None:
    with pytest.raises(ValueError, match="Malformed clause"):
        parse_query("REF_AREA: Germany && malformed_part", {"REF_AREA", "ICP_ITEM"})

    with pytest.raises(ValueError, match="No values"):
        parse_query("REF_AREA: ", {"REF_AREA", "ICP_ITEM"})


def test_structured_search_execution() -> None:
    entries = [
        Entity(namespace="ns", code="A", title="Title A", metadata={"REF_AREA": "Germany", "ICP_ITEM": "energy"}),
        Entity(namespace="ns", code="B", title="Title B", metadata={"REF_AREA": "Italy", "ICP_ITEM": "energy"}),
        Entity(namespace="ns", code="C", title="Title C", metadata={"REF_AREA": "Germany", "ICP_ITEM": "food"}),
        Entity(namespace="ns", code="D", title="Title D", metadata={"REF_AREA": "France", "ICP_ITEM": "food"}),
        # Add dummy entries to prevent IDF from going to 0 for 50% frequency terms in small corpus
        Entity(namespace="ns", code="E", title="Title E", metadata={"REF_AREA": "Spain", "ICP_ITEM": "housing"}),
        Entity(namespace="ns", code="F", title="Title F", metadata={"REF_AREA": "Portugal", "ICP_ITEM": "health"}),
        Entity(namespace="ns", code="G", title="Title G", metadata={"REF_AREA": "Greece", "ICP_ITEM": "education"}),
    ]

    cat = Catalog("test_cat")
    cat.set_indexes(
        {
            "title": BM25Index(),
            "REF_AREA": BM25Index(),
            "ICP_ITEM": BM25Index(),
        }
    )
    cat.set_entities(entries)
    cat.build()

    with pytest.raises(UnknownIndexedFieldError, match="UNKNOWN_FIELD"):
        cat.search("UNKNOWN_FIELD: Germany", limit=5)

    res = cat.search("REF_AREA: Germany", limit=5)
    assert {m.code for m in res} == {"A", "C"}

    res_or = cat.search("REF_AREA: Germany, Italy", limit=5)
    assert {m.code for m in res_or} == {"A", "B", "C"}

    res_and = cat.search(
        filter={"REF_AREA": ["Germany"], "ICP_ITEM": ["energy"]},
        limit=5,
    )
    assert {m.code for m in res_and} == {"A"}

    res_comb = cat.search(
        filter={"REF_AREA": ["Germany", "France"], "ICP_ITEM": ["food"]},
        limit=5,
    )
    assert {m.code for m in res_comb} == {"C", "D"}
