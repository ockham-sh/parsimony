"""Tests for broad (plain-text, no ``fields=``) catalog search."""

from __future__ import annotations

from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    BroadSearchUnavailableError,
    Catalog,
    Entity,
)


def _built(indexes: dict[str, BM25Index], entities: list[Entity]) -> Catalog:
    catalog = Catalog("cat", indexes=indexes)
    catalog.set_entities(entities)
    catalog.build()
    return catalog


def test_broad_search_targets_title_index() -> None:
    catalog = _built(
        {"title": BM25Index(), "code": BM25Index()},
        [
            Entity(namespace="ns", code="A", title="renewable energy", metadata={}),
            Entity(namespace="ns", code="B", title="fiscal balance", metadata={}),
        ],
    )
    hits = catalog.search("renewable energy", limit=5)
    assert hits[0].code == "A"


def test_broad_search_without_title_index_raises() -> None:
    catalog = _built(
        {"code": BM25Index()},
        [Entity(namespace="ns", code="A", title="alpha", metadata={})],
    )
    with pytest.raises(BroadSearchUnavailableError, match="field="):
        catalog.search("plain text", limit=5)


def test_explicit_field_search_needs_no_title_index() -> None:
    catalog = _built(
        {"code": BM25Index(), "region": BM25Index()},
        [
            Entity(namespace="ns", code="A", title="alpha", metadata={"region": "germany"}),
            Entity(namespace="ns", code="B", title="beta", metadata={"region": "france"}),
        ],
    )
    hits = catalog.search("germany", field="region", limit=5)
    assert [h.code for h in hits] == ["A"]


def test_broad_search_works_after_save_load_roundtrip(tmp_path: Path) -> None:
    catalog = _built(
        {"title": BM25Index()},
        [
            Entity(namespace="ns", code="A", title="renewable energy", metadata={}),
            Entity(namespace="ns", code="B", title="fiscal balance", metadata={}),
        ],
    )
    catalog.save(f"file://{tmp_path}/snapshot")

    loaded = Catalog.load(f"file://{tmp_path}/snapshot")
    hits = loaded.search("renewable energy", limit=5)
    assert hits[0].code == "A"
