"""Default catalog index policy: BM25 on code, title, and metadata fields."""

from __future__ import annotations

from parsimony.catalog import BM25Index, Catalog, Entity


def test_default_policy_indexes_metadata_at_build() -> None:
    cat = Catalog("demo")
    cat.set_entities(
        [
            Entity(
                namespace="demo",
                code="A",
                title="Alpha",
                metadata={"sector": "tech", "region": "us"},
            ),
            Entity(
                namespace="demo",
                code="B",
                title="Beta",
                metadata={"sector": "finance"},
            ),
        ]
    )
    cat.build()

    assert set(cat.indexes) == {"code", "title", "region", "sector"}
    # Indexed means addressable as a scoring field. Filters address entity fields
    # (including non-indexed metadata) by name.
    res = cat.search("tech", field="sector", limit=5)
    assert {m.code for m in res} == {"A"}
    assert {m.code for m in cat.search(filter={"sector": "tech"}, limit=5)} == {"A"}


def test_default_policy_skips_bool_metadata() -> None:
    """Bool flags are facets for filter=, not BM25 ranking surfaces."""
    cat = Catalog("demo")
    cat.set_entities(
        [
            Entity(
                namespace="demo",
                code="A",
                title="Alpha",
                metadata={"sector": "tech", "active": True},
            ),
            Entity(
                namespace="demo",
                code="B",
                title="Beta",
                metadata={"sector": "finance", "active": False},
            ),
        ]
    )
    cat.build()

    assert set(cat.indexes) == {"code", "title", "sector"}
    assert "active" not in cat.indexes
    # Still filterable — just not ranked.
    assert {m.code for m in cat.search(filter={"active": True}, limit=5)} == {"A"}
    assert {m.code for m in cat.search(filter={"active": False}, limit=5)} == {"B"}


def test_explicit_indexes_are_not_augmented() -> None:
    cat = Catalog("demo", indexes={"title": BM25Index()})
    cat.set_entities(
        [
            Entity(
                namespace="demo",
                code="A",
                title="Alpha",
                metadata={"sector": "tech"},
            ),
        ]
    )
    cat.build()

    assert set(cat.indexes) == {"title"}
