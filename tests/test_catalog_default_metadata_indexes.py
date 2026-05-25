"""Default catalog index policy: BM25 on code, title, and metadata fields."""

from __future__ import annotations

import pytest

from parsimony.catalog import BM25Index, Catalog, Entity


@pytest.mark.asyncio
async def test_default_policy_indexes_metadata_at_build() -> None:
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
    await cat.build()

    assert set(cat._indexed_fields()) == {"code", "title", "region", "sector"}
    res, _ = await cat.search("sector: tech", limit=5)
    assert {m.code for m in res} == {"A"}


@pytest.mark.asyncio
async def test_explicit_indexes_are_not_augmented() -> None:
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
    await cat.build()

    assert cat._indexed_fields() == ["title"]
