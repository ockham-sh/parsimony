"""Tests for HybridIndex."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    CatalogEntry,
    HybridIndex,
    VectorIndex,
)
from parsimony.embedder import SentenceTransformerEmbedder
from parsimony.ranking import ZScoreFusion


@pytest.mark.asyncio
async def test_hybrid_index_field_validation() -> None:
    idx1 = BM25Index("title_bm25", field="title")
    idx2 = VectorIndex("desc_vector", field="desc")

    with pytest.raises(ValueError, match="field"):
        HybridIndex("hybrid_title", "title", indexes=[idx1, idx2])


@pytest.mark.asyncio
async def test_hybrid_index_unique_names() -> None:
    idx1 = BM25Index("title_bm25", field="title")
    idx2 = BM25Index("title_bm25", field="title")

    with pytest.raises(ValueError, match="unique"):
        HybridIndex("hybrid_title", "title", indexes=[idx1, idx2])


@pytest.mark.asyncio
async def test_hybrid_index_default_fusion() -> None:
    idx1 = BM25Index("title_bm25", field="title")
    hybrid = HybridIndex("hybrid_title", "title", indexes=[idx1])
    assert isinstance(hybrid._fusion, ZScoreFusion)


@pytest.mark.asyncio
async def test_hybrid_index_build_and_ranking() -> None:
    entries = [
        CatalogEntry(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        CatalogEntry(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]

    idx1 = BM25Index("title_bm25", field="title")
    idx2 = VectorIndex("title_vector", field="title", embedder=SentenceTransformerEmbedder())

    hybrid = HybridIndex("hybrid_title", "title", indexes=[idx1, idx2])
    await hybrid.build(entries)

    ranking = await hybrid.ranking("Germany GDP", limit=5)
    table = ranking.to_table()
    assert len(table) > 0
    assert table.loc[0, "code"] == "A"


@pytest.mark.asyncio
async def test_hybrid_index_save_and_load_roundtrip() -> None:
    entries = [
        CatalogEntry(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        CatalogEntry(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]

    idx1 = BM25Index("title_bm25", field="title")
    idx2 = VectorIndex("title_vector", field="title", embedder=SentenceTransformerEmbedder())

    hybrid = HybridIndex("hybrid_title", "title", indexes=[idx1, idx2])
    await hybrid.build(entries)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "hybrid_title"
        hybrid.save(path)

        loaded = HybridIndex.load(path)
        assert loaded.name == "hybrid_title"
        assert loaded.field == "title"
        assert len(loaded._indexes) == 2
        assert {idx.name for idx in loaded._indexes} == {"title_bm25", "title_vector"}

        ranking = await loaded.ranking("Germany GDP", limit=5)
        table = ranking.to_table()
        assert len(table) > 0
        assert table.loc[0, "code"] == "A"
