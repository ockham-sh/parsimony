"""Tests for HybridIndex."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    CatalogEntry,
    HybridIndex,
    VectorIndex,
)
from parsimony.embedder import EmbedderInfo
from parsimony.ranking import ZScoreFusion


class _StubEmbedder:
    DIM = 8

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            out.append([x / norm for x in raw])
        return out

    async def embed_query(self, query: str) -> list[float]:
        # Bias toward titles that share tokens with the query so hybrid ranking
        # stays deterministic under the hash stub (BM25 + vector agree on A).
        anchor = "GDP of Germany" if "Germany" in query else query
        (vector,) = await self.embed_texts([anchor])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub/hash-sha256", dim=self.DIM, normalize=True, package="test-stub")


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
    idx2 = VectorIndex("title_vector", field="title", embedder=_StubEmbedder())

    hybrid = HybridIndex("hybrid_title", "title", indexes=[idx1, idx2])
    await hybrid.build(entries)

    ranking = await hybrid.ranking("Germany GDP", limit=5)
    assert len(ranking.items) > 0
    assert ranking.items[0].code == "A"


@pytest.mark.asyncio
async def test_hybrid_index_save_and_load_roundtrip() -> None:
    entries = [
        CatalogEntry(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        CatalogEntry(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]

    idx1 = BM25Index("title_bm25", field="title")
    idx2 = VectorIndex("title_vector", field="title", embedder=_StubEmbedder())

    hybrid = HybridIndex("hybrid_title", "title", indexes=[idx1, idx2])
    await hybrid.build(entries)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "hybrid_title"
        hybrid.save(path)

        loaded = HybridIndex.load(path)
        stub = _StubEmbedder()
        for idx in loaded._indexes:
            if isinstance(idx, VectorIndex):
                idx._embedder = stub
                idx._embedder_info = stub.info()
        assert loaded.name == "hybrid_title"
        assert loaded.field == "title"
        assert len(loaded._indexes) == 2
        assert {idx.name for idx in loaded._indexes} == {"title_bm25", "title_vector"}

        ranking = await loaded.ranking("Germany GDP", limit=5)
        assert len(ranking.items) > 0
        assert ranking.items[0].code == "A"
