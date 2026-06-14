"""Tests for HybridIndex."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    Entity,
    HybridIndex,
    VectorIndex,
)
from parsimony.catalog.indexes import IndexBuildContext, embed_query_vectors
from parsimony.embedder import EmbedderInfo
from parsimony.ranking import ZScoreFusion


class _StubEmbedder:
    DIM = 8

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            out.append([x / norm for x in raw])
        return out

    def embed_query(self, query: str) -> list[float]:
        anchor = "GDP of Germany" if "Germany" in query else query
        (vector,) = self.embed_texts([anchor])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub/hash-sha256", dim=self.DIM, normalize=True, package="test-stub")


def test_hybrid_index_duplicate_component_kind() -> None:
    with pytest.raises(ValueError, match="duplicate component kind"):
        HybridIndex(components=[BM25Index(), BM25Index()])


def test_hybrid_index_requires_components() -> None:
    with pytest.raises(ValueError, match="at least one component"):
        HybridIndex(components=[])


def test_hybrid_index_default_fusion() -> None:
    hybrid = HybridIndex(components=[BM25Index()])
    assert isinstance(hybrid._fusion, ZScoreFusion)


def test_hybrid_index_build_and_ranking() -> None:
    entries = [
        Entity(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        Entity(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]

    hybrid = HybridIndex(
        components=[
            BM25Index(),
            VectorIndex(embedder=_StubEmbedder()),
        ]
    )
    ctx = IndexBuildContext(field="title", vector_cache={})
    hybrid.build(entries, ctx=ctx)

    query_vectors = embed_query_vectors("Germany GDP", [hybrid])
    ranking = hybrid.ranking("Germany GDP", limit=5, entries=entries, query_vectors=query_vectors)
    assert len(ranking.items) > 0
    assert ranking.items[0].code == "A"


def test_hybrid_index_save_and_load_roundtrip() -> None:
    entries = [
        Entity(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        Entity(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]

    hybrid = HybridIndex(
        components=[
            BM25Index(),
            VectorIndex(embedder=_StubEmbedder()),
        ]
    )
    ctx = IndexBuildContext(field="title", vector_cache={})
    hybrid.build(entries, ctx=ctx)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "title"
        hybrid.save(path)

        loaded = HybridIndex.load(path)
        stub = _StubEmbedder()
        for component in loaded._components.values():
            if isinstance(component, VectorIndex):
                component._embedder = stub
                component._embedder_info = stub.info()
        assert set(loaded._components) == {"bm25", "vector"}

        query_vectors = embed_query_vectors("Germany GDP", [loaded])
        ranking = loaded.ranking("Germany GDP", limit=5, entries=entries, query_vectors=query_vectors)
        assert len(ranking.items) > 0
        assert ranking.items[0].code == "A"
