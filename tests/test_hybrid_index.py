"""Tests for HybridIndex."""

from __future__ import annotations

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
    """Deterministic embedder mapping known texts to fixed unit vectors.

    Both the titles and the test query are pinned, so vector similarity is
    predictable. (The previous hash-based stub coincidentally ranked the wrong
    doc top, which masked whether the hybrid actually applied its fusion.)
    Unknown text maps to an orthogonal axis (far from both titles).
    """

    DIM = 3
    _VECTORS = {
        "GDP of Germany": [1.0, 0.0, 0.0],
        "CPI of France": [0.0, 1.0, 0.0],
        "Germany GDP": [0.97, 0.05, 0.0],  # query embeds near "GDP of Germany"
    }

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [list(self._VECTORS.get(t, [0.0, 0.0, 1.0])) for t in texts]

    def embed_query(self, query: str) -> list[float]:
        (vector,) = self.embed_texts([query])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub/fixed", dim=self.DIM, normalize=True, package="test-stub")


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


class _DisagreeEmbedder:
    """Vector view that disagrees with BM25: the query embeds near 'beta gamma' (B),
    while BM25 (token overlap) prefers the token-richer 'alpha gamma' (A)."""

    DIM = 3
    _VECTORS = {
        "alpha gamma": [1.0, 0.0, 0.0],
        "beta gamma": [0.0, 1.0, 0.0],
        "gamma alpha": [0.1, 0.95, 0.0],  # query embeds near B
    }

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [list(self._VECTORS.get(t, [0.0, 0.0, 1.0])) for t in texts]

    def embed_query(self, query: str) -> list[float]:
        (vector,) = self.embed_texts([query])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="disagree", dim=self.DIM, normalize=True, package="test")


def test_fusion_weights_drive_ranking() -> None:
    """The configured fusion + weights must change the final order.

    Proof the hybrid applies its fusion rather than ranking by max(raw component
    score): BM25 prefers A (more query tokens), the vector view prefers B. Under a
    raw-max scheme the winner can't depend on weights; under real fusion it does.
    """
    entries = [
        Entity(namespace="ns", code="A", title="alpha gamma", metadata={}),
        Entity(namespace="ns", code="B", title="beta gamma", metadata={}),
    ]

    def _top(weights: dict[str, float]) -> str:
        hybrid = HybridIndex(
            components=[BM25Index(), VectorIndex(embedder=_DisagreeEmbedder())],
            fusion=ZScoreFusion(weights=weights),
        )
        ctx = IndexBuildContext(field="title", vector_cache={})
        hybrid.build(entries, ctx=ctx)
        qv = embed_query_vectors("gamma alpha", [hybrid])
        ranking = hybrid.ranking("gamma alpha", limit=5, entries=entries, query_vectors=qv)
        assert {it.code for it in ranking.items} == {"A", "B"}  # neither candidate dropped
        return ranking.items[0].code

    assert _top({"bm25": 10.0, "vector": 0.01}) == "A"  # BM25-weighted → token match wins
    assert _top({"bm25": 0.01, "vector": 10.0}) == "B"  # vector-weighted → semantic match wins


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
