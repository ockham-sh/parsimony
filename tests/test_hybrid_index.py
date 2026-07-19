"""Tests for HybridIndex construction, serialization, and value scoring."""

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
from parsimony.catalog.indexes import IndexBuildContext, embed_query_vectors, search_index_values
from parsimony.embedder import EmbedderInfo


class _StubEmbedder:
    """Deterministic embedder mapping known texts to fixed unit vectors.

    Both the titles and the test query are pinned, so vector similarity is
    predictable. Unknown text maps to an orthogonal axis (far from both titles).
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


def _entries() -> list[Entity]:
    return [
        Entity(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        Entity(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]


def _build_hybrid(entries: list[Entity]) -> HybridIndex:
    hybrid = HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder())])
    hybrid.build(entries, ctx=IndexBuildContext(field="title", vector_cache={}))
    return hybrid


def _rebind_stub_embedder(index: HybridIndex) -> None:
    """Re-inject the stub embedder into a loaded index's vector component."""
    stub = _StubEmbedder()
    for component in index._components.values():
        if isinstance(component, VectorIndex):
            component._embedder = stub
            component._embedder_info = stub.info()


def test_hybrid_index_duplicate_component_kind() -> None:
    with pytest.raises(ValueError, match="duplicate component kind"):
        HybridIndex(components=[BM25Index(), BM25Index()])


def test_hybrid_index_requires_components() -> None:
    with pytest.raises(ValueError, match="at least one component"):
        HybridIndex(components=[])


def test_hybrid_index_keeps_distinct_component_kinds() -> None:
    hybrid = HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder())])
    assert set(hybrid._components) == {"bm25", "vector"}


def test_hybrid_index_value_scoring_surfaces_lexical_and_vector_agreement() -> None:
    """A single-field hybrid fuses BM25 and vector: the value both agree on wins.

    Query "Germany GDP" matches "GDP of Germany" on both tokens (BM25) and as
    the vector's nearest neighbour, so it ranks first and its ``matched`` is
    "both".
    """
    hybrid = _build_hybrid(_entries())
    query_vectors = embed_query_vectors("Germany GDP", [hybrid])
    scored = search_index_values(hybrid, "Germany GDP", limit=5, query_vectors=query_vectors)

    assert scored
    top_text, _, _, top_matched = scored[0]
    assert top_text == "GDP of Germany"
    assert top_matched == "both"


def test_hybrid_index_save_and_load_roundtrip() -> None:
    entries = _entries()
    hybrid = _build_hybrid(entries)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "title"
        hybrid.save(path)

        loaded = HybridIndex.load(path)
        _rebind_stub_embedder(loaded)
        assert set(loaded._components) == {"bm25", "vector"}

        query_vectors = embed_query_vectors("Germany GDP", [loaded])
        scored = search_index_values(loaded, "Germany GDP", limit=5, query_vectors=query_vectors)
        assert scored
        assert scored[0][0] == "GDP of Germany"
