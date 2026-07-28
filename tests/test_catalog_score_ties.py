"""Tie behaviour of index value scoring: equal relevance yields equal scores."""

from __future__ import annotations

import pytest

from parsimony.catalog import BM25Index, Entity, VectorIndex
from parsimony.catalog.indexes import IndexBuildContext, QueryContext, search_index_values
from parsimony.embedder import EmbedderInfo


class _LiteralEmbedder:
    def __init__(self) -> None:
        self.embedded_text_batches: list[list[str]] = []

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="literal", dim=2, normalize=False, package="test")

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.embedded_text_batches.append(list(texts))
        return [self._vector(text) for text in texts]

    def embed_query(self, query: str) -> list[float]:
        return self._vector(query)

    def _vector(self, text: str) -> list[float]:
        if "alpha" in text or "beta" in text or "query" in text:
            return [1.0, 0.0]
        if "rare" in text:
            return [0.0, 1.0]
        return [0.5, 0.5]


def _entries() -> list[Entity]:
    return [
        Entity(namespace="test", code="A", title="a", metadata={"label": "alpha"}),
        Entity(namespace="test", code="B", title="b", metadata={"label": "beta"}),
        Entity(namespace="test", code="C", title="c", metadata={"label": "rare gamma"}),
        Entity(namespace="test", code="D", title="d", metadata={"label": "other gamma"}),
    ]


def test_vector_index_assigns_equal_score_to_equal_neighbours() -> None:
    entries = _entries()
    index = VectorIndex(embedder=_LiteralEmbedder())
    ctx = IndexBuildContext(field="label", vector_cache={})
    index.build(entries, ctx=ctx)

    scored = search_index_values(index, QueryContext(query="query"), limit=10)
    by_text = {sv.text: sv.relevance for sv in scored}

    # "alpha" and "beta" both embed onto the query's axis, so they tie exactly.
    assert by_text["alpha"] == by_text["beta"]
    # A less-similar neighbour scores strictly below the tied pair.
    assert by_text["other gamma"] < by_text["alpha"]
    assert by_text["alpha"] == pytest.approx(1.0)


def test_vector_index_embeds_duplicate_field_text_once_per_build() -> None:
    embedder = _LiteralEmbedder()
    index = VectorIndex(embedder=embedder)
    entries = [
        Entity(namespace="test", code="A", title="a", metadata={"label": "alpha"}),
        Entity(namespace="test", code="B", title="b", metadata={"label": "alpha"}),
        Entity(namespace="test", code="C", title="c", metadata={"label": "rare gamma"}),
    ]
    ctx = IndexBuildContext(field="label", vector_cache={})
    index.build(entries, ctx=ctx)

    assert embedder.embedded_text_batches == [["alpha", "rare gamma"]]


def test_bm25_index_assigns_equal_score_to_equal_matches() -> None:
    entries = [
        Entity(
            namespace="test",
            code="A",
            title="a",
            metadata={"label": "instantaneous forward rate 1 year"},
        ),
        Entity(
            namespace="test",
            code="B",
            title="b",
            metadata={"label": "instantaneous forward rate 2 year"},
        ),
        Entity(namespace="test", code="C", title="c", metadata={"label": "unrelated label"}),
        Entity(namespace="test", code="D", title="d", metadata={"label": "other unrelated"}),
        Entity(namespace="test", code="E", title="e", metadata={"label": "different words"}),
    ]
    index = BM25Index()
    ctx = IndexBuildContext(field="label", vector_cache={})
    index.build(entries, ctx=ctx)

    scored = search_index_values(index, QueryContext(query="instantaneous forward rate one year"), limit=10)
    by_text = {sv.text: sv.relevance for sv in scored}

    # The two labels differ only in the token the query doesn't carry ("1"/"2"
    # vs "one"), so they share the same four matched tokens and tie exactly.
    a = "instantaneous forward rate 1 year"
    b = "instantaneous forward rate 2 year"
    assert a in by_text
    assert by_text[a] == by_text[b]
