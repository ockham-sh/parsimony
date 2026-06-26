"""Tests for DisMaxIndex."""

from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from parsimony.catalog import BM25Index, Catalog, DisMaxIndex, Entity, VectorIndex
from parsimony.catalog.indexes import IndexBuildContext, collect_vector_indexes, embed_query_vectors
from parsimony.embedder import EmbedderInfo


class _StubEmbedder:
    DIM = 8

    def __init__(self) -> None:
        self.embed_texts_calls: list[list[str]] = []

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.embed_texts_calls.append(list(texts))
        out: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            out.append([x / norm for x in raw])
        return out

    def embed_query(self, query: str) -> list[float]:
        (vector,) = self.embed_texts([query])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub/hash-sha256", dim=self.DIM, normalize=True, package="test-stub")


@dataclass
class _ScoreStub:
    scores: dict[int, float]

    def score_candidates(
        self,
        query: str,
        *,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> dict[int, float]:
        del query, query_vectors
        return dict(self.scores)


def _sample_entries() -> list[Entity]:
    return [
        Entity(
            namespace="ns",
            code="A",
            title="placeholder",
            metadata={
                "short_title": "World Bank GDP growth",
                "long_title": "WB macro indicator",
            },
        ),
        Entity(
            namespace="ns",
            code="B",
            title="placeholder",
            metadata={
                "short_title": "CPI inflation France",
                "long_title": "Consumer price index",
            },
        ),
    ]


def test_dismax_rejects_empty_fields() -> None:
    with pytest.raises(ValueError, match="at least one field"):
        DisMaxIndex(fields=[], component_factory=BM25Index)


def test_dismax_rejects_duplicate_fields() -> None:
    with pytest.raises(ValueError, match="must be unique"):
        DisMaxIndex(fields=["a", "a"], component_factory=BM25Index)


def test_dismax_rejects_bad_tie_breaker() -> None:
    with pytest.raises(ValueError, match="tie_breaker"):
        DisMaxIndex(fields=["a"], component_factory=BM25Index, tie_breaker=-0.1)
    with pytest.raises(ValueError, match="tie_breaker"):
        DisMaxIndex(fields=["a"], component_factory=BM25Index, tie_breaker=1.1)


def test_dismax_rejects_heterogeneous_kinds() -> None:
    counter = {"n": 0}

    def factory() -> BM25Index | VectorIndex:
        counter["n"] += 1
        return BM25Index() if counter["n"] == 1 else VectorIndex()

    with pytest.raises(ValueError, match="uniform component kind"):
        DisMaxIndex(fields=["a", "b"], component_factory=factory)


def test_dismax_bm25_picks_better_field() -> None:
    entries = _sample_entries()
    dismax = DisMaxIndex(
        fields=["short_title", "long_title"],
        component_factory=BM25Index,
    )
    ctx = IndexBuildContext(field="title", vector_cache={})
    dismax.build(entries, ctx=ctx)

    scores = dismax.score_candidates("World Bank GDP")
    assert 0 in scores
    assert scores[0] > scores.get(1, 0.0)


def test_dismax_tie_breaker_zero_is_pure_max() -> None:
    dismax = DisMaxIndex(fields=["a", "b"], component_factory=BM25Index, tie_breaker=0.0)
    dismax._per_field = {
        "a": _ScoreStub({0: 10.0, 1: 2.0}),
        "b": _ScoreStub({0: 4.0, 1: 8.0}),
    }
    scores = dismax.score_candidates("query")
    assert scores[0] == 10.0
    assert scores[1] == 8.0


def test_dismax_tie_breaker_one_sums_all() -> None:
    dismax = DisMaxIndex(fields=["a", "b"], component_factory=BM25Index, tie_breaker=1.0)
    dismax._per_field = {
        "a": _ScoreStub({0: 10.0, 1: 2.0}),
        "b": _ScoreStub({0: 4.0, 1: 8.0}),
    }
    scores = dismax.score_candidates("query")
    assert scores[0] == 14.0
    assert scores[1] == 10.0


def test_dismax_intermediate_tie_breaker() -> None:
    dismax = DisMaxIndex(fields=["a", "b"], component_factory=BM25Index, tie_breaker=0.3)
    dismax._per_field = {
        "a": _ScoreStub({0: 10.0}),
        "b": _ScoreStub({0: 4.0}),
    }
    scores = dismax.score_candidates("query")
    assert scores[0] == pytest.approx(10.0 + 0.3 * 4.0)


def test_dismax_union_of_candidates() -> None:
    dismax = DisMaxIndex(fields=["a", "b"], component_factory=BM25Index)
    dismax._per_field = {
        "a": _ScoreStub({0: 5.0}),
        "b": _ScoreStub({1: 7.0}),
    }
    scores = dismax.score_candidates("query")
    assert scores == {0: 5.0, 1: 7.0}


def test_dismax_save_load_roundtrip_bm25() -> None:
    entries = _sample_entries()
    dismax = DisMaxIndex(
        fields=["short_title", "long_title"],
        component_factory=BM25Index,
        tie_breaker=0.2,
    )
    ctx = IndexBuildContext(field="title", vector_cache={})
    dismax.build(entries, ctx=ctx)
    before = dismax.score_candidates("World Bank GDP")

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "title"
        dismax.save(path)
        loaded = DisMaxIndex.load(path)
        after = loaded.score_candidates("World Bank GDP")

    assert before == after


def test_dismax_save_load_roundtrip_vector() -> None:
    entries = [
        Entity(namespace="ns", code="A", title="GDP Germany", metadata={"short_title": "GDP Germany"}),
        Entity(namespace="ns", code="B", title="CPI France", metadata={"short_title": "CPI France"}),
    ]
    stub = _StubEmbedder()
    dismax = DisMaxIndex(
        fields=["title", "short_title"],
        component_factory=lambda: VectorIndex(embedder=stub),
    )
    ctx = IndexBuildContext(field="ignored", vector_cache={})
    dismax.build(entries, ctx=ctx)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "title"
        dismax.save(path)
        loaded = DisMaxIndex.load(path, embedder=stub)
        for component in loaded._per_field.values():
            assert isinstance(component, VectorIndex)
            assert component.embedder_info.model == stub.info().model

        query_vectors = embed_query_vectors("Germany GDP", [loaded])
        ranking = loaded.ranking("Germany GDP", limit=5, entries=entries, query_vectors=query_vectors)
        assert ranking.items[0].code == "A"


def test_dismax_vector_shares_vector_cache() -> None:
    entries = [
        Entity(
            namespace="ns",
            code="A",
            title="shared text",
            metadata={"short_title": "shared text", "long_title": "shared text"},
        ),
    ]
    stub = _StubEmbedder()
    dismax = DisMaxIndex(
        fields=["short_title", "long_title"],
        component_factory=lambda: VectorIndex(embedder=stub),
    )
    ctx = IndexBuildContext(field="title", vector_cache={})
    dismax.build(entries, ctx=ctx)
    assert stub.embed_texts_calls
    unique_texts = {text for call in stub.embed_texts_calls for text in call}
    assert unique_texts == {"shared text"}


def test_collect_vector_indexes_recurse_dismax() -> None:
    stub = _StubEmbedder()
    dismax = DisMaxIndex(
        fields=["a", "b"],
        component_factory=lambda: VectorIndex(embedder=stub),
    )
    vector_indexes = collect_vector_indexes(dismax)
    assert len(vector_indexes) == 2
    assert all(isinstance(idx, VectorIndex) for idx in vector_indexes)


def test_dismax_inside_catalog_search() -> None:
    entries = _sample_entries()
    catalog = Catalog(
        name="demo",
        indexes={
            "title": DisMaxIndex(
                fields=["short_title", "long_title"],
                component_factory=BM25Index,
            ),
        },
        default_field="title",
    )
    catalog.set_entities(entries)
    catalog.build()

    matches = catalog.search("title: World Bank GDP", limit=5)
    assert matches
    assert matches[0].code == "A"


def test_dismax_inside_catalog_broad_search() -> None:
    entries = _sample_entries()
    catalog = Catalog(
        name="demo",
        indexes={
            "title": DisMaxIndex(
                fields=["short_title", "long_title"],
                component_factory=BM25Index,
            ),
        },
        default_field="title",
    )
    catalog.set_entities(entries)
    catalog.build()

    matches = catalog.search("World Bank GDP", limit=5)
    assert matches
    assert matches[0].code == "A"
