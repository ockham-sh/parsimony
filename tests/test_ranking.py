"""Tests for ranking wrappers and rankers."""

from __future__ import annotations

import pytest

from parsimony.ranking import (
    RRF,
    MinMaxScoreFusion,
    MinMaxScoreFusionSpec,
    RankedItem,
    RankedSetItem,
    Ranking,
    RankingSet,
    RRFSpec,
    concat,
    ranker_from_spec,
    ranker_to_spec,
)


def _ranking(*rows: tuple[str, str, int, float]) -> Ranking:
    return Ranking(tuple(RankedItem(namespace=ns, code=code, rank=rank, score=score) for ns, code, rank, score in rows))


def _ranking_set(*rows: tuple[str, str, str, int, float]) -> RankingSet:
    return RankingSet(
        tuple(
            RankedSetItem(index=index, namespace=ns, code=code, rank=rank, score=score)
            for index, ns, code, rank, score in rows
        )
    )


def test_ranking_items_are_immutable() -> None:
    ranking = _ranking(("n", "A", 0, 0.5))
    assert ranking.items[0].score == 0.5


def test_ranking_rejects_duplicate_identity() -> None:
    with pytest.raises(ValueError, match="unique"):
        Ranking(
            (
                RankedItem(namespace="n", code="A", rank=0, score=0.5),
                RankedItem(namespace="n", code="A", rank=1, score=0.4),
            )
        )


def test_concat_builds_named_ranking_set() -> None:
    rankings = concat(
        {
            "title_bm25": _ranking(("n", "A", 0, 3.0)),
            "title_vector": _ranking(("n", "B", 0, 0.9)),
        }
    )
    assert [item.index for item in rankings.items] == ["title_bm25", "title_vector"]


def test_ranking_set_rejects_duplicate_index_identity() -> None:
    with pytest.raises(ValueError, match="unique"):
        RankingSet(
            (
                RankedSetItem(index="i", namespace="n", code="A", rank=0, score=1.0),
                RankedSetItem(index="i", namespace="n", code="A", rank=1, score=0.5),
            )
        )


def test_rrf_prefers_identity_present_in_multiple_rankings() -> None:
    rankings = _ranking_set(
        ("a", "n", "A", 0, 100.0),
        ("a", "n", "B", 1, 90.0),
        ("b", "n", "B", 0, 0.1),
    )
    final = RRF()(rankings, limit=2)
    assert [item.code for item in final.items] == ["B", "A"]
    assert [item.rank for item in final.items] == [0, 1]


def test_rrf_applies_index_weights() -> None:
    rankings = _ranking_set(
        ("weak", "n", "A", 0, 1.0),
        ("strong", "n", "B", 0, 1.0),
    )
    final = RRF(weights={"strong": 10.0})(rankings, limit=2)
    assert [item.code for item in final.items] == ["B", "A"]


def test_rrf_rejects_invalid_k() -> None:
    with pytest.raises(ValueError, match="positive"):
        RRF(k=0)


def test_rankers_reject_invalid_weights() -> None:
    with pytest.raises(ValueError, match="finite non-negative"):
        RRF(weights={"bad": -1.0})
    with pytest.raises(ValueError, match="finite non-negative"):
        MinMaxScoreFusion(weights={"bad": float("nan")})


def test_minmax_score_fusion_normalizes_per_index_and_weights() -> None:
    rankings = _ranking_set(
        ("bm25", "n", "A", 0, 10.0),
        ("bm25", "n", "B", 1, 0.0),
        ("vector", "n", "B", 0, 0.9),
        ("vector", "n", "A", 1, 0.8),
    )
    final = MinMaxScoreFusion(weights={"bm25": 0.2, "vector": 1.0})(rankings, limit=2)
    assert [item.code for item in final.items] == ["B", "A"]


def test_minmax_score_fusion_ignores_constant_index_scores() -> None:
    rankings = _ranking_set(
        ("constant", "n", "A", 0, 1.0),
        ("constant", "n", "B", 0, 1.0),
    )
    final = MinMaxScoreFusion()(rankings, limit=2)
    assert not final.items


def test_rrf_spec_round_trips_to_runtime_ranker() -> None:
    spec = ranker_to_spec(RRF(weights={"title_bm25": 0.2}, k=42))

    assert spec == RRFSpec(weights={"title_bm25": 0.2}, k=42)
    ranker = ranker_from_spec(spec)

    assert isinstance(ranker, RRF)
    assert ranker.weights == {"title_bm25": 0.2}
    assert ranker.k == 42


def test_minmax_score_fusion_spec_round_trips_to_runtime_ranker() -> None:
    spec = ranker_to_spec(MinMaxScoreFusion(weights={"dense": 2.0}))

    assert spec == MinMaxScoreFusionSpec(weights={"dense": 2.0})
    ranker = ranker_from_spec(spec)

    assert isinstance(ranker, MinMaxScoreFusion)
    assert ranker.weights == {"dense": 2.0}


def test_runtime_only_ranker_is_not_serializable() -> None:
    class RuntimeOnlyRanker:
        def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
            return RRF()(rankings, limit=limit)

    with pytest.raises(TypeError, match="runtime-only"):
        ranker_to_spec(RuntimeOnlyRanker())
