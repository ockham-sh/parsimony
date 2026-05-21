"""Tests for ranking wrappers and rankers."""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.ranking import (
    RRF,
    MinMaxScoreFusion,
    MinMaxScoreFusionSpec,
    Ranking,
    RankingSet,
    RRFSpec,
    concat,
    ranker_from_spec,
    ranker_to_spec,
)


def test_ranking_to_table_returns_copy() -> None:
    ranking = Ranking(pd.DataFrame([{"namespace": "n", "code": "A", "rank": 0, "score": 0.5}]))
    table = ranking.to_table()
    table.loc[0, "score"] = 0.0
    assert ranking.to_table().loc[0, "score"] == 0.5


def test_ranking_rejects_duplicate_identity() -> None:
    with pytest.raises(ValueError, match="unique"):
        Ranking(
            pd.DataFrame(
                [
                    {"namespace": "n", "code": "A", "rank": 0, "score": 0.5},
                    {"namespace": "n", "code": "A", "rank": 1, "score": 0.4},
                ]
            )
        )


def test_concat_builds_named_ranking_set() -> None:
    rankings = concat(
        {
            "title_bm25": Ranking(pd.DataFrame([{"namespace": "n", "code": "A", "rank": 0, "score": 3.0}])),
            "title_vector": Ranking(pd.DataFrame([{"namespace": "n", "code": "B", "rank": 0, "score": 0.9}])),
        }
    )
    assert list(rankings.to_table()["index"]) == ["title_bm25", "title_vector"]


def test_ranking_set_rejects_duplicate_index_identity() -> None:
    with pytest.raises(ValueError, match="unique"):
        RankingSet(
            pd.DataFrame(
                [
                    {"index": "i", "namespace": "n", "code": "A", "rank": 0, "score": 1.0},
                    {"index": "i", "namespace": "n", "code": "A", "rank": 1, "score": 0.5},
                ]
            )
        )


def test_rrf_prefers_identity_present_in_multiple_rankings() -> None:
    rankings = RankingSet(
        pd.DataFrame(
            [
                {"index": "a", "namespace": "n", "code": "A", "rank": 0, "score": 100.0},
                {"index": "a", "namespace": "n", "code": "B", "rank": 1, "score": 90.0},
                {"index": "b", "namespace": "n", "code": "B", "rank": 0, "score": 0.1},
            ]
        )
    )
    final = RRF()(rankings, limit=2).to_table()
    assert list(final["code"]) == ["B", "A"]
    assert list(final["rank"]) == [0, 1]


def test_rrf_applies_index_weights() -> None:
    rankings = RankingSet(
        pd.DataFrame(
            [
                {"index": "weak", "namespace": "n", "code": "A", "rank": 0, "score": 1.0},
                {"index": "strong", "namespace": "n", "code": "B", "rank": 0, "score": 1.0},
            ]
        )
    )
    final = RRF(weights={"strong": 10.0})(rankings, limit=2).to_table()
    assert list(final["code"]) == ["B", "A"]


def test_rrf_rejects_invalid_k() -> None:
    with pytest.raises(ValueError, match="positive"):
        RRF(k=0)


def test_rankers_reject_invalid_weights() -> None:
    with pytest.raises(ValueError, match="finite non-negative"):
        RRF(weights={"bad": -1.0})
    with pytest.raises(ValueError, match="finite non-negative"):
        MinMaxScoreFusion(weights={"bad": float("nan")})


def test_minmax_score_fusion_normalizes_per_index_and_weights() -> None:
    rankings = RankingSet(
        pd.DataFrame(
            [
                {"index": "bm25", "namespace": "n", "code": "A", "rank": 0, "score": 10.0},
                {"index": "bm25", "namespace": "n", "code": "B", "rank": 1, "score": 0.0},
                {"index": "vector", "namespace": "n", "code": "B", "rank": 0, "score": 0.9},
                {"index": "vector", "namespace": "n", "code": "A", "rank": 1, "score": 0.8},
            ]
        )
    )
    final = MinMaxScoreFusion(weights={"bm25": 0.2, "vector": 1.0})(rankings, limit=2).to_table()

    assert list(final["code"]) == ["B", "A"]


def test_minmax_score_fusion_ignores_constant_index_scores() -> None:
    rankings = RankingSet(
        pd.DataFrame(
            [
                {"index": "constant", "namespace": "n", "code": "A", "rank": 0, "score": 1.0},
                {"index": "constant", "namespace": "n", "code": "B", "rank": 0, "score": 1.0},
            ]
        )
    )

    final = MinMaxScoreFusion()(rankings, limit=2).to_table()

    assert final.empty


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
