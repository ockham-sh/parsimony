"""Tests for ZScoreFusion ranking."""

from __future__ import annotations

import pandas as pd

from parsimony.ranking import (
    Ranking,
    ZScoreFusion,
    ZScoreFusionSpec,
    concat,
    ranker_from_spec,
    ranker_to_spec,
)


def test_z_score_fusion_identical_scores() -> None:
    r1 = Ranking(
        pd.DataFrame(
            [
                {"namespace": "n", "code": "A", "rank": 0, "score": 10.0},
                {"namespace": "n", "code": "B", "rank": 1, "score": 10.0},
            ]
        )
    )
    rs = concat({"idx1": r1})
    fuser = ZScoreFusion()
    final = fuser(rs, limit=5)

    table = final.to_table()
    assert len(table) == 2
    assert (table["score"] == 0.0).all()


def test_z_score_fusion_single_row() -> None:
    r1 = Ranking(
        pd.DataFrame(
            [
                {"namespace": "n", "code": "A", "rank": 0, "score": 5.0},
            ]
        )
    )
    rs = concat({"idx1": r1})
    fuser = ZScoreFusion()
    final = fuser(rs, limit=5)

    table = final.to_table()
    assert len(table) == 1
    assert table.loc[0, "score"] == 0.0


def test_z_score_fusion_weighted_sum() -> None:
    r1 = Ranking(
        pd.DataFrame(
            [
                {"namespace": "n", "code": "A", "rank": 0, "score": 10.0},
                {"namespace": "n", "code": "B", "rank": 1, "score": 20.0},
                {"namespace": "n", "code": "C", "rank": 2, "score": 30.0},
            ]
        )
    )

    r2 = Ranking(
        pd.DataFrame(
            [
                {"namespace": "n", "code": "A", "rank": 0, "score": 1.0},
                {"namespace": "n", "code": "B", "rank": 1, "score": 2.0},
                {"namespace": "n", "code": "C", "rank": 2, "score": 3.0},
            ]
        )
    )

    rs = concat({"idx1": r1, "idx2": r2})

    fuser = ZScoreFusion()
    final = fuser(rs, limit=5)
    table = final.to_table()
    assert list(table["code"]) == ["C", "B", "A"]
    assert list(table["score"]) == [2.0, 0.0, -2.0]

    fuser_weighted = ZScoreFusion(weights={"idx1": 2.0, "idx2": 0.5})
    final_w = fuser_weighted(rs, limit=5)
    table_w = final_w.to_table()
    assert list(table_w["code"]) == ["C", "B", "A"]
    assert list(table_w["score"]) == [2.5, 0.0, -2.5]


def test_z_score_fusion_spec_roundtrip() -> None:
    fuser = ZScoreFusion(weights={"idx1": 2.5})
    spec = ranker_to_spec(fuser)
    assert isinstance(spec, ZScoreFusionSpec)
    assert spec.weights == {"idx1": 2.5}

    reconstructed = ranker_from_spec(spec)
    assert isinstance(reconstructed, ZScoreFusion)
    assert reconstructed.weights == {"idx1": 2.5}
