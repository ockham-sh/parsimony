"""Tests for ZScoreFusion ranking."""

from __future__ import annotations

from parsimony.ranking import (
    RankedItem,
    Ranking,
    ZScoreFusion,
    ZScoreFusionSpec,
    concat,
    ranker_from_spec,
    ranker_to_spec,
)


def _ranking(*rows: tuple[str, str, int, float]) -> Ranking:
    return Ranking(tuple(RankedItem(namespace=ns, code=code, rank=rank, score=score) for ns, code, rank, score in rows))


def test_z_score_fusion_identical_scores() -> None:
    r1 = _ranking(("n", "A", 0, 10.0), ("n", "B", 1, 10.0))
    rs = concat({"idx1": r1})
    final = ZScoreFusion()(rs, limit=5)

    assert len(final.items) == 2
    assert all(item.score == 0.0 for item in final.items)


def test_z_score_fusion_single_row() -> None:
    r1 = _ranking(("n", "A", 0, 5.0))
    rs = concat({"idx1": r1})
    final = ZScoreFusion()(rs, limit=5)

    assert len(final.items) == 1
    assert final.items[0].score == 0.0


def test_z_score_fusion_weighted_sum() -> None:
    r1 = _ranking(("n", "A", 0, 10.0), ("n", "B", 1, 20.0), ("n", "C", 2, 30.0))
    r2 = _ranking(("n", "A", 0, 1.0), ("n", "B", 1, 2.0), ("n", "C", 2, 3.0))
    rs = concat({"idx1": r1, "idx2": r2})

    final = ZScoreFusion()(rs, limit=5)
    assert [item.code for item in final.items] == ["C", "B", "A"]
    assert [item.score for item in final.items] == [2.0, 0.0, -2.0]

    final_w = ZScoreFusion(weights={"idx1": 2.0, "idx2": 0.5})(rs, limit=5)
    assert [item.code for item in final_w.items] == ["C", "B", "A"]
    assert [item.score for item in final_w.items] == [2.5, 0.0, -2.5]


def test_z_score_fusion_spec_roundtrip() -> None:
    fuser = ZScoreFusion(weights={"idx1": 2.5})
    spec = ranker_to_spec(fuser)
    assert isinstance(spec, ZScoreFusionSpec)
    assert spec.weights == {"idx1": 2.5}

    reconstructed = ranker_from_spec(spec)
    assert isinstance(reconstructed, ZScoreFusion)
    assert reconstructed.weights == {"idx1": 2.5}
