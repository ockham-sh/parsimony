"""The one fusion primitive: weighted, tie-aware Reciprocal Rank Fusion.

`rrf` is the only place in the kernel that turns rankings into contributions,
and every fusion level uses it: lexical + semantic inside a hybrid index, and
field rankings into a row score. Output is always top-normalized (best = 1.0).
These tests pin the properties the rest of the catalog relies on.
"""

from __future__ import annotations

import math

import pytest

from parsimony.indexes import RRF_K, rrf


def test_single_ranking_is_top_normalized_reciprocal_ranks() -> None:
    """One source: reciprocal ranks, then best → 1.0."""
    fused = rrf({"only": {"a": 9.1, "b": 4.2, "c": 0.3}})
    assert fused["a"] == pytest.approx(1.0)
    assert fused["b"] == pytest.approx((1.0 / (RRF_K + 2)) / (1.0 / (RRF_K + 1)))
    assert fused["c"] == pytest.approx((1.0 / (RRF_K + 3)) / (1.0 / (RRF_K + 1)))


def test_raw_magnitudes_do_not_survive_fusion() -> None:
    """Only order crosses the boundary: a landslide and a squeaker rank the same.

    This is the property that lets BM25 (unbounded) and cosine similarity
    (roughly [-1, 1]) be combined at all.
    """
    landslide = rrf({"s": {"a": 1_000_000.0, "b": 0.000_1}})
    squeaker = rrf({"s": {"a": 0.51, "b": 0.50}})
    assert landslide == squeaker
    assert landslide["a"] == pytest.approx(1.0)


def test_tied_scores_share_a_competition_rank() -> None:
    """A plateau contributes identically for every tied item, and skips ranks."""
    fused = rrf({"s": {"a": 2.0, "b": 2.0, "c": 1.0}})
    assert fused["a"] == pytest.approx(fused["b"])
    assert fused["a"] == pytest.approx(1.0)
    # Competition ranks: two items tie at 1, so the next is rank 3, not rank 2.
    assert fused["c"] == pytest.approx((1.0 / (RRF_K + 3)) / (1.0 / (RRF_K + 1)))


def test_agreement_across_sources_outranks_a_single_strong_hit() -> None:
    """Two sources agreeing beats one source's favourite — the reason for RRF."""
    fused = rrf({"lexical": {"agreed": 0.1, "lexical_only": 99.0}, "semantic": {"agreed": 0.2}})
    assert fused["agreed"] > fused["lexical_only"]
    assert fused["agreed"] == pytest.approx(1.0)


def test_weights_scale_a_source_contribution() -> None:
    heavy = rrf({"a": {"x": 1.0}, "b": {"y": 1.0}}, weights={"a": 2.0, "b": 1.0})
    assert heavy["x"] == pytest.approx(1.0)
    assert heavy["y"] == pytest.approx(0.5)


def test_absent_items_score_nothing_from_that_source() -> None:
    fused = rrf({"a": {"x": 1.0}, "b": {"y": 1.0}})
    assert set(fused) == {"x", "y"}
    assert fused["x"] == pytest.approx(fused["y"])
    assert fused["x"] == pytest.approx(1.0)


def test_empty_inputs_fuse_to_nothing() -> None:
    assert rrf({}) == {}
    assert rrf({"s": {}}) == {}


def test_generic_item_identity_needs_no_ordering() -> None:
    """Items are keyed by identity, not sorted by it: unorderable keys are fine."""

    class Opaque:
        """Deliberately not orderable — no __lt__."""

    first, second = Opaque(), Opaque()
    fused = rrf({"s": {first: 1.0, second: 2.0}})
    assert fused[second] == pytest.approx(1.0)
    assert fused[second] > fused[first]


def test_integer_and_string_keys_both_work() -> None:
    """Value ids fuse inside an index; value texts fuse across fields."""
    assert set(rrf({"s": {7: 1.0}})) == {7}
    assert set(rrf({"s": {"seven": 1.0}})) == {"seven"}
    assert rrf({"s": {7: 1.0}})[7] == pytest.approx(1.0)


def test_identical_input_yields_identical_output() -> None:
    rankings = {"a": {"x": 1.0, "y": 1.0, "z": 1.0}, "b": {"z": 5.0, "x": 5.0}}
    assert list(rrf(rankings).items()) == list(rrf(rankings).items())


@pytest.mark.parametrize("score", [math.inf, -math.inf, math.nan])
def test_non_finite_scores_are_rejected(score: float) -> None:
    """Callers must resolve infinities themselves; fusion stays arithmetic."""
    with pytest.raises(ValueError, match="finite scores"):
        rrf({"s": {"a": score, "b": 1.0}})


@pytest.mark.parametrize("weight", [0.0, -1.0, math.inf, math.nan])
def test_non_positive_or_non_finite_weights_are_rejected(weight: float) -> None:
    with pytest.raises(ValueError, match="positive finite number"):
        rrf({"s": {"a": 1.0}}, weights={"s": weight})


def test_missing_weight_for_a_declared_source_is_an_error() -> None:
    with pytest.raises(KeyError):
        rrf({"a": {"x": 1.0}, "b": {"y": 1.0}}, weights={"a": 1.0})


@pytest.mark.parametrize("k", [0, -1])
def test_k_below_one_is_rejected(k: int) -> None:
    with pytest.raises(ValueError, match="k must be at least 1"):
        rrf({"s": {"a": 1.0}}, k=k)


def test_k_controls_how_much_rank_position_matters() -> None:
    """Small k spreads ranks apart; large k compresses them toward equality."""
    spread = rrf({"s": {"a": 2.0, "b": 1.0}}, k=1)
    compressed = rrf({"s": {"a": 2.0, "b": 1.0}}, k=10_000)
    assert spread["a"] == pytest.approx(1.0)
    assert compressed["a"] == pytest.approx(1.0)
    assert spread["a"] / spread["b"] > compressed["a"] / compressed["b"]
