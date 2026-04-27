"""Tokenizer + fusion tests for BM25/FAISS retrieval primitives."""

from __future__ import annotations

from parsimony.indexes import rrf_fuse, tokenize


def test_tokenize_splits_on_whitespace() -> None:
    assert tokenize("Total Public Debt Outstanding") == ["total", "public", "debt", "outstanding"]


def test_tokenize_splits_on_underscores_and_slashes_and_hash() -> None:
    assert tokenize("v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt") == [
        "v2",
        "accounting",
        "od",
        "debt",
        "to",
        "penny",
        "tot",
        "pub",
        "debt",
        "out",
        "amt",
    ]


def test_tokenize_lowercases() -> None:
    assert tokenize("CURRENCY0") == ["currency0"]


def test_tokenize_drops_punctuation_and_empty_runs() -> None:
    assert tokenize("Rate (%) — FRN Daily") == ["rate", "frn", "daily"]


def test_tokenize_handles_empty() -> None:
    assert tokenize("") == []


def test_rrf_fuse_prefers_items_in_both_rankings() -> None:
    bm25 = [(0, 0), (1, 1), (2, 2)]
    dense = [(2, 0), (0, 1), (3, 2)]
    fused = dict(rrf_fuse(bm25, dense))
    assert fused[0] > fused[1]  # idx 0 appeared high in both lists
    assert fused[2] > fused[3]  # idx 2 appeared in both; idx 3 only in one
    assert fused[0] > fused[3]


def test_rrf_fuse_single_ranking_is_monotone() -> None:
    bm25 = [(10, 0), (20, 1), (30, 2)]
    fused = rrf_fuse(bm25)
    assert [idx for idx, _ in fused] == [10, 20, 30]
