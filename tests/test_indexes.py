"""Tokenizer tests for catalog indexing primitives."""

from __future__ import annotations

from parsimony.indexes import tokenize


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
