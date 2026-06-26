"""Tests for the ad-hoc runtime catalog: ``auto_catalog``."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from parsimony import auto_catalog
from parsimony.catalog import BM25Index, Catalog


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"country": "Spain", "metric": "unemployment rate", "value": 12.3},
            {"country": "France", "metric": "unemployment rate", "value": 7.1},
            {"country": "Spain", "metric": "inflation", "value": 3.4},
        ]
    )


def test_returns_a_built_catalog() -> None:
    cat = auto_catalog(_frame())
    assert isinstance(cat, Catalog)
    assert len(cat) == 3
    # Already built: search works with no build() call and returns a plain list.
    matches = cat.search("unemployment", limit=10)
    assert isinstance(matches, list)


def test_default_policy_is_bm25_only() -> None:
    cat = auto_catalog(_frame())
    assert cat.indexes  # not empty
    assert all(isinstance(idx, BM25Index) for idx in cat.indexes.values())
    # Default policy covers code, title, and every column.
    assert {"code", "title", "country", "metric", "value"} <= set(cat.indexes)


def test_broad_search_scores_row_text() -> None:
    cat = auto_catalog(_frame())
    matches = cat.search("unemployment", limit=10)
    codes = {m.code for m in matches}
    assert codes == {"0", "1"}  # the two unemployment rows


def test_structured_column_value_search() -> None:
    cat = auto_catalog(_frame())
    matches = cat.search("country: spain", limit=10)
    assert {m.code for m in matches} == {"0", "2"}  # both Spain rows


def test_code_is_row_position_recovers_row() -> None:
    df = _frame()
    cat = auto_catalog(df)
    match = cat.search("inflation", limit=1)[0]
    assert match.code == "2"
    recovered = df.iloc[int(match.code)]
    assert recovered["metric"] == "inflation"
    assert recovered["country"] == "Spain"


def test_nulls_excluded_from_metadata_and_title() -> None:
    df = pd.DataFrame([{"a": "alpha", "b": np.nan}, {"a": None, "b": "beta"}])
    cat = auto_catalog(df)
    by_code = {e.code: e for e in cat.entities}
    assert "b" not in by_code["0"].metadata  # NaN dropped
    assert by_code["0"].title == "alpha"  # NaN not joined into title
    assert "a" not in by_code["1"].metadata  # None dropped
    assert by_code["1"].title == "beta"


def test_numpy_scalars_coerced_to_json_safe_metadata() -> None:
    df = _frame()
    cat = auto_catalog(df)
    value = cat.entities[0].metadata["value"]
    # Stored as a native float, not a numpy scalar — JSON-safe for snapshotting.
    assert isinstance(value, float)
    assert not isinstance(value, np.generic)


def test_reserved_columns_stored_in_metadata() -> None:
    # A column literally named "code" is kept in metadata; the structured "code:"
    # query still resolves to the Entity's own code (the row position).
    df = pd.DataFrame([{"code": "XYZ", "label": "alpha"}, {"code": "ABC", "label": "beta"}])
    cat = auto_catalog(df)
    assert cat.entities[0].metadata["code"] == "XYZ"
    assert cat.entities[0].code == "0"  # row position, not "XYZ"


def test_duplicate_columns_rejected() -> None:
    df = pd.DataFrame([[1, 2]], columns=["x", "x"])
    with pytest.raises(ValueError, match="unique column names"):
        auto_catalog(df)


def test_empty_frame_is_searchable_and_empty() -> None:
    cat = auto_catalog(pd.DataFrame())
    assert len(cat) == 0
    assert cat.search("anything", limit=5) == []


def test_custom_name_sets_namespace() -> None:
    cat = auto_catalog(_frame(), name="gdp_data")
    assert cat.name == "gdp_data"
    assert all(e.namespace == "gdp_data" for e in cat.entities)


def test_invalid_name_raises_like_catalog() -> None:
    # The name is the catalog namespace; Catalog's own snake_case contract applies.
    with pytest.raises(ValueError, match="snake_case"):
        auto_catalog(_frame(), name="My Frame")
