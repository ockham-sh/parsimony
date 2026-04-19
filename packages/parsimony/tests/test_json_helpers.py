"""Tests for parsimony.transport.json_helpers module."""

from __future__ import annotations

import json

import pytest

from parsimony.transport.json_helpers import (
    _is_date_keyed_dict,
    _is_indexed_dict,
    interpolate_path,
    json_to_df,
)

# ---------------------------------------------------------------------------
# _is_indexed_dict
# ---------------------------------------------------------------------------


class TestIsIndexedDict:
    def test_empty_dict_returns_false(self) -> None:
        assert _is_indexed_dict({}) is False

    def test_all_digit_keys(self) -> None:
        assert _is_indexed_dict({"0": "a", "1": "b", "2": "c"}) is True

    def test_non_digit_key(self) -> None:
        assert _is_indexed_dict({"0": "a", "foo": "b"}) is False

    def test_mixed_digit_and_non_digit(self) -> None:
        assert _is_indexed_dict({"1": "x", "two": "y"}) is False

    def test_non_string_key(self) -> None:
        # dict with int keys — isinstance(k, str) fails
        assert _is_indexed_dict({0: "a", 1: "b"}) is False

    def test_single_digit_key(self) -> None:
        assert _is_indexed_dict({"42": "val"}) is True


# ---------------------------------------------------------------------------
# _is_date_keyed_dict
# ---------------------------------------------------------------------------


class TestIsDateKeyedDict:
    def test_empty_dict_returns_false(self) -> None:
        assert _is_date_keyed_dict({}) is False

    def test_valid_date_keys(self) -> None:
        assert _is_date_keyed_dict({"2024-01-01": 1, "2024-06-15": 2}) is True

    def test_invalid_date_key(self) -> None:
        assert _is_date_keyed_dict({"not-a-date": 1}) is False

    def test_mixed_valid_and_invalid(self) -> None:
        assert _is_date_keyed_dict({"2024-01-01": 1, "nope": 2}) is False

    def test_non_string_key(self) -> None:
        assert _is_date_keyed_dict({20240101: 1}) is False

    def test_various_date_formats(self) -> None:
        # pandas to_datetime accepts many formats
        assert _is_date_keyed_dict({"Jan 1, 2024": 1, "2024/06/15": 2}) is True


# ---------------------------------------------------------------------------
# json_to_df — list inputs
# ---------------------------------------------------------------------------


class TestJsonToDfList:
    def test_list_of_dicts(self) -> None:
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        df = json_to_df(data)
        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2
        assert df["a"].tolist() == [1, 3]

    def test_list_of_dicts_with_nested_value(self) -> None:
        """Nested dict/list in a list-of-dicts row should be serialized to JSON string."""
        data = [{"a": 1, "nested": {"x": 10}}, {"a": 2, "nested": [1, 2]}]
        df = json_to_df(data)
        assert df["nested"].iloc[0] == json.dumps({"x": 10}, sort_keys=True, ensure_ascii=True)
        assert df["nested"].iloc[1] == json.dumps([1, 2], sort_keys=True, ensure_ascii=True)

    def test_simple_list_of_scalars(self) -> None:
        data = [10, 20, 30]
        df = json_to_df(data)
        assert list(df.columns) == ["value"]
        assert df["value"].tolist() == [10, 20, 30]

    def test_simple_list_with_nested(self) -> None:
        """Non-dict items in a list go through _to_scalar; nested structures become JSON."""
        data = [1, {"key": "val"}, [5, 6]]
        df = json_to_df(data)
        assert list(df.columns) == ["value"]
        assert df["value"].iloc[0] == 1
        assert df["value"].iloc[1] == json.dumps({"key": "val"}, sort_keys=True, ensure_ascii=True)

    def test_empty_list(self) -> None:
        df = json_to_df([])
        assert list(df.columns) == ["value"]
        assert len(df) == 0

    def test_list_with_one_non_dict(self) -> None:
        """If not ALL items are dicts, fall through to scalar branch."""
        data = [{"a": 1}, "string"]
        df = json_to_df(data)
        assert list(df.columns) == ["value"]


# ---------------------------------------------------------------------------
# json_to_df — single-key dict (unwrapping)
# ---------------------------------------------------------------------------


class TestJsonToDfSingleKeyDict:
    def test_single_key_dict_unwraps(self) -> None:
        data = {"results": [{"x": 1}, {"x": 2}]}
        df = json_to_df(data)
        assert list(df.columns) == ["x"]
        assert len(df) == 2

    def test_single_key_dict_preserves_prefix(self) -> None:
        """When prefix is empty, the single key becomes the new prefix."""
        data = {"wrapper": {"inner": [{"a": 1}]}}
        # First unwrap: key="wrapper", prefix="" -> recursive with prefix="wrapper"
        # Second unwrap: key="inner", prefix="wrapper" -> keeps "wrapper"
        df = json_to_df(data)
        assert list(df.columns) == ["a"]

    def test_single_key_dict_with_explicit_prefix(self) -> None:
        data = {"key": [1, 2, 3]}
        df = json_to_df(data, prefix="my_prefix")
        assert list(df.columns) == ["value"]
        assert df["value"].tolist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# json_to_df — indexed dict (digit keys)
# ---------------------------------------------------------------------------


class TestJsonToDfIndexedDict:
    def test_indexed_dict_of_dicts(self) -> None:
        data = {"0": {"a": 10}, "1": {"a": 20}, "2": {"a": 30}}
        # single-key check fails (len > 1), indexed check succeeds
        # but wait, len(data) == 3 so it won't unwrap
        df = json_to_df(data)
        assert list(df.columns) == ["a"]
        assert df["a"].tolist() == [10, 20, 30]

    def test_indexed_dict_of_scalars(self) -> None:
        data = {"1": "hello", "0": "world"}
        df = json_to_df(data)
        assert list(df.columns) == ["value"]
        # sorted by int key: "0" -> "world", "1" -> "hello"
        assert df["value"].tolist() == ["world", "hello"]

    def test_indexed_dict_of_mixed(self) -> None:
        """When not all values are dicts, falls to scalar branch."""
        data = {"0": "scalar", "1": {"nested": True}}
        df = json_to_df(data)
        assert list(df.columns) == ["value"]
        assert df["value"].iloc[0] == "scalar"


# ---------------------------------------------------------------------------
# json_to_df — date-keyed dict
# ---------------------------------------------------------------------------


class TestJsonToDfDateKeyedDict:
    def test_date_keyed_dict_with_dict_values(self) -> None:
        data = {"2024-01-01": {"open": 100, "close": 105}, "2024-01-02": {"open": 106, "close": 110}}
        df = json_to_df(data)
        assert "date" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns
        assert len(df) == 2
        # sorted by key
        assert df["date"].tolist() == ["2024-01-01", "2024-01-02"]

    def test_date_keyed_dict_with_scalar_values(self) -> None:
        data = {"2024-01-01": 42, "2024-01-02": 99}
        df = json_to_df(data)
        assert "date" in df.columns
        assert "value" in df.columns
        assert df["value"].tolist() == [42, 99]

    def test_date_keyed_dict_with_nested_values(self) -> None:
        """Nested list/dict values in date-keyed rows get serialized via _sanitize."""
        data = {"2024-01-01": {"info": [1, 2, 3]}, "2024-01-02": {"info": [4, 5]}}
        df = json_to_df(data)
        assert "date" in df.columns
        assert df["info"].iloc[0] == json.dumps([1, 2, 3], sort_keys=True, ensure_ascii=True)


# ---------------------------------------------------------------------------
# json_to_df — generic dict with nested structures (TableRef)
# ---------------------------------------------------------------------------


class TestJsonToDfNestedDict:
    def test_dict_with_nested_dict_creates_tableref(self) -> None:
        data = {"name": "test", "details": {"x": 1, "y": 2}}
        df = json_to_df(data)
        assert len(df) == 1
        assert df["name"].iloc[0] == "test"
        ref = df["details"].iloc[0]
        assert ref.startswith("TableRef(details,")

    def test_dict_with_nested_list_creates_tableref(self) -> None:
        data = {"name": "test", "items": [{"id": 1}, {"id": 2}]}
        df = json_to_df(data)
        ref = df["items"].iloc[0]
        assert ref.startswith("TableRef(items,")

    def test_dict_with_prefix_creates_prefixed_tableref(self) -> None:
        data = {"scalar": 1, "child": {"a": 10}}
        df = json_to_df(data, prefix="root")
        ref = df["child"].iloc[0]
        assert ref.startswith("TableRef(root::child,")

    def test_dict_with_only_scalar_values(self) -> None:
        data = {"a": 1, "b": "two", "c": 3.0}
        df = json_to_df(data)
        assert len(df) == 1
        assert df["a"].iloc[0] == 1
        assert df["b"].iloc[0] == "two"


# ---------------------------------------------------------------------------
# json_to_df — scalar input
# ---------------------------------------------------------------------------


class TestJsonToDfScalar:
    def test_string_scalar(self) -> None:
        df = json_to_df("hello")
        assert list(df.columns) == ["value"]
        assert df["value"].iloc[0] == "hello"

    def test_int_scalar(self) -> None:
        df = json_to_df(42)
        assert df["value"].iloc[0] == 42

    def test_none_scalar(self) -> None:
        df = json_to_df(None)
        assert df["value"].iloc[0] is None

    def test_float_scalar(self) -> None:
        df = json_to_df(3.14)
        assert df["value"].iloc[0] == pytest.approx(3.14)

    def test_bool_scalar(self) -> None:
        df = json_to_df(True)
        assert df["value"].iloc[0] == True  # noqa: E712


# ---------------------------------------------------------------------------
# interpolate_path
# ---------------------------------------------------------------------------


class TestInterpolatePath:
    def test_no_placeholders(self) -> None:
        path = "/api/v1/data"
        rendered, remaining = interpolate_path(path, {"key": "val"})
        assert rendered == "/api/v1/data"
        assert remaining == {"key": "val"}

    def test_single_placeholder(self) -> None:
        path = "/api/v1/{symbol}/quote"
        rendered, remaining = interpolate_path(path, {"symbol": "AAPL", "limit": 10})
        assert rendered == "/api/v1/AAPL/quote"
        assert remaining == {"limit": 10}

    def test_multiple_placeholders(self) -> None:
        path = "/api/{version}/{resource}"
        rendered, remaining = interpolate_path(path, {"version": "v2", "resource": "users", "page": 1})
        assert rendered == "/api/v2/users"
        assert remaining == {"page": 1}

    def test_none_value_replaced_with_empty_string(self) -> None:
        path = "/api/{optional}/data"
        rendered, remaining = interpolate_path(path, {"optional": None})
        assert rendered == "/api//data"
        assert remaining == {}

    def test_all_params_used(self) -> None:
        path = "/{a}/{b}"
        rendered, remaining = interpolate_path(path, {"a": "x", "b": "y"})
        assert rendered == "/x/y"
        assert remaining == {}

    def test_no_params(self) -> None:
        path = "/static/path"
        rendered, remaining = interpolate_path(path, {})
        assert rendered == "/static/path"
        assert remaining == {}

    def test_numeric_param_value(self) -> None:
        path = "/api/{id}/detail"
        rendered, remaining = interpolate_path(path, {"id": 123, "extra": "foo"})
        assert rendered == "/api/123/detail"
        assert remaining == {"extra": "foo"}
