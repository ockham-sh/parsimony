"""Tests for Result and OutputSpec."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.result import (
    Column,
    ColumnRole,
    OutputSpec,
    Provenance,
    Result,
)


def _prov(**kwargs: object) -> Provenance:
    base = {"source": "test", "source_description": "test source"}
    base.update(kwargs)
    return Provenance(**base)  # type: ignore[arg-type]


def test_column_kind_alias_maps_to_role() -> None:
    c = Column.model_validate({"name": "m", "kind": "metadata"})
    assert c.role == ColumnRole.METADATA


def test_column_rejects_removed_transformation_fields() -> None:
    """dtype/mapped_name are gone; passing them must fail loudly, not silently no-op."""
    with pytest.raises(ValidationError):
        Column(name="v", dtype="numeric")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        Column(name="v", mapped_name="value")  # type: ignore[call-arg]


def test_result_data_is_untouched_by_output_spec() -> None:
    """Attaching a spec never renames, coerces, or filters the payload."""
    df = pd.DataFrame({"v": ["1", "2.5"], "extra": ["x", "y"]})
    spec = OutputSpec(columns=[Column(name="v", role=ColumnRole.DATA)])
    r = Result(data=df, output_spec=spec)
    assert r.data is df
    assert list(r.data.columns) == ["v", "extra"]
    assert r.data["v"].tolist() == ["1", "2.5"]


def test_output_spec_allows_absent_declared_columns() -> None:
    """A declared column missing from the data is not an error at result construction."""
    df = pd.DataFrame({"present": [1]})
    spec = OutputSpec(columns=[Column(name="absent", role=ColumnRole.DATA)])
    r = Result(data=df, output_spec=spec)
    assert r.is_tabular
    assert [c.name for c in r.columns] == ["absent"]


def test_output_spec_requires_data_key_or_title() -> None:
    with pytest.raises(ValidationError, match="at least one data, key, or title"):
        OutputSpec(
            columns=[
                Column(name="m", role=ColumnRole.METADATA),
            ]
        )


def test_output_spec_rejects_multiple_key_columns() -> None:
    with pytest.raises(ValidationError, match="at most one KEY"):
        OutputSpec(
            columns=[
                Column(name="a", role=ColumnRole.KEY),
                Column(name="b", role=ColumnRole.KEY),
                Column(name="c", role=ColumnRole.DATA),
            ]
        )


def test_output_spec_rejects_multiple_title_columns() -> None:
    with pytest.raises(ValidationError, match="at most one TITLE"):
        OutputSpec(
            columns=[
                Column(name="a", role=ColumnRole.TITLE),
                Column(name="b", role=ColumnRole.TITLE),
                Column(name="c", role=ColumnRole.DATA),
            ]
        )


def test_key_without_title_output_spec_valid_for_loader() -> None:
    """KEY + DATA without TITLE is valid for :func:`loader` schemas."""
    cfg = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="v", role=ColumnRole.DATA),
        ]
    )
    assert len([c for c in cfg.columns if c.role == ColumnRole.KEY]) == 1
    assert len([c for c in cfg.columns if c.role == ColumnRole.DATA]) == 1


def test_column_llm_annotation() -> None:
    assert Column(name="d", role=ColumnRole.KEY, namespace="fred").llm_annotation() == "(KEY ns:fred)"
    assert Column(name="v", role=ColumnRole.DATA).llm_annotation() == "(DATA)"
    assert Column(name="m", role=ColumnRole.METADATA).llm_annotation() == "(METADATA)"


def test_column_namespace_only_on_key_or_metadata() -> None:
    assert Column(name="m", role=ColumnRole.METADATA, namespace="currency").namespace == "currency"
    with pytest.raises(ValidationError, match="namespace is only allowed on KEY or METADATA"):
        Column(name="x", role=ColumnRole.DATA, namespace="fred")


def test_result_from_dataframe_infers_data_columns() -> None:
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    r = Result.from_dataframe(df)
    assert r.is_tabular
    assert list(r.data.columns) == ["a", "b"]
    assert r.output_spec is None
    assert r.columns == []


def test_result_from_dataframe_rejects_empty() -> None:
    with pytest.raises(ValueError, match="empty"):
        Result.from_dataframe(pd.DataFrame())


def test_provenance_field_set_is_locked() -> None:
    expected = {
        "source",
        "source_description",
        "params",
        "fetched_at",
        "properties",
    }
    assert set(Provenance.model_fields) == expected


def test_provenance_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Provenance.model_validate({"source": "fred", "source_description": "FRED", "title": "should not be here"})


def test_provenance_requires_source_and_description() -> None:
    with pytest.raises(ValidationError):
        Provenance.model_validate({})  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        Provenance.model_validate({"source": "fred"})  # type: ignore[arg-type]


def test_result_with_properties_is_cumulative() -> None:
    df = pd.DataFrame({"a": [1]})
    r = Result.from_dataframe(df)._with_properties(a=1)._with_properties(b=2)
    assert r.provenance.properties == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# to_llm — opaque Result (data: Any)
# ---------------------------------------------------------------------------


def test_preview_str_payload() -> None:
    out = Result(data="hello world " * 5).to_llm()
    assert out.startswith("Result (str):")
    assert "chars" in out
    assert "hello world" in out


def test_preview_truncates_long_string() -> None:
    out = Result(data="x" * 5000).to_llm(max_chars=100)
    assert "Result (str): 5000 chars" in out
    assert "…" in out
    assert len(out) < 300


def test_preview_dict_payload() -> None:
    out = Result(data={"name": "Alice", "items": [1, 2, 3], "meta": {"a": 1, "b": 2}}).to_llm()
    assert "Result (dict): 3 keys" in out
    assert "- name: str" in out
    assert "- items: list[3]" in out
    assert "- meta: dict[2 keys]" in out


def test_preview_nested_is_depth_limited() -> None:
    out = Result(data={"outer": {"inner": {"deep": 1}}}).to_llm()
    assert "- outer: dict[1 keys]" in out
    assert "deep" not in out


def test_preview_caps_many_keys() -> None:
    out = Result(data={f"k{i}": i for i in range(30)}).to_llm()
    assert "Result (dict): 30 keys" in out
    assert "more keys)" in out


def test_preview_list_payload() -> None:
    out = Result(data=[{"a": 1}] * 480).to_llm()
    assert "Result (list): 480 items of dict" in out


def test_preview_scalar_payload() -> None:
    assert Result(data=42).to_llm() == "Result (int): 42"
    assert Result(data=True).to_llm() == "Result (bool): True"


def test_preview_bytes_payload() -> None:
    assert Result(data=b"\x00\x01\x02\x03").to_llm() == "Result (bytes): 4 bytes"


def test_preview_pydantic_payload() -> None:
    out = Result(data=Provenance(source="s", source_description="d")).to_llm()
    assert out.startswith("Result (Provenance):")
    assert "fields" in out
    assert "- source: str" in out


# ---------------------------------------------------------------------------
# to_llm — Result (governed schema + sample)
# ---------------------------------------------------------------------------


def _preview_df_schema() -> Result:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "value": [1.0, 2.0],
            "note": ["x", "y"],
        }
    )
    cols = [
        Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
        Column(name="value", role=ColumnRole.DATA),
        Column(name="note", role=ColumnRole.METADATA),
    ]
    return Result(data=df, output_spec=OutputSpec(columns=cols))


def test_preview_shape_line() -> None:
    assert _preview_df_schema().to_llm().startswith("Result (table): 2 rows × 3 columns")


def test_preview_schema_lists_dtype_role_namespace() -> None:
    out = _preview_df_schema().to_llm()
    assert "- date: datetime64[ns] (KEY ns:fred_series)" in out
    assert "- value: float64 (DATA)" in out
    assert "- note: object (METADATA)" in out


def test_preview_without_schema_shows_dtype_only() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out = Result.from_dataframe(df).to_llm()
    assert "- a: int64" in out
    assert "(DATA)" not in out


def test_preview_omits_excluded_columns() -> None:
    df = pd.DataFrame({"internal_id": [1, 2], "value": [10.0, 20.0]})
    cols = [
        Column(name="internal_id", role=ColumnRole.KEY, exclude_from_llm_view=True),
        Column(name="value", role=ColumnRole.DATA),
    ]
    out = Result(data=df, output_spec=OutputSpec(columns=cols)).to_llm()
    assert "internal_id" not in out
    assert "- value: float64 (DATA)" in out
    assert "(1 hidden from LLM view)" in out


def test_preview_small_frame_shows_all_rows() -> None:
    out = _preview_df_schema().to_llm()
    assert "..." not in out
    assert "Rows (2):" in out


def test_preview_large_frame_first_page_no_tail() -> None:
    df = pd.DataFrame({"i": list(range(100))})
    out = Result.from_dataframe(df).to_llm(max_rows=4)
    assert "Rows (showing 4 of 100):" in out
    # Honest first page: the first rows are shown, the tail is NOT smuggled
    # in as a head/tail sample masquerading as the whole.
    assert out.rstrip().endswith("3")
    assert "99" not in out
    assert "50" not in out


def test_preview_truncates_wide_cells() -> None:
    df = pd.DataFrame({"text": ["A" * 200, "B" * 200]})
    out = Result.from_dataframe(df).to_llm()
    assert "…" in out
    assert "A" * 200 not in out


def test_preview_max_rows_param() -> None:
    df = pd.DataFrame({"i": list(range(100))})
    out = Result.from_dataframe(df).to_llm(max_rows=2)
    assert "Rows (showing 2 of 100):" in out


def test_preview_empty_frame() -> None:
    df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
    out = Result(data=df).to_llm()
    assert "0 rows × 1 columns" in out


def test_preview_all_columns_hidden() -> None:
    df = pd.DataFrame({"k": [1, 2]})
    cols = [Column(name="k", role=ColumnRole.KEY, exclude_from_llm_view=True)]
    out = Result(data=df, output_spec=OutputSpec(columns=cols)).to_llm()
    assert "(all hidden from LLM view)" in out


def test_preview_handles_integer_column_labels() -> None:
    # Default RangeIndex columns (int labels) must not crash governed_view —
    # selecting by str(name) would KeyError on the real int key.
    out = Result(data=pd.DataFrame([[1, 2], [3, 4]])).to_llm()
    assert "2 rows × 2 columns" in out
    assert "- 0: int64" in out and "- 1: int64" in out
    assert "1,2" in out and "3,4" in out


def test_preview_handles_duplicate_column_names() -> None:
    # Duplicate column names (common in SQL joins) must not crash: frame[name]
    # would return a DataFrame and .dtype would raise.
    df = pd.DataFrame([[1, 2, 3]], columns=["a", "a", "b"])
    out = Result(data=df).to_llm()
    assert "1 rows × 3 columns" in out
    assert out.count("- a:") == 2
    assert "1,2,3" in out


def test_governance_hides_all_duplicates_of_a_hidden_name() -> None:
    # Two declared columns share the name "x": one hidden, one visible. The
    # spec is a verbatim annotation with no positional alignment to the frame,
    # so the ambiguity is real — governance errs on the safe side and hides
    # every frame column with that name rather than risk leaking the
    # sensitive sibling.
    df = pd.DataFrame([[1, 10, 20]], columns=["id", "x", "x"])
    cols = [
        Column(name="id", role=ColumnRole.DATA),
        Column(name="x", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="x", role=ColumnRole.METADATA),
    ]
    out = Result(data=df, output_spec=OutputSpec(columns=cols)).to_llm()
    assert "2 hidden from LLM view" in out
    assert "- x:" not in out
    assert "10" not in out and "20" not in out


def test_governance_pairs_by_name_regardless_of_column_order() -> None:
    # Same column count as the spec but a different order: annotations and
    # hiding must follow names, never positions.
    df = pd.DataFrame({"units": ["secret"], "series_id": ["A"], "value": [1.0]})
    cols = [
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="value", role=ColumnRole.DATA),
        Column(name="units", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
    out = Result(data=df, output_spec=OutputSpec(columns=cols)).to_llm()
    assert "secret" not in out and "units" not in out
    assert "- series_id: object (KEY ns:fred)" in out
    assert "- value: float64 (DATA)" in out


def test_governance_absent_hidden_declaration_hides_nothing_real() -> None:
    # A declared-but-absent hidden column must not swallow a real returned
    # column that happens to occupy its position.
    df = pd.DataFrame({"ts": ["2024-01-01"], "price": ["n/a"], "undeclared": [1]})
    cols = [
        Column(name="ts", role=ColumnRole.DATA),
        Column(name="price", role=ColumnRole.DATA),
        Column(name="absent", role=ColumnRole.KEY, exclude_from_llm_view=True),
    ]
    out = Result(data=df, output_spec=OutputSpec(columns=cols)).to_llm()
    assert "hidden from LLM view" not in out
    assert "- undeclared: int64" in out
