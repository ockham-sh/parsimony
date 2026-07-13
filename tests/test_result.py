"""Tests for Result, OutputSpec, and the entity projection."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.entity import Entity
from parsimony.result import (
    Column,
    ColumnRole,
    EntityResult,
    OutputSpec,
    Provenance,
    Result,
)


def _prov(**kwargs: object) -> Provenance:
    base = {"source": "test", "source_description": "test source"}
    base.update(kwargs)
    return Provenance(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Column declaration invariants
# ---------------------------------------------------------------------------


def test_column_has_no_dtype_or_mapped_name_fields() -> None:
    c = Column(name="x")
    assert not hasattr(c, "dtype")
    assert not hasattr(c, "mapped_name")


def test_column_namespace_only_allowed_on_key() -> None:
    assert Column(name="k", role=ColumnRole.KEY, namespace="fred").namespace == "fred"
    with pytest.raises(ValidationError, match="namespace is only allowed on KEY"):
        Column(name="m", role=ColumnRole.METADATA, namespace="currency")
    with pytest.raises(ValidationError, match="namespace is only allowed on KEY"):
        Column(name="v", role=ColumnRole.DATA, namespace="fred")


def test_column_namespace_must_be_non_empty_when_set() -> None:
    with pytest.raises(ValidationError, match="non-empty"):
        Column(name="k", role=ColumnRole.KEY, namespace="   ")


def test_column_exclude_from_llm_view_not_allowed_for_data_or_title() -> None:
    with pytest.raises(ValidationError, match="not allowed for data"):
        Column(name="v", role=ColumnRole.DATA, exclude_from_llm_view=True)
    with pytest.raises(ValidationError, match="not allowed for title"):
        Column(name="t", role=ColumnRole.TITLE, exclude_from_llm_view=True)


def test_column_llm_annotation() -> None:
    assert Column(name="d", role=ColumnRole.KEY, namespace="fred").llm_annotation() == "(KEY ns:fred)"
    assert Column(name="v", role=ColumnRole.DATA).llm_annotation() == "(DATA)"
    assert Column(name="m", role=ColumnRole.METADATA).llm_annotation() == "(METADATA)"


# ---------------------------------------------------------------------------
# OutputSpec declaration invariants
# ---------------------------------------------------------------------------


def test_output_spec_is_a_pure_ordered_declaration() -> None:
    spec = OutputSpec(columns=[Column(name="a"), Column(name="b")])
    assert not hasattr(spec, "build_table_result")
    assert not hasattr(spec, "build_entities")
    assert not hasattr(spec, "validate_columns")


def test_output_spec_rejects_duplicate_names() -> None:
    with pytest.raises(ValidationError, match="unique"):
        OutputSpec(columns=[Column(name="a"), Column(name="a")])


def test_output_spec_rejects_multiple_key_columns() -> None:
    with pytest.raises(ValidationError, match="at most one KEY"):
        OutputSpec(
            columns=[
                Column(name="a", role=ColumnRole.KEY, namespace="ns"),
                Column(name="b", role=ColumnRole.KEY, namespace="ns"),
            ]
        )


def test_output_spec_rejects_multiple_title_columns() -> None:
    with pytest.raises(ValidationError, match="at most one TITLE"):
        OutputSpec(
            columns=[
                Column(name="a", role=ColumnRole.TITLE),
                Column(name="b", role=ColumnRole.TITLE),
            ]
        )


def test_output_spec_allows_key_without_namespace() -> None:
    """A KEY column may omit namespace at declaration time (e.g. a per-call

    dynamic namespace resolved later). Namespace is only required when a
    projection is actually requested — see
    ``test_entities_requires_key_namespace_for_projection``.
    """
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY)])
    assert spec.columns[0].namespace is None


def test_entities_requires_key_namespace_for_projection() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY), Column(name="v", role=ColumnRole.DATA)])
    df = pd.DataFrame({"k": ["a"], "v": [1]})
    r = Result(data=df, output_spec=spec)
    with pytest.raises(ValueError, match="must declare namespace"):
        _ = r.entities


def test_output_spec_rejects_multiple_wildcards() -> None:
    with pytest.raises(ValidationError, match="one '\\*' wildcard"):
        OutputSpec(
            columns=[
                Column(name="*", role=ColumnRole.DATA),
                Column(name="*", role=ColumnRole.METADATA),
            ]
        )


def test_output_spec_wildcard_cannot_be_key_or_title() -> None:
    with pytest.raises(ValidationError, match="DATA or METADATA"):
        OutputSpec(columns=[Column(name="*", role=ColumnRole.KEY, namespace="ns")])
    with pytest.raises(ValidationError, match="DATA or METADATA"):
        OutputSpec(columns=[Column(name="*", role=ColumnRole.TITLE)])


# ---------------------------------------------------------------------------
# Connector execution contract: data attached unchanged
# ---------------------------------------------------------------------------


def test_result_data_is_the_exact_raw_object() -> None:
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    r = Result(data=df, provenance=_prov())
    assert r.data is df


def test_result_has_no_transforming_methods() -> None:
    r = Result(data=pd.DataFrame({"a": [1]}))
    assert not hasattr(r, "to_table")
    assert not hasattr(r, "from_dataframe")


def test_result_is_tabular_only_for_dataframe() -> None:
    assert Result(data=pd.DataFrame({"a": [1]})).is_tabular
    assert not Result(data=pd.Series([1, 2])).is_tabular
    assert not Result(data={"a": 1}).is_tabular


def test_result_frame_raises_when_not_tabular() -> None:
    with pytest.raises(TypeError, match="not tabular"):
        _ = Result(data={"a": 1}).frame


# ---------------------------------------------------------------------------
# Entity projection
# ---------------------------------------------------------------------------


def _series_output() -> OutputSpec:
    return OutputSpec(
        columns=[
            Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="units", role=ColumnRole.METADATA),
            Column(name="date", role=ColumnRole.DATA),
            Column(name="value", role=ColumnRole.DATA),
        ]
    )


def test_entities_tuple_lookup_and_shared_provenance() -> None:
    df = pd.DataFrame(
        {
            "series_id": ["UNRATE", "UNRATE", "GDP"],
            "title": ["Unemployment Rate", "Unemployment Rate", "Gross Domestic Product"],
            "units": ["Percent", "Percent", "Billions"],
            "date": ["2020-01-01", "2020-02-01", "2020-01-01"],
            "value": [3.5, 3.6, 21000.0],
        }
    )
    prov = _prov()
    r = Result(data=df, provenance=prov, output_spec=_series_output())
    entities = r.entities
    unrate = entities["fred", "UNRATE"]
    assert isinstance(unrate, EntityResult)
    assert unrate.namespace == "fred"
    assert unrate.code == "UNRATE"
    assert unrate.title == "Unemployment Rate"
    assert unrate.metadata == {"units": "Percent"}
    assert list(unrate.data.columns) == ["date", "value"]
    assert len(unrate.data) == 2
    assert unrate.provenance is prov


def test_entities_preserve_first_appearance_order() -> None:
    df = pd.DataFrame(
        {
            "series_id": ["GDP", "UNRATE", "GDP"],
            "title": ["GDP", "Unemployment Rate", "GDP"],
            "units": [None, None, None],
            "date": ["1", "2", "3"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    r = Result(data=df, output_spec=_series_output())
    assert list(r.entities.keys()) == [("fred", "GDP"), ("fred", "UNRATE")]


def test_entities_normalizes_colliding_codes() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    df = pd.DataFrame({"k": [" a ", "a"]})
    r = Result(data=df, output_spec=spec)
    assert list(r.entities.keys()) == [("ns", "a")]


def test_entities_to_entities_is_lossy_identity_projection() -> None:
    df = pd.DataFrame(
        {
            "series_id": ["UNRATE"],
            "title": ["Unemployment Rate"],
            "units": ["Percent"],
            "date": ["2020-01-01"],
            "value": [3.5],
        }
    )
    r = Result(data=df, output_spec=_series_output())
    entries = r.to_entities()
    expected = Entity(namespace="fred", code="UNRATE", title="Unemployment Rate", metadata={"units": "Percent"})
    assert entries == [expected]


def test_entities_per_row_namespace_convention() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="__row__"),
            Column(name="entity_namespace", role=ColumnRole.METADATA),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    df = pd.DataFrame(
        {
            "code": ["A", "B"],
            "entity_namespace": ["ns1", "ns2"],
            "title": ["Alpha", "Beta"],
        }
    )
    r = Result(data=df, output_spec=spec)
    assert set(r.entities.keys()) == {("ns1", "A"), ("ns2", "B")}
    # entity_namespace is consumed for identity, not stored as metadata.
    assert r.entities["ns1", "A"].metadata == {}


def test_entities_row_namespace_requires_entity_namespace_column() -> None:
    spec = OutputSpec(columns=[Column(name="code", role=ColumnRole.KEY, namespace="__row__")])
    r = Result(data=pd.DataFrame({"code": ["A"]}), output_spec=spec)
    with pytest.raises(ValueError, match="entity_namespace"):
        _ = r.entities


def test_entities_wildcard_data_captures_unclaimed_columns() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="*", role=ColumnRole.DATA),
        ]
    )
    df = pd.DataFrame({"k": ["a", "a"], "x": [1, 2], "y": [3, 4]})
    r = Result(data=df, output_spec=spec)
    assert list(r.entities["ns", "a"].data.columns) == ["x", "y"]


def test_entities_wildcard_metadata_captures_unclaimed_columns() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="*", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame({"k": ["a"], "extra": ["hi"]})
    r = Result(data=df, output_spec=spec)
    assert r.entities["ns", "a"].metadata == {"extra": "hi"}
    assert list(r.entities["ns", "a"].data.columns) == []


def test_entities_undeclared_columns_ignored_without_wildcard() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    df = pd.DataFrame({"k": ["a"], "untouched": [1]})
    r = Result(data=df, output_spec=spec)
    assert list(r.entities["ns", "a"].data.columns) == []
    assert "untouched" in r.data.columns  # untouched in the raw payload


def test_entities_requires_output_spec() -> None:
    r = Result(data=pd.DataFrame({"a": [1]}))
    with pytest.raises(ValueError, match="OutputSpec"):
        _ = r.entities


def test_entities_requires_tabular_data() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    r = Result(data=pd.Series([1, 2]), output_spec=spec)
    with pytest.raises(TypeError, match="tabular"):
        _ = r.entities


def test_entities_requires_exactly_one_key_column() -> None:
    spec = OutputSpec(columns=[Column(name="v", role=ColumnRole.DATA)])
    r = Result(data=pd.DataFrame({"v": [1]}), output_spec=spec)
    with pytest.raises(ValueError, match="exactly one KEY"):
        _ = r.entities


def test_entities_raises_on_missing_declared_columns() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="missing", role=ColumnRole.DATA),
        ]
    )
    r = Result(data=pd.DataFrame({"k": ["a"]}), output_spec=spec)
    with pytest.raises(ValueError, match="missing"):
        _ = r.entities


def test_entities_raises_on_duplicate_dataframe_labels() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    df = pd.DataFrame([[1, 2]], columns=["k", "k"])
    r = Result(data=df, output_spec=spec)
    with pytest.raises(ValueError, match="duplicate labels"):
        _ = r.entities


def test_entities_raises_on_null_key() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    r = Result(data=pd.DataFrame({"k": ["a", None]}), output_spec=spec)
    with pytest.raises(ValueError, match="null values"):
        _ = r.entities


def test_entities_raises_on_conflicting_title() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    df = pd.DataFrame({"k": ["a", "a"], "title": ["X", "Y"]})
    r = Result(data=df, output_spec=spec)
    with pytest.raises(ValueError, match="conflicting values"):
        _ = r.entities


def test_entities_raises_on_conflicting_metadata() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="m", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame({"k": ["a", "a"], "m": ["X", "Y"]})
    r = Result(data=df, output_spec=spec)
    with pytest.raises(ValueError, match="conflicting values"):
        _ = r.entities


def test_entities_accepts_null_plus_one_distinct_metadata_value() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="m", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame({"k": ["a", "a"], "m": ["X", None]})
    r = Result(data=df, output_spec=spec)
    assert r.entities["ns", "a"].metadata == {"m": "X"}


def test_entities_all_null_metadata_is_omitted() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="m", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame({"k": ["a"], "m": [None]})
    r = Result(data=df, output_spec=spec)
    assert r.entities["ns", "a"].metadata == {}


def test_entities_missing_title_falls_back_to_code() -> None:
    spec = OutputSpec(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    df = pd.DataFrame({"k": ["a"], "title": [None]})
    r = Result(data=df, output_spec=spec)
    assert r.entities["ns", "a"].title == "a"


def test_entities_empty_frame_with_declared_columns_returns_empty_mapping() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    df = pd.DataFrame({"k": pd.Series([], dtype=object)})
    r = Result(data=df, output_spec=spec)
    assert dict(r.entities) == {}


def test_entities_empty_frame_missing_declared_column_still_raises() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    r = Result(data=pd.DataFrame(), output_spec=spec)
    with pytest.raises(ValueError, match="missing declared columns"):
        _ = r.entities


def test_entities_mapping_is_read_only() -> None:
    spec = OutputSpec(columns=[Column(name="k", role=ColumnRole.KEY, namespace="ns")])
    r = Result(data=pd.DataFrame({"k": ["a"]}), output_spec=spec)
    with pytest.raises(TypeError):
        r.entities["ns", "a"] = None  # type: ignore[index]


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------


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
    out = Result(data=df).to_llm()
    assert "- a: int64" in out
    assert "(DATA)" not in out


def test_preview_omits_excluded_columns() -> None:
    df = pd.DataFrame({"internal_id": [1, 2], "value": [10.0, 20.0]})
    cols = [
        Column(name="internal_id", role=ColumnRole.KEY, namespace="ns", exclude_from_llm_view=True),
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
    out = Result(data=df).to_llm(max_rows=4)
    assert "Rows (showing 4 of 100):" in out
    # Honest first page: the first rows are shown, the tail is NOT smuggled
    # in as a head/tail sample masquerading as the whole.
    assert out.rstrip().endswith("3")
    assert "99" not in out
    assert "50" not in out


def test_preview_truncates_wide_cells() -> None:
    df = pd.DataFrame({"text": ["A" * 200, "B" * 200]})
    out = Result(data=df).to_llm()
    assert "…" in out
    assert "A" * 200 not in out


def test_preview_max_rows_param() -> None:
    df = pd.DataFrame({"i": list(range(100))})
    out = Result(data=df).to_llm(max_rows=2)
    assert "Rows (showing 2 of 100):" in out


def test_preview_empty_frame() -> None:
    df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
    out = Result(data=df).to_llm()
    assert "0 rows × 1 columns" in out


def test_preview_all_columns_hidden() -> None:
    df = pd.DataFrame({"k": [1, 2]})
    cols = [Column(name="k", role=ColumnRole.KEY, namespace="ns", exclude_from_llm_view=True)]
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
    # would return a DataFrame and .dtype would raise. With no schema (or a
    # schema whose length doesn't match the frame), governed_view falls back
    # to unannotated name-based display rather than crashing.
    df = pd.DataFrame([[1, 2, 3]], columns=["a", "a", "b"])
    out = Result(data=df).to_llm()
    assert "1 rows × 3 columns" in out
    assert out.count("- a:") == 2
    assert "1,2,3" in out


def test_governance_pairs_schema_to_frame_by_position() -> None:
    # A schema covering the frame one-to-one is paired by position, not by
    # name — so annotations land on the right column even when a later
    # positional column happens to share an earlier one's dtype/shape.
    df = pd.DataFrame([[1, 10, 20]], columns=["id", "hidden", "visible"])
    cols = [
        Column(name="id", role=ColumnRole.DATA),
        Column(name="hidden", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="visible", role=ColumnRole.DATA),
    ]
    out = Result(data=df, output_spec=OutputSpec(columns=cols)).to_llm()
    assert "1 hidden from LLM view" in out
    assert "- visible:" in out
    assert "- hidden:" not in out
    assert "20" in out  # visible value kept
    assert "10" not in out  # hidden value suppressed
