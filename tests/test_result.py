"""Tests for Result and OutputConfig."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    TabularResult,
)


def _prov(**kwargs: object) -> Provenance:
    base = {"source": "test", "source_description": "test source"}
    base.update(kwargs)
    return Provenance(**base)  # type: ignore[arg-type]


def test_build_table_result_rename_and_dtypes() -> None:
    raw = pd.DataFrame(
        {
            "d": ["2020-01-01", "2021-06-15"],
            "v": ["1", "2.5"],
            "meta": ["x", "y"],
        }
    )
    cfg = OutputConfig(
        columns=[
            Column(name="d", dtype="datetime", role=ColumnRole.DATA),
            Column(name="v", dtype="numeric", role=ColumnRole.DATA, mapped_name="value"),
            Column(name="meta", role=ColumnRole.METADATA),
        ]
    )
    r = cfg.build_table_result(raw)
    assert isinstance(r, TabularResult)
    assert r.output_schema is not None
    assert list(r.data.columns) == ["d", "value", "meta"]
    assert r.provenance.properties.get("metadata") is None
    assert len(r.metadata_columns) == 1
    assert r.metadata_columns[0].name == "meta"
    assert r.metadata_columns[0].role == ColumnRole.METADATA


def test_build_table_result_wildcard() -> None:
    raw = pd.DataFrame({"a": [1], "b": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="*", dtype="numeric", role=ColumnRole.DATA),
        ]
    )
    r = cfg.build_table_result(raw)
    assert set(r.data.columns) == {"a", "b"}


def test_column_kind_alias_maps_to_role() -> None:
    c = Column.model_validate({"name": "m", "kind": "metadata"})
    assert c.role == ColumnRole.METADATA


def test_entity_keys() -> None:
    df = pd.DataFrame({"sym": ["A", "B"], "title": ["Alpha", "Beta"], "v": [1, 2]})
    cols = [
        Column(name="sym", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="v", role=ColumnRole.DATA),
    ]
    r = TabularResult(data=df, output_schema=OutputConfig(columns=cols))
    assert list(r.entity_keys.columns) == ["sym"]


def test_build_table_result_rejects_empty_frame() -> None:
    cfg = OutputConfig(columns=[Column(name="x", role=ColumnRole.DATA)])
    with pytest.raises(ValueError, match="empty"):
        cfg.build_table_result(pd.DataFrame())


def test_output_config_requires_data_key_or_title() -> None:
    with pytest.raises(ValidationError, match="at least one data, key, or title"):
        OutputConfig(
            columns=[
                Column(name="m", role=ColumnRole.METADATA),
            ]
        )


def test_output_config_rejects_multiple_key_columns() -> None:
    with pytest.raises(ValidationError, match="at most one KEY"):
        OutputConfig(
            columns=[
                Column(name="a", role=ColumnRole.KEY),
                Column(name="b", role=ColumnRole.KEY),
                Column(name="c", role=ColumnRole.DATA),
            ]
        )


def test_output_config_rejects_multiple_title_columns() -> None:
    with pytest.raises(ValidationError, match="at most one TITLE"):
        OutputConfig(
            columns=[
                Column(name="a", role=ColumnRole.TITLE),
                Column(name="b", role=ColumnRole.TITLE),
                Column(name="c", role=ColumnRole.DATA),
            ]
        )


def test_key_without_title_output_config_valid_for_loader() -> None:
    """KEY + DATA without TITLE is valid for :func:`loader` schemas."""
    cfg = OutputConfig(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="ns"),
            Column(name="v", role=ColumnRole.DATA),
        ]
    )
    assert len([c for c in cfg.columns if c.role == ColumnRole.KEY]) == 1
    assert len([c for c in cfg.columns if c.role == ColumnRole.DATA]) == 1


def test_column_namespace_only_on_key_or_metadata() -> None:
    assert Column(name="m", role=ColumnRole.METADATA, namespace="currency").namespace == "currency"
    with pytest.raises(ValidationError, match="namespace is only allowed on KEY or METADATA"):
        Column(name="x", role=ColumnRole.DATA, namespace="fred")


def test_result_from_dataframe_infers_data_columns() -> None:
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    r = TabularResult.from_dataframe(df)
    assert isinstance(r, TabularResult)
    assert list(r.data.columns) == ["a", "b"]
    assert r.output_schema is None
    assert r.columns == []


def test_result_from_dataframe_rejects_empty() -> None:
    with pytest.raises(ValueError, match="empty"):
        TabularResult.from_dataframe(pd.DataFrame())


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


def test_build_table_result_metadata_columns_are_schema_roles() -> None:
    raw = pd.DataFrame(
        {
            "series_id": ["UNRATE"],
            "title": ["Unemployment Rate"],
            "units": ["Percent"],
            "date": ["2020-01-01"],
            "value": [3.5],
        }
    )
    cfg = OutputConfig(
        columns=[
            Column(name="series_id", role=ColumnRole.KEY),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="units", role=ColumnRole.METADATA),
            Column(name="date", role=ColumnRole.DATA),
            Column(name="value", role=ColumnRole.DATA),
        ]
    )
    r = cfg.build_table_result(raw)
    assert r.provenance.properties == {}
    assert [c.name for c in r.metadata_columns] == ["units"]
    assert r.data.loc[0, "units"] == "Percent"


def test_result_with_properties_is_cumulative() -> None:
    df = pd.DataFrame({"a": [1]})
    r = TabularResult.from_dataframe(df).with_properties(a=1).with_properties(b=2)
    assert r.provenance.properties == {"a": 1, "b": 2}


def test_result_to_table_adds_unmapped_as_data() -> None:
    df = pd.DataFrame({"k": ["a"], "title": ["T"], "obs": [1.0]})
    r = TabularResult(data=df, provenance=_prov())
    schema = OutputConfig(
        columns=[
            Column(name="k", role=ColumnRole.KEY),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    t = r.to_table(schema)
    assert isinstance(t, TabularResult)
    assert t.output_schema is not None
    roles = {c.name: c.role for c in t.output_schema.columns}
    assert roles["obs"] == ColumnRole.DATA


def test_table_result_to_table_reapplies_schema() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    t1 = TabularResult.from_dataframe(df)
    t2 = t1.to_table(
        OutputConfig(
            columns=[
                Column(name="a", role=ColumnRole.KEY),
                Column(name="b", role=ColumnRole.TITLE),
            ]
        )
    )
    assert t2.entity_keys.shape == (1, 1)


# ---------------------------------------------------------------------------
# Column-match diagnostics
# ---------------------------------------------------------------------------


def test_build_table_result_no_warning_when_all_match(caplog) -> None:
    """Fully matched config should emit no warning."""
    raw = pd.DataFrame({"a": [1], "b": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="a", role=ColumnRole.DATA),
            Column(name="b", role=ColumnRole.DATA),
        ]
    )
    with caplog.at_level("WARNING", logger="parsimony.result"):
        cfg.build_table_result(raw)
    assert not caplog.records


def test_build_table_result_warns_on_unmatched_column(caplog) -> None:
    """Partial match should log a WARNING naming the missing column and available columns."""
    raw = pd.DataFrame({"a": [1], "b": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="a", role=ColumnRole.DATA),
            Column(name="missing_col", role=ColumnRole.DATA),
        ]
    )
    with caplog.at_level("WARNING", logger="parsimony.result"):
        cfg.build_table_result(raw)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings
    msg = warnings[0].message
    assert "missing_col" in msg
    assert "a" in msg
    assert "b" in msg


def test_build_table_result_warns_on_multiple_unmatched_columns(caplog) -> None:
    """Multiple unmatched columns should all appear in the warning message."""
    raw = pd.DataFrame({"a": [1]})
    cfg = OutputConfig(
        columns=[
            Column(name="a", role=ColumnRole.DATA),
            Column(name="gone_x", role=ColumnRole.DATA),
            Column(name="gone_y", role=ColumnRole.DATA),
        ]
    )
    with caplog.at_level("WARNING", logger="parsimony.result"):
        cfg.build_table_result(raw)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings
    msg = warnings[0].message
    assert "gone_x" in msg
    assert "gone_y" in msg


def test_build_table_result_wildcard_not_reported_as_unmatched(caplog) -> None:
    """Wildcard '*' should never appear as an unmatched column."""
    raw = pd.DataFrame({"x": [1], "y": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="*", role=ColumnRole.DATA),
        ]
    )
    with caplog.at_level("WARNING", logger="parsimony.result"):
        cfg.build_table_result(raw)
    assert not caplog.records


def test_validate_columns_returns_unmatched() -> None:
    """validate_columns should return unmatched config column names."""
    df = pd.DataFrame({"a": [1], "b": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="a", role=ColumnRole.DATA),
            Column(name="missing", role=ColumnRole.DATA),
        ]
    )
    assert cfg.validate_columns(df) == ["missing"]


def test_validate_columns_returns_empty_when_all_match() -> None:
    """validate_columns should return empty list when all columns match."""
    df = pd.DataFrame({"a": [1], "b": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="a", role=ColumnRole.DATA),
            Column(name="b", role=ColumnRole.DATA),
        ]
    )
    assert cfg.validate_columns(df) == []


def test_validate_columns_excludes_wildcard() -> None:
    """Wildcard '*' should not appear in validate_columns output."""
    df = pd.DataFrame({"x": [1]})
    cfg = OutputConfig(
        columns=[
            Column(name="*", role=ColumnRole.DATA),
        ]
    )
    assert cfg.validate_columns(df) == []


def test_build_table_result_warns_then_raises_on_total_mismatch(caplog) -> None:
    """When all config columns are absent, warn AND raise ValueError."""
    raw = pd.DataFrame({"x": [1], "y": [2]})
    cfg = OutputConfig(
        columns=[
            Column(name="absent_a", role=ColumnRole.DATA),
            Column(name="absent_b", role=ColumnRole.DATA),
        ]
    )
    with (
        caplog.at_level("WARNING", logger="parsimony.result"),
        pytest.raises(ValueError, match="matched no input columns"),
    ):
        cfg.build_table_result(raw)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings
    msg = warnings[0].message
    assert "absent_a" in msg
    assert "absent_b" in msg
