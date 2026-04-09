"""Tests for Result, OutputConfig, and Parquet/Arrow round-trip."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest
from pydantic import ValidationError

from ockham.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
    SemanticTableResult,
)


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
    prov = Provenance(source="test", params={"series_id": "S"})
    r = cfg.build_table_result(raw, provenance=prov, params={"series_id": "S"})
    assert isinstance(r, SemanticTableResult)
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
    r = cfg.build_table_result(raw, provenance=Provenance(), params={})
    assert set(r.data.columns) == {"a", "b"}


def test_result_parquet_roundtrip(tmp_path) -> None:
    df = pd.DataFrame({"x": [1, 2], "y": [3.0, 4.0]})
    cols = [
        Column(name="x", role=ColumnRole.DATA, dtype="numeric"),
        Column(name="y", role=ColumnRole.DATA, dtype="numeric"),
    ]
    prov = Provenance(
        source="fred",
        params={"series_id": "GDP"},
        fetched_at=datetime(2024, 1, 1, tzinfo=UTC),
        title="T",
        properties={"metadata": [{"name": "n", "value": "v"}]},
    )
    res = SemanticTableResult(data=df, output_schema=OutputConfig(columns=cols), provenance=prov)
    path = tmp_path / "out.parquet"
    res.to_parquet(path)
    back = Result.from_parquet(path)
    assert isinstance(back, SemanticTableResult)
    pd.testing.assert_frame_equal(back.data.reset_index(drop=True), df)
    assert back.provenance.source == "fred"
    assert back.provenance.params["series_id"] == "GDP"
    assert back.provenance.properties["metadata"][0]["name"] == "n"
    assert len(back.columns) == 2


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
    r = Result(data=df, output_schema=OutputConfig(columns=cols), provenance=Provenance())
    assert list(r.entity_keys.columns) == ["sym"]


def test_build_table_result_rejects_empty_frame() -> None:
    cfg = OutputConfig(columns=[Column(name="x", role=ColumnRole.DATA)])
    with pytest.raises(ValueError, match="empty"):
        cfg.build_table_result(pd.DataFrame(), provenance=Provenance())


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


def test_column_namespace_only_on_key() -> None:
    with pytest.raises(ValidationError, match="namespace is only allowed on KEY"):
        Column(name="x", role=ColumnRole.DATA, namespace="fred")


def test_column_namespace_on_key_roundtrip_parquet(tmp_path) -> None:
    df = pd.DataFrame({"k": ["a"], "title": ["A"], "v": [1]})
    cfg = OutputConfig(
        columns=[
            Column(name="k", role=ColumnRole.KEY, namespace="fred"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="v", role=ColumnRole.DATA),
        ]
    )
    t = SemanticTableResult(
        data=df,
        output_schema=cfg,
        provenance=Provenance(source="t"),
    )
    path = tmp_path / "t.parquet"
    t.to_parquet(path)
    back = Result.from_parquet(path)
    assert isinstance(back, SemanticTableResult)
    key_col = next(c for c in back.output_schema.columns if c.role == ColumnRole.KEY)
    assert key_col.namespace == "fred"


def test_result_from_dataframe_infers_data_columns() -> None:
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    prov = Provenance(source="test", params={"k": "v"})
    r = Result.from_dataframe(df, prov)
    assert isinstance(r, Result)
    assert not isinstance(r, SemanticTableResult)
    assert list(r.data.columns) == ["a", "b"]
    assert r.output_schema is None
    assert r.columns == []
    assert r.provenance.source == "test"


def test_result_from_dataframe_rejects_empty() -> None:
    with pytest.raises(ValueError, match="empty"):
        Result.from_dataframe(pd.DataFrame(), Provenance())


def test_result_to_table_adds_unmapped_as_data() -> None:
    df = pd.DataFrame({"k": ["a"], "title": ["T"], "obs": [1.0]})
    r = Result(data=df, provenance=Provenance(source="x"))
    schema = OutputConfig(
        columns=[
            Column(name="k", role=ColumnRole.KEY),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    t = r.to_table(schema)
    assert isinstance(t, SemanticTableResult)
    roles = {c.name: c.role for c in t.output_schema.columns}
    assert roles["obs"] == ColumnRole.DATA


def test_table_result_to_table_reapplies_schema() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    t1 = Result.from_dataframe(df, Provenance())
    t2 = t1.to_table(
        OutputConfig(
            columns=[
                Column(name="a", role=ColumnRole.KEY),
                Column(name="b", role=ColumnRole.TITLE),
            ]
        )
    )
    assert t2.entity_keys.shape == (1, 1)
