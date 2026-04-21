"""Tests for Result Arrow/Parquet round-trips."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa

from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)


def _df() -> pd.DataFrame:
    return pd.DataFrame({"code": ["UNRATE", "GDPC1"], "title": ["Unemployment", "Real GDP"]})


def _schema() -> OutputConfig:
    return OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="fred"),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )


# ---------------------------------------------------------------------------
# Arrow
# ---------------------------------------------------------------------------


def test_to_arrow_embeds_provenance_metadata() -> None:
    result = Result(data=_df(), provenance=Provenance(source="fred"))
    table = result.to_arrow()
    assert b"parsimony.result" in (table.schema.metadata or {})


def test_arrow_roundtrip_schemaless_result() -> None:
    prov = Provenance(source="fred", params={"k": "v"})
    result = Result(data=_df(), provenance=prov)
    table = result.to_arrow()
    roundtrip = Result.from_arrow(table)
    assert roundtrip.output_schema is None
    assert roundtrip.provenance.source == "fred"
    assert roundtrip.provenance.params == {"k": "v"}
    pd.testing.assert_frame_equal(roundtrip.df, _df())


def test_arrow_roundtrip_with_schema() -> None:
    """When output_schema is set, from_arrow restores it."""
    result = Result(
        data=_df(),
        provenance=Provenance(source="fred"),
        output_schema=_schema(),
    )
    table = result.to_arrow()
    roundtrip = Result.from_arrow(table)
    assert roundtrip.output_schema is not None
    cols = roundtrip.output_schema.columns
    assert [c.name for c in cols] == ["code", "title"]
    assert [c.role for c in cols] == [ColumnRole.KEY, ColumnRole.TITLE]
    assert cols[0].namespace == "fred"


def test_from_arrow_accepts_vanilla_parquet_without_metadata() -> None:
    table = pa.Table.from_pandas(_df(), preserve_index=False)
    result = Result.from_arrow(table)
    assert result.output_schema is None
    pd.testing.assert_frame_equal(result.df, _df())


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


def test_parquet_roundtrip(tmp_path: Path) -> None:
    result = Result(
        data=_df(),
        provenance=Provenance(source="fred", params={"q": "unemployment"}),
        output_schema=_schema(),
    )
    path = tmp_path / "data.parquet"
    result.to_parquet(path)
    roundtrip = Result.from_parquet(path)
    assert roundtrip.output_schema is not None
    assert roundtrip.provenance.source == "fred"
    assert roundtrip.provenance.params == {"q": "unemployment"}
    pd.testing.assert_frame_equal(roundtrip.df, _df())
