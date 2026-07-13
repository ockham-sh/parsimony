"""Tests for Result Arrow/Parquet round-trips."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa

from parsimony.result import (
    Column,
    ColumnRole,
    OutputSpec,
    Provenance,
    Result,
)


def _df() -> pd.DataFrame:
    return pd.DataFrame({"code": ["UNRATE", "GDPC1"], "title": ["Unemployment", "Real GDP"]})


def _schema() -> OutputSpec:
    return OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="fred"),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )


# ---------------------------------------------------------------------------
# Arrow
# ---------------------------------------------------------------------------


def test_to_arrow_embeds_provenance_metadata() -> None:
    result = Result(
        data=_df(),
        provenance=Provenance(source="fred", source_description="FRED"),
    )
    table = result.to_arrow()
    assert b"parsimony.result" in (table.schema.metadata or {})


def test_from_arrow_tolerates_legacy_column_fields() -> None:
    """Files written before dtype/mapped_name were removed must still load.

    Column is extra="forbid" at authoring time; the read path drops unknown
    embedded keys instead of failing on its own history.
    """
    import json

    table = pa.Table.from_pandas(_df(), preserve_index=False)
    legacy_payload = {
        "provenance": {"source": "fred", "source_description": "FRED"},
        "columns": [
            {"name": "code", "kind": "key", "namespace": "fred", "dtype": "auto", "mapped_name": None},
            {"name": "title", "role": "title", "dtype": "datetime", "mapped_name": "renamed"},
        ],
    }
    meta = dict(table.schema.metadata or {})
    meta[b"parsimony.result"] = json.dumps(legacy_payload).encode("utf-8")
    restored = Result.from_arrow(table.replace_schema_metadata(meta))
    assert restored.output_spec is not None
    roles = {c.name: c.role for c in restored.output_spec.columns}
    assert roles == {"code": ColumnRole.KEY, "title": ColumnRole.TITLE}
    assert restored.provenance.source == "fred"


def test_arrow_roundtrip_schemaless_result() -> None:
    prov = Provenance(
        source="fred",
        source_description="FRED",
        params={"k": "v"},
        properties={"series_url": "https://example.com/UNRATE"},
    )
    result = Result(data=_df(), provenance=prov)
    table = result.to_arrow()
    roundtrip = Result.from_arrow(table)
    assert roundtrip.output_spec is None
    assert roundtrip.provenance.source == "fred"
    assert roundtrip.provenance.params == {"k": "v"}
    assert roundtrip.provenance.properties == {"series_url": "https://example.com/UNRATE"}
    pd.testing.assert_frame_equal(roundtrip.df, _df())


def test_arrow_roundtrip_with_schema() -> None:
    """When output_spec is set, from_arrow restores it."""
    result = Result(
        data=_df(),
        provenance=Provenance(source="fred", source_description="FRED"),
        output_spec=_schema(),
    )
    table = result.to_arrow()
    roundtrip = Result.from_arrow(table)
    assert roundtrip.output_spec is not None
    cols = roundtrip.output_spec.columns
    assert [c.name for c in cols] == ["code", "title"]
    assert [c.role for c in cols] == [ColumnRole.KEY, ColumnRole.TITLE]
    assert cols[0].namespace == "fred"


def test_from_arrow_accepts_vanilla_parquet_without_metadata() -> None:
    table = pa.Table.from_pandas(_df(), preserve_index=False)
    result = Result.from_arrow(table)
    assert result.output_spec is None
    pd.testing.assert_frame_equal(result.df, _df())


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


def test_parquet_roundtrip(tmp_path: Path) -> None:
    result = Result(
        data=_df(),
        provenance=Provenance(
            source="fred",
            source_description="FRED",
            params={"q": "unemployment"},
            properties={"series_url": "https://example.com/UNRATE"},
        ),
        output_spec=_schema(),
    )
    path = tmp_path / "data.parquet"
    result.to_parquet(path)
    roundtrip = Result.from_parquet(path)
    assert roundtrip.output_spec is not None
    assert roundtrip.provenance.source == "fred"
    assert roundtrip.provenance.params == {"q": "unemployment"}
    assert roundtrip.provenance.properties == {"series_url": "https://example.com/UNRATE"}
    pd.testing.assert_frame_equal(roundtrip.df, _df())
