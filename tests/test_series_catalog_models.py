"""Tests for the series catalog framework models and pipeline helpers."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.catalog import (
    SeriesEntry,
    entries_from_result,
    normalize_code,
    normalize_entity_code,
)
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


def test_normalize_code_accepts_snake_case() -> None:
    assert normalize_code("fred") == "fred"


def test_normalize_code_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        normalize_code("")
    with pytest.raises(ValueError):
        normalize_code("Bad Code")
    with pytest.raises(ValueError):
        normalize_code("1bad")


def test_normalize_entity_code_accepts_connector_native_ids() -> None:
    assert normalize_entity_code("GDPC1") == "GDPC1"
    assert normalize_entity_code("  B.U.Y.10Y ") == "B.U.Y.10Y"


def test_series_entry_requires_title_and_namespace_code() -> None:
    SeriesEntry(
        namespace="fred",
        code="UNRATE",
        title="Unemployment",
    )
    with pytest.raises(ValidationError):
        SeriesEntry(namespace="fred", code="x", title="")
    with pytest.raises(ValidationError):
        SeriesEntry(namespace="fred", code="", title="T")


def test_series_entry_first_class_tags_metadata() -> None:
    e = SeriesEntry(
        namespace="sdmx_ecb_yc",
        code="M.US",
        title="ECB YC",
        tags=["ecb", "rates"],
        metadata={"k": "v"},
    )
    assert e.tags == ["ecb", "rates"]
    assert e.metadata == {"k": "v"}


def test_embedding_text_joins_title_metadata_tags() -> None:
    e = SeriesEntry(
        namespace="fred",
        code="GDPC1",
        title="Real GDP",
        metadata={"frequency_short": "Q", "units_short": "Bil. of $"},
        tags=["macro", "usa"],
    )
    text = e.embedding_text()
    assert "Real GDP" in text
    assert "frequency_short: Q" in text
    assert "units_short: Bil. of $" in text
    assert "tags: macro, usa" in text


def test_embedding_text_omits_empty_metadata() -> None:
    e = SeriesEntry(
        namespace="fred",
        code="X",
        title="T",
        metadata={},
    )
    text = e.embedding_text()
    assert text == "T"


def test_entries_from_result_unions_provenance_tags_source_and_extra_tags() -> None:
    df = pd.DataFrame(
        {
            "code": ["UNRATE", "UNRATE"],
            "title": ["Unemployment Rate", "Unemployment Rate"],
        }
    )
    result = Result(
        data=df,
        provenance=Provenance(source="fred", tags=["macro", "monthly", "macro"]),
        output_schema=OutputConfig(
            columns=[
                Column(name="code", role=ColumnRole.KEY, namespace="fred"),
                Column(name="title", role=ColumnRole.TITLE),
            ]
        ),
    )
    entries = entries_from_result(result, extra_tags=["monthly", "labor"])
    assert len(entries) == 1
    assert entries[0].tags == ["fred", "macro", "monthly", "labor"]
