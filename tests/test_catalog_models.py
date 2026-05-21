"""Tests for the clean catalog entry and result adapter contract."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.catalog import CatalogEntry, entries_from_result, normalize_code, normalize_entity_code
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


def test_catalog_entry_requires_title_and_namespace_code() -> None:
    CatalogEntry(namespace="fred", code="UNRATE", title="Unemployment")
    with pytest.raises(ValidationError):
        CatalogEntry(namespace="fred", code="x", title="")
    with pytest.raises(ValidationError):
        CatalogEntry(namespace="fred", code="", title="T")


def test_catalog_entry_metadata_is_the_only_open_field_space() -> None:
    entry = CatalogEntry(
        namespace="sdmx_ecb_yc",
        code="M.US",
        title="ECB YC",
        metadata={"tags": ["ecb", "rates"], "currency": "EUR"},
    )
    assert entry.metadata == {"tags": ["ecb", "rates"], "currency": "EUR"}
    assert "tags" not in CatalogEntry.model_fields
    assert "description" not in CatalogEntry.model_fields


def test_catalog_entry_rejects_old_first_class_fields() -> None:
    with pytest.raises(ValidationError):
        CatalogEntry(namespace="fred", code="X", title="T", description="old")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        CatalogEntry(namespace="fred", code="X", title="T", tags=["old"])  # type: ignore[call-arg]


def test_entries_from_result_populates_metadata_columns() -> None:
    df = pd.DataFrame(
        {
            "code": ["UNRATE", "UNRATE"],
            "title": ["Unemployment Rate", "Unemployment Rate"],
            "frequency": ["M", "M"],
            "description": ["Civilian unemployment rate", "Civilian unemployment rate"],
        }
    )
    result = Result(
        data=df,
        provenance=Provenance(source="fred", source_description="FRED"),
        output_schema=OutputConfig(
            columns=[
                Column(name="code", role=ColumnRole.KEY, namespace="fred"),
                Column(name="title", role=ColumnRole.TITLE),
                Column(name="frequency", role=ColumnRole.METADATA),
                Column(name="description", role=ColumnRole.METADATA),
            ]
        ),
    )
    entries = entries_from_result(result)
    assert len(entries) == 1
    assert entries[0].metadata == {
        "frequency": "M",
        "description": "Civilian unemployment rate",
    }


def test_entries_from_result_requires_key_namespace() -> None:
    df = pd.DataFrame({"code": ["A"], "title": ["Alpha"]})
    result = Result(
        data=df,
        provenance=Provenance(source="test", source_description="test"),
        output_schema=OutputConfig(
            columns=[
                Column(name="code", role=ColumnRole.KEY),
                Column(name="title", role=ColumnRole.TITLE),
            ]
        ),
    )
    with pytest.raises(ValueError, match="KEY column must declare namespace"):
        entries_from_result(result)


def test_metadata_column_namespace_is_allowed_as_annotation() -> None:
    column = Column(name="currency", role=ColumnRole.METADATA, namespace="iso_currency")
    assert column.namespace == "iso_currency"
