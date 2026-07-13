"""Tests for the clean catalog entry and result adapter contract."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.catalog import Entity, field_values, normalize_entity_code, normalize_namespace
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result


def test_normalize_namespace_accepts_snake_case() -> None:
    assert normalize_namespace("fred") == "fred"


def test_normalize_namespace_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        normalize_namespace("")
    with pytest.raises(ValueError):
        normalize_namespace("Bad Code")
    with pytest.raises(ValueError):
        normalize_namespace("1bad")


def test_normalize_entity_code_accepts_connector_native_ids() -> None:
    assert normalize_entity_code("GDPC1") == "GDPC1"
    assert normalize_entity_code("  B.U.Y.10Y ") == "B.U.Y.10Y"


def test_catalog_entry_requires_title_and_namespace_code() -> None:
    Entity(namespace="fred", code="UNRATE", title="Unemployment")
    with pytest.raises(ValidationError):
        Entity(namespace="fred", code="x", title="")
    with pytest.raises(ValidationError):
        Entity(namespace="fred", code="", title="T")


def test_catalog_entry_metadata_is_the_only_open_field_space() -> None:
    entry = Entity(
        namespace="sdmx_ecb_yc",
        code="M.US",
        title="ECB YC",
        metadata={"tags": ["ecb", "rates"], "currency": "EUR"},
    )
    assert entry.metadata == {"tags": ["ecb", "rates"], "currency": "EUR"}
    assert "tags" not in Entity.model_fields
    assert "description" not in Entity.model_fields


def test_catalog_entry_rejects_old_first_class_fields() -> None:
    with pytest.raises(ValidationError):
        Entity(namespace="fred", code="X", title="T", description="old")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        Entity(namespace="fred", code="X", title="T", tags=["old"])  # type: ignore[call-arg]


def test_to_entities_populates_metadata_columns() -> None:
    df = pd.DataFrame(
        {
            "code": ["UNRATE", "UNRATE"],
            "title": ["Unemployment Rate", "Unemployment Rate"],
            "frequency": ["M", "M"],
            "description": ["Civilian unemployment rate", "Civilian unemployment rate"],
        }
    )
    schema = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="fred"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="frequency", role=ColumnRole.METADATA),
            Column(name="description", role=ColumnRole.METADATA),
        ]
    )
    provenance = Provenance(source="t", source_description="t")
    entries = Result(data=df, provenance=provenance, output_spec=schema).to_entities()
    assert len(entries) == 1
    assert entries[0].metadata == {
        "frequency": "M",
        "description": "Civilian unemployment rate",
    }


def test_to_entities_requires_key_namespace() -> None:
    schema = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    df = pd.DataFrame({"code": ["A"], "title": ["Alpha"]})
    result = Result(data=df, provenance=Provenance(source="t", source_description="t"), output_spec=schema)
    with pytest.raises(ValueError, match="must declare namespace"):
        result.to_entities()


def test_to_entities_allows_metadata_constant_per_entity_key() -> None:
    df = pd.DataFrame(
        {
            "code": ["A", "A", "B"],
            "title": ["Alpha", "Alpha", "Beta"],
            "sector": ["Tech", "Tech", "Energy"],
        }
    )
    schema = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="demo"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="sector", role=ColumnRole.METADATA),
        ]
    )
    entries = Result(data=df, output_spec=schema).to_entities()
    assert {e.code: e.metadata["sector"] for e in entries} == {"A": "Tech", "B": "Energy"}


def test_to_entities_rejects_varying_metadata_within_entity_key() -> None:
    df = pd.DataFrame(
        {
            "code": ["bench", "bench"],
            "title": ["Benchmark", "Benchmark"],
            "isin": ["US123", "US456"],
        }
    )
    schema = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="demo"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="isin", role=ColumnRole.METADATA),
        ]
    )
    with pytest.raises(ValueError, match="conflicting values"):
        Result(data=df, output_spec=schema).to_entities()


def test_metadata_column_namespace_is_no_longer_allowed() -> None:
    """namespace has no enforced behavior on METADATA; it is KEY-only now."""
    with pytest.raises(ValidationError, match="namespace is only allowed on KEY"):
        Column(name="currency", role=ColumnRole.METADATA, namespace="iso_currency")


def test_field_values_scalar_and_missing() -> None:
    entry = Entity(namespace="fred", code="X", title="Title", metadata={"freq": "M"})
    assert field_values(entry, "title") == ["Title"]
    assert field_values(entry, "freq") == ["M"]
    assert field_values(entry, "missing") == []


def test_field_values_list_set_and_dict() -> None:
    entry = Entity(
        namespace="fred",
        code="X",
        title="Title",
        metadata={
            "tags": ["a", "b"],
            "regions": {"US", "EU"},
            "attrs": {"unit": "pct", "scale": 1},
        },
    )
    assert field_values(entry, "tags") == ["a", "b"]
    assert sorted(field_values(entry, "regions")) == ["EU", "US"]
    assert field_values(entry, "attrs") == ["unit: pct", "scale: 1"]
