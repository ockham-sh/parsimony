"""Tests for template-namespace support in Column / OutputConfig / entries_from_table_result.

Reverse-resolution tests (_find_enumerator / _template_to_regex) belong to
the LazyNamespaceCatalog wrapper under :mod:`parsimony.bundles.lazy_catalog`
and are covered separately there.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from pydantic import ValidationError

from parsimony.catalog.catalog import entries_from_table_result
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    namespace_placeholders,
    resolve_namespace_template,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_namespace_placeholders_extracts_in_order_and_dedupes() -> None:
    assert namespace_placeholders("sdmx_series_{agency}_{dataset_id}") == ["agency", "dataset_id"]
    assert namespace_placeholders("plain") == []
    assert namespace_placeholders("{x}_{x}_{y}") == ["x", "y"]


def test_resolve_namespace_template_substitutes() -> None:
    out = resolve_namespace_template("sdmx_series_{agency}_{dataset_id}", {"agency": "ECB", "dataset_id": "YC"})
    assert out == "sdmx_series_ECB_YC"


def test_resolve_namespace_template_missing_key_raises() -> None:
    with pytest.raises(KeyError):
        resolve_namespace_template("sdmx_series_{agency}", {})


# ---------------------------------------------------------------------------
# Column validation
# ---------------------------------------------------------------------------


def test_column_namespace_is_template_detects_placeholders() -> None:
    static = Column(name="code", role=ColumnRole.KEY, namespace="sdmx_datasets")
    tmpl = Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_{agency}_{dataset_id}")
    assert static.namespace_is_template is False
    assert static.namespace_placeholders == []
    assert tmpl.namespace_is_template is True
    assert tmpl.namespace_placeholders == ["agency", "dataset_id"]


def test_column_rejects_unbalanced_braces() -> None:
    with pytest.raises(ValidationError, match="unbalanced braces"):
        Column(name="code", role=ColumnRole.KEY, namespace="sdmx_{agency")


# ---------------------------------------------------------------------------
# OutputConfig validation
# ---------------------------------------------------------------------------


def test_output_config_requires_template_placeholders_to_reference_declared_columns() -> None:
    with pytest.raises(ValidationError, match="placeholders not declared as columns"):
        OutputConfig(
            columns=[
                Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_{agency}_{dataset_id}"),
                Column(name="title", role=ColumnRole.TITLE),
                # dataset_id missing → should fail
                Column(name="agency", role=ColumnRole.METADATA),
            ]
        )


def test_output_config_accepts_template_when_placeholders_all_declared() -> None:
    cfg = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_{agency}_{dataset_id}"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="agency", role=ColumnRole.METADATA),
            Column(name="dataset_id", role=ColumnRole.METADATA),
        ]
    )
    assert cfg.columns[0].namespace_is_template is True


# ---------------------------------------------------------------------------
# Per-row resolution in entries_from_table_result
# ---------------------------------------------------------------------------


def _make_table(cfg: OutputConfig, df: pd.DataFrame) -> Any:
    return cfg.build_table_result(df, provenance=Provenance(source="test"))


def testentries_from_table_result_static_namespace_unchanged() -> None:
    cfg = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="sdmx_datasets"),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    df = pd.DataFrame({"code": ["ECB|YC", "ESTAT|prc"], "title": ["Yield curve", "Prices"]})
    entries = entries_from_table_result(_make_table(cfg, df))
    assert [e.namespace for e in entries] == ["sdmx_datasets", "sdmx_datasets"]
    # normalize_entity_code preserves case / punctuation, only strips.
    assert [e.code for e in entries] == ["ECB|YC", "ESTAT|prc"]


def testentries_from_table_result_template_resolves_per_row() -> None:
    cfg = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_{agency}_{dataset_id}"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="agency", role=ColumnRole.METADATA),
            Column(name="dataset_id", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame(
        {
            "code": ["B.U2.EUR", "M.FR.N.000000"],
            "title": ["Euro yield 10y", "French HICP"],
            "agency": ["ECB", "ESTAT"],
            "dataset_id": ["YC", "prc_hicp_manr"],
        }
    )
    entries = entries_from_table_result(_make_table(cfg, df))
    assert [e.namespace for e in entries] == [
        "sdmx_series_ecb_yc",
        "sdmx_series_estat_prc_hicp_manr",
    ]


def testentries_from_table_result_template_null_placeholder_raises() -> None:
    cfg = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_{agency}_{dataset_id}"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="agency", role=ColumnRole.METADATA),
            Column(name="dataset_id", role=ColumnRole.METADATA),
        ]
    )
    df = pd.DataFrame(
        {
            "code": ["orphan"],
            "title": ["Orphan series"],
            "agency": ["ECB"],
            "dataset_id": [None],
        }
    )
    with pytest.raises(ValueError, match="is null for row with key"):
        entries_from_table_result(_make_table(cfg, df))
