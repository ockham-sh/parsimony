"""Tests for SDMX discovery/DSD/codelist/series_keys connectors and namespace helpers."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pandas as pd
import pytest

from parsimony.connectors.sdmx import (
    SdmxCodelistParams,
    SdmxDsdParams,
    SdmxListDatasetsParams,
    SdmxSeriesKeysParams,
    _build_sdmx_title,
    _format_code_with_label,
    _ordered_non_time_dimension_ids,
    _resolve_series_dimension_ids,
    _sdmx_fetch_output,
    _sdmx_series_keys_output,
    sdmx_agency_namespace,
    sdmx_codelist_namespace,
    sdmx_list_datasets,
    sdmx_namespace_from_dataset_key,
)
from parsimony.result import ColumnRole


def test_sdmx_namespace_helpers() -> None:
    assert sdmx_namespace_from_dataset_key("ECB-YC") == "sdmx_ecb_yc"
    assert sdmx_agency_namespace("ECB") == "sdmx_ecb_datasets"
    assert sdmx_agency_namespace("IMF_DATA") == "sdmx_imf_data_datasets"
    assert sdmx_codelist_namespace("ECB", "CL_FREQ") == "sdmx_ecb_cl_freq"


def test_sdmx_list_datasets_params_validation() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        SdmxListDatasetsParams(agency="   ")


def test_sdmx_dataset_key_params_validation() -> None:
    with pytest.raises(ValueError, match="agency prefix"):
        SdmxDsdParams(dataset_key="nohyphen")
    with pytest.raises(ValueError, match="non-empty"):
        SdmxCodelistParams(dataset_key="ECB-YC", dimension="  ")


def test_sdmx_output_contracts_include_key_title_metadata() -> None:
    dim_ids = ["FREQ", "REF_AREA"]
    fetch = _sdmx_fetch_output("sdmx_ecb_yc", dim_ids)
    fetch_roles = {c.name: c.role for c in fetch.columns}
    assert fetch_roles["series_key"] == ColumnRole.KEY
    assert fetch_roles["title"] == ColumnRole.TITLE
    assert fetch_roles["FREQ"] == ColumnRole.METADATA
    assert fetch_roles["REF_AREA"] == ColumnRole.METADATA
    assert fetch_roles["TIME_PERIOD"] == ColumnRole.DATA
    assert fetch_roles["value"] == ColumnRole.DATA

    enum = _sdmx_series_keys_output("sdmx_ecb_yc", dim_ids)
    enum_roles = {c.name: c.role for c in enum.columns}
    assert enum_roles["series_key"] == ColumnRole.KEY
    assert enum_roles["title"] == ColumnRole.TITLE
    assert enum_roles["dataset_key"] == ColumnRole.METADATA
    assert enum_roles["FREQ"] == ColumnRole.METADATA
    assert enum_roles["REF_AREA"] == ColumnRole.METADATA


def test_format_code_with_label_includes_parenthesized_label() -> None:
    assert _format_code_with_label("M", "Monthly") == "M (Monthly)"
    assert _format_code_with_label("USD", "usd") == "USD"
    assert _format_code_with_label("EUR", None) == "EUR"


def test_build_sdmx_title_uses_labels_dash_separated() -> None:

    row = pd.Series({"FREQ": "M", "REF_AREA": "US"})
    labels = {"FREQ": {"M": "Monthly"}, "REF_AREA": {"US": "United States"}}
    assert (
        _build_sdmx_title(row, ["FREQ", "REF_AREA"], labels)
        == "Monthly - United States"
    )


def test_ordered_non_time_dimension_ids_preserves_dsd_order() -> None:
    dsd = SimpleNamespace(
        dimensions=[
            SimpleNamespace(id="FREQ"),
            SimpleNamespace(id="UNIT"),
            SimpleNamespace(id="TIME_PERIOD"),
            SimpleNamespace(id="GEO"),
        ]
    )
    assert _ordered_non_time_dimension_ids(dsd) == ["FREQ", "UNIT", "GEO"]


def test_resolve_series_dimension_ids_uses_dsd_order_not_dataframe_order() -> None:
    assert _resolve_series_dimension_ids(
        ["GEO", "UNIT", "FREQ"],
        ["FREQ", "UNIT", "GEO"],
    ) == ["FREQ", "UNIT", "GEO"]


def test_resolve_series_dimension_ids_rejects_missing_dsd_columns() -> None:
    with pytest.raises(ValueError, match="missing dimension column"):
        _resolve_series_dimension_ids(["UNIT", "FREQ"], ["FREQ", "UNIT", "GEO"])


def _require_sdmx_integration() -> None:
    if not os.environ.get("RUN_SDMX_INTEGRATION"):
        pytest.skip("Set RUN_SDMX_INTEGRATION=1 to run SDMX live API tests")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sdmx_list_datasets_ecb_network() -> None:
    _require_sdmx_integration()
    res = await sdmx_list_datasets(SdmxListDatasetsParams(agency="ECB"))
    assert res.output_schema is not None
    df = res.df
    assert not df.empty
    assert "dataset_id" in df.columns and "name" in df.columns
    assert (df["dataset_id"] == "YC").any()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sdmx_codelist_ecb_yc_freq_network() -> None:
    _require_sdmx_integration()
    from parsimony.connectors.sdmx import sdmx_codelist

    res = await sdmx_codelist(SdmxCodelistParams(dataset_key="ECB-YC", dimension="FREQ"))
    df = res.df
    assert not df.empty
    assert "code" in df.columns and "name" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sdmx_dsd_ecb_yc_network() -> None:
    _require_sdmx_integration()
    from parsimony.connectors.sdmx import sdmx_dsd

    res = await sdmx_dsd(SdmxDsdParams(dataset_key="ECB-YC"))
    assert res.output_schema is None
    df = res.df
    assert not df.empty
    assert "dimension_id" in df.columns
    assert "codelist_size" in df.columns


@pytest.mark.asyncio
async def test_sdmx_series_keys_invalid_filter_key() -> None:
    from parsimony.connectors.sdmx import sdmx_series_keys

    with pytest.raises(ValueError, match="not a dimension"):
        await sdmx_series_keys(
            SdmxSeriesKeysParams(dataset_key="ECB-YC", filters={"NO_SUCH_DIM": ["x"]})
        )
