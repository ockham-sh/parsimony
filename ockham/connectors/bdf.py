"""Banque de France (BdF): fetch + catalog enumeration.

Uses the SDMX-based Webstat API at api.webstat.banque-france.fr.
Requires free API key via BANQUEDEFRANCE_KEY environment variable.
Register at: https://developer.webstat.banque-france.fr/

Auth header: X-IBM-Client-Id (IBM API Connect gateway).
Reference: rwebstat R package (official client).
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Annotated, Any

import httpx
import pandas as pd
from pydantic import BaseModel, Field, field_validator

from ockham.connector import Connectors, Namespace, connector, enumerator
from ockham.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)

logger = logging.getLogger(__name__)

_SDMX_BASE = "https://api.webstat.banque-france.fr/webstat-en/v1"


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BdfFetchParams(BaseModel):
    """Parameters for fetching Banque de France time series."""

    key: Annotated[str, Namespace("bdf")] = Field(
        ...,
        description=(
            "SDMX series key, optionally prefixed with dataset "
            "(e.g. EXR.M.USD.EUR.SP00.E or ICP.M.FR.N.000000.4.ANR)"
        ),
    )
    start_period: str | None = Field(default=None, description="Start period (e.g. 2020-01)")
    end_period: str | None = Field(default=None, description="End period (e.g. 2024-12)")

    @field_validator("key")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("key must be non-empty")
        return v


class BdfEnumerateParams(BaseModel):
    """No parameters needed — enumerates BdF datasets."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BDF_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="dataset_id", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

BDF_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, param_key="key", namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BDF_FETCH_OUTPUT, tags=["macro", "fr"])
async def bdf_fetch(params: BdfFetchParams, *, api_key: str) -> Result:
    """Fetch Banque de France time series via SDMX API.

    Key format: DATASET.DIM1.DIM2... (e.g. EXR.M.USD.EUR.SP00.E)
    or just the series key if dataset is embedded.
    """
    headers = {"X-IBM-Client-Id": api_key}
    req_params: dict[str, str] = {"format": "csv"}
    if params.start_period:
        req_params["startPeriod"] = params.start_period
    if params.end_period:
        req_params["endPeriod"] = params.end_period

    url = f"{_SDMX_BASE}/data/{params.key}"

    async with httpx.AsyncClient(timeout=60.0, headers=headers) as client:
        response = await client.get(url, params=req_params)
        response.raise_for_status()

    text = response.text
    if text.startswith("\ufeff"):
        text = text[1:]

    try:
        df = pd.read_csv(io.StringIO(text), sep=";", dtype=str)
    except Exception as exc:
        raise ValueError(f"Failed to parse BdF CSV response: {exc}") from exc

    if df.empty:
        raise ValueError(f"No data returned for key: {params.key}")

    # Normalize column names
    col_map = {c: c.lower().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=col_map)

    # Extract standard fields
    result_rows: list[dict[str, Any]] = []
    date_col = next((c for c in df.columns if "time_period" in c or "date" in c), None)
    value_col = next((c for c in df.columns if "obs_value" in c or "value" in c), None)
    title_col = next((c for c in df.columns if "series_title" in c or "title" in c), None)
    key_col = next((c for c in df.columns if "series_key" in c), None)

    if date_col and value_col:
        for _, row in df.iterrows():
            try:
                value = float(row[value_col]) if row[value_col] not in (None, "", "NaN") else None
            except (ValueError, TypeError):
                value = None
            series_key = str(row[key_col]) if key_col else params.key
            title = str(row.get(title_col, series_key)) if title_col else series_key
            result_rows.append({
                "key": series_key,
                "title": title,
                "date": str(row[date_col]),
                "value": value,
            })

    if not result_rows:
        raise ValueError(f"Could not parse observations for key: {params.key}")

    return Result.from_dataframe(
        pd.DataFrame(result_rows),
        Provenance(
            source="bdf",
            params={"key": params.key},
            properties={"source_url": "https://webstat.banque-france.fr"},
        ),
    )


@enumerator(output=BDF_ENUMERATE_OUTPUT, tags=["macro", "fr"])
async def enumerate_bdf(params: BdfEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate all BdF datasets via the SDMX catalogue endpoint."""
    headers = {"X-IBM-Client-Id": api_key}
    url = f"{_SDMX_BASE}/catalogue/"

    async with httpx.AsyncClient(timeout=60.0, headers=headers) as client:
        response = await client.get(url, params={"format": "csv"})
        response.raise_for_status()

    text = response.text
    if text.startswith("\ufeff"):
        text = text[1:]

    try:
        df = pd.read_csv(io.StringIO(text), sep=";", dtype=str)
    except Exception as exc:
        logger.warning("Failed to parse BdF catalogue CSV: %s", exc)
        return pd.DataFrame(columns=["dataset_id", "title"])

    rows: list[dict[str, str]] = []
    # CSV columns vary; try common patterns
    id_col = next(
        (c for c in df.columns if any(k in c.lower() for k in ("code", "dataset", "id"))),
        df.columns[0] if len(df.columns) > 0 else None,
    )
    title_col = next(
        (c for c in df.columns if any(k in c.lower() for k in ("name", "title", "label"))),
        df.columns[1] if len(df.columns) > 1 else None,
    )

    if id_col:
        for _, row in df.iterrows():
            did = str(row.get(id_col, "")).strip()
            if did:
                rows.append({
                    "dataset_id": did,
                    "title": str(row.get(title_col, did)).strip() if title_col else did,
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["dataset_id", "title"])


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([bdf_fetch])
CONNECTORS = Connectors([bdf_fetch, enumerate_bdf])
