"""US Energy Information Administration (EIA): fetch + catalog enumeration.

API docs: https://www.eia.gov/opendata/documentation.php
Requires EIA_API_KEY.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from parsimony.connector import Connectors, EmptyDataError, Namespace, connector, enumerator
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient

_BASE_URL = "https://api.eia.gov/v2"

ENV_VARS: dict[str, str] = {"api_key": "EIA_API_KEY"}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class EiaFetchParams(BaseModel):
    """Parameters for fetching EIA energy data."""

    route: Annotated[str, Namespace("eia")] = Field(
        ..., description="API route (e.g. petroleum/pri/spt)"
    )
    frequency: str | None = Field(
        default=None, description="Data frequency: monthly, weekly, daily, annual"
    )
    start: str | None = Field(default=None, description="Start date (YYYY-MM or YYYY)")
    end: str | None = Field(default=None, description="End date (YYYY-MM or YYYY)")

    @field_validator("route")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("route must be non-empty")
        return v


class EiaEnumerateParams(BaseModel):
    """No parameters needed — enumerates EIA API routes."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

EIA_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="route", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

EIA_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="route", role=ColumnRole.KEY, param_key="route", namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="period", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=EIA_FETCH_OUTPUT, tags=["macro", "energy", "us"])
async def eia_fetch(params: EiaFetchParams, *, api_key: str) -> Result:
    """Fetch EIA energy data by API route.

    Returns the dataset with period parsed and numeric columns converted.
    Columns retain their original names from the EIA API.
    """
    http = HttpClient(_BASE_URL, query_params={"api_key": api_key})

    req_params: dict[str, Any] = {}
    if params.frequency:
        req_params["frequency"] = params.frequency
    if params.start:
        req_params["start"] = params.start
    if params.end:
        req_params["end"] = params.end

    response = await http.request("GET", f"/{params.route}/data", params=req_params)
    response.raise_for_status()
    body = response.json()

    resp = body.get("response", {})
    data = resp.get("data", [])
    if not data:
        raise EmptyDataError(provider="eia", message=f"No data returned for route: {params.route}")

    description = resp.get("description", params.route)

    df = pd.DataFrame(data)

    # Convert period to datetime-like
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], errors="coerce", format="mixed")

    # Convert detected numeric columns
    for col in df.columns:
        if col in ("period", "series-description", "seriesDescription"):
            continue
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        except (ValueError, TypeError):
            pass

    df["route"] = params.route
    df["title"] = description

    return Result.from_dataframe(
        df,
        Provenance(
            source="eia",
            params={"route": params.route},
            properties={"source_url": "https://www.eia.gov/opendata/"},
        ),
    )


@enumerator(output=EIA_ENUMERATE_OUTPUT, tags=["macro", "energy", "us"])
async def enumerate_eia(params: EiaEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate top-level EIA API routes for catalog indexing."""
    http = HttpClient(_BASE_URL, query_params={"api_key": api_key})

    response = await http.request("GET", "/")
    response.raise_for_status()
    body = response.json()

    routes = body.get("response", {}).get("routes", [])
    rows: list[dict[str, str]] = []
    for route in routes:
        rows.append({
            "route": route.get("id", ""),
            "title": route.get("name", route.get("id", "")),
            "category": "EIA",
            "frequency": route.get("frequency", ""),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["route", "title", "category", "frequency"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([eia_fetch])
CONNECTORS = Connectors([eia_fetch, enumerate_eia])
