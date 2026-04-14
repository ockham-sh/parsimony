"""US Treasury Fiscal Data: fetch + catalog enumeration.

API docs: https://fiscaldata.treasury.gov/api-documentation/
No authentication required.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, Namespace, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient

_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
_METADATA_URL = "https://api.fiscaldata.treasury.gov/services/dtg/metadata/"



# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class TreasuryFetchParams(BaseModel):
    """Parameters for fetching US Treasury fiscal data."""

    endpoint: Annotated[str, Namespace("treasury")] = Field(
        ..., description="API endpoint path (e.g. v2/accounting/od/debt_to_penny)"
    )
    filter: str | None = Field(
        default=None,
        description="Filter expression (e.g. record_date:gte:2024-01-01)",
    )
    sort: str | None = Field(
        default=None,
        description="Sort expression (e.g. -record_date for descending)",
    )
    page_size: int = Field(default=100, ge=1, le=10000, description="Records per page")


class TreasuryEnumerateParams(BaseModel):
    """No parameters needed — enumerates the full Treasury API catalog."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

TREASURY_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="endpoint", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

# Treasury returns tabular datasets — the output is a DataFrame whose
# columns depend on the endpoint.  We use a minimal schema with just
# the identity key; actual data columns vary per endpoint.
TREASURY_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="endpoint", role=ColumnRole.KEY, param_key="endpoint", namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="record_date", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


def _make_http() -> HttpClient:
    return HttpClient(_BASE_URL, query_params={"format": "json"})


@connector(output=TREASURY_FETCH_OUTPUT, tags=["macro", "us"])
async def treasury_fetch(params: TreasuryFetchParams) -> Result:
    """Fetch US Treasury fiscal data by endpoint.

    Returns the dataset as-is with ``record_date`` parsed and numeric
    columns converted.  Each row is one record from the Treasury API.
    """
    http = _make_http()
    req_params: dict[str, Any] = {"page[size]": params.page_size}
    if params.filter:
        req_params["filter"] = params.filter
    if params.sort:
        req_params["sort"] = params.sort

    response = await http.request("GET", f"/{params.endpoint}", params=req_params)
    response.raise_for_status()
    body = response.json()

    data = body.get("data", [])
    if not data:
        raise EmptyDataError(provider="treasury", message=f"No data returned for endpoint: {params.endpoint}")

    meta = body.get("meta", {})
    labels = meta.get("labels", {})
    data_types = meta.get("dataTypes", {})

    df = pd.DataFrame(data)

    # Parse record_date
    if "record_date" in df.columns:
        df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")

    # Convert numeric columns identified by API metadata
    numeric_types = {"CURRENCY", "NUMBER", "PERCENTAGE", "RATE"}
    for col, dtype in data_types.items():
        if dtype in numeric_types and col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

    # Add identity columns
    table_name = labels.get("record_date", params.endpoint)
    df["endpoint"] = params.endpoint
    df["title"] = table_name

    return Result.from_dataframe(
        df,
        Provenance(
            source="treasury",
            params={"endpoint": params.endpoint},
            properties={
                "total_records": meta.get("total-count"),
                "source_url": f"https://fiscaldata.treasury.gov/datasets/{params.endpoint}",
            },
        ),
    )


@enumerator(output=TREASURY_ENUMERATE_OUTPUT, tags=["macro", "us"])
async def enumerate_treasury(params: TreasuryEnumerateParams) -> pd.DataFrame:
    """Enumerate all US Treasury Fiscal Data API endpoints for catalog indexing."""
    import httpx

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_METADATA_URL)
        resp.raise_for_status()
        raw = resp.json()

    datasets: list[dict] = []
    if isinstance(raw, list):
        datasets = raw
    elif isinstance(raw, dict):
        for key in ("datasets", "data", "result"):
            if key in raw and isinstance(raw[key], list):
                datasets = raw[key]
                break

    prefix = "/services/api/fiscal_service/"
    rows: list[dict[str, str]] = []

    for ds in datasets:
        apis = ds.get("apis", [])
        if not apis:
            endpoint = ds.get("endpoint_txt", "")
            if endpoint.startswith(prefix):
                endpoint = endpoint[len(prefix):]
            rows.append({
                "endpoint": endpoint or ds.get("dataset_id", ""),
                "title": ds.get("table_name", ds.get("dataset_name", "")),
                "category": ds.get("publisher", ""),
                "frequency": ds.get("update_frequency", ""),
            })
        else:
            for api in apis:
                endpoint = api.get("endpoint_txt") or ""
                if endpoint.startswith(prefix):
                    endpoint = endpoint[len(prefix):]
                if not endpoint:
                    endpoint = api.get("api_id", "")
                if not endpoint:
                    continue
                rows.append({
                    "endpoint": endpoint,
                    "title": api.get("table_name") or ds.get("dataset_name", ""),
                    "category": ds.get("publisher", ""),
                    "frequency": api.get("update_frequency") or ds.get("update_frequency", ""),
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["endpoint", "title", "category", "frequency"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([treasury_fetch])
CONNECTORS = Connectors([treasury_fetch, enumerate_treasury])
