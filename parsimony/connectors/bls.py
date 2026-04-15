"""US Bureau of Labor Statistics: fetch + catalog enumeration.

API docs: https://www.bls.gov/developers/
API key optional but recommended (higher rate limits).
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from parsimony.connector import Connectors, Namespace, connector, enumerator
from parsimony.errors import EmptyDataError, ProviderError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient

_BASE_URL = "https://api.bls.gov/publicAPI/v2"

ENV_VARS: dict[str, str] = {"api_key": "BLS_API_KEY"}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BlsFetchParams(BaseModel):
    """Parameters for fetching a BLS time series."""

    series_id: Annotated[str, Namespace("bls")] = Field(..., description="BLS series ID (e.g. LNS14000000)")
    start_year: str = Field(..., description="Start year (YYYY)")
    end_year: str = Field(..., description="End year (YYYY)")

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_id must be non-empty")
        return v

    @field_validator("start_year", "end_year")
    @classmethod
    def _validate_year(cls, v: str) -> str:
        v = v.strip()
        if not v.isdigit() or len(v) != 4:
            raise ValueError("Year must be 4-digit string (YYYY)")
        return v


class BlsEnumerateParams(BaseModel):
    """No parameters needed — enumerates popular BLS series across surveys."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BLS_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="bls"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="survey", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

BLS_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, param_key="series_id", namespace="bls"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _period_to_date(year: str, period: str) -> str:
    """Convert BLS year + period code to ISO date string."""
    if period.startswith("M") and len(period) == 3:
        return f"{year}-{period[1:]}-01"
    if period.startswith("Q") and len(period) == 3:
        quarter = int(period[1:])
        month = (quarter - 1) * 3 + 1
        return f"{year}-{month:02d}-01"
    if period == "A01":
        return f"{year}-01-01"
    return f"{year}-01-01"


def _infer_frequency(period: str) -> str:
    if period.startswith("M"):
        return "Monthly"
    if period.startswith("Q"):
        return "Quarterly"
    if period.startswith("S"):
        return "Semiannual"
    if period == "A01":
        return "Annual"
    return "Monthly"


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BLS_FETCH_OUTPUT, tags=["macro", "us"])
async def bls_fetch(params: BlsFetchParams, *, api_key: str = "") -> Result:
    """Fetch a single BLS time series by series_id.

    Returns date + value with series metadata (title, frequency).
    API key is optional but recommended for higher rate limits.
    """
    payload: dict[str, Any] = {
        "seriesid": [params.series_id],
        "startyear": params.start_year,
        "endyear": params.end_year,
        "catalog": True,
    }
    if api_key:
        payload["registrationkey"] = api_key

    http = HttpClient(_BASE_URL, timeout=60.0)
    response = await http.request("POST", "/timeseries/data/", json=payload)
    response.raise_for_status()
    body = response.json()

    status = body.get("status", "")
    if status != "REQUEST_SUCCEEDED":
        messages = body.get("message", [])
        raise ProviderError(provider="bls", status_code=0, message=f"BLS API error ({status}): {'; '.join(messages)}")

    series_list = body.get("Results", {}).get("series", [])
    if not series_list:
        raise EmptyDataError(provider="bls", message=f"No data returned for series: {params.series_id}")

    series_block = series_list[0]
    catalog = series_block.get("catalog", {})
    title = catalog.get("series_title", params.series_id)

    rows: list[dict[str, Any]] = []
    for obs in series_block.get("data", []):
        val_str = obs.get("value", "")
        if val_str in ("-", ""):
            value = None
        else:
            try:
                value = float(val_str)
            except (ValueError, TypeError):
                value = None

        period = obs["period"]
        rows.append(
            {
                "series_id": params.series_id,
                "title": title,
                "frequency": _infer_frequency(period),
                "date": _period_to_date(obs["year"], period),
                "value": value,
            }
        )

    if not rows:
        raise EmptyDataError(provider="bls", message=f"No observations for series: {params.series_id}")

    metadata_list = [{"name": k, "value": str(v)} for k, v in catalog.items() if v and k != "series_title"]

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="bls",
            params={"series_id": params.series_id},
            properties={
                "metadata": metadata_list,
                "source_url": f"https://data.bls.gov/timeseries/{params.series_id}",
            },
        ),
    )


@enumerator(output=BLS_ENUMERATE_OUTPUT, tags=["macro", "us"])
async def enumerate_bls(params: BlsEnumerateParams, *, api_key: str = "") -> pd.DataFrame:
    """Enumerate popular BLS series across all surveys.

    Uses the BLS surveys + popular series endpoints.
    """
    import asyncio

    import httpx

    base_params: dict[str, str] = {}
    if api_key:
        base_params["registrationkey"] = api_key

    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=60.0) as client:
        resp = await client.get("/surveys", params=base_params)
        resp.raise_for_status()
        surveys_data = resp.json()

        surveys: list[dict[str, str]] = []
        if isinstance(surveys_data, dict) and "Results" in surveys_data:
            for s in surveys_data["Results"].get("survey", []):
                surveys.append(
                    {
                        "code": s.get("survey_abbreviation", ""),
                        "name": s.get("survey_name", ""),
                    }
                )

        rows: list[dict[str, str]] = []
        for survey in surveys:
            code = survey["code"]
            if not code:
                continue
            try:
                await asyncio.sleep(0.35)
                resp = await client.get("/timeseries/popular", params={**base_params, "survey": code})
                resp.raise_for_status()
                results = resp.json().get("Results") or {}
                for s in results.get("series") or []:
                    if s is None:
                        continue
                    sid = s.get("seriesID", "")
                    if not sid:
                        continue
                    rows.append(
                        {
                            "series_id": sid,
                            "title": s.get("seriesTitle") or s.get("title") or sid,
                            "survey": survey["name"],
                            "frequency": "Monthly",
                        }
                    )
            except (httpx.HTTPError, KeyError):
                continue

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["series_id", "title", "survey", "frequency"])


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

CONNECTORS = Connectors([bls_fetch, enumerate_bls])
