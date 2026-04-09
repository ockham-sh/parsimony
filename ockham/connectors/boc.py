"""Bank of Canada (BoC): fetch + catalog enumeration.

API docs: https://www.bankofcanada.ca/valet/docs
No authentication required.

"""

from __future__ import annotations

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

_BASE_URL = "https://www.bankofcanada.ca/valet"


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BocFetchParams(BaseModel):
    """Parameters for fetching Bank of Canada time series."""

    series_name: Annotated[str, Namespace("boc")] = Field(
        ...,
        description=(
            "Comma-separated BoC series names (e.g. FXUSDCAD,FXEURCAD) "
            "or a group name prefixed with 'group:' (e.g. group:FX_RATES_DAILY)"
        ),
    )
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_name must be non-empty")
        return v


class BocEnumerateParams(BaseModel):
    """No parameters needed — enumerates all BoC series groups."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BOC_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_name", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="group", role=ColumnRole.METADATA),
    ]
)

BOC_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_name", role=ColumnRole.KEY, param_key="series_name", namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_observations(
    json_data: dict[str, Any],
    series_details: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Parse BoC Valet API observations response into long-format DataFrame.

    The observations array has entries like:
    {"d": "2024-01-15", "FXUSDCAD": {"v": "1.3456"}, "FXEURCAD": {"v": "1.4678"}}
    """
    observations = json_data.get("observations", [])
    if not observations:
        return pd.DataFrame(columns=["series_name", "title", "date", "value"])

    # Discover series columns (everything except "d")
    sample = observations[0]
    series_cols = [k for k in sample if k != "d"]

    rows: list[dict[str, Any]] = []
    for obs in observations:
        date = obs.get("d", "")
        for col in series_cols:
            raw = obs.get(col)
            if raw is None:
                continue
            raw_value = raw.get("v") if isinstance(raw, dict) else raw
            try:
                value = float(raw_value) if raw_value not in (None, "", "NaN") else None
            except (ValueError, TypeError):
                value = None

            # Resolve title from seriesDetail if available
            title = col
            if series_details and col in series_details:
                detail = series_details[col]
                title = detail.get("label", detail.get("description", col))

            rows.append({
                "series_name": col,
                "title": title,
                "date": date,
                "value": value,
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_name", "title", "date", "value"]
    )


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BOC_FETCH_OUTPUT, tags=["macro", "ca"])
async def boc_fetch(params: BocFetchParams) -> Result:
    """Fetch Bank of Canada time series by series name(s) or group name.

    Use 'group:GROUP_NAME' syntax for group queries (e.g. group:FX_RATES_DAILY).
    Otherwise, pass comma-separated series names (e.g. FXUSDCAD,FXEURCAD).
    """
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=60.0) as client:
        req_params: dict[str, str] = {}
        if params.start_date:
            req_params["start_date"] = params.start_date
        if params.end_date:
            req_params["end_date"] = params.end_date

        if params.series_name.startswith("group:"):
            group_name = params.series_name[6:].strip()
            url = f"/observations/group/{group_name}/json"
        else:
            url = f"/observations/{params.series_name}/json"

        response = await client.get(url, params=req_params)
        response.raise_for_status()
        json_data = response.json()

    series_details = json_data.get("seriesDetail")
    df = _parse_observations(json_data, series_details)
    if df.empty:
        raise ValueError(f"No observations returned for: {params.series_name}")

    return Result.from_dataframe(
        df,
        Provenance(
            source="boc",
            params={"series_name": params.series_name},
            properties={"source_url": "https://www.bankofcanada.ca/valet/docs"},
        ),
    )


@enumerator(output=BOC_ENUMERATE_OUTPUT, tags=["macro", "ca"])
async def enumerate_boc(params: BocEnumerateParams) -> pd.DataFrame:
    """Enumerate all Bank of Canada series via /lists/series/json.

    Single API call returns all 15,000+ series with label and description.
    """
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=60.0) as client:
        resp = await client.get("/lists/series/json")
        resp.raise_for_status()
        data = resp.json()

    series = data.get("series", {})
    rows: list[dict[str, str]] = []
    for series_name, info in series.items():
        if not series_name:
            continue
        label = info.get("label", series_name) if isinstance(info, dict) else str(info)
        desc = info.get("description", "") if isinstance(info, dict) else ""
        # Use description as group hint (no group info in this endpoint)
        rows.append({
            "series_name": series_name,
            "title": label,
            "group": desc,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_name", "title", "group"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([boc_fetch])
CONNECTORS = Connectors([boc_fetch, enumerate_boc])
