"""Sveriges Riksbank (Sweden): fetch + catalog enumeration.

API docs: https://developer.api.riksbank.se/
API key optional but recommended.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd

logger = logging.getLogger(__name__)
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

_BASE_URL = "https://api.riksbank.se/swea/v1"

ENV_VARS: dict[str, str] = {"api_key": "RIKSBANK_API_KEY"}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class RiksbankFetchParams(BaseModel):
    """Parameters for fetching a Riksbank time series."""

    series_id: Annotated[str, Namespace("riksbank")] = Field(
        ..., description="Riksbank series ID (e.g. SEKEURPMI)"
    )
    from_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_id must be non-empty")
        return v

    @field_validator("to_date")
    @classmethod
    def _both_dates_or_neither(cls, v: str | None, info: Any) -> str | None:
        from_date = info.data.get("from_date")
        if (from_date is None) != (v is None):
            raise ValueError("Provide both from_date and to_date, or neither")
        return v


class RiksbankEnumerateParams(BaseModel):
    """No parameters needed — enumerates all Riksbank series."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

RIKSBANK_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="group", role=ColumnRole.METADATA),
    ]
)

RIKSBANK_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, param_key="series_id", namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str = "") -> HttpClient:
    headers: dict[str, str] = {}
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return HttpClient(_BASE_URL, headers=headers)


def _infer_frequency(series_id: str) -> str:
    sid = series_id.upper()
    if sid.endswith("PMI") or sid.endswith("PMD"):
        return "Daily"
    if sid.endswith("PMM") or sid.endswith("PMW"):
        return "Monthly"
    if sid.endswith("PMQ"):
        return "Quarterly"
    if sid.endswith("PMA"):
        return "Annual"
    return "Unknown"


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=RIKSBANK_FETCH_OUTPUT, tags=["macro", "se"])
async def riksbank_fetch(params: RiksbankFetchParams, *, api_key: str = "") -> Result:
    """Fetch a single Riksbank time series by series_id.

    Returns date + value with series name.
    """
    http = _make_http(api_key)

    if params.from_date and params.to_date:
        path = f"/Observations/{params.series_id}/{params.from_date}/{params.to_date}"
    else:
        path = f"/Observations/Latest/{params.series_id}"

    response = await http.request("GET", path)
    response.raise_for_status()
    data = response.json()

    # Resolve series title from /Series endpoint
    title = params.series_id
    try:
        series_resp = await http.request("GET", "/Series")
        if series_resp.status_code == 200:
            series_list = series_resp.json()
            if isinstance(series_list, dict):
                series_list = [series_list]
            for s in series_list:
                sid = s.get("seriesId", s.get("id", ""))
                if sid == params.series_id:
                    title = s.get("seriesName", s.get("name", params.series_id))
                    break
    except Exception:
        logger.debug("Could not resolve title for %s, using series_id", params.series_id)

    items = data if isinstance(data, list) else [data]
    rows: list[dict[str, Any]] = []
    for item in items:
        date = item.get("date") or item.get("Date")
        raw_value = item.get("value") or item.get("Value")
        if date is None:
            continue
        try:
            value = float(raw_value) if raw_value not in (None, "", "NaN") else None
        except (ValueError, TypeError):
            value = None
        rows.append({
            "series_id": params.series_id,
            "title": title,
            "date": date,
            "value": value,
        })

    if not rows:
        raise EmptyDataError(provider="riksbank", message=f"No observations returned for: {params.series_id}")

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="riksbank",
            params={"series_id": params.series_id},
            properties={"source_url": "https://www.riksbank.se/en-gb/statistics/"},
        ),
    )


@enumerator(output=RIKSBANK_ENUMERATE_OUTPUT, tags=["macro", "se"])
async def enumerate_riksbank(params: RiksbankEnumerateParams, *, api_key: str = "") -> pd.DataFrame:
    """Enumerate all Riksbank series via the Groups and Series endpoints."""
    http = _make_http(api_key)

    groups_resp = await http.request("GET", "/Groups")
    groups_resp.raise_for_status()
    groups_data = groups_resp.json()

    group_lookup: dict[str, str] = {}

    def _flatten(nodes: Any, parent: str = "") -> None:
        if isinstance(nodes, dict):
            nodes = [nodes]
        if not isinstance(nodes, list):
            return
        for node in nodes:
            gid = node.get("groupId", node.get("id", ""))
            name = node.get("groupName", node.get("name", ""))
            full = f"{parent} > {name}" if parent else name
            group_lookup[str(gid)] = full
            _flatten(node.get("groupInfos", node.get("children", [])), full)

    _flatten(groups_data)

    series_resp = await http.request("GET", "/Series")
    series_resp.raise_for_status()
    series_data = series_resp.json()
    if isinstance(series_data, dict):
        series_data = [series_data]

    rows: list[dict[str, str]] = []
    for s in series_data:
        sid = s.get("seriesId", s.get("id", ""))
        if not sid:
            continue
        rows.append({
            "series_id": sid,
            "title": s.get("seriesName", s.get("name", sid)),
            "frequency": _infer_frequency(sid),
            "group": group_lookup.get(str(s.get("groupId", s.get("group", ""))), ""),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_id", "title", "frequency", "group"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([riksbank_fetch])
CONNECTORS = Connectors([riksbank_fetch, enumerate_riksbank])
