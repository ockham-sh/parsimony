"""FRED source: search + fetch connectors, and catalog enumeration."""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from parsimony.connector import Connectors, Namespace, connector, enumerator
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient

# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class FredSearchParams(BaseModel):
    """Parameters for FRED keyword search."""

    search_text: str = Field(..., min_length=1, description="Search query")


class FredFetchParams(BaseModel):
    """Parameters for fetching FRED time series observations."""

    series_id: Annotated[str, Namespace("fred")] = Field(
        ..., description="FRED series identifier (e.g. GDPC1, UNRATE)"
    )
    observation_start: str | None = Field(
        default=None, description="Start date (YYYY-MM-DD)"
    )
    observation_end: str | None = Field(
        default=None, description="End date (YYYY-MM-DD)"
    )

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        stripped = str(value).strip()
        if not stripped:
            raise ValueError("series_id must be non-empty")
        return stripped


class FredEnumerateParams(BaseModel):
    """Parameters for enumerating FRED series in a release (catalog indexing)."""

    release_id: int = Field(..., ge=1, description="FRED release ID")


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

# Shared identity + metadata for enumerate and fetch; observation columns map explicitly or default to DATA.
_FRED_IDENTITY_METADATA: list[Column] = [
    Column(
        name="series_id",
        role=ColumnRole.KEY,
        param_key="series_id",
        namespace="fred",
    ),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="units_short", role=ColumnRole.METADATA),
    Column(name="frequency_short", role=ColumnRole.METADATA),
    Column(name="seasonal_adjustment_short", role=ColumnRole.METADATA),
]

FRED_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units_short", role=ColumnRole.METADATA),
        Column(name="frequency_short", role=ColumnRole.METADATA),
        Column(name="seasonal_adjustment_short", role=ColumnRole.METADATA),
        Column(name="release_id", role=ColumnRole.METADATA),
    ]
)

FETCH_OUTPUT = OutputConfig(
    columns=[
        *_FRED_IDENTITY_METADATA,
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

SEARCH_COLUMNS = [
    "id",
    "title",
    "units",
    "frequency_short",
    "seasonal_adjustment_short",
    "observation_start",
    "observation_end",
    "last_updated",
]


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        "https://api.stlouisfed.org/fred",
        query_params={"api_key": api_key, "file_type": "json"},
    )


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(tags=["macro", "tool"])
async def fred_search(params: FredSearchParams, *, api_key: str) -> Result:
    """Keyword search for FRED economic time series.

    Returns series metadata (id, title, units, frequency).
    Use short, specific queries like 'US unemployment rate' or 'GDPC1'.
    """
    http = _make_http(api_key)
    response = await http.request(
        "GET",
        "/series/search",
        params={"search_text": params.search_text},
    )
    response.raise_for_status()
    seriess = response.json().get("seriess", [])
    if not seriess:
        raise ValueError(f"No series found for: {params.search_text}")
    df = pd.DataFrame(seriess)
    cols = [c for c in SEARCH_COLUMNS if c in df.columns]
    df = df[cols]
    return Result.from_dataframe(
        df,
        Provenance(source="fred", params={"search_text": params.search_text}),
    )


@connector(output=FETCH_OUTPUT, tags=["macro"])
async def fred_fetch(params: FredFetchParams, *, api_key: str) -> Result:
    """Fetch FRED time series observations by series_id.

    Returns date + value columns with rich metadata (title, units, frequency, seasonal adjustment).
    """
    http = _make_http(api_key)
    series_id = params.series_id

    req_params: dict[str, Any] = {"series_id": series_id}
    if params.observation_start is not None:
        req_params["observation_start"] = params.observation_start
    if params.observation_end is not None:
        req_params["observation_end"] = params.observation_end

    obs_response = await http.request("GET", "/series/observations", params=req_params)
    obs_response.raise_for_status()
    obs_data = obs_response.json()["observations"]

    series_response = await http.request("GET", "/series", params={"series_id": series_id})
    series_response.raise_for_status()
    series_data = series_response.json()["seriess"][0]

    df = pd.DataFrame(obs_data)
    df["series_id"] = series_id
    df["title"] = str(series_data.get("title", ""))
    df["units_short"] = series_data.get("units_short")
    df["frequency_short"] = series_data.get("frequency_short")
    df["seasonal_adjustment_short"] = series_data.get("seasonal_adjustment_short")

    meta_keys = [
        ("id", False),
        ("title", False),
        ("units", False),
        ("units_short", True),
        ("frequency", False),
        ("frequency_short", False),
        ("seasonal_adjustment", False),
        ("seasonal_adjustment_short", True),
        ("last_updated", False),
        ("notes", True),
    ]
    metadata_list = [
        {"name": k, "value": str(series_data[k]), "exclude_from_llm_view": excl}
        for k, excl in meta_keys
        if k in series_data
    ]
    metadata_list.append(
        {
            "name": "series_url",
            "value": f"https://fred.stlouisfed.org/series/{series_id}",
        }
    )

    prov_params: dict[str, Any] = {"series_id": series_id}
    if params.observation_start is not None:
        prov_params["observation_start"] = params.observation_start
    if params.observation_end is not None:
        prov_params["observation_end"] = params.observation_end

    prov = Provenance(
        source="fred",
        params=prov_params,
        properties={"metadata": metadata_list},
    )
    return FETCH_OUTPUT.build_table_result(
        df,
        provenance=prov,
        params=prov_params,
    )


FETCH_CONNECTORS = Connectors([fred_fetch])

CONNECTORS = Connectors([fred_search, fred_fetch])


# ---------------------------------------------------------------------------
# Catalog enumeration
# ---------------------------------------------------------------------------


async def _fetch_release_series_page(
    http: HttpClient,
    release_id: int,
    *,
    limit: int = 1000,
    offset: int = 0,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "release_id": release_id,
        "limit": limit,
        "offset": offset,
        "file_type": "json",
    }
    response = await http.request("GET", "/release/series", params=params)
    response.raise_for_status()
    return response.json().get("seriess") or []


async def _enumerate_release_series(
    http: HttpClient,
    release_id: int,
    *,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    all_series: list[dict[str, Any]] = []
    offset = 0
    while True:
        batch = await _fetch_release_series_page(
            http, release_id, limit=page_size, offset=offset
        )
        if not batch:
            break
        all_series.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_series


@enumerator(
    output=FRED_ENUMERATE_OUTPUT,
    tags=["fred"],
)
async def enumerate_fred_release(
    params: FredEnumerateParams,
    *,
    api_key: str,
) -> pd.DataFrame:
    """Enumerate FRED series for a release (catalog indexing).

    Return one row per series in the release with id, title, and metadata columns.
    """
    http = _make_http(api_key)
    seriess = await _enumerate_release_series(http, params.release_id, page_size=1000)
    rows: list[dict[str, Any]] = []
    rid = params.release_id
    for item in seriess:
        series_id = str(item.get("id", "")).strip()
        if not series_id:
            continue
        title = str(item.get("title", "")).strip() or series_id
        rows.append(
            {
                "series_id": series_id,
                "title": title,
                "frequency_short": item.get("frequency_short"),
                "units_short": item.get("units_short"),
                "seasonal_adjustment_short": item.get("seasonal_adjustment_short"),
                "release_id": rid,
            }
        )
    return pd.DataFrame(rows)


