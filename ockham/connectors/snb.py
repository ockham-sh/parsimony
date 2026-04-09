"""Swiss National Bank (SNB): fetch + catalog enumeration.

Data portal: https://data.snb.ch
No authentication required.
"""

from __future__ import annotations

import asyncio
import csv
import io
import re
from typing import Annotated, Any

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
from ockham.transport.http import HttpClient

_BASE_URL = "https://data.snb.ch"
_CUBE_LIST_URL = "https://raw.githubusercontent.com/cardsX/SNB-Data-Streamliner/main/cube_list.csv"

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "Interest rates": ["zins", "interest", "rate", "libor", "saron"],
    "Exchange rates": ["kurs", "exchange", "devisen", "wechsel"],
    "Monetary aggregates": ["geldmenge", "monetary", "aggregat"],
    "Balance of payments": ["zahlungsbilanz", "balance", "payment"],
    "Banking statistics": ["bank", "kredit", "credit"],
    "Securities": ["wertpapier", "securit", "obligation"],
    "Prices": ["preis", "price", "index"],
    "National accounts": ["volkswirtschaft", "national", "account", "bip", "gdp"],
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class SnbFetchParams(BaseModel):
    """Parameters for fetching SNB data from a cube."""

    cube_id: Annotated[str, Namespace("snb")] = Field(
        ..., description="SNB cube identifier (e.g. rendoblim, devkum)"
    )
    from_date: str | None = Field(
        default=None, description="Start date (YYYY or YYYY-MM or YYYY-MM-DD)"
    )
    to_date: str | None = Field(
        default=None, description="End date (YYYY or YYYY-MM or YYYY-MM-DD)"
    )
    dim_sel: str | None = Field(
        default=None, description="Dimension selection (e.g. D0(V0,V1),D1(ALL))"
    )
    lang: str = Field(default="en", description="Language: en, de, fr, it")

    @field_validator("cube_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("cube_id must be non-empty")
        return v


class SnbEnumerateParams(BaseModel):
    """No parameters needed — enumerates all SNB cubes."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

SNB_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="cube_id", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

SNB_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="cube_id", role=ColumnRole.KEY, param_key="cube_id", namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _infer_category(cube_id: str, description: str) -> str:
    text = f"{cube_id} {description}".lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "Other"


def _infer_frequency_from_dates(dates: list[str]) -> str:
    if not dates:
        return "Unknown"
    sample = dates[0]
    if re.match(r"^\d{4}$", sample):
        return "Annual"
    if re.match(r"^\d{4}-Q\d$", sample):
        return "Quarterly"
    if re.match(r"^\d{4}-\d{2}$", sample):
        return "Monthly"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", sample):
        return "Daily"
    return "Unknown"


def _parse_snb_csv(text: str) -> pd.DataFrame:
    """Parse SNB CSV response, skipping metadata preamble.

    Returns the data as a clean DataFrame with the first column as
    the date index — no melting.  Columns retain their original names.
    """
    lines = text.strip().split("\n")
    sep = ";" if ";" in text else ","

    # Find header line (first line with 2+ separators)
    header_idx = 0
    for i, line in enumerate(lines):
        if line.count(sep) >= 2:
            header_idx = i
            break

    data_text = "\n".join(lines[header_idx:])
    try:
        df = pd.read_csv(io.StringIO(data_text), sep=sep, dtype=str)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    # First column is the date
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "date"})

    # Convert value columns to numeric
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=SNB_FETCH_OUTPUT, tags=["macro", "ch"])
async def snb_fetch(params: SnbFetchParams) -> Result:
    """Fetch SNB cube data by cube_id.

    Returns the cube's time series as a DataFrame with date + value
    columns named by the SNB.  Column names are the original dimension
    labels from the cube.
    """
    http = HttpClient(_BASE_URL)

    req_params: dict[str, str] = {}
    if params.from_date:
        req_params["fromDate"] = params.from_date
    if params.to_date:
        req_params["toDate"] = params.to_date
    if params.dim_sel:
        req_params["dimSel"] = params.dim_sel

    response = await http.request(
        "GET", f"/api/cube/{params.cube_id}/data/csv/{params.lang}", params=req_params,
    )
    response.raise_for_status()

    df = _parse_snb_csv(response.text)
    if df.empty:
        raise ValueError(f"No data returned for cube: {params.cube_id}")

    df["cube_id"] = params.cube_id
    df["title"] = params.cube_id

    # Fetch cube title from dimensions endpoint
    try:
        dim_resp = await http.request("GET", f"/api/cube/{params.cube_id}/dimensions/{params.lang}")
        if dim_resp.status_code == 200:
            dim_data = dim_resp.json()
            if isinstance(dim_data, dict):
                df["title"] = dim_data.get("name", dim_data.get("cubeName", params.cube_id))
    except Exception:
        pass

    return Result.from_dataframe(
        df,
        Provenance(
            source="snb",
            params={"cube_id": params.cube_id},
            properties={"source_url": f"https://data.snb.ch/en/topics/{params.cube_id}"},
        ),
    )


@enumerator(output=SNB_ENUMERATE_OUTPUT, tags=["macro", "ch"])
async def enumerate_snb(params: SnbEnumerateParams) -> pd.DataFrame:
    """Enumerate all SNB data cubes for catalog indexing.

    Fetches cube list from external reference, then queries each cube
    for frequency inference.
    """
    import httpx

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_CUBE_LIST_URL)
        resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    cubes: list[dict[str, str]] = []
    for row in reader:
        cube_id = row.get("cube_id", row.get("id", "")).strip()
        if cube_id:
            cubes.append({
                "cube_id": cube_id,
                "title": row.get("description", row.get("name", cube_id)).strip(),
            })

    rows: list[dict[str, str]] = []
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0) as client:
        for cube in cubes:
            cid = cube["cube_id"]
            title = cube["title"]
            category = _infer_category(cid, title)
            frequency = "Unknown"

            try:
                await asyncio.sleep(0.15)
                data_resp = await client.get(
                    f"/api/cube/{cid}/data/csv/en", params={"fromDate": "2020"},
                )
                if data_resp.status_code == 200:
                    dates = []
                    sep = ";" if ";" in data_resp.text else ","
                    for line in data_resp.text.strip().split("\n")[:50]:
                        parts = line.split(sep)
                        if parts and re.match(r"^\d{4}", parts[0].strip()):
                            dates.append(parts[0].strip())
                    frequency = _infer_frequency_from_dates(dates)
            except Exception:
                pass

            rows.append({
                "cube_id": cid, "title": title,
                "category": category, "frequency": frequency,
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["cube_id", "title", "category", "frequency"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([snb_fetch])
CONNECTORS = Connectors([snb_fetch, enumerate_snb])
