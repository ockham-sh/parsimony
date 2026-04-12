"""Destatis (German Federal Statistical Office): fetch + catalog enumeration.

GENESIS API: https://www-genesis.destatis.de/genesis/online
Uses guest credentials (GAST/GAST) by default.
"""

from __future__ import annotations

import csv
import io
import re
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

_BASE_URL = "https://www-genesis.destatis.de/genesisWS/rest/2020"

_GERMAN_MONTHS = {
    "Januar": "01", "Februar": "02", "März": "03", "April": "04",
    "Mai": "05", "Juni": "06", "Juli": "07", "August": "08",
    "September": "09", "Oktober": "10", "November": "11", "Dezember": "12",
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class DestatisFetchParams(BaseModel):
    """Parameters for fetching Destatis table data."""

    table_id: Annotated[str, Namespace("destatis")] = Field(
        ..., description="GENESIS table ID (e.g. 61111-0001)"
    )
    start_year: str | None = Field(default=None, description="Start year (YYYY)")
    end_year: str | None = Field(default=None, description="End year (YYYY)")

    @field_validator("table_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("table_id must be non-empty")
        return v


class DestatisEnumerateParams(BaseModel):
    """No parameters needed — queries the GENESIS table catalogue."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

DESTATIS_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="table_id", role=ColumnRole.KEY, namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

DESTATIS_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="table_id", role=ColumnRole.KEY, param_key="table_id", namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_german_number(s: str) -> str:
    s = s.strip()
    s = s.replace(".", "").replace(",", ".")
    return s


def _normalize_german_date(s: str) -> str:
    s = s.strip()
    if re.match(r"^\d{4}$", s):
        return f"{s}-01-01"
    q_match = re.match(r"(\d)\.\s*Quartal\s+(\d{4})", s)
    if q_match:
        quarter = int(q_match.group(1))
        month = (quarter - 1) * 3 + 1
        return f"{q_match.group(2)}-{month:02d}-01"
    for month_de, month_num in _GERMAN_MONTHS.items():
        if month_de in s:
            year_match = re.search(r"(\d{4})", s)
            if year_match:
                return f"{year_match.group(1)}-{month_num}-01"
    if re.match(r"^\d{4}-\d{2}$", s):
        return f"{s}-01"
    return s


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=DESTATIS_FETCH_OUTPUT, tags=["macro", "de"])
async def destatis_fetch(
    params: DestatisFetchParams,
    *,
    username: str = "GAST",
    password: str = "GAST",
) -> Result:
    """Fetch a Destatis GENESIS table by table_id.

    Returns the table data with dates normalized and numeric columns
    converted.  German number formats (1.234,56) are handled
    automatically.
    """
    http = HttpClient(_BASE_URL)

    req_params: dict[str, Any] = {
        "username": username, "password": password,
        "name": params.table_id, "format": "ffcsv", "language": "de",
    }
    if params.start_year:
        req_params["startyear"] = params.start_year
    if params.end_year:
        req_params["endyear"] = params.end_year

    import httpx as _httpx

    async with _httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(f"{_BASE_URL}/data/tablefile", params=req_params)

    if response.status_code != 200:
        raise ValueError(f"Destatis API error: HTTP {response.status_code}")

    text = response.text
    if "<html" in text.lower() or "announcement" in text.lower() or "datenbank/online" in str(response.url):
        raise ValueError(
            "Destatis GAST credentials redirected to announcement page. "
            "Try again later or use registered credentials via DESTATIS_USERNAME/DESTATIS_PASSWORD."
        )

    # Parse ffcsv: find header (first line with semicolons)
    lines = text.strip().split("\n")
    header_idx = 0
    for i, line in enumerate(lines):
        if ";" in line:
            header_idx = i
            break

    data_text = "\n".join(lines[header_idx:])
    try:
        df = pd.read_csv(io.StringIO(data_text), sep=";", dtype=str)
    except Exception as exc:
        raise ValueError(f"Failed to parse Destatis response: {exc}") from exc

    if df.empty:
        raise ValueError(f"No data returned for table: {params.table_id}")

    # Identify and normalize time columns
    time_cols: list[str] = []
    for col in df.columns:
        if any(kw in col.lower() for kw in ("zeit", "jahr", "monat", "quartal", "time", "year")):
            time_cols.append(col)

    # Build a date column from time columns
    if time_cols:
        df["date"] = df[time_cols].apply(
            lambda row: _normalize_german_date(" ".join(row.dropna().astype(str))),
            axis=1,
        )
    else:
        df["date"] = ""

    # Convert numeric columns (German format)
    for col in df.columns:
        if col in time_cols or col == "date":
            continue
        converted = df[col].apply(lambda v: _normalize_german_number(str(v)) if pd.notna(v) else v)
        df[col] = pd.to_numeric(converted, errors="ignore")

    df["table_id"] = params.table_id
    df["title"] = params.table_id

    return Result.from_dataframe(
        df,
        Provenance(
            source="destatis",
            params={"table_id": params.table_id},
            properties={"source_url": "https://www-genesis.destatis.de/genesis/online"},
        ),
    )


@enumerator(output=DESTATIS_ENUMERATE_OUTPUT, tags=["macro", "de"])
async def enumerate_destatis(
    params: DestatisEnumerateParams,
    *,
    username: str = "GAST",
    password: str = "GAST",
) -> pd.DataFrame:
    """Enumerate Destatis tables via the GENESIS catalogue API."""
    http = HttpClient(_BASE_URL)

    response = await http.request(
        "GET", "/catalogue/tables",
        params={
            "username": username, "password": password,
            "language": "en", "pagelength": "500",
        },
    )
    response.raise_for_status()
    body = response.json()

    tables = body.get("Tables", body.get("tables", []))
    rows: list[dict[str, str]] = []
    for t in tables:
        rows.append({
            "table_id": t.get("Code", t.get("code", "")),
            "title": t.get("Content", t.get("content", "")),
            "category": t.get("Subject", t.get("subject", "")),
            "frequency": t.get("Time", t.get("time", "")),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["table_id", "title", "category", "frequency"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([destatis_fetch])
CONNECTORS = Connectors([destatis_fetch, enumerate_destatis])
