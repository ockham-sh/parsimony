"""Reserve Bank of Australia (RBA): fetch + catalog enumeration.

Data: https://www.rba.gov.au/statistics/tables/
No authentication required. Uses Akamai CDN — curl_cffi optional for
TLS fingerprint impersonation (falls back to httpx).

Discovery scrapes the tables index page for CSV links, then parses
each CSV's metadata header rows to extract series info.
"""

from __future__ import annotations

import asyncio
import csv
import io
import re
from datetime import datetime
from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, Namespace, connector, enumerator
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)

_BASE_URL = "https://www.rba.gov.au"
_TABLES_URL = f"{_BASE_URL}/statistics/tables/"
_CSV_LINK_PATTERN = re.compile(r'href="(/statistics/tables/csv/[^"]+\.csv)"')
_REQUEST_DELAY = 0.5

_USER_AGENT = "parsimony-data/1.0 (https://parsimony.dev)"

_CATEGORY_PREFIXES = {
    "a": "Reserve Bank",
    "b": "Banking and Finance",
    "c": "Credit and Charge Cards",
    "d": "Monetary Aggregates",
    "e": "Household and Business Finance",
    "f": "Interest Rates and Yields",
    "g": "Exchange Rates",
    "h": "Economic Activity",
    "i": "Balance of Payments",
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class RbaFetchParams(BaseModel):
    """Parameters for fetching RBA statistical table data."""

    table_id: Annotated[str, Namespace("rba")] = Field(
        ..., description="RBA table identifier (e.g. f01, a01, d01)"
    )


class RbaEnumerateParams(BaseModel):
    """No parameters needed — scrapes the RBA tables page."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

RBA_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

RBA_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="table_id", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        Column(name="series_key", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# HTTP helper — curl_cffi with httpx fallback
# ---------------------------------------------------------------------------


async def _http_get(url: str) -> str:
    """GET a URL, using curl_cffi if available (Akamai bypass)."""
    try:
        from curl_cffi.requests import AsyncSession  # type: ignore[import-untyped]

        async with AsyncSession() as s:
            resp = await s.get(url, impersonate="chrome")
            resp.raise_for_status()
            return resp.text
    except ImportError:
        pass

    import httpx

    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": _USER_AGENT}) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text


# ---------------------------------------------------------------------------
# CSV parsing helpers
# ---------------------------------------------------------------------------


def _normalize_date(s: str) -> str:
    for fmt in ("%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def _parse_rba_csv(text: str, table_id: str) -> pd.DataFrame:
    """Parse RBA CSV: skip metadata header, melt to long format."""
    lines = text.strip().split("\n")

    header_idx = 0
    series_id_row: dict[str, str] = {}

    for i, line in enumerate(lines):
        lower = line.strip().lower()
        if lower.startswith("series id"):
            reader = csv.reader(io.StringIO(line))
            parts = list(reader)[0]
            for j, part in enumerate(parts):
                series_id_row[str(j)] = part.strip()
        if lower.startswith("title"):
            header_idx = i

    if header_idx == 0:
        header_idx = min(10, len(lines) - 2)

    data_text = "\n".join(lines[header_idx:])
    rows_list = list(csv.reader(io.StringIO(data_text)))

    if len(rows_list) < 2:
        return pd.DataFrame(columns=["table_id", "title", "date", "value", "series_key"])

    header = rows_list[0]
    all_rows: list[dict[str, Any]] = []

    for row in rows_list[1:]:
        if not row or not row[0].strip():
            continue
        date = _normalize_date(row[0].strip())
        for col_idx in range(1, min(len(header), len(row))):
            col_name = header[col_idx].strip()
            if not col_name:
                continue
            val_str = row[col_idx].strip()
            try:
                value = float(val_str) if val_str else None
            except (ValueError, TypeError):
                value = None
            all_rows.append({
                "table_id": table_id,
                "title": col_name,
                "date": date,
                "value": value,
                "series_key": series_id_row.get(str(col_idx), col_name),
            })

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=["table_id", "title", "date", "value", "series_key"]
    )


def _parse_csv_metadata(text: str, csv_url: str) -> list[dict[str, str]]:
    """Extract series metadata from an RBA CSV file's header rows."""
    lines = text.strip().split("\n")
    content_lines = [ln for ln in lines[1:] if ln.strip()]
    if len(content_lines) < 8:
        return []

    try:
        reader = pd.read_csv(io.StringIO("\n".join(content_lines)), header=None, dtype=str, nrows=10)
    except Exception:
        return []

    if reader.empty or len(reader) < 8:
        return []

    title_row = description_row = frequency_row = units_row = series_id_row_idx = None
    for i in range(min(10, len(reader))):
        first_val = str(reader.iloc[i, 0]).strip() if pd.notna(reader.iloc[i, 0]) else ""
        if first_val == "Title":
            title_row = i
        elif first_val == "Description":
            description_row = i
        elif first_val == "Frequency":
            frequency_row = i
        elif first_val == "Units":
            units_row = i
        elif first_val == "Series ID":
            series_id_row_idx = i

    if series_id_row_idx is None or title_row is None:
        return []

    csv_filename = csv_url.split("/")[-1].replace(".csv", "")
    category = _CATEGORY_PREFIXES.get(csv_filename[0].lower() if csv_filename else "", "")

    rows: list[dict[str, str]] = []
    for col in reader.columns[1:]:
        sid = str(reader.iloc[series_id_row_idx, col]).strip() if pd.notna(reader.iloc[series_id_row_idx, col]) else ""
        if not sid or sid == "nan":
            continue
        title = str(reader.iloc[title_row, col]).strip() if pd.notna(reader.iloc[title_row, col]) else sid
        frequency = str(reader.iloc[frequency_row, col]).strip() if frequency_row is not None and pd.notna(reader.iloc[frequency_row, col]) else ""

        rows.append({
            "series_id": sid,
            "title": title,
            "category": category,
            "frequency": frequency,
        })

    return rows


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=RBA_FETCH_OUTPUT, tags=["macro", "au"])
async def rba_fetch(params: RbaFetchParams) -> Result:
    """Fetch RBA statistical table data by table ID."""
    url = f"{_BASE_URL}/statistics/tables/csv/{params.table_id}.csv"
    text = await _http_get(url)

    df = _parse_rba_csv(text, params.table_id)
    if df.empty:
        raise ValueError(f"No data returned for table: {params.table_id}")

    return Result.from_dataframe(
        df,
        Provenance(
            source="rba", params={"table_id": params.table_id},
            properties={"source_url": _TABLES_URL},
        ),
    )


@enumerator(output=RBA_ENUMERATE_OUTPUT, tags=["macro", "au"])
async def enumerate_rba(params: RbaEnumerateParams) -> pd.DataFrame:
    """Discover RBA series by scraping the tables page for CSV links,
    then parsing each CSV's metadata header rows.
    """
    # Step 1: scrape tables index for CSV links
    html = await _http_get(_TABLES_URL)
    csv_links = _CSV_LINK_PATTERN.findall(html)

    if not csv_links:
        return pd.DataFrame(columns=["series_id", "title", "category", "frequency"])

    # Step 2: download each CSV and parse metadata
    all_rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for link in csv_links:
        url = f"{_BASE_URL}{link}"
        try:
            text = await _http_get(url)
            for row in _parse_csv_metadata(text, url):
                if row["series_id"] not in seen:
                    seen.add(row["series_id"])
                    all_rows.append(row)
        except Exception:
            pass
        await asyncio.sleep(_REQUEST_DELAY)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=["series_id", "title", "category", "frequency"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([rba_fetch])
CONNECTORS = Connectors([rba_fetch, enumerate_rba])
