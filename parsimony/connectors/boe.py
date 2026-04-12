"""Bank of England (BOE): fetch + catalog enumeration.

Interactive database: https://www.bankofengland.co.uk/boeapps/database/
No authentication required.

Discovery uses a cascading strategy:
1. Scrape tables.asp for series codes (~5K), batch-fetch XML metadata.
2. Year-by-year bulk XML fallback.
3. Curated fallback list of ~30 well-known series.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Annotated, Any

logger = logging.getLogger(__name__)

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

_BASE_URL = "https://www.bankofengland.co.uk"
_XML_URL = f"{_BASE_URL}/boeapps/database/_iadb-fromshowcolumns.asp"
_TABLES_URL = f"{_BASE_URL}/boeapps/database/tables.asp"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

_EC_PATTERN = re.compile(r"EC=([A-Z0-9]+)", re.IGNORECASE)

_BATCH_SIZE = 100
_REQUEST_DELAY = 1.0

# Well-known BOE series codes — last-resort fallback.
_FALLBACK_CODES = [
    "IUDBEDR", "IUDSOIA",
    "LPMAUYN", "LPMAVQK", "LPMB3TA", "LPMB3UB", "LPMB59E",
    "D7BT", "CZBH", "ABMI", "IHYP",
    "MGSX", "MGSV", "LF24",
    "BOKI", "BOKH",
    "LPMVWTI", "LPMVWTL",
    "RPMB3QA", "RPMB3QB",
    "LPQAUYN", "LPQAVQK",
    "XUDLUSS", "XUDLERS", "XUDLJYS", "XUDLBK73",
    "IUMABEDR", "IUMAAMIJ",
]


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BoeFetchParams(BaseModel):
    """Parameters for fetching Bank of England time series."""

    series_ids: Annotated[str, Namespace("boe")] = Field(
        ...,
        description="Comma-separated BOE series codes (max 300, e.g. IUDBEDR,IUDLOBS)",
    )
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_ids")
    @classmethod
    def _validate(cls, v: str) -> str:
        ids = [s.strip() for s in v.split(",") if s.strip()]
        if not ids:
            raise ValueError("At least one series code required")
        if len(ids) > 300:
            raise ValueError("Maximum 300 series per request")
        return ",".join(ids)


class BoeEnumerateParams(BaseModel):
    """No parameters needed — discovers series from the BOE database."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BOE_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="boe"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

BOE_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="boe"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# XML parsing helpers
# ---------------------------------------------------------------------------


def _extract_category(concat_path: str) -> str | None:
    skip = ("not seasonally", "seasonally adjusted", "sa", "nsa", "not_available")
    segments = [s.strip() for s in concat_path.split("#") if s.strip()]
    for seg in segments:
        if seg.lower() not in skip and not seg.lower().startswith(skip):
            return seg
    return segments[0] if segments else None


def _parse_series_xml(xml_text: str) -> list[dict[str, str]]:
    """Parse BOE XML response into catalog rows."""
    stripped = xml_text.lstrip()
    if stripped.startswith(("<!DOCTYPE", "<!doctype", "<html", "<HTML")):
        return []
    if not stripped.startswith(("<?xml", "\ufeff<?xml", "<Envelope")):
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for elem in root.iter():
        tag = elem.tag.split("}", 1)[-1] if "}" in elem.tag else elem.tag
        if tag != "Cube" or not elem.get("SCODE"):
            continue

        sid = elem.get("SCODE", "")
        if sid in seen:
            continue
        seen.add(sid)

        title = (elem.get("DESC") or sid).strip()
        concat = (elem.get("CONCAT") or "").strip()
        category = _extract_category(concat) if concat else ""
        frequency = ""

        for child in elem:
            ctag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
            if ctag == "Cube" and child.get("FREQ_NAME") and not frequency:
                frequency = (child.get("FREQ_NAME") or "").strip()

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


def _format_boe_date(iso_date: str) -> str:
    dt = datetime.strptime(iso_date, "%Y-%m-%d")
    return f"{dt.day:02d}/{_MONTH_ABBR[dt.month]}/{dt.year}"


@connector(output=BOE_FETCH_OUTPUT, tags=["macro", "gb"])
async def boe_fetch(params: BoeFetchParams) -> Result:
    """Fetch Bank of England time series by series code(s).

    Uses the BOE XML endpoint which returns series metadata + observations.
    """
    http = HttpClient(_BASE_URL, headers={"User-Agent": _USER_AGENT})

    req_params: dict[str, Any] = {
        "CodeVer": "new",
        "xml.x": "yes",
        "SeriesCodes": params.series_ids,
    }
    if params.start_date:
        req_params["Datefrom"] = _format_boe_date(params.start_date)
    if params.end_date:
        req_params["Dateto"] = _format_boe_date(params.end_date)

    response = await http.request(
        "GET", "/boeapps/database/_iadb-fromshowcolumns.asp", params=req_params,
    )
    response.raise_for_status()

    # Detect error page redirect
    resp_url = str(response.url)
    if "errorpage" in resp_url.lower():
        raise ValueError(f"BOE redirected to error page for: {params.series_ids}")

    xml_text = response.text
    if xml_text.lstrip().startswith(("<!DOCTYPE", "<html", "<HTML")):
        raise ValueError(f"BOE returned HTML instead of XML for: {params.series_ids}")

    root = ET.fromstring(xml_text)

    all_rows: list[dict[str, Any]] = []
    for elem in root.iter():
        tag = elem.tag.split("}", 1)[-1] if "}" in elem.tag else elem.tag
        if tag != "Cube" or not elem.get("SCODE"):
            continue

        sid = elem.get("SCODE", "")
        title = (elem.get("DESC") or sid).strip()

        # Extract observations from nested Cube elements with TIME/OBS_VALUE
        for child in elem.iter():
            ctag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
            if ctag != "Cube":
                continue
            time_val = child.get("TIME")
            obs_val = child.get("OBS_VALUE")
            if time_val is None:
                continue
            try:
                value = float(obs_val) if obs_val not in (None, "", "NaN") else None
            except (ValueError, TypeError):
                value = None
            all_rows.append({
                "series_id": sid, "title": title,
                "date": time_val, "value": value,
            })

    if not all_rows:
        raise ValueError(f"No observations parsed for: {params.series_ids}")

    return Result.from_dataframe(
        pd.DataFrame(all_rows),
        Provenance(
            source="boe", params={"series_ids": params.series_ids},
            properties={"source_url": "https://www.bankofengland.co.uk/boeapps/database/"},
        ),
    )


@enumerator(output=BOE_ENUMERATE_OUTPUT, tags=["macro", "gb"])
async def enumerate_boe(params: BoeEnumerateParams) -> pd.DataFrame:
    """Discover BOE series by scraping tables.asp for codes, then batch-fetching XML metadata.

    Cascading: tables.asp scrape → year-by-year bulk XML → curated fallback.
    """
    import httpx

    async with httpx.AsyncClient(
        timeout=120.0, headers={"User-Agent": _USER_AGENT}, follow_redirects=True,
    ) as client:
        # --- Primary: scrape tables.asp for EC= codes ---
        rows: list[dict[str, str]] = []
        try:
            resp = await client.get(_TABLES_URL)
            resp.raise_for_status()
            codes = list(dict.fromkeys(m.group(1).upper() for m in _EC_PATTERN.finditer(resp.text)))

            if codes:
                rows = await _batch_fetch_xml_metadata(client, codes)
        except httpx.HTTPError as exc:
            logger.warning("BOE tables.asp scrape failed: %s", exc)
        except ET.ParseError as exc:
            logger.warning("BOE XML parse error during scrape: %s", exc)

        # --- Fallback: year-by-year bulk XML ---
        if len(rows) < 100:
            try:
                year_rows = await _year_by_year_bulk(client)
                seen = {r["series_id"] for r in rows}
                for r in year_rows:
                    if r["series_id"] not in seen:
                        seen.add(r["series_id"])
                        rows.append(r)
            except httpx.HTTPError as exc:
                logger.warning("BOE year-by-year bulk fallback failed: %s", exc)

        # --- Last resort: curated fallback ---
        if len(rows) < 30:
            try:
                fallback = await _batch_fetch_xml_metadata(client, _FALLBACK_CODES)
                seen = {r["series_id"] for r in rows}
                for r in fallback:
                    if r["series_id"] not in seen:
                        rows.append(r)
            except httpx.HTTPError as exc:
                logger.warning("BOE curated fallback failed: %s", exc)

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_id", "title", "category", "frequency"]
    )


async def _batch_fetch_xml_metadata(
    client: Any, codes: list[str],
) -> list[dict[str, str]]:
    """Batch-fetch XML metadata for a list of series codes."""
    all_rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for i in range(0, len(codes), _BATCH_SIZE):
        batch = codes[i : i + _BATCH_SIZE]
        try:
            resp = await client.get(
                _XML_URL,
                params={
                    "CodeVer": "new", "xml.x": "yes",
                    "SeriesCodes": ",".join(batch),
                    "Datefrom": datetime.now().strftime("01/%b/%Y"),
                    "Dateto": datetime.now().strftime("01/%b/%Y"),
                    "Omit": "-G",
                },
            )
            resp.raise_for_status()
            for row in _parse_series_xml(resp.text):
                if row["series_id"] not in seen:
                    seen.add(row["series_id"])
                    all_rows.append(row)
        except Exception:
            pass
        await asyncio.sleep(_REQUEST_DELAY)

    return all_rows


async def _year_by_year_bulk(client: Any) -> list[dict[str, str]]:
    """Fallback: fetch SeriesCodes=all with narrow year ranges."""
    now = datetime.now()
    current_year = now.year
    ranges = [
        (f"01/Jan/{y}", f"01/Jan/{y + 1}")
        for y in range(current_year - 3, current_year + 1)
    ]
    all_rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for date_from, date_to in ranges:
        try:
            resp = await client.get(
                _XML_URL,
                params={
                    "CodeVer": "new", "xml.x": "yes",
                    "SeriesCodes": "all",
                    "Datefrom": date_from, "Dateto": date_to,
                    "Omit": "-G",
                },
            )
            resp.raise_for_status()
            for row in _parse_series_xml(resp.text):
                if row["series_id"] not in seen:
                    seen.add(row["series_id"])
                    all_rows.append(row)
        except Exception:
            pass
        await asyncio.sleep(_REQUEST_DELAY)

    return all_rows


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([boe_fetch])
CONNECTORS = Connectors([boe_fetch, enumerate_boe])
