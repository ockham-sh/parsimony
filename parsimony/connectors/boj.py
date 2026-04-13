"""Bank of Japan (BoJ): fetch + catalog enumeration.

API manual: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf
No authentication required. Max 250 codes per request.

"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import httpx
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

logger = logging.getLogger(__name__)


_BASE_URL = "https://www.stat-search.boj.or.jp/api/v1"

_FREQ_MAP = {
    "am": "Annual",
    "qm": "Quarterly",
    "mm": "Monthly",
    "dm": "Daily",
    "sm": "Semi-annual",
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BojFetchParams(BaseModel):
    """Parameters for fetching Bank of Japan time series."""

    db: str = Field(
        ...,
        description="Database code (e.g. FM08 for FX rates, PR01 for prices)",
    )
    code: Annotated[str, Namespace("boj")] = Field(
        ...,
        description="Comma-separated series codes (max 250, e.g. FXERD01)",
    )
    start_date: str | None = Field(
        default=None,
        description="Start date (YYYYMMDD, YYYYMM, or YYYY depending on frequency)",
    )
    end_date: str | None = Field(
        default=None,
        description="End date (same format as start_date)",
    )
    lang: str = Field(default="en", description="Language: en or jp")

    @field_validator("db")
    @classmethod
    def _non_empty_db(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("db must be non-empty")
        return v

    @field_validator("code")
    @classmethod
    def _validate_codes(cls, v: str) -> str:
        codes = [s.strip() for s in v.split(",") if s.strip()]
        if not codes:
            raise ValueError("At least one series code required")
        if len(codes) > 250:
            raise ValueError("Maximum 250 codes per request")
        return ",".join(codes)


class BojEnumerateParams(BaseModel):
    """No parameters needed — enumerates BoJ statistics databases."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BOJ_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="database", role=ColumnRole.METADATA),
    ]
)

BOJ_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, param_key="code", namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_boj_date(date_str: str, freq: str) -> str:
    """Parse BoJ date string based on frequency code."""
    freq_lower = freq.lower()
    if freq_lower in ("dm", "daily"):
        # YYYYMMDD
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    elif freq_lower in ("mm", "monthly"):
        # YYYYMM
        if len(date_str) >= 6:
            return f"{date_str[:4]}-{date_str[4:6]}-01"
    elif freq_lower in ("qm", "quarterly"):
        # YYYYQQ where QQ is 01-04
        if len(date_str) >= 6:
            quarter = int(date_str[4:6])
            month = (quarter - 1) * 3 + 1
            return f"{date_str[:4]}-{month:02d}-01"
    elif freq_lower in ("am", "annual"):
        if len(date_str) >= 4:
            return f"{date_str[:4]}-01-01"
    return date_str


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BOJ_FETCH_OUTPUT, tags=["macro", "jp"])
async def boj_fetch(params: BojFetchParams) -> Result:
    """Fetch Bank of Japan time series by database and series code(s).

    Returns date + value with series metadata. Max 250 codes per request.
    """
    url = f"{_BASE_URL}/getDataCode"
    req_params: dict[str, str] = {
        "db": params.db,
        "code": params.code,
        "lang": params.lang,
    }
    if params.start_date:
        req_params["startDate"] = params.start_date
    if params.end_date:
        req_params["endDate"] = params.end_date

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url, params=req_params)
        response.raise_for_status()
        json_data = response.json()

    result_set = json_data.get("RESULTSET", [])
    if not result_set:
        raise EmptyDataError(provider="boj", message=f"No data returned for db={params.db}, code={params.code}")

    rows: list[dict[str, Any]] = []
    for series in result_set:
        code = series.get("SERIES_CODE", "")
        name = series.get("NAME_OF_TIME_SERIES", series.get("NAME_OF_TIME_SERIES_J", code))
        freq = series.get("FREQUENCY", "").lower()
        dates = series.get("VALUES", {}).get("SURVEY_DATES", [])
        values = series.get("VALUES", {}).get("VALUES", [])

        if isinstance(dates, str):
            dates = [dates]
        if isinstance(values, (str, int, float)):
            values = [values]

        for date_str, raw_value in zip(dates, values):
            try:
                value = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                value = None
            if value is None:
                continue
            rows.append({
                "code": code,
                "title": name,
                "date": _parse_boj_date(str(date_str), freq),
                "value": value,
            })

    if not rows:
        raise EmptyDataError(provider="boj", message=f"No observations parsed for db={params.db}, code={params.code}")

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="boj",
            params={"db": params.db, "code": params.code},
            properties={"source_url": "https://www.stat-search.boj.or.jp"},
        ),
    )


@enumerator(output=BOJ_ENUMERATE_OUTPUT, tags=["macro", "jp"])
async def enumerate_boj(params: BojEnumerateParams) -> pd.DataFrame:
    """Enumerate Bank of Japan series by querying metadata for known databases."""
    # All 45 BoJ database codes (discovered via brute-force scan)
    known_dbs = [
        "BP01", "BP02",  # Balance of payments
        "BS01", "BS02",  # Bank accounts / banking statistics
        "FM01", "FM02", "FM03", "FM04", "FM05", "FM06", "FM07", "FM08", "FM09",  # Financial markets
        "IR01", "IR02", "IR03", "IR04",  # Interest rates
        "LA01", "LA02", "LA03", "LA04", "LA05",  # Loans
        "MD01", "MD02", "MD03", "MD04", "MD05", "MD06", "MD07", "MD08",  # Monetary
        "MD09", "MD10", "MD11", "MD12", "MD13", "MD14",  # Monetary (continued)
        "OB01", "OB02",  # BoJ operations
        "PF01", "PF02",  # Public finance
        "PR01", "PR02", "PR03", "PR04",  # Prices
        "PS01", "PS02",  # Payment systems
    ]

    rows: list[dict[str, str]] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        for db in known_dbs:
            try:
                url = f"{_BASE_URL}/getMetadata"
                response = await client.get(url, params={"db": db, "lang": "en"})
                if response.status_code != 200:
                    continue
                json_data = response.json()
                result_set = json_data.get("RESULTSET", [])
                for series in result_set:
                    code = series.get("SERIES_CODE", "")
                    name = series.get("NAME_OF_TIME_SERIES", code)
                    freq = series.get("FREQUENCY", "").lower()
                    if code:
                        rows.append({
                            "code": code,
                            "title": name,
                            "frequency": _FREQ_MAP.get(freq, freq),
                            "database": db,
                        })
            except (httpx.HTTPError, Exception) as exc:
                logger.debug("BoJ enumerate failed for db %s: %s", db, exc)

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["code", "title", "frequency", "database"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([boj_fetch])
CONNECTORS = Connectors([boj_fetch, enumerate_boj])
