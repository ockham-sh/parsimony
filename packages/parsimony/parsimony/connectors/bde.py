"""Banco de España (BdE): fetch + catalog enumeration.

API docs: https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html
Series search: https://app.bde.es/bie_www/bie_wwwias/xml/Arranque.html (BIEST)
No authentication required.

"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from typing import Annotated, Any

import httpx
import pandas as pd
from pydantic import BaseModel, Field, field_validator

from parsimony.connector import Connectors, Namespace, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)

logger = logging.getLogger(__name__)


_BASE_URL = "https://app.bde.es/bierest/resources/srdatosapp"

# Spanish → English column mapping
_COLUMN_MAP = {
    "serie": "key",
    "descripcion": "description",
    "descripcionCorta": "title",
    "codFrecuencia": "freq",
    "decimales": "decimals",
    "simbolo": "symbol",
    "fechaInicio": "start_date",
    "fechaFin": "end_date",
    "fechas": "date",
    "valores": "value",
}

# Frequency code → human-readable
_FREQ_MAP = {
    "D": "Daily",
    "M": "Monthly",
    "Q": "Quarterly",
    "A": "Annual",
    "S": "Semi-annual",
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BdeFetchParams(BaseModel):
    """Parameters for fetching Banco de España time series."""

    key: Annotated[str, Namespace("bde")] = Field(
        ...,
        description="Comma-separated BdE series codes (e.g. D_1NBAF472)",
    )
    time_range: str | None = Field(
        default=None,
        description=("Time range: 30M, 60M, MAX, or a year (e.g. 2024). Default uses the full available range."),
    )
    lang: str = Field(default="en", description="Language: en or es")

    @field_validator("key")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("At least one series code required")
        return v

    @field_validator("time_range")
    @classmethod
    def _valid_range(cls, v: str | None) -> str | None:
        if v is None:
            return v
        v = v.strip()
        # BdE API rejects short range codes (3M, 12M); accept 30M, 60M, MAX, or year
        _VALID_RANGES = {"30M", "60M", "MAX"}
        if v.upper() in _VALID_RANGES or v.isdigit():
            return v
        raise ValueError(f"Invalid time_range '{v}'. Use 30M, 60M, MAX, or a year (e.g. 2024).")

    @field_validator("lang")
    @classmethod
    def _valid_lang(cls, v: str) -> str:
        if v not in ("en", "es"):
            raise ValueError("lang must be 'en' or 'es'")
        return v


class BdeEnumerateParams(BaseModel):
    """No parameters needed — discovers series from BdE."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BDE_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

BDE_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, param_key="key", namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_bde_response(json_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Parse BdE JSON response into a long-format DataFrame.

    Each element in json_data represents one series with parallel
    fechas (dates) and valores (values) arrays.
    """
    all_rows: list[dict[str, Any]] = []

    for series in json_data:
        key = series.get("serie", "")
        title = series.get("descripcionCorta", series.get("descripcion", key))
        series.get("codFrecuencia", "")
        dates = series.get("fechas", [])
        values = series.get("valores", [])

        if not dates or not values:
            continue

        for date_str, raw_value in zip(dates, values, strict=False):
            try:
                value = float(raw_value) if raw_value not in (None, "", "NaN") else None
            except (ValueError, TypeError):
                value = None

            # Parse ISO datetime: "2024-01-31T00:00:00Z" → date
            date_val = date_str
            if isinstance(date_str, str) and "T" in date_str:
                with contextlib.suppress(ValueError):
                    date_val = datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%Y-%m-%d")

            all_rows.append(
                {
                    "key": key,
                    "title": title,
                    "date": date_val,
                    "value": value,
                }
            )

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=["key", "title", "date", "value"])


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BDE_FETCH_OUTPUT, tags=["macro", "es"])
async def bde(params: BdeFetchParams) -> Result:
    """Fetch Banco de España time series by series code(s).

    Uses the BdE REST API (BIEST). Returns date + value with series metadata.
    """
    url = f"{_BASE_URL}/listaSeries"

    # BdE API: fetch each series individually and merge results.
    # Multi-series in a single request is unreliable (412 errors).
    keys = [k.strip() for k in params.key.split(",") if k.strip()]
    json_data: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        for key in keys:
            req_params: dict[str, str] = {
                "idioma": params.lang,
                "series": key,
            }
            if params.time_range is not None:
                req_params["rango"] = str(params.time_range)

            response = await client.get(url, params=req_params)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                json_data.extend(data)

    if not isinstance(json_data, list) or not json_data:
        raise EmptyDataError(provider="bde", message=f"BdE returned empty or invalid response for: {params.key}")

    df = _parse_bde_response(json_data)
    if df.empty:
        raise EmptyDataError(provider="bde", message=f"No observations parsed for: {params.key}")

    return Result.from_dataframe(
        df,
        Provenance(
            source="bde",
            params={"key": params.key, "time_range": params.time_range},
            properties={
                "source_url": "https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html",
            },
        ),
    )


@enumerator(output=BDE_ENUMERATE_OUTPUT, tags=["macro", "es"])
async def enumerate_bde(params: BdeEnumerateParams) -> pd.DataFrame:
    """Enumerate BdE series by fetching well-known series codes and their metadata.

    Uses the favoritas (latest) endpoint with known series codes.
    """
    # Well-known BdE series codes for catalog seeding
    known_codes = [
        "D_1NBAF472",  # Interest rates
        "DTNPDE2010_P0000P_PS_APU",  # National accounts
        "DTNSEC2010_S0000P_APU_SUMAMOVIL",  # National accounts
        "BE_1_1_1",  # Balance sheet
        "BE_1_1_2",  # Balance sheet
        "SI_1_1",  # Financial indicators
        "SI_1_2",  # Financial indicators
        "TI_1_1",  # Interest rates
        "TI_1_2",  # Interest rates
    ]

    rows: list[dict[str, str]] = []
    url = f"{_BASE_URL}/favoritas"

    async with httpx.AsyncClient(timeout=60.0) as client:
        for code in known_codes:
            try:
                response = await client.get(url, params={"idioma": "en", "series": code})
                if response.status_code != 200:
                    continue
                json_data = response.json()
                if not isinstance(json_data, list):
                    continue
                for series in json_data:
                    key = series.get("serie", "")
                    title = series.get("descripcionCorta", key)
                    freq_code = series.get("codFrecuencia", "")
                    if key:
                        rows.append(
                            {
                                "key": key,
                                "title": title,
                                "frequency": _FREQ_MAP.get(freq_code, freq_code),
                            }
                        )
            except (httpx.HTTPError, Exception) as exc:
                logger.debug("BdE enumerate failed for %s: %s", code, exc)

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["key", "title", "frequency"])


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

CONNECTORS = Connectors([bde, enumerate_bde])
