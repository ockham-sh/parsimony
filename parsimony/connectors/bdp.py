"""Banco de Portugal (BdP): fetch + catalog enumeration.

API docs: https://bpstat.bportugal.pt/data/docs
No authentication required.

"""

from __future__ import annotations

import logging
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


_BASE_URL = "https://bpstat.bportugal.pt/data/v1"


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BdpFetchParams(BaseModel):
    """Parameters for fetching Banco de Portugal time series."""

    domain_id: int = Field(
        ..., description="Domain ID (use enumerate to discover)"
    )
    dataset_id: Annotated[str, Namespace("bdp")] = Field(
        ..., description="Dataset ID within the domain"
    )
    series_ids: str | None = Field(
        default=None,
        description="Comma-separated series IDs to filter (optional)",
    )
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    lang: str = Field(default="en", description="Language: en or pt")

    @field_validator("dataset_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("dataset_id must be non-empty")
        return v


class BdpEnumerateParams(BaseModel):
    """No parameters needed — enumerates BdP domains and datasets."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BDP_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="dataset_id", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="domain", role=ColumnRole.METADATA),
    ]
)

BDP_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, param_key="dataset_id", namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _infer_frequency(dates: list[str]) -> str:
    """Infer frequency from date spacing."""
    if len(dates) < 2:
        return "annual"
    try:
        from datetime import datetime

        d1 = datetime.strptime(dates[0], "%Y-%m-%d")
        d2 = datetime.strptime(dates[1], "%Y-%m-%d")
        diff = abs((d2 - d1).days)
        if diff <= 1:
            return "daily"
        if diff <= 35:
            return "monthly"
        if diff <= 100:
            return "quarterly"
        return "annual"
    except (ValueError, IndexError):
        return "annual"


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BDP_FETCH_OUTPUT, tags=["macro", "pt"])
async def bdp_fetch(params: BdpFetchParams) -> Result:
    """Fetch Banco de Portugal time series by domain and dataset ID.

    Uses the BPstat API. Two-step workflow: domain → dataset → observations.
    """
    url = f"{_BASE_URL}/domains/{params.domain_id}/datasets/{params.dataset_id}/"
    req_params: dict[str, str] = {"lang": params.lang.upper()}

    if params.series_ids:
        req_params["series_ids"] = params.series_ids
    if params.start_date:
        req_params["obs_since"] = params.start_date
    if params.end_date:
        req_params["obs_to"] = params.end_date

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(url, params=req_params)
        response.raise_for_status()
        json_data = response.json()

    # Parse JSON-stat style response
    time_dim_key = None
    role = json_data.get("role", {})
    time_dims = role.get("time", [])
    if time_dims:
        time_dim_key = time_dims[0]

    dimension = json_data.get("dimension", {})
    dates: list[str] = []
    if time_dim_key and time_dim_key in dimension:
        cat = dimension[time_dim_key].get("category", {})
        index = cat.get("index", {})
        if isinstance(index, dict):
            dates = list(index.keys())
        elif isinstance(index, list):
            dates = index

    raw_values = json_data.get("value", [])
    # JSON-stat value can be a dict with string keys or a list
    if isinstance(raw_values, dict):
        values_list: list[Any] = [raw_values.get(str(i)) for i in range(max(int(k) for k in raw_values) + 1)] if raw_values else []
    else:
        values_list = list(raw_values)

    if not dates or not values_list:
        raise EmptyDataError(
            provider="bdp",
            message=f"No observations for domain={params.domain_id}, dataset={params.dataset_id}",
        )

    # Extract series metadata
    series_info = json_data.get("extension", {}).get("series", [])
    n_dates = len(dates)
    n_series = len(values_list) // n_dates if n_dates else 1

    rows: list[dict[str, Any]] = []
    for s_idx in range(n_series):
        sid = str(series_info[s_idx]["id"]) if s_idx < len(series_info) else str(s_idx)
        label = series_info[s_idx].get("label", sid) if s_idx < len(series_info) else sid

        for d_idx, date_str in enumerate(dates):
            val_idx = s_idx * n_dates + d_idx
            if val_idx >= len(values_list):
                break
            raw = values_list[val_idx]
            try:
                value = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                value = None
            rows.append({
                "series_id": sid,
                "title": label,
                "date": date_str,
                "value": value,
            })

    if not rows:
        raise EmptyDataError(
            provider="bdp",
            message=f"No observations parsed for domain={params.domain_id}, dataset={params.dataset_id}",
        )

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="bdp",
            params={
                "domain_id": params.domain_id,
                "dataset_id": params.dataset_id,
            },
            properties={"source_url": "https://bpstat.bportugal.pt"},
        ),
    )


@enumerator(output=BDP_ENUMERATE_OUTPUT, tags=["macro", "pt"])
async def enumerate_bdp(params: BdpEnumerateParams) -> pd.DataFrame:
    """Enumerate Banco de Portugal datasets via domain → dataset traversal.

    77 domains, ~216 datasets, ~72K series. Enumerates at dataset level
    (series are discovered on fetch via extension.series).
    """
    rows: list[dict[str, str]] = []

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        # Get all domains (trailing slash required)
        domain_resp = await client.get(f"{_BASE_URL}/domains/", params={"lang": "EN"})
        domain_resp.raise_for_status()
        domains = domain_resp.json()

        if not isinstance(domains, list):
            domains = [domains]

        # Only process leaf domains (has_series=true)
        leaf_domains = [d for d in domains if d.get("has_series", False)]

        for domain in leaf_domains:
            domain_id = domain.get("id", "")
            domain_name = domain.get("label", domain.get("description", str(domain_id)))
            num_series = domain.get("num_series", 0)

            # Get datasets for this domain
            try:
                ds_resp = await client.get(
                    f"{_BASE_URL}/domains/{domain_id}/datasets/",
                    params={"lang": "EN"},
                )
                if ds_resp.status_code != 200:
                    continue
                ds_data = ds_resp.json()

                # BPstat returns SDMX-like structure with link.item[]
                items = ds_data
                if isinstance(ds_data, dict):
                    items = ds_data.get("link", {}).get("item", [])
                if not isinstance(items, list):
                    items = [items]

                for item in items:
                    ext = item.get("extension", {})
                    did = str(ext.get("id", "")).strip()
                    # Label is at item level, not in extension
                    label = item.get("label", ext.get("label", did))
                    if did:
                        rows.append({
                            "dataset_id": did,
                            "title": str(label).strip(),
                            "domain": domain_name,
                        })
            except (httpx.HTTPError, Exception) as exc:
                logger.debug("BdP enumerate failed for domain %s: %s", domain_id, exc)

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["dataset_id", "title", "domain"]
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([bdp_fetch])
CONNECTORS = Connectors([bdp_fetch, enumerate_bdp])
