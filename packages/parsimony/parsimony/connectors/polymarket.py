"""Polymarket source: Gamma/CLOB HTTP API with expand + response_path."""

from __future__ import annotations

import json
from typing import Any, Literal

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from parsimony.connector import Connectors, connector
from parsimony.errors import ProviderError
from parsimony.result import Provenance, Result
from parsimony.transport.http import HttpClient
from parsimony.transport.json_helpers import interpolate_path


class PolymarketFetchParams(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: Literal["GET", "POST"] = "GET"
    path: str = Field(..., min_length=1)
    response_path: str | None = Field(default=None, description="Dot-separated path into JSON")
    expand: Literal["markets", "outcomes"] | None = Field(default=None, description="Expand nested data")


def _expand_markets(df: pd.DataFrame) -> pd.DataFrame:
    if "markets" not in df.columns or df["markets"].iloc[0] is None:
        return df
    markets = df["markets"].iloc[0]
    if not isinstance(markets, list) or len(markets) == 0:
        return df
    rows: list[dict[str, Any]] = []
    for market in markets:
        if isinstance(market, dict):
            outcomes = market.get("outcomes")
            outcomes_count = 0
            if outcomes:
                try:
                    if isinstance(outcomes, str):
                        outcomes = json.loads(outcomes)
                    outcomes_count = len(outcomes) if isinstance(outcomes, list) else 0
                except (json.JSONDecodeError, TypeError):
                    pass
            rows.append(
                {
                    "market_slug": market.get("slug"),
                    "market_question": market.get("question"),
                    "market_description": market.get("description", ""),
                    "market_outcomes_count": outcomes_count,
                    "market_liquidity": market.get("liquidity", 0),
                    "market_volume": market.get("volume", 0),
                    "market_active": market.get("active", True),
                    "market_closed": market.get("closed", False),
                }
            )
    return pd.DataFrame(rows)


def _expand_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    if "outcomes" not in df.columns or "clobTokenIds" not in df.columns:
        return df
    outcomes = df["outcomes"].iloc[0]
    clob_token_ids = df["clobTokenIds"].iloc[0]
    try:
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(clob_token_ids, str):
            clob_token_ids = json.loads(clob_token_ids)
    except (json.JSONDecodeError, TypeError):
        return df
    if not isinstance(outcomes, list) or not isinstance(clob_token_ids, list) or len(outcomes) == 0:
        return df
    rows = []
    for idx, outcome in enumerate(outcomes):
        rows.append(
            {
                "outcome": outcome,
                "outcome_clob_token_id": clob_token_ids[idx] if idx < len(clob_token_ids) else None,
            }
        )
    return pd.DataFrame(rows)


def make_polymarket_connector(
    base_url: str,
    source_name: str,
    description: str | None = None,
) -> Any:
    """Build a Polymarket :class:`~parsimony.connector.Connector` for a given API base URL."""
    http = HttpClient(base_url, timeout=10.0)
    desc = description or f"Polymarket HTTP API ({source_name})."

    # Escape hatch: implementation is `_fetch`; public connector name is the source name.
    @connector(name=source_name, description=desc, tags=["polymarket"])
    async def _fetch(params: PolymarketFetchParams) -> Result:
        raw = params.model_dump()
        raw.update(params.model_extra or {})
        method = str(raw.pop("method", "GET")).upper()
        path = raw.pop("path")
        response_path = raw.pop("response_path", None)
        expand = raw.pop("expand", None)

        rendered_path, request_params = interpolate_path(path, raw)

        try:
            response = await http.request(method, rendered_path, params=request_params)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ProviderError(
                provider="polymarket",
                status_code=e.response.status_code,
                message=f"HTTP {e.response.status_code}: {e.response.text}",
            ) from e

        data = response.json()
        if response_path:
            for part in response_path.split("."):
                if isinstance(data, dict) and part in data:
                    data = data[part]
                else:
                    data = []
                    break

        df = pd.DataFrame(data if isinstance(data, list) else [data])

        if expand and len(df) == 1:
            if expand == "markets":
                df = _expand_markets(df)
            elif expand == "outcomes":
                df = _expand_outcomes(df)
        elif "markets" in df.columns:
            df = df.copy()
            df["markets_count"] = df["markets"].apply(lambda x: len(x) if isinstance(x, list) else 0)

        return Result.from_dataframe(
            df,
            Provenance(source=source_name, params=params.model_dump()),
        )

    return _fetch


_GAMMA_BASE = "https://gamma-api.polymarket.com"
_CLOB_BASE = "https://clob.polymarket.com"

POLYMARKET_GAMMA = make_polymarket_connector(
    _GAMMA_BASE,
    "polymarket_gamma",
    description=f"Polymarket Gamma API ({_GAMMA_BASE}).",
)
POLYMARKET_CLOB = make_polymarket_connector(
    _CLOB_BASE,
    "polymarket_clob",
    description=f"Polymarket CLOB API ({_CLOB_BASE}).",
)

CONNECTORS = Connectors([POLYMARKET_GAMMA, POLYMARKET_CLOB])
