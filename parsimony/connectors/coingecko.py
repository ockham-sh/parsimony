"""CoinGecko source: crypto market data via CoinGecko and GeckoTerminal APIs.

API docs: https://docs.coingecko.com/v3.0.1/reference/authentication
Authentication: Demo API key via ``x-cg-demo-api-key`` header.
Base URL: https://api.coingecko.com/api/v3
GeckoTerminal (on-chain): /onchain appended to base URL.
Rate limit: 30 calls/min (Demo plan).

Provides 11 connectors:
  - Discovery: search, trending coins, top gainers/losers
  - Market data: price, markets listing, coin detail
  - Historical: market chart (days or range), OHLC candlesticks
  - On-chain: token price by contract address (GeckoTerminal)
  - Enumerator: full coin list for catalog indexing
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from parsimony.connector import (
    Connectors,
    EmptyDataError,
    Namespace,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
    connector,
    enumerator,
)
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient


ENV_VARS: dict[str, str] = {"api_key": "COINGECKO_API_KEY"}

_BASE_URL = "https://api.coingecko.com/api/v3"
_TIMEOUT = 15.0


# ---------------------------------------------------------------------------
# Transport helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        _BASE_URL,
        headers={"x-cg-demo-api-key": api_key},
        timeout=_TIMEOUT,
    )


def _iso_to_unix(date_str: str) -> int:
    """Convert YYYY-MM-DD ISO date string to Unix timestamp (seconds, UTC)."""
    dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


async def _cg_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared CoinGecko GET with typed error mapping.

    Returns the parsed JSON body. Raises typed connector exceptions.
    """
    try:
        response = await http.request("GET", path, params=params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        match status:
            case 401:
                # CoinGecko reuses 401 for plan-gated restrictions:
                #   error_code 10005 = Pro-only endpoint
                #   error_code 10012 = date range exceeds Demo limit (365 days)
                # Body format: {"status": {"error_code": N, "error_message": "..."}}
                # or:          {"error": {"status": {"error_code": N, ...}}}
                try:
                    body = e.response.json()
                    status = body.get("status") or body.get("error", {}).get("status", {})
                    code = status.get("error_code", 0)
                    msg = status.get("error_message", "")
                    if code in (10005, 10006, 10012):
                        raise PaymentRequiredError(
                            provider="coingecko",
                            message=f"CoinGecko plan restriction (error_code={code}): {msg}",
                        ) from e
                except (ValueError, AttributeError):
                    pass
                raise UnauthorizedError(
                    provider="coingecko",
                    message="Invalid or missing CoinGecko API key",
                ) from e
            case 402:
                raise PaymentRequiredError(
                    provider="coingecko",
                    message="Your CoinGecko plan does not include this endpoint",
                ) from e
            case 429:
                retry_after = float(e.response.headers.get("Retry-After", "60"))
                raise RateLimitError(
                    provider="coingecko",
                    retry_after=retry_after,
                    message=f"CoinGecko rate limit hit on '{op_name}', retry after {retry_after:.0f}s",
                ) from e
            case _:
                # CoinGecko returns plan-gated errors as non-standard codes in the body
                try:
                    body = e.response.json()
                    code = body.get("status", {}).get("error_code", 0)
                    if code in (10005, 10006):
                        raise PaymentRequiredError(
                            provider="coingecko",
                            message=f"CoinGecko endpoint requires a higher plan (error_code={code})",
                        ) from e
                except (ValueError, AttributeError):
                    pass
                raise ProviderError(
                    provider="coingecko",
                    status_code=status,
                    message=f"CoinGecko API error {status} on '{op_name}'",
                ) from e

    return response.json()


# ---------------------------------------------------------------------------
# Search / Discovery — OutputConfigs
# ---------------------------------------------------------------------------

_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="thumb", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

_TRENDING_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

_GAINERS_LOSERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="direction", role=ColumnRole.METADATA),
        Column(name="usd_price_percent_change", dtype="numeric"),
    ]
)


# ---------------------------------------------------------------------------
# Search / Discovery — Params + Connectors
# ---------------------------------------------------------------------------


class CoinGeckoSearchParams(BaseModel):
    """Search CoinGecko for coins, exchanges, or NFTs by name or symbol."""

    query: str = Field(..., min_length=1, description="Search term, e.g. 'solana' or 'SOL'")


@connector(output=_SEARCH_OUTPUT, tags=["crypto", "tool"])
async def coingecko_search(params: CoinGeckoSearchParams, *, api_key: str) -> Result:
    """Search CoinGecko for coins by name or symbol. Use this first to resolve
    CoinGecko coin IDs before calling coingecko_price, coingecko_markets, or
    coingecko_market_chart. Returns id (the stable identifier), name, symbol,
    and market_cap_rank.

    Example: query='bitcoin' → id='bitcoin'; query='ETH' → id='ethereum'.
    """
    http = _make_http(api_key)
    data = await _cg_fetch(http, path="/search", params={"query": params.query}, op_name="coingecko_search")

    coins = data.get("coins", [])
    if not coins:
        raise EmptyDataError(
            provider="coingecko",
            message=f"No coins found for query: {params.query}",
        )

    rows = [
        {
            "id": c.get("id", ""),
            "name": c.get("name", ""),
            "symbol": c.get("symbol", ""),
            "market_cap_rank": c.get("market_cap_rank"),
            "thumb": c.get("thumb", ""),
        }
        for c in coins
        if c.get("id")
    ]
    df = pd.DataFrame(rows)
    return _SEARCH_OUTPUT.build_table_result(df, provenance=Provenance(source="coingecko_search", params={"query": params.query}), params={"query": params.query})


class CoinGeckoTrendingParams(BaseModel):
    """No parameters — trending is always the last 24 hours."""

    pass


@connector(output=_TRENDING_OUTPUT, tags=["crypto", "tool"])
async def coingecko_trending(params: CoinGeckoTrendingParams, *, api_key: str) -> Result:
    """[Demo+] Fetch trending coins on CoinGecko in the last 24 hours.
    Returns top 7 trending coins by search volume, with name, symbol,
    market_cap_rank, and trending score. Use coin id with coingecko_price or
    coingecko_market_chart for live data.
    """
    http = _make_http(api_key)
    data = await _cg_fetch(http, path="/search/trending", op_name="coingecko_trending")

    coins = data.get("coins", [])
    if not coins:
        raise EmptyDataError(provider="coingecko", message="No trending coins returned")

    rows = [
        {
            "id": c["item"].get("id", ""),
            "name": c["item"].get("name", ""),
            "symbol": c["item"].get("symbol", ""),
            "market_cap_rank": c["item"].get("market_cap_rank"),
            "score": c["item"].get("score"),
        }
        for c in coins
        if c.get("item", {}).get("id")
    ]
    df = pd.DataFrame(rows)
    return _TRENDING_OUTPUT.build_table_result(df, provenance=Provenance(source="coingecko_trending", params={}), params={})


class CoinGeckoTopMoversParams(BaseModel):
    """Parameters for top gainers and losers."""

    vs_currency: str = Field(
        default="usd",
        description="Target currency for price data, e.g. usd, eur, btc",
    )
    duration: Literal["1h", "24h", "7d", "14d", "30d", "60d", "1y"] = Field(
        default="24h",
        description="Time window: 1h, 24h, 7d, 14d, 30d, 60d, or 1y",
    )
    top_coins: Literal["300", "1000"] = Field(
        default="1000",
        description="Pool size to rank from: 300 or 1000 top coins by market cap",
    )


@connector(output=_GAINERS_LOSERS_OUTPUT, tags=["crypto", "tool"])
async def coingecko_top_gainers_losers(params: CoinGeckoTopMoversParams, *, api_key: str) -> Result:
    """[Demo+] Fetch top gaining and losing coins over a given time window.
    Returns combined rows with a 'direction' column ('gainer' or 'loser') and
    usd_price_percent_change. Use coin id with coingecko_market_chart to dig
    into historical performance.
    """
    http = _make_http(api_key)
    data = await _cg_fetch(
        http,
        path="/coins/top_gainers_losers",
        params={"vs_currency": params.vs_currency, "duration": params.duration, "top_coins": params.top_coins},
        op_name="coingecko_top_gainers_losers",
    )

    rows: list[dict] = []
    for coin in data.get("top_gainers", []):
        rows.append({
            "id": coin.get("id", ""),
            "name": coin.get("name", ""),
            "symbol": coin.get("symbol", ""),
            "direction": "gainer",
            "usd_price_percent_change": coin.get("usd_price_percent_change"),
        })
    for coin in data.get("top_losers", []):
        rows.append({
            "id": coin.get("id", ""),
            "name": coin.get("name", ""),
            "symbol": coin.get("symbol", ""),
            "direction": "loser",
            "usd_price_percent_change": coin.get("usd_price_percent_change"),
        })

    if not rows:
        raise EmptyDataError(provider="coingecko", message="No top gainers/losers returned")

    df = pd.DataFrame(rows)
    return _GAINERS_LOSERS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_top_gainers_losers", params={"vs_currency": params.vs_currency, "duration": params.duration}),
        params={"vs_currency": params.vs_currency, "duration": params.duration},
    )


# ---------------------------------------------------------------------------
# Market Data — OutputConfigs
# ---------------------------------------------------------------------------

_PRICE_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
    ]
)

_MARKETS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="current_price", dtype="numeric"),
        Column(name="market_cap", dtype="numeric"),
        Column(name="total_volume", dtype="numeric"),
        Column(name="high_24h", dtype="numeric"),
        Column(name="low_24h", dtype="numeric"),
        Column(name="price_change_percentage_24h", dtype="numeric"),
        Column(name="ath", dtype="numeric"),
        Column(name="atl", dtype="numeric"),
        Column(name="circulating_supply", dtype="numeric"),
        Column(name="total_supply", dtype="numeric"),
        Column(name="last_updated", role=ColumnRole.METADATA),
    ]
)

_MARKET_CHART_OUTPUT = OutputConfig(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY, dtype="timestamp"),
        Column(name="price", dtype="numeric"),
        Column(name="market_cap", dtype="numeric"),
        Column(name="total_volume", dtype="numeric"),
    ]
)

_OHLC_OUTPUT = OutputConfig(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY, dtype="timestamp"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
    ]
)


# ---------------------------------------------------------------------------
# Market Data — Params + Connectors
# ---------------------------------------------------------------------------


class CoinGeckoPriceParams(BaseModel):
    """Parameters for simple price lookup."""

    ids: str = Field(
        ...,
        description="Comma-separated CoinGecko coin IDs, e.g. 'bitcoin,ethereum'. Use coingecko_search to resolve IDs.",
    )
    vs_currencies: str = Field(
        default="usd",
        description="Comma-separated target currencies, e.g. 'usd,eur,btc'",
    )
    include_market_cap: bool = Field(default=True, description="Include market cap values")
    include_24hr_vol: bool = Field(default=True, description="Include 24h trading volume")
    include_24hr_change: bool = Field(default=True, description="Include 24h price change percentage")


@connector(output=_PRICE_OUTPUT, tags=["crypto"])
async def coingecko_price(params: CoinGeckoPriceParams, *, api_key: str) -> Result:
    """[Demo+] Fetch current price(s) for one or more coins in one or more currencies.
    Returns one row per coin with dynamic columns: {currency}, {currency}_market_cap,
    {currency}_24h_vol, {currency}_24h_change. Use coingecko_search to resolve coin IDs
    first. For full market rankings use coingecko_markets.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "ids": params.ids,
        "vs_currencies": params.vs_currencies,
        "include_market_cap": str(params.include_market_cap).lower(),
        "include_24hr_vol": str(params.include_24hr_vol).lower(),
        "include_24hr_change": str(params.include_24hr_change).lower(),
    }
    data = await _cg_fetch(http, path="/simple/price", params=req, op_name="coingecko_price")

    if not data:
        raise EmptyDataError(
            provider="coingecko",
            message=f"No price data returned for: {params.ids}",
        )

    rows = [{"id": coin_id, **vals} for coin_id, vals in data.items() if isinstance(vals, dict)]
    if not rows:
        raise EmptyDataError(provider="coingecko", message=f"Empty price response for: {params.ids}")

    df = pd.DataFrame(rows)
    return _PRICE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_price", params={"ids": params.ids, "vs_currencies": params.vs_currencies}),
        params={"ids": params.ids},
    )


class CoinGeckoMarketsParams(BaseModel):
    """Parameters for paginated coin market listings."""

    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    ids: str | None = Field(
        default=None,
        description="Comma-separated coin IDs to filter, e.g. 'bitcoin,ethereum'. Omit for top N by market cap.",
    )
    order: Literal[
        "market_cap_desc", "market_cap_asc", "volume_desc", "volume_asc",
        "id_desc", "id_asc",
    ] = Field(default="market_cap_desc", description="Sort order")
    per_page: int = Field(default=100, ge=1, le=250, description="Results per page (max 250)")
    page: int = Field(default=1, ge=1, description="Page number")
    sparkline: bool = Field(default=False, description="Include 7-day sparkline data")


@connector(output=_MARKETS_OUTPUT, tags=["crypto"])
async def coingecko_markets(params: CoinGeckoMarketsParams, *, api_key: str) -> Result:
    """[Demo+] Fetch ranked market data for coins: price, market cap, volume, ATH/ATL,
    24h change. Returns up to 250 coins per page sorted by market_cap_desc by default.
    Pass ids= to retrieve specific coins only. Use coingecko_search to resolve coin IDs.
    For time-series history use coingecko_market_chart.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "vs_currency": params.vs_currency,
        "order": params.order,
        "per_page": params.per_page,
        "page": params.page,
        "sparkline": str(params.sparkline).lower(),
    }
    if params.ids:
        req["ids"] = params.ids

    data = await _cg_fetch(http, path="/coins/markets", params=req, op_name="coingecko_markets")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(provider="coingecko", message="No market data returned")

    df = pd.DataFrame(data)
    return _MARKETS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_markets", params={"vs_currency": params.vs_currency, "page": params.page}),
        params={"vs_currency": params.vs_currency},
    )


class CoinGeckoCoinDetailParams(BaseModel):
    """Parameters for fetching full coin metadata."""

    coin_id: Annotated[str, Namespace("coingecko_coin")] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    localization: bool = Field(default=False, description="Include localized language data (inflates response)")
    tickers: bool = Field(default=False, description="Include exchange ticker data")
    market_data: bool = Field(default=True, description="Include current market data")
    community_data: bool = Field(default=False, description="Include community stats")
    developer_data: bool = Field(default=False, description="Include developer/GitHub stats")


@connector(tags=["crypto"], result_type="dict")
async def coingecko_coin_detail(params: CoinGeckoCoinDetailParams, *, api_key: str) -> Result:
    """[Demo+] Fetch full metadata for a single coin: description, links, categories,
    genesis date, hashing algorithm, current market data, and optional community/
    developer stats. Returns a rich dict — use coingecko_markets for tabular price
    listings across many coins, and coingecko_market_chart for time-series history.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "localization": str(params.localization).lower(),
        "tickers": str(params.tickers).lower(),
        "market_data": str(params.market_data).lower(),
        "community_data": str(params.community_data).lower(),
        "developer_data": str(params.developer_data).lower(),
    }
    data = await _cg_fetch(
        http, path=f"/coins/{params.coin_id}", params=req, op_name="coingecko_coin_detail"
    )

    if not isinstance(data, dict) or "id" not in data:
        raise ParseError(
            provider="coingecko",
            message=f"Unexpected response structure for coin detail: {params.coin_id}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="coingecko_coin_detail", params={"coin_id": params.coin_id}),
    )


# ---------------------------------------------------------------------------
# Historical Data — Params + Connectors
# ---------------------------------------------------------------------------


class CoinGeckoMarketChartParams(BaseModel):
    """Parameters for historical price chart by number of days."""

    coin_id: Annotated[str, Namespace("coingecko_coin")] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    days: str = Field(
        ...,
        description=(
            "Number of days of data: integer (e.g. '30') or 'max' for full history. "
            "Auto-granularity: 1d→5-min, 2-90d→hourly, 90d+→daily. "
            "Override with interval= parameter."
        ),
    )
    interval: Literal["5m", "hourly", "daily"] | None = Field(
        default=None,
        description="Force data interval: '5m', 'hourly', or 'daily'. None = auto.",
    )


@connector(output=_MARKET_CHART_OUTPUT, tags=["crypto"])
async def coingecko_market_chart(params: CoinGeckoMarketChartParams, *, api_key: str) -> Result:
    """[Demo+] Fetch historical price, market cap, and total volume for a coin over
    the last N days. Auto-granularity: 1d→5-min intervals, 2-90d→hourly, 90d+→daily.
    Pass interval='daily' to force daily candles regardless of range. Use
    coingecko_market_chart_range for a precise date range with ISO start/end dates.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"vs_currency": params.vs_currency, "days": params.days}
    if params.interval:
        req["interval"] = params.interval

    data = await _cg_fetch(
        http,
        path=f"/coins/{params.coin_id}/market_chart",
        params=req,
        op_name="coingecko_market_chart",
    )

    df = _build_market_chart_df(data, op_name="coingecko_market_chart")
    return _MARKET_CHART_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_market_chart", params={"coin_id": params.coin_id, "vs_currency": params.vs_currency, "days": params.days}),
        params={"coin_id": params.coin_id},
    )


class CoinGeckoMarketChartRangeParams(BaseModel):
    """Parameters for historical price chart between two dates."""

    model_config = ConfigDict(populate_by_name=True)

    coin_id: Annotated[str, Namespace("coingecko_coin")] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    from_date: str = Field(
        ...,
        alias="from",
        description="Start date ISO 8601, e.g. '2024-01-01'. Use as from_date='2024-01-01'",
    )
    to_date: str = Field(
        ...,
        alias="to",
        description="End date ISO 8601, e.g. '2024-12-31'. Use as to_date='2024-12-31'",
    )


@connector(output=_MARKET_CHART_OUTPUT, tags=["crypto"])
async def coingecko_market_chart_range(params: CoinGeckoMarketChartRangeParams, *, api_key: str) -> Result:
    """[Demo+] Fetch historical price, market cap, and total volume for a coin between
    two ISO dates. More precise than coingecko_market_chart when you need a specific
    date window. Granularity is automatic based on range width (hourly for < 90 days,
    daily for longer). Use from_date='YYYY-MM-DD' and to_date='YYYY-MM-DD'.

    Demo plan: limited to data within the last 365 days (raises PaymentRequiredError
    for older ranges). Use coingecko_market_chart with days='max' on Demo for full
    history (Pro plan removes the restriction).
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "vs_currency": params.vs_currency,
        "from": _iso_to_unix(params.from_date),
        "to": _iso_to_unix(params.to_date),
    }
    data = await _cg_fetch(
        http,
        path=f"/coins/{params.coin_id}/market_chart/range",
        params=req,
        op_name="coingecko_market_chart_range",
    )

    df = _build_market_chart_df(data, op_name="coingecko_market_chart_range")
    return _MARKET_CHART_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_market_chart_range",
            params={"coin_id": params.coin_id, "vs_currency": params.vs_currency, "from": params.from_date, "to": params.to_date},
        ),
        params={"coin_id": params.coin_id},
    )


def _build_market_chart_df(data: Any, *, op_name: str) -> pd.DataFrame:
    """Convert CoinGecko market_chart response [[ts, val], ...] arrays into a DataFrame."""
    if not isinstance(data, dict):
        raise ParseError(provider="coingecko", message=f"Unexpected market chart response type from '{op_name}'")

    prices = data.get("prices", [])
    if not prices:
        raise EmptyDataError(provider="coingecko", message=f"No price data returned from '{op_name}'")

    df_price = pd.DataFrame(prices, columns=["timestamp", "price"])
    df_cap = pd.DataFrame(data.get("market_caps", []), columns=["timestamp", "market_cap"])
    df_vol = pd.DataFrame(data.get("total_volumes", []), columns=["timestamp", "total_volume"])

    df = df_price.merge(df_cap, on="timestamp", how="left").merge(df_vol, on="timestamp", how="left")
    # Convert ms → s so OutputConfig dtype="timestamp" can parse correctly (expects seconds)
    df["timestamp"] = df["timestamp"] / 1000
    return df


class CoinGeckoOhlcParams(BaseModel):
    """Parameters for OHLC candlestick data."""

    coin_id: Annotated[str, Namespace("coingecko_coin")] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    days: Literal[1, 7, 14, 30, 90, 180, 365] = Field(
        default=30, description="Candle range in days: 1, 7, 14, 30, 90, 180, or 365"
    )


@connector(output=_OHLC_OUTPUT, tags=["crypto"])
async def coingecko_ohlc(params: CoinGeckoOhlcParams, *, api_key: str) -> Result:
    """[Demo+] Fetch OHLC (open-high-low-close) candlestick data for a coin.
    Candlestick body: 1-2d→30-min candles, 3-30d→4-hour candles, 31-365d→4-day candles.
    Use coingecko_market_chart for continuous price history with market cap and volume.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"vs_currency": params.vs_currency, "days": params.days}
    data = await _cg_fetch(
        http,
        path=f"/coins/{params.coin_id}/ohlc",
        params=req,
        op_name="coingecko_ohlc",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(provider="coingecko", message=f"No OHLC data returned for {params.coin_id}")

    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
    # Convert ms → s for OutputConfig dtype="timestamp"
    df["timestamp"] = df["timestamp"] / 1000
    return _OHLC_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_ohlc", params={"coin_id": params.coin_id, "vs_currency": params.vs_currency, "days": params.days}),
        params={"coin_id": params.coin_id},
    )


# ---------------------------------------------------------------------------
# On-Chain / GeckoTerminal — OutputConfigs + Connectors
# ---------------------------------------------------------------------------

_ONCHAIN_PRICE_OUTPUT = OutputConfig(
    columns=[
        Column(name="contract_address", role=ColumnRole.KEY, namespace="coingecko_onchain"),
        Column(name="price_usd", dtype="numeric"),
    ]
)


class CoinGeckoTokenPriceOnchainParams(BaseModel):
    """Parameters for on-chain token price lookup via GeckoTerminal."""

    network: str = Field(
        ...,
        description=(
            "Blockchain network ID, e.g. 'eth' (Ethereum), 'bsc' (BNB Chain), "
            "'polygon-pos', 'arbitrum-one', 'solana'. Use lowercase with hyphens."
        ),
    )
    contract_addresses: str = Field(
        ...,
        description="Comma-separated token contract addresses (checksum or lowercase), e.g. '0xdac17f958d2ee523a2206206994597c13d831ec7'",
    )
    vs_currencies: str = Field(
        default="usd",
        description="Comma-separated target currencies for price. Only 'usd' is reliably available.",
    )


@connector(output=_ONCHAIN_PRICE_OUTPUT, tags=["crypto", "onchain"])
async def coingecko_token_price_onchain(params: CoinGeckoTokenPriceOnchainParams, *, api_key: str) -> Result:
    """[Demo+] Fetch on-chain token price by contract address via GeckoTerminal.
    Use for long-tail tokens not listed on CoinGecko's main index. Prefer
    coingecko_price for well-known assets. Supports multiple addresses in a
    single call (comma-separated). Returns one row per address with price_usd.
    """
    http = _make_http(api_key)
    path = f"/onchain/simple/networks/{params.network}/token_price/{params.contract_addresses}"
    data = await _cg_fetch(http, path=path, params={"vs_currencies": params.vs_currencies}, op_name="coingecko_token_price_onchain")

    token_prices: dict = {}
    try:
        token_prices = data.get("data", {}).get("attributes", {}).get("token_prices", {})
    except AttributeError:
        raise ParseError(provider="coingecko", message="Unexpected on-chain price response structure")

    if not token_prices:
        raise EmptyDataError(
            provider="coingecko",
            message=f"No on-chain price data for addresses: {params.contract_addresses}",
        )

    rows = [{"contract_address": addr, "price_usd": float(price)} for addr, price in token_prices.items()]
    df = pd.DataFrame(rows)
    return _ONCHAIN_PRICE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_token_price_onchain", params={"network": params.network}),
        params={"network": params.network},
    )


# ---------------------------------------------------------------------------
# Enumerator — full coin list for catalog indexing
# ---------------------------------------------------------------------------

_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="platforms", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)


class CoinGeckoEnumerateParams(BaseModel):
    """No parameters — enumerates the full CoinGecko coin catalog (~15 000 entries)."""

    include_platform: bool = Field(
        default=False,
        description="Include contract address platforms (significantly increases response size)",
    )


@enumerator(output=_ENUMERATE_OUTPUT, tags=["crypto"])
async def enumerate_coingecko(params: CoinGeckoEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate all coins from CoinGecko for catalog indexing.

    Calls /coins/list — returns ~15 000 rows with id, name, symbol.
    Used to build the parsimony catalog for offline search without
    hitting the rate limit.
    """
    async with httpx.AsyncClient(
        headers={"x-cg-demo-api-key": api_key},
        timeout=60.0,
    ) as client:
        resp = await client.get(
            f"{_BASE_URL}/coins/list",
            params={"include_platform": str(params.include_platform).lower()},
        )
        resp.raise_for_status()
        data: list[dict] = resp.json()

    if not data:
        return pd.DataFrame(columns=["id", "name", "symbol", "platforms"])

    rows = [
        {
            "id": c.get("id", ""),
            "name": c.get("name", c.get("id", "")),
            "symbol": c.get("symbol", ""),
            "platforms": str(c.get("platforms", {})) if c.get("platforms") else "",
        }
        for c in data
        if c.get("id")
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Connector collections
# ---------------------------------------------------------------------------

COINGECKO_FETCH_CONNECTORS = Connectors(
    [
        coingecko_price,
        coingecko_markets,
        coingecko_coin_detail,
        coingecko_market_chart,
        coingecko_market_chart_range,
        coingecko_ohlc,
        coingecko_token_price_onchain,
    ]
)

COINGECKO_DISCOVERY_CONNECTORS = Connectors(
    [
        coingecko_search,
        coingecko_trending,
        coingecko_top_gainers_losers,
    ]
)

FETCH_CONNECTORS = COINGECKO_FETCH_CONNECTORS
CONNECTORS = COINGECKO_DISCOVERY_CONNECTORS + COINGECKO_FETCH_CONNECTORS + Connectors([enumerate_coingecko])
