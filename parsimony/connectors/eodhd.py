"""EODHD source: typed connectors per endpoint.

API docs: https://eodhd.com/financial-apis/api-for-historical-data-and-volumes/
Authentication: API key via ?api_token= query param.
Base URL: https://eodhd.com/api

Provides 17 connectors covering the full EODHD REST surface:
  - Market data: EOD prices, live quotes, intraday, bulk EOD
  - Corporate actions: dividends, splits
  - Reference: search, exchanges, exchange symbol lists
  - Fundamentals (raw dict — nested JSON blob)
  - Calendars: earnings, IPO, trends
  - News
  - Macro indicators
  - Technical indicators
  - Insider transactions
  - Screener
"""

from __future__ import annotations

import json
import re
from typing import Annotated, Any, Literal

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from parsimony.connector import (
    Connectors,
    Namespace,
    connector,
)
from parsimony.errors import (
    EmptyDataError,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient

ENV_VARS: dict[str, str] = {"api_key": "EODHD_API_KEY"}

_LATENCY_TIMEOUT: float = 10.0
_BULK_TIMEOUT: float = 60.0


# ---------------------------------------------------------------------------
# Transport helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str, timeout: float = _LATENCY_TIMEOUT) -> HttpClient:
    return HttpClient(
        "https://eodhd.com/api",
        timeout=timeout,
        query_params={"api_token": api_key, "fmt": "json"},
    )


def _to_bracket_params(params: dict[str, Any]) -> dict[str, Any]:
    """Transform filter_x → filter[x] and page_x → page[x] for EODHD bracket syntax.

    Pure function: does not mutate input. None values are dropped.
    """
    result: dict[str, Any] = {}
    for k, v in params.items():
        if v is None:
            continue
        if k.startswith("filter_"):
            result[f"filter[{k[7:]}]"] = v
        elif k.startswith("page_"):
            result[f"page[{k[5:]}]"] = v
        else:
            result[k] = v
    return result


async def _eodhd_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any],
    op_name: str,
    output_config: OutputConfig | None = None,
    raw: bool = False,
) -> Result:
    """Shared EODHD fetch: path interpolation, bracket params, JSON extraction, Result building.

    Error mapping:
      401 → UnauthorizedError
      402 → PaymentRequiredError
      429 → RateLimitError (surfaces immediately — no retry)
      5xx → ProviderError

    The EODHD API key is never included in exception messages.
    asyncio.CancelledError propagates unchanged.
    """
    # Path template substitution: {key} → value; remainder → query params
    rendered = path
    query_params: dict[str, Any] = {}

    for key, value in params.items():
        if value is None:
            continue
        placeholder = f"{{{key}}}"
        if placeholder in rendered:
            rendered = rendered.replace(placeholder, str(value))
        else:
            query_params[key] = value

    # Remove any unfilled optional placeholders
    rendered = re.sub(r"\{[^}]+\}", "", rendered)

    # Apply EODHD bracket syntax transformation (filter_x → filter[x], page_x → page[x])
    query_params = _to_bracket_params(query_params)

    try:
        response = await http.request("GET", f"/{rendered.lstrip('/')}", params=query_params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        match status:
            case 401:
                raise UnauthorizedError(
                    provider="eodhd",
                    message="Invalid or missing EODHD API token",
                ) from e
            case 402:
                raise PaymentRequiredError(
                    provider="eodhd",
                    message="Your EODHD plan is not eligible for this data request",
                ) from e
            case 429:
                try:
                    retry_after = float(e.response.headers.get("Retry-After", "60"))
                except ValueError:
                    retry_after = 60.0
                raise RateLimitError(
                    provider="eodhd",
                    retry_after=retry_after,
                    message=f"EODHD rate limit hit on '{op_name}', retry after {retry_after:.0f}s",
                ) from e
            case _:
                raise ProviderError(
                    provider="eodhd",
                    status_code=status,
                    message=f"EODHD API error {status} on '{op_name}'",
                ) from e

    data = response.json()
    prov = Provenance(source=op_name, params=dict(params))

    # 200-body error detection (EODHD returns error strings in the body on some endpoints)
    if isinstance(data, dict) and "error" in data and isinstance(data["error"], str):
        raise ProviderError(
            provider="eodhd",
            status_code=200,
            message=f"EODHD error on '{op_name}': {data['error']}",
        )

    # Raw return path (fundamentals): bypass DataFrame pipeline entirely
    if raw:
        return Result(data=data, provenance=prov)

    # DataFrame construction
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for key in ("earnings", "ipos", "splits", "trends", "data", "results"):
            if key in data and isinstance(data[key], list):
                df = pd.DataFrame(data[key])
                break
        else:
            df = pd.DataFrame([data])
    else:
        raise ParseError(
            provider="eodhd",
            message=f"Unexpected response type from EODHD '{op_name}': {type(data).__name__}",
        )

    if df.empty:
        raise EmptyDataError(
            provider="eodhd",
            message=f"No data returned from EODHD endpoint '{op_name}'",
            query_params=dict(params),
        )

    if output_config is not None:
        return output_config.build_table_result(df, provenance=prov, params=dict(params))
    return Result.from_dataframe(df, prov)


# ---------------------------------------------------------------------------
# Market Data — OutputConfigs
# ---------------------------------------------------------------------------

_EOD_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="adjusted_close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
    ]
)

_LIVE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="timestamp", role=ColumnRole.METADATA, dtype="timestamp"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="previousClose", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="change_p", dtype="numeric"),
    ]
)

_INTRADAY_OUTPUT = OutputConfig(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY, dtype="timestamp"),
        Column(name="datetime", role=ColumnRole.METADATA),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
    ]
)

_BULK_EOD_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange_short_name", role=ColumnRole.METADATA),
        Column(name="date", dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="adjusted_close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
    ]
)


# ---------------------------------------------------------------------------
# Market Data — Connectors
# ---------------------------------------------------------------------------


class EodhdEodParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US or BARC.LSE",
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-15. Use as from_date='2024-01-15'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'"
    )
    period: Literal["d", "w", "m"] | None = Field(
        default=None,
        description="Aggregation period: d (daily), w (weekly), m (monthly). Default: d",
    )


@connector(output=_EOD_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_eod(params: EodhdEodParams, *, api_key: str) -> Result:
    """[Free+] Fetch end-of-day OHLCV prices for a ticker. Supports daily, weekly, and monthly
    aggregation. Use from/to to limit the date range (ISO 8601). Empty result may indicate an
    invalid ticker or exchange code — verify with eodhd_search first."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"ticker": params.ticker}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    if params.period:
        p["period"] = params.period
    return await _eodhd_fetch(http, path="/eod/{ticker}", params=p, op_name="eodhd_eod", output_config=_EOD_OUTPUT)


class EodhdLiveParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US",
    )


@connector(output=_LIVE_OUTPUT, tags=["eodhd", "equity", "tool"])
async def eodhd_live(params: EodhdLiveParams, *, api_key: str) -> Result:
    """[Free+] Fetch live (real-time or 15-min delayed) quote for a ticker. Use eodhd_search
    to resolve a company name to its EODHD ticker format (e.g. AAPL.US)."""
    http = _make_http(api_key, timeout=_LATENCY_TIMEOUT)
    return await _eodhd_fetch(
        http,
        path="/real-time/{ticker}",
        params={"ticker": params.ticker},
        op_name="eodhd_live",
        output_config=_LIVE_OUTPUT,
    )


class EodhdIntradayParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US",
    )
    interval: Literal["1m", "5m", "1h"] = Field(
        ..., description="Intraday interval: 1m, 5m, or 1h"
    )
    from_unix: int | None = Field(
        default=None, description="Start time as Unix timestamp (seconds since epoch)"
    )
    to_unix: int | None = Field(
        default=None, description="End time as Unix timestamp (seconds since epoch)"
    )


@connector(output=_INTRADAY_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_intraday(params: EodhdIntradayParams, *, api_key: str) -> Result:
    """[EOD+Intraday+] Fetch intraday OHLCV data for a ticker. Intervals: 1m, 5m, 1h.
    Provide from_unix / to_unix as Unix timestamps (seconds) to bound the range.
    Returns at most the last 100 data points when no range is specified."""
    http = _make_http(api_key, timeout=_LATENCY_TIMEOUT)
    p: dict[str, Any] = {"ticker": params.ticker, "interval": params.interval}
    if params.from_unix is not None:
        p["from"] = params.from_unix
    if params.to_unix is not None:
        p["to"] = params.to_unix
    return await _eodhd_fetch(
        http, path="/intraday/{ticker}", params=p, op_name="eodhd_intraday", output_config=_INTRADAY_OUTPUT
    )


class EodhdBulkEodParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exchange: str = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Exchange code, e.g. US, LSE, XETRA, TSX",
    )
    date: str | None = Field(
        default=None, description="Trading date, ISO 8601, e.g. 2024-01-15. Defaults to last trading day."
    )


@connector(output=_BULK_EOD_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_bulk_eod(params: EodhdBulkEodParams, *, api_key: str) -> Result:
    """[EOD Historical+] Fetch end-of-day prices for all symbols on an exchange in a single request.
    Returns the last trading day by default; pass date to fetch a specific day.
    Large response — use for batch ingestion, not per-ticker lookups."""
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    p: dict[str, Any] = {"exchange": params.exchange}
    if params.date:
        p["date"] = params.date
    return await _eodhd_fetch(
        http, path="/eod/bulk_last_day/{exchange}", params=p, op_name="eodhd_bulk_eod", output_config=_BULK_EOD_OUTPUT
    )


# ---------------------------------------------------------------------------
# Corporate Actions — OutputConfigs
# ---------------------------------------------------------------------------

_DIVIDENDS_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="declarationDate", dtype="date"),
        Column(name="recordDate", dtype="date"),
        Column(name="paymentDate", dtype="date"),
        Column(name="period", role=ColumnRole.METADATA),
        Column(name="value", dtype="numeric"),
        Column(name="unadjustedValue", dtype="numeric"),
        Column(name="currency", role=ColumnRole.METADATA),
    ]
)

_SPLITS_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="split", dtype="auto"),
    ]
)


# ---------------------------------------------------------------------------
# Corporate Actions — Connectors
# ---------------------------------------------------------------------------


class EodhdDividendsParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ..., pattern=r"^[A-Z0-9._\-]+$", description="Ticker in EODHD format, e.g. AAPL.US"
    )
    from_date: str | None = Field(default=None, alias="from", description="Start date ISO 8601. Use as from_date='2024-01-15'")
    to_date: str | None = Field(default=None, alias="to", description="End date ISO 8601. Use as to_date='2024-12-31'")


@connector(output=_DIVIDENDS_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_dividends(params: EodhdDividendsParams, *, api_key: str) -> Result:
    """[Free+] Fetch dividend history for a ticker. Use from/to to limit the range."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"ticker": params.ticker}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/div/{ticker}", params=p, op_name="eodhd_dividends", output_config=_DIVIDENDS_OUTPUT
    )


class EodhdSplitsParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ..., pattern=r"^[A-Z0-9._\-]+$", description="Ticker in EODHD format, e.g. AAPL.US"
    )
    from_date: str | None = Field(default=None, alias="from", description="Start date ISO 8601. Use as from_date='2024-01-15'")
    to_date: str | None = Field(default=None, alias="to", description="End date ISO 8601. Use as to_date='2024-12-31'")


@connector(output=_SPLITS_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_splits(params: EodhdSplitsParams, *, api_key: str) -> Result:
    """[Free+] Fetch stock split history for a ticker. The split ratio column contains the
    ratio string as returned by the API (e.g. "4/1" for a 4-for-1 split). Use from/to to limit the range."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"ticker": params.ticker}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/splits/{ticker}", params=p, op_name="eodhd_splits", output_config=_SPLITS_OUTPUT
    )


# ---------------------------------------------------------------------------
# Reference Data — OutputConfigs
# ---------------------------------------------------------------------------

_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="Code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="Exchange", role=ColumnRole.METADATA),
        Column(name="Type", role=ColumnRole.METADATA),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="ISIN", role=ColumnRole.METADATA),
    ]
)

_EXCHANGES_OUTPUT = OutputConfig(
    columns=[
        Column(name="Code", role=ColumnRole.KEY),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="OperatingMIC", role=ColumnRole.METADATA),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="CountryISO2", role=ColumnRole.METADATA),
        Column(name="CountryISO3", role=ColumnRole.METADATA),
    ]
)

_EXCHANGE_SYMBOLS_OUTPUT = OutputConfig(
    columns=[
        Column(name="Code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Exchange", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="Type", role=ColumnRole.METADATA),
    ]
)


# ---------------------------------------------------------------------------
# Reference Data — Connectors
# ---------------------------------------------------------------------------


class EodhdSearchParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(..., description="Company name or partial ticker to search for, e.g. 'Apple' or 'AAPL'")
    limit: int = Field(default=50, description="Maximum number of results (default 50)")
    type: Literal["Q", "ETF", "FUND", "BOND", "INDEX"] | None = Field(
        default=None, description="Instrument type filter: Q (equity), ETF, FUND, BOND, INDEX"
    )


@connector(output=_SEARCH_OUTPUT, tags=["eodhd", "tool"])
async def eodhd_search(params: EodhdSearchParams, *, api_key: str) -> Result:
    """[Free+] Search for instruments by company name or partial ticker. Use to resolve company
    names to EODHD ticker codes (format: TICKER.EXCHANGE, e.g. AAPL.US). Filter by type to
    narrow results."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"query": params.query, "limit": params.limit}
    if params.type:
        p["type"] = params.type
    return await _eodhd_fetch(
        http, path="/search/{query}", params=p, op_name="eodhd_search", output_config=_SEARCH_OUTPUT
    )


class EodhdExchangesParams(BaseModel):
    model_config = ConfigDict(extra="forbid")


@connector(output=_EXCHANGES_OUTPUT, tags=["eodhd", "tool"])
async def eodhd_exchanges(params: EodhdExchangesParams, *, api_key: str) -> Result:
    """[Free+] List all exchanges supported by EODHD. Use to find valid exchange codes for
    eodhd_bulk_eod and eodhd_exchange_symbols."""
    http = _make_http(api_key)
    return await _eodhd_fetch(
        http, path="/exchanges-list", params={}, op_name="eodhd_exchanges", output_config=_EXCHANGES_OUTPUT
    )


class EodhdExchangeSymbolsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exchange: str = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Exchange code, e.g. US, LSE, XETRA. Use eodhd_exchanges to list valid codes.",
    )
    type: Literal["common_stock", "preferred_stock", "stock", "etf", "fund"] | None = Field(
        default=None, description="Instrument type filter: common_stock, preferred_stock, stock, etf, fund"
    )


@connector(output=_EXCHANGE_SYMBOLS_OUTPUT, tags=["eodhd"])
async def eodhd_exchange_symbols(params: EodhdExchangeSymbolsParams, *, api_key: str) -> Result:
    """[Free+] List all symbols traded on an exchange. Large response for major exchanges
    (US has 20 000+ symbols) — use type filter to limit. Empty result may indicate an
    invalid exchange code."""
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    p: dict[str, Any] = {"exchange": params.exchange}
    if params.type:
        p["type"] = params.type
    return await _eodhd_fetch(
        http, path="/exchange-symbol-list/{exchange}", params=p, op_name="eodhd_exchange_symbols",
        output_config=_EXCHANGE_SYMBOLS_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Fundamentals — Connector
# ---------------------------------------------------------------------------


class EodhdFundamentalsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US or SPY.US (ETFs supported)",
    )


@connector(tags=["eodhd", "equity"], result_type="dict")
async def eodhd_fundamentals(params: EodhdFundamentalsParams, *, api_key: str) -> Result:
    """[Fundamentals+] Fetch full fundamentals for a stock or ETF. Returns a large nested dict
    (not a DataFrame). Typical top-level keys for equities: General, Highlights, Valuation,
    SharesStats, Technicals, SplitsDividends, AnalystRatings, Holders, InsiderTransactions,
    Financials, Earnings. ETF top-level keys differ: General, Technicals, ETF_Data.

    Navigate by key path, e.g.:
      result.data['Highlights']['MarketCapitalization']
      result.data['Financials']['Income_Statement']['annual']

    Returns raw dict — use result.data to access the nested structure."""
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    return await _eodhd_fetch(
        http,
        path="/fundamentals/{ticker}",
        params={"ticker": params.ticker},
        op_name="eodhd_fundamentals",
        raw=True,
    )


# ---------------------------------------------------------------------------
# Calendars — Dispatch map + OutputConfig + Connector
# ---------------------------------------------------------------------------

_CALENDAR_PATHS: dict[str, str] = {
    "earnings": "calendar/earnings",
    "ipo": "calendar/ipo",
    "trends": "calendar/trends",
}

_CALENDAR_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="date", dtype="date"),
        Column(name="report_date", dtype="date"),
        Column(name="before_after_market", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="actual", dtype="numeric"),
        Column(name="estimate", dtype="numeric"),
        Column(name="difference", dtype="numeric"),
        Column(name="percent", dtype="numeric"),
    ]
)


class EodhdCalendarParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["earnings", "ipo", "trends"] = Field(
        ..., description="Calendar type: earnings (EPS calendar), ipo (IPO calendar), trends (analyst trends)"
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-01. Use as from_date='2024-01-01'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-03-31. Use as to_date='2024-03-31'"
    )
    symbols: str | None = Field(
        default=None, description="Comma-separated EODHD ticker codes to filter, e.g. AAPL.US,MSFT.US"
    )


@connector(output=_CALENDAR_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_calendar(params: EodhdCalendarParams, *, api_key: str) -> Result:
    """[Fundamentals+] Fetch market calendar data. Three types available:
      - earnings: upcoming earnings announcements with EPS estimates and actuals
      - ipo: upcoming and recent IPO listings
      - trends: analyst recommendation trends by sector

    Use from/to to narrow the date window (max 90 days recommended for earnings)."""
    http = _make_http(api_key)
    path = _CALENDAR_PATHS[params.type]
    p: dict[str, Any] = {}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    if params.symbols:
        p["symbols"] = params.symbols
    return await _eodhd_fetch(
        http, path=path, params=p, op_name="eodhd_calendar", output_config=_CALENDAR_OUTPUT
    )


# ---------------------------------------------------------------------------
# News — OutputConfig + Connector
# ---------------------------------------------------------------------------

_NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="datetime"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="content"),
        Column(name="link", role=ColumnRole.METADATA),
        Column(name="symbols", role=ColumnRole.METADATA),
        Column(name="tags", role=ColumnRole.METADATA),
    ]
)


class EodhdNewsParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: str | None = Field(
        default=None,
        description="EODHD ticker to filter news, e.g. AAPL.US. Omit for market-wide news.",
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-15. Use as from_date='2024-01-15'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'"
    )
    limit: int = Field(default=50, description="Max number of articles to return (default 50)")
    offset: int = Field(default=0, description="Offset for pagination (0-indexed)")


@connector(output=_NEWS_OUTPUT, tags=["eodhd", "tool"])
async def eodhd_news(params: EodhdNewsParams, *, api_key: str) -> Result:
    """[Free+] Fetch financial news articles. Filter by ticker (e.g. AAPL.US) or leave
    empty for broad market news. Use from/to for date filtering and limit/offset for pagination.
    Empty result may indicate no news in the date range for the specified ticker."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"limit": params.limit, "offset": params.offset}
    if params.ticker:
        p["s"] = params.ticker  # EODHD uses 's=' for symbol filtering on news endpoint
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/news", params=p, op_name="eodhd_news", output_config=_NEWS_OUTPUT
    )


# ---------------------------------------------------------------------------
# Macro Indicators — OutputConfig + Connectors
# ---------------------------------------------------------------------------

_MACRO_OUTPUT = OutputConfig(
    columns=[
        Column(name="Date", role=ColumnRole.KEY, dtype="date"),
        Column(name="Value", dtype="numeric"),
        Column(name="Period", role=ColumnRole.METADATA),
        Column(name="LastUpdated", role=ColumnRole.METADATA),
    ]
)


class EodhdMacroParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str = Field(
        ...,
        pattern=r"^[A-Z]{3}$",
        description="ISO 3-letter country code, e.g. USA, DEU, GBR, FRA, JPN",
    )
    indicator: str = Field(
        ...,
        description=(
            "Macro indicator code, e.g. gdp_current_usd, unemployment_total_percent, "
            "inflation_consumer_prices_annual, real_interest_rate, population_total"
        ),
    )


@connector(output=_MACRO_OUTPUT, tags=["eodhd", "macro"])
async def eodhd_macro(params: EodhdMacroParams, *, api_key: str) -> Result:
    """[Fundamentals+] Fetch a macro indicator time series for a country.
    Country must be an ISO 3-letter code (e.g. USA, DEU). Common indicators:
      gdp_current_usd, unemployment_total_percent, inflation_consumer_prices_annual,
      real_interest_rate, population_total, exports_of_goods_and_services_usd."""
    http = _make_http(api_key)
    return await _eodhd_fetch(
        http,
        path="/macro-indicator/{country}",
        params={"country": params.country, "indicator": params.indicator},
        op_name="eodhd_macro",
        output_config=_MACRO_OUTPUT,
    )


class EodhdMacroBulkParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str = Field(
        ...,
        pattern=r"^[A-Z]{3}$",
        description="ISO 3-letter country code, e.g. USA, DEU, GBR",
    )
    topic: str | None = Field(
        default=None,
        description=(
            "Optional topic filter to narrow the result set. "
            "Verify valid values against EODHD macro-indicator documentation."
        ),
    )


@connector(output=_MACRO_OUTPUT, tags=["eodhd", "macro"])
async def eodhd_macro_bulk(params: EodhdMacroBulkParams, *, api_key: str) -> Result:
    """[Fundamentals+] Fetch all available macro indicators for a country in a single request.
    Large response — use eodhd_macro for a specific indicator.
    Country must be an ISO 3-letter code (e.g. USA)."""
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    p: dict[str, Any] = {"country": params.country}
    if params.topic:
        p["topic"] = params.topic
    return await _eodhd_fetch(
        http, path="/macro-indicator/{country}", params=p, op_name="eodhd_macro_bulk", output_config=_MACRO_OUTPUT
    )


# ---------------------------------------------------------------------------
# Technical Indicators — OutputConfig + Connector
# ---------------------------------------------------------------------------

_TECHNICAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="*"),  # indicator-specific columns vary by function
    ]
)

_EodhdTechnicalFunction = Literal[
    "sma", "ema", "wma", "volatility", "stochastic", "rsi", "stddev",
    "stochrsi", "slope", "dmi", "adx", "macd", "atr", "cci",
    "sar", "bbands", "splitadjusted", "avgvol", "avgvolacave", "williams_r",
]


class EodhdTechnicalParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ..., pattern=r"^[A-Z0-9._\-]+$", description="Ticker in EODHD format, e.g. AAPL.US"
    )
    function: _EodhdTechnicalFunction = Field(
        ...,
        description=(
            "Technical indicator function: sma, ema, rsi, macd, bbands, atr, stochastic, "
            "adx, cci, sar, williams_r, wma, volatility, stddev, dmi, slope, stochrsi, avgvol — "
            "see EODHD docs for full parameter set per function"
        ),
    )
    period: int = Field(default=50, description="Lookback period (number of bars, default 50)")
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-01. Use as from_date='2024-01-01'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'"
    )
    order: Literal["a", "d"] = Field(
        default="d", description="Sort order: a (ascending) or d (descending, default)"
    )


@connector(output=_TECHNICAL_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_technical(params: EodhdTechnicalParams, *, api_key: str) -> Result:
    """[EOD+Intraday+] Fetch technical indicator values for a ticker alongside OHLCV data.
    Indicator-specific output columns vary by function:
      - sma/ema/wma → sma/ema/wma column
      - macd → macd, macd_signal, macd_hist
      - bbands → uband, mband, lband
      - stochastic → stoch_kd, stoch_d
      - adx/dmi → adx, plusDI, minusDI

    Use period to control the lookback window (default 50)."""
    http = _make_http(api_key)
    p: dict[str, Any] = {
        "ticker": params.ticker,
        "function": params.function,
        "period": params.period,
        "order": params.order,
    }
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/technicals/{ticker}", params=p, op_name="eodhd_technical", output_config=_TECHNICAL_OUTPUT
    )


# ---------------------------------------------------------------------------
# Insider Transactions & Screener — OutputConfigs
# ---------------------------------------------------------------------------

_INSIDER_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="date", dtype="date"),
        Column(name="ownerName", role=ColumnRole.METADATA),
        Column(name="ownerCik", role=ColumnRole.METADATA),
        Column(name="transactionType", role=ColumnRole.METADATA),
        Column(name="transactionDate", dtype="date"),
        Column(name="value", dtype="numeric"),
        Column(name="sharesOwned", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="*"),
    ]
)

_SCREENER_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="sector", role=ColumnRole.METADATA),
        Column(name="industry", role=ColumnRole.METADATA),
        Column(name="market_capitalization", dtype="numeric"),
        Column(name="*"),
    ]
)


# ---------------------------------------------------------------------------
# Insider Transactions & Screener — Connectors
# ---------------------------------------------------------------------------


class EodhdInsiderParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] | None = Field(
        default=None,
        description="EODHD ticker to filter by, e.g. AAPL.US. Omit for all recent transactions.",
    )
    limit: int = Field(default=100, description="Max transactions to return (default 100)")
    offset: int = Field(default=0, description="Offset for pagination (0-indexed)")


@connector(output=_INSIDER_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_insider(params: EodhdInsiderParams, *, api_key: str) -> Result:
    """[Fundamentals+] Fetch insider (executive and director) transactions. Filter by ticker
    or omit for recent cross-market transactions. Use limit/offset to page."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"limit": params.limit, "offset": params.offset}
    if params.ticker:
        p["code"] = params.ticker
    return await _eodhd_fetch(
        http, path="/insider-transactions", params=p, op_name="eodhd_insider", output_config=_INSIDER_OUTPUT
    )


class EodhdScreenerParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filters: list[tuple[str, str, str]] | None = Field(
        default=None,
        description=(
            "List of filter triples [field, operator, value], e.g. "
            "[['market_capitalization', '>', '1000000000'], ['exchange', '=', 'US']]. "
            "Valid operators: >, <, =, >=, <=. "
            "Common fields: market_capitalization, earnings_share, dividend_yield, "
            "pe_ratio, revenue, sector, exchange."
        ),
    )
    signals: str | None = Field(
        default=None,
        description="Signal filter, e.g. 'bookvalue_neg,wallstreet_lo'. See EODHD screener docs.",
    )
    sort: str | None = Field(
        default=None,
        description="Field to sort by, e.g. market_capitalization",
    )
    order: Literal["asc", "desc"] = Field(
        default="desc", description="Sort order: asc or desc (default)"
    )
    limit: int = Field(default=50, description="Max results (default 50)")
    offset: int = Field(default=0, description="Offset for pagination (0-indexed)")


@connector(output=_SCREENER_OUTPUT, tags=["eodhd", "equity", "tool"])
async def eodhd_screener(params: EodhdScreenerParams, *, api_key: str) -> Result:
    """[EOD+Intraday+] Screen stocks by fundamental, price, and exchange criteria.
    Filters are structured triples [field, operator, value] — see EodhdScreenerParams.filters.
    Empty result may indicate invalid filter field or operator — verify against the EODHD
    screener field list in their documentation."""
    http = _make_http(api_key)
    p: dict[str, Any] = {"limit": params.limit, "offset": params.offset, "order": params.order}
    if params.filters:
        p["filters"] = json.dumps([[f[0], f[1], f[2]] for f in params.filters])
    if params.signals:
        p["signals"] = params.signals
    if params.sort:
        p["sort"] = params.sort
    return await _eodhd_fetch(
        http, path="/screener", params=p, op_name="eodhd_screener", output_config=_SCREENER_OUTPUT
    )


# ---------------------------------------------------------------------------
# Connector collections
# ---------------------------------------------------------------------------

EODHD_FETCH_CONNECTORS = Connectors(
    [
        # Market data
        eodhd_eod,
        eodhd_live,
        eodhd_intraday,
        eodhd_bulk_eod,
        # Corporate actions
        eodhd_dividends,
        eodhd_splits,
        # Reference
        eodhd_exchange_symbols,
        # Fundamentals
        eodhd_fundamentals,
        # Calendars
        eodhd_calendar,
        # Macro
        eodhd_macro,
        eodhd_macro_bulk,
        # Technical
        eodhd_technical,
        # Transactions
        eodhd_insider,
    ]
)

EODHD_DISCOVERY_CONNECTORS = Connectors(
    [
        eodhd_search,
        eodhd_exchanges,
        eodhd_news,
        eodhd_screener,
    ]
)

CONNECTORS = EODHD_DISCOVERY_CONNECTORS + EODHD_FETCH_CONNECTORS
