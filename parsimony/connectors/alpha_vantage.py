"""Alpha Vantage source: equities, fundamentals, forex, crypto, commodities, economic indicators, and news.

API docs: https://www.alphavantage.co/documentation/
Authentication: ``apikey`` query parameter.
Base URL: https://www.alphavantage.co/query
Rate limit: 25 calls/day, 5 calls/min (free tier).

Quirks:
  - Single endpoint (``/query``), differentiated by ``function`` param.
  - Errors return HTTP 200 with ``Error Message``, ``Note``, or ``Information`` keys.
  - Many JSON field names are numbered (``"1. open"``) — stripped during parsing.
  - All values are strings — numeric coercion required.
  - Missing data represented as ``"None"`` (string) or ``"."`` (commodities/economic).

Provides 28 connectors:
  - Discovery: symbol search
  - Market data: real-time quote, daily/weekly/monthly/intraday OHLCV
  - Company: overview, income statement, balance sheet, cash flow, earnings, ETF profile
  - Calendars: earnings calendar, IPO calendar (CSV endpoints)
  - Forex: real-time exchange rate, daily/weekly/monthly historical
  - Crypto: daily/weekly/monthly historical
  - Economic indicators: 10 US macro series (GDP, CPI, unemployment, etc.)
  - Precious metals: gold/silver spot price and historical (real-time, not in FRED)
  - Technical indicators: 50+ indicators via unified endpoint (SMA, EMA, RSI, MACD, etc.)
  - Alpha intelligence: news sentiment, top gainers/losers
  - Options: historical options chain (premium only)
  - Enumerator: listing status for catalog indexing

Commodity data (WTI, Brent, natural gas, copper, etc.) is omitted — use the
FRED connector instead, which has superior historical coverage for those series.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

import httpx
import pandas as pd
from pydantic import BaseModel, Field

from parsimony.connector import (
    Connectors,
    EmptyDataError,
    Namespace,
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


ENV_VARS: dict[str, str] = {"api_key": "ALPHA_VANTAGE_API_KEY"}

_BASE_URL = "https://www.alphavantage.co"
_TIMEOUT = 20.0

_PROVIDER = "alpha_vantage"


# ---------------------------------------------------------------------------
# Transport helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        _BASE_URL,
        query_params={"apikey": api_key},
        timeout=_TIMEOUT,
    )


def _strip_numbered_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Strip numbered prefixes from Alpha Vantage keys.

    ``"1. open"`` → ``"open"``, ``"01. symbol"`` → ``"symbol"``.
    """
    return {k.split(". ", 1)[-1] if ". " in k else k: v for k, v in d.items()}


def _clean_none_strings(d: dict[str, Any]) -> dict[str, Any]:
    """Replace ``"None"`` string values with ``None`` for proper NaN coercion."""
    return {k: (None if v == "None" else v) for k, v in d.items()}


async def _av_fetch(
    http: HttpClient,
    *,
    function: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared Alpha Vantage GET with error detection. Returns parsed JSON body.

    Alpha Vantage always returns HTTP 200 — errors are embedded in the JSON body
    as ``Error Message``, ``Note`` (rate limit), or ``Information`` keys.
    """
    req_params: dict[str, Any] = {"function": function}
    if params:
        req_params.update(params)

    try:
        response = await http.request("GET", "/query", params=req_params)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        match status:
            case 401 | 403:
                raise UnauthorizedError(
                    provider=_PROVIDER,
                    message="Invalid or missing Alpha Vantage API key",
                ) from e
            case 429:
                raise RateLimitError(
                    provider=_PROVIDER,
                    retry_after=60.0,
                    message=f"Alpha Vantage rate limit hit on '{op_name}'",
                ) from e
            case _:
                raise ProviderError(
                    provider=_PROVIDER,
                    status_code=status,
                    message=f"Alpha Vantage API error {status} on '{op_name}'",
                ) from e

    body = response.json()

    # Alpha Vantage embeds errors in 200 responses
    if "Error Message" in body:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"Alpha Vantage error on '{op_name}': {body['Error Message']}",
        )
    if "Note" in body:
        raise RateLimitError(
            provider=_PROVIDER,
            retry_after=60.0,
            message=f"Alpha Vantage rate limit: {body['Note']}",
        )
    if "Information" in body and len(body) == 1:
        info_msg = body["Information"]
        if "per-second" in info_msg.lower() or "rate limit" in info_msg.lower():
            raise RateLimitError(
                provider=_PROVIDER,
                retry_after=60.0,
                message=f"Alpha Vantage rate limit: {info_msg}",
            )
        raise PaymentRequiredError(
            provider=_PROVIDER,
            message=f"Alpha Vantage: {info_msg}",
        )

    return body


# ---------------------------------------------------------------------------
# Discovery — Symbol Search
# ---------------------------------------------------------------------------

_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="region", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="matchScore", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageSearchParams(BaseModel):
    """Search Alpha Vantage for stocks, ETFs, and mutual funds by name or ticker."""

    keywords: str = Field(
        ..., min_length=1, description="Search term, e.g. 'apple' or 'AAPL'"
    )


@connector(output=_SEARCH_OUTPUT, tags=["equities", "tool"])
async def alpha_vantage_search(
    params: AlphaVantageSearchParams, *, api_key: str
) -> Result:
    """Search Alpha Vantage for stocks, ETFs, and mutual funds by name or ticker.

    Returns symbol (the ticker), name, type (Equity/ETF), region, and currency.
    Use symbol with alpha_vantage_quote, alpha_vantage_daily, or
    alpha_vantage_overview for further data.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="SYMBOL_SEARCH",
        params={"keywords": params.keywords},
        op_name="alpha_vantage_search",
    )

    matches = data.get("bestMatches", [])
    if not matches:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No symbols found for: {params.keywords}",
        )

    rows = []
    for m in matches:
        s = _strip_numbered_keys(m)
        if not s.get("symbol"):
            continue
        rows.append(
            {
                "symbol": s.get("symbol", ""),
                "name": s.get("name", ""),
                "type": s.get("type", ""),
                "region": s.get("region", ""),
                "currency": s.get("currency", ""),
                "matchScore": s.get("matchScore", ""),
            }
        )
    df = pd.DataFrame(rows)
    return _SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_search", params={"keywords": params.keywords}
        ),
        params={"keywords": params.keywords},
    )


# ---------------------------------------------------------------------------
# Market Data — Real-time Quote
# ---------------------------------------------------------------------------

_QUOTE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="price", dtype="numeric"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="latest_trading_day", role=ColumnRole.METADATA, dtype="date"),
        Column(name="previous_close", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="change_percent", dtype="numeric"),
    ]
)


class AlphaVantageQuoteParams(BaseModel):
    """Real-time quote for a single stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


@connector(output=_QUOTE_OUTPUT, tags=["equities"])
async def alpha_vantage_quote(
    params: AlphaVantageQuoteParams, *, api_key: str
) -> Result:
    """Fetch real-time quote for a stock: current price, day high/low/open,
    volume, previous close, and change/change percent.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="GLOBAL_QUOTE",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_quote",
    )

    quote = data.get("Global Quote", {})
    if not quote:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No quote data returned for symbol: {params.symbol}",
        )

    q = _strip_numbered_keys(quote)
    change_pct_raw = q.get("change percent", "0")
    change_pct = change_pct_raw.rstrip("%") if isinstance(change_pct_raw, str) else change_pct_raw

    row = {
        "symbol": q.get("symbol", params.symbol),
        "price": q.get("price"),
        "open": q.get("open"),
        "high": q.get("high"),
        "low": q.get("low"),
        "volume": q.get("volume"),
        "latest_trading_day": q.get("latest trading day"),
        "previous_close": q.get("previous close"),
        "change": q.get("change"),
        "change_percent": change_pct,
    }
    df = pd.DataFrame([row])
    return _QUOTE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_quote", params={"symbol": params.symbol}
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Market Data — Daily Time Series
# ---------------------------------------------------------------------------

_DAILY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageDailyParams(BaseModel):
    """Daily OHLCV time series for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    outputsize: Literal["compact", "full"] = Field(
        default="compact",
        description="'compact' returns last 100 data points; 'full' returns 20+ years of history.",
    )


@connector(output=_DAILY_OUTPUT, tags=["equities"])
async def alpha_vantage_daily(
    params: AlphaVantageDailyParams, *, api_key: str
) -> Result:
    """Fetch daily OHLCV (open, high, low, close, volume) time series for a stock.

    outputsize='compact' returns the last 100 trading days (default).
    outputsize='full' returns 20+ years of daily history.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_DAILY",
        params={"symbol": params.symbol, "outputsize": params.outputsize},
        op_name="alpha_vantage_daily",
    )

    ts_key = "Time Series (Daily)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No daily data returned for symbol: {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)

    meta = data.get("Meta Data", {})
    meta_clean = _strip_numbered_keys(meta)
    metadata_list = [{"name": k, "value": str(v)} for k, v in meta_clean.items()]

    prov = Provenance(
        source="alpha_vantage_daily",
        params={"symbol": params.symbol, "outputsize": params.outputsize},
        properties={"metadata": metadata_list},
    )
    return _DAILY_OUTPUT.build_table_result(
        df,
        provenance=prov,
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Company — Overview
# ---------------------------------------------------------------------------


class AlphaVantageOverviewParams(BaseModel):
    """Company overview / fundamentals for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


@connector(tags=["equities"], result_type="dict")
async def alpha_vantage_overview(
    params: AlphaVantageOverviewParams, *, api_key: str
) -> Result:
    """Fetch company fundamentals for a stock: name, exchange, sector, industry,
    market cap, PE ratio, EPS, dividend yield, 52-week high/low, beta, and ~50
    more financial metrics. Returns a flat dict of string values.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="OVERVIEW",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_overview",
    )

    if not isinstance(data, dict) or not data.get("Symbol"):
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No overview data returned for symbol: {params.symbol}",
        )

    return Result(
        data=data,
        provenance=Provenance(
            source="alpha_vantage_overview", params={"symbol": params.symbol}
        ),
    )


# ---------------------------------------------------------------------------
# Company — Financial Statements (income, balance sheet, cash flow)
# ---------------------------------------------------------------------------

_STATEMENT_FUNCTIONS = {
    "income_statement": "INCOME_STATEMENT",
    "balance_sheet": "BALANCE_SHEET",
    "cash_flow": "CASH_FLOW",
}


class AlphaVantageStatementParams(BaseModel):
    """Financial statement for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    period: Literal["annual", "quarterly"] = Field(
        default="annual",
        description="'annual' for yearly reports; 'quarterly' for quarterly.",
    )


@connector(tags=["equities"], result_type="dict")
async def alpha_vantage_income_statement(
    params: AlphaVantageStatementParams, *, api_key: str
) -> Result:
    """Fetch income statement for a stock: revenue, gross profit, operating income,
    EBITDA, net income, R&D, SGA, and ~20 more line items. Returns annual or
    quarterly reports (up to 20 annual, 81 quarterly). All values are strings.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="INCOME_STATEMENT",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_income_statement",
    )

    key = "annualReports" if params.period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No income statement data for {params.symbol} ({params.period})",
        )

    return Result(
        data=reports,
        provenance=Provenance(
            source="alpha_vantage_income_statement",
            params={"symbol": params.symbol, "period": params.period},
        ),
    )


@connector(tags=["equities"], result_type="dict")
async def alpha_vantage_balance_sheet(
    params: AlphaVantageStatementParams, *, api_key: str
) -> Result:
    """Fetch balance sheet for a stock: total assets, liabilities, equity,
    cash, receivables, goodwill, long-term debt, and ~35 more line items.
    Returns annual or quarterly reports. All values are strings.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="BALANCE_SHEET",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_balance_sheet",
    )

    key = "annualReports" if params.period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No balance sheet data for {params.symbol} ({params.period})",
        )

    return Result(
        data=reports,
        provenance=Provenance(
            source="alpha_vantage_balance_sheet",
            params={"symbol": params.symbol, "period": params.period},
        ),
    )


@connector(tags=["equities"], result_type="dict")
async def alpha_vantage_cash_flow(
    params: AlphaVantageStatementParams, *, api_key: str
) -> Result:
    """Fetch cash flow statement for a stock: operating cash flow, capex,
    dividends, buybacks, financing, investing activities, and ~25 more items.
    Returns annual or quarterly reports. All values are strings.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="CASH_FLOW",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_cash_flow",
    )

    key = "annualReports" if params.period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No cash flow data for {params.symbol} ({params.period})",
        )

    return Result(
        data=reports,
        provenance=Provenance(
            source="alpha_vantage_cash_flow",
            params={"symbol": params.symbol, "period": params.period},
        ),
    )


# ---------------------------------------------------------------------------
# Company — Earnings
# ---------------------------------------------------------------------------

_EARNINGS_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="fiscalDateEnding", dtype="date", role=ColumnRole.DATA),
        Column(name="reportedDate", dtype="date", role=ColumnRole.DATA),
        Column(name="reportedEPS", dtype="numeric", role=ColumnRole.DATA),
        Column(name="estimatedEPS", dtype="numeric", role=ColumnRole.DATA),
        Column(name="surprise", dtype="numeric", role=ColumnRole.DATA),
        Column(name="surprisePercentage", dtype="numeric", role=ColumnRole.DATA),
        Column(name="reportTime", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageEarningsParams(BaseModel):
    """Earnings data for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


@connector(output=_EARNINGS_OUTPUT, tags=["equities"])
async def alpha_vantage_earnings(
    params: AlphaVantageEarningsParams, *, api_key: str
) -> Result:
    """Fetch quarterly earnings for a stock: reported EPS, estimated EPS,
    surprise, surprise percentage, and report timing (pre/post market).
    Returns up to 120 quarters of history.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="EARNINGS",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_earnings",
    )

    # Use quarterly earnings for richer data
    reports = data.get("quarterlyEarnings", [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No earnings data for {params.symbol}",
        )

    rows = [_clean_none_strings(r) for r in reports if r.get("fiscalDateEnding")]
    df = pd.DataFrame(rows)
    return _EARNINGS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_earnings", params={"symbol": params.symbol}
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Forex — Real-time Exchange Rate
# ---------------------------------------------------------------------------

_FX_RATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="from_currency", role=ColumnRole.KEY, namespace="alpha_vantage_fx"),
        Column(name="from_currency_name", role=ColumnRole.TITLE),
        Column(name="to_currency", role=ColumnRole.METADATA),
        Column(name="to_currency_name", role=ColumnRole.METADATA),
        Column(name="exchange_rate", dtype="numeric"),
        Column(name="bid_price", dtype="numeric"),
        Column(name="ask_price", dtype="numeric"),
        Column(name="last_refreshed", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageFxRateParams(BaseModel):
    """Real-time exchange rate between two currencies (forex or crypto)."""

    from_currency: str = Field(
        ..., description="Source currency code, e.g. 'EUR', 'BTC'"
    )
    to_currency: str = Field(
        ..., description="Target currency code, e.g. 'USD', 'JPY'"
    )


@connector(output=_FX_RATE_OUTPUT, tags=["forex", "crypto", "tool"])
async def alpha_vantage_fx_rate(
    params: AlphaVantageFxRateParams, *, api_key: str
) -> Result:
    """Fetch real-time exchange rate between two currencies. Works for both
    forex (EUR/USD) and crypto (BTC/USD). Returns bid/ask prices.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="CURRENCY_EXCHANGE_RATE",
        params={
            "from_currency": params.from_currency,
            "to_currency": params.to_currency,
        },
        op_name="alpha_vantage_fx_rate",
    )

    rate_data = data.get("Realtime Currency Exchange Rate", {})
    if not rate_data:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No exchange rate for {params.from_currency}/{params.to_currency}",
        )

    r = _strip_numbered_keys(rate_data)
    row = {
        "from_currency": r.get("From_Currency Code", params.from_currency),
        "from_currency_name": r.get("From_Currency Name", ""),
        "to_currency": r.get("To_Currency Code", params.to_currency),
        "to_currency_name": r.get("To_Currency Name", ""),
        "exchange_rate": r.get("Exchange Rate"),
        "bid_price": r.get("Bid Price"),
        "ask_price": r.get("Ask Price"),
        "last_refreshed": r.get("Last Refreshed", ""),
    }
    df = pd.DataFrame([row])
    return _FX_RATE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_rate",
            params={
                "from_currency": params.from_currency,
                "to_currency": params.to_currency,
            },
        ),
        params={"from_currency": params.from_currency},
    )


# ---------------------------------------------------------------------------
# Forex — Historical Daily
# ---------------------------------------------------------------------------

_FX_DAILY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="pair",
            role=ColumnRole.KEY,
            param_key="pair",
            namespace="alpha_vantage_fx",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageFxDailyParams(BaseModel):
    """Daily forex time series."""

    from_symbol: str = Field(..., description="Source currency code, e.g. 'EUR'")
    to_symbol: str = Field(..., description="Target currency code, e.g. 'USD'")
    outputsize: Literal["compact", "full"] = Field(
        default="compact",
        description="'compact' returns last 100 data points; 'full' returns full history.",
    )


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"])
async def alpha_vantage_fx_daily(
    params: AlphaVantageFxDailyParams, *, api_key: str
) -> Result:
    """Fetch daily forex OHLC time series for a currency pair.

    outputsize='compact' returns last 100 days (default); 'full' for full history.
    Note: no volume data for forex pairs.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="FX_DAILY",
        params={
            "from_symbol": params.from_symbol,
            "to_symbol": params.to_symbol,
            "outputsize": params.outputsize,
        },
        op_name="alpha_vantage_fx_daily",
    )

    ts_key = "Time Series FX (Daily)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No FX daily data for {params.from_symbol}/{params.to_symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
            }
        )

    pair = f"{params.from_symbol}/{params.to_symbol}"
    df = pd.DataFrame(rows)
    return _FX_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_daily",
            params={
                "from_symbol": params.from_symbol,
                "to_symbol": params.to_symbol,
            },
        ),
        params={"pair": pair},
    )


# ---------------------------------------------------------------------------
# Crypto — Historical Daily
# ---------------------------------------------------------------------------

_CRYPTO_DAILY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage_crypto",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageCryptoDailyParams(BaseModel):
    """Daily crypto time series."""

    symbol: str = Field(..., description="Crypto symbol, e.g. 'BTC', 'ETH'")
    market: str = Field(
        default="USD", description="Exchange market currency, e.g. 'USD', 'EUR'"
    )


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"])
async def alpha_vantage_crypto_daily(
    params: AlphaVantageCryptoDailyParams, *, api_key: str
) -> Result:
    """Fetch daily OHLCV time series for a cryptocurrency priced in a market
    currency (default USD). Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="DIGITAL_CURRENCY_DAILY",
        params={"symbol": params.symbol, "market": params.market},
        op_name="alpha_vantage_crypto_daily",
    )

    ts_key = "Time Series (Digital Currency Daily)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No crypto daily data for {params.symbol}/{params.market}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_crypto_daily",
            params={"symbol": params.symbol, "market": params.market},
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Economic Indicators
# ---------------------------------------------------------------------------

_ECON_FUNCTIONS = (
    "REAL_GDP", "REAL_GDP_PER_CAPITA", "TREASURY_YIELD", "FEDERAL_FUNDS_RATE",
    "CPI", "INFLATION", "RETAIL_SALES", "DURABLES", "UNEMPLOYMENT",
    "NONFARM_PAYROLL",
)

_ECON_OUTPUT = OutputConfig(
    columns=[
        Column(name="name", role=ColumnRole.KEY, namespace="alpha_vantage_econ"),
        Column(name="series_name", role=ColumnRole.TITLE),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="interval", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageEconParams(BaseModel):
    """US economic indicator time series."""

    function: Literal[_ECON_FUNCTIONS] = Field(  # type: ignore[valid-type]
        ...,
        description=(
            "Indicator: REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD, "
            "FEDERAL_FUNDS_RATE, CPI, INFLATION, RETAIL_SALES, DURABLES, "
            "UNEMPLOYMENT, NONFARM_PAYROLL."
        ),
    )
    interval: Literal["daily", "weekly", "monthly", "quarterly", "annual"] | None = Field(
        default=None,
        description=(
            "Data frequency. GDP: quarterly/annual. Rates: daily/weekly/monthly. "
            "Most indicators: monthly or annual only. Invalid values silently ignored."
        ),
    )
    maturity: Literal["3month", "2year", "5year", "7year", "10year", "30year"] | None = Field(
        default=None,
        description="Treasury maturity (TREASURY_YIELD only). Default: 10year.",
    )


@connector(output=_ECON_OUTPUT, tags=["macro"])
async def alpha_vantage_econ(
    params: AlphaVantageEconParams, *, api_key: str
) -> Result:
    """Fetch US economic indicator time series. Covers real GDP, CPI, inflation,
    unemployment, federal funds rate, treasury yield (with maturity selection),
    retail sales, durables, and nonfarm payroll. Commodity data is available
    via the FRED connector instead (superior historical coverage).
    Use maturity param for TREASURY_YIELD (default 10year).
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {}
    if params.interval is not None:
        req_params["interval"] = params.interval
    if params.maturity is not None and params.function == "TREASURY_YIELD":
        req_params["maturity"] = params.maturity

    data = await _av_fetch(
        http,
        function=params.function,
        params=req_params or None,
        op_name="alpha_vantage_econ",
    )

    observations = data.get("data", [])
    if not observations:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No data for {params.function}",
        )

    series_name = data.get("name", params.function)
    unit = data.get("unit", "")
    interval = data.get("interval", "")

    rows = []
    for obs in observations:
        val = obs.get("value")
        if val == ".":
            val = None
        rows.append(
            {
                "name": params.function,
                "series_name": series_name,
                "date": obs.get("date"),
                "value": val,
                "unit": unit,
                "interval": interval,
            }
        )

    df = pd.DataFrame(rows)
    prov_params: dict[str, Any] = {"function": params.function}
    if params.interval:
        prov_params["interval"] = params.interval
    if params.maturity:
        prov_params["maturity"] = params.maturity

    return _ECON_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_econ", params=prov_params),
        params={"name": params.function},
    )


# ---------------------------------------------------------------------------
# Alpha Intelligence — News Sentiment
# ---------------------------------------------------------------------------

_NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="url", role=ColumnRole.KEY, namespace="alpha_vantage_news"),
        Column(name="time_published", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="overall_sentiment_score", dtype="numeric", role=ColumnRole.DATA),
        Column(name="overall_sentiment_label", role=ColumnRole.METADATA),
        Column(name="summary", role=ColumnRole.METADATA),
        Column(name="banner_image", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)


class AlphaVantageNewsParams(BaseModel):
    """News sentiment articles."""

    tickers: str | None = Field(
        default=None,
        description="Comma-separated ticker(s), e.g. 'AAPL' or 'AAPL,MSFT'. Omit for general news.",
    )
    topics: str | None = Field(
        default=None,
        description=(
            "Comma-separated topics: technology, earnings, ipo, mergers_and_acquisitions, "
            "financial_markets, economy_fiscal, economy_monetary, economy_macro, "
            "energy_transportation, finance, life_sciences, manufacturing, real_estate, "
            "retail_wholesale, blockchain."
        ),
    )
    sort: Literal["LATEST", "EARLIEST", "RELEVANCE"] = Field(
        default="LATEST", description="Sort order for results."
    )
    limit: int = Field(
        default=50, ge=1, le=1000, description="Number of results (max 1000)."
    )


@connector(output=_NEWS_OUTPUT, tags=["news", "tool"])
async def alpha_vantage_news(
    params: AlphaVantageNewsParams, *, api_key: str
) -> Result:
    """Fetch news articles with sentiment scores. Filter by ticker(s) and/or
    topics. Each article includes title, summary, source, sentiment score
    (-1 to 1), and sentiment label. For ticker-specific sentiment, check the
    ticker_sentiment array in the raw response.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {"sort": params.sort, "limit": params.limit}
    if params.tickers:
        req_params["tickers"] = params.tickers
    if params.topics:
        req_params["topics"] = params.topics

    data = await _av_fetch(
        http,
        function="NEWS_SENTIMENT",
        params=req_params,
        op_name="alpha_vantage_news",
    )

    feed = data.get("feed", [])
    if not feed:
        raise EmptyDataError(
            provider=_PROVIDER,
            message="No news articles found",
        )

    rows = [
        {
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "time_published": item.get("time_published", ""),
            "source": item.get("source", ""),
            "overall_sentiment_score": item.get("overall_sentiment_score"),
            "overall_sentiment_label": item.get("overall_sentiment_label", ""),
            "summary": item.get("summary", ""),
            "banner_image": item.get("banner_image", ""),
        }
        for item in feed
    ]
    df = pd.DataFrame(rows)
    prov_params: dict[str, Any] = {}
    if params.tickers:
        prov_params["tickers"] = params.tickers
    if params.topics:
        prov_params["topics"] = params.topics

    return _NEWS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_news", params=prov_params),
        params=prov_params,
    )


# ---------------------------------------------------------------------------
# Alpha Intelligence — Top Gainers/Losers
# ---------------------------------------------------------------------------

_MOVERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="category", role=ColumnRole.TITLE),
        Column(name="price", dtype="numeric", role=ColumnRole.DATA),
        Column(name="change_amount", dtype="numeric", role=ColumnRole.DATA),
        Column(name="change_percentage", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageTopMoversParams(BaseModel):
    """Top gainers, losers, and most actively traded."""

    pass


@connector(output=_MOVERS_OUTPUT, tags=["equities", "tool"])
async def alpha_vantage_top_movers(
    params: AlphaVantageTopMoversParams, *, api_key: str
) -> Result:
    """Fetch today's top 20 gainers, top 20 losers, and top 20 most actively
    traded US equities. Each entry includes ticker, price, change amount,
    change percentage, and volume. No parameters required.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TOP_GAINERS_LOSERS",
        op_name="alpha_vantage_top_movers",
    )

    rows = []
    for category in ("top_gainers", "top_losers", "most_actively_traded"):
        items = data.get(category, [])
        for item in items:
            pct_raw = item.get("change_percentage", "0")
            pct = pct_raw.rstrip("%") if isinstance(pct_raw, str) else pct_raw
            rows.append(
                {
                    "ticker": item.get("ticker", ""),
                    "category": category,
                    "price": item.get("price"),
                    "change_amount": item.get("change_amount"),
                    "change_percentage": pct,
                    "volume": item.get("volume"),
                }
            )

    if not rows:
        raise EmptyDataError(
            provider=_PROVIDER, message="No market movers data returned"
        )

    df = pd.DataFrame(rows)
    return _MOVERS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_top_movers", params={}),
        params={},
    )


# ---------------------------------------------------------------------------
# Options — Historical Options Chain (premium only)
# ---------------------------------------------------------------------------

_OPTIONS_OUTPUT = OutputConfig(
    columns=[
        Column(name="contractID", role=ColumnRole.KEY, namespace="alpha_vantage_options"),
        Column(name="symbol", role=ColumnRole.TITLE),
        Column(name="expiration", dtype="date", role=ColumnRole.DATA),
        Column(name="strike", dtype="numeric", role=ColumnRole.DATA),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="last", dtype="numeric", role=ColumnRole.DATA),
        Column(name="bid", dtype="numeric", role=ColumnRole.DATA),
        Column(name="ask", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
        Column(name="open_interest", dtype="numeric", role=ColumnRole.DATA),
        Column(name="implied_volatility", dtype="numeric", role=ColumnRole.DATA),
        Column(name="delta", dtype="numeric", role=ColumnRole.DATA),
        Column(name="gamma", dtype="numeric", role=ColumnRole.DATA),
        Column(name="theta", dtype="numeric", role=ColumnRole.DATA),
        Column(name="vega", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageOptionsParams(BaseModel):
    """Historical options chain for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    date: str | None = Field(
        default=None,
        description="Options date (YYYY-MM-DD). Omit for latest available.",
    )


@connector(output=_OPTIONS_OUTPUT, tags=["equities", "options"])
async def alpha_vantage_options(
    params: AlphaVantageOptionsParams, *, api_key: str
) -> Result:
    """[Premium] Fetch historical options chain for a stock: contract ID,
    expiration, strike, type (call/put), last price, bid/ask, volume,
    open interest, implied volatility, and Greeks (delta, gamma, theta, vega).
    Requires a premium Alpha Vantage plan.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {"symbol": params.symbol}
    if params.date:
        req_params["date"] = params.date

    data = await _av_fetch(
        http,
        function="HISTORICAL_OPTIONS",
        params=req_params,
        op_name="alpha_vantage_options",
    )

    # Response is a list of option contracts
    contracts = data if isinstance(data, list) else data.get("data", [])
    if not contracts:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No options data for {params.symbol}",
        )

    df = pd.DataFrame(contracts)
    prov_params: dict[str, Any] = {"symbol": params.symbol}
    if params.date:
        prov_params["date"] = params.date

    return _OPTIONS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_options", params=prov_params
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Market Data — Weekly & Monthly Time Series
# ---------------------------------------------------------------------------


class AlphaVantageWeeklyParams(BaseModel):
    """Weekly OHLCV time series for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


@connector(output=_DAILY_OUTPUT, tags=["equities"])
async def alpha_vantage_weekly(
    params: AlphaVantageWeeklyParams, *, api_key: str
) -> Result:
    """Fetch weekly OHLCV (open, high, low, close, volume) time series for a stock.

    Returns 20+ years of weekly data. Last trading day of each week is the timestamp.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_WEEKLY",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_weekly",
    )

    ts_key = "Weekly Time Series"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No weekly data returned for symbol: {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_weekly", params={"symbol": params.symbol}
        ),
        params={"symbol": params.symbol},
    )


class AlphaVantageMonthlyParams(BaseModel):
    """Monthly OHLCV time series for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


@connector(output=_DAILY_OUTPUT, tags=["equities"])
async def alpha_vantage_monthly(
    params: AlphaVantageMonthlyParams, *, api_key: str
) -> Result:
    """Fetch monthly OHLCV (open, high, low, close, volume) time series for a stock.

    Returns 20+ years of monthly data. Last trading day of each month is the timestamp.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_MONTHLY",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_monthly",
    )

    ts_key = "Monthly Time Series"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No monthly data returned for symbol: {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_monthly", params={"symbol": params.symbol}
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Market Data — Intraday Time Series
# ---------------------------------------------------------------------------

_INTRADAY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="timestamp", dtype="datetime", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageIntradayParams(BaseModel):
    """Intraday OHLCV time series for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    interval: Literal["1min", "5min", "15min", "30min", "60min"] = Field(
        default="60min",
        description="Time interval: 1min, 5min, 15min, 30min, or 60min.",
    )
    outputsize: Literal["compact", "full"] = Field(
        default="compact",
        description="'compact' returns last 100 data points; 'full' returns full intraday history.",
    )


@connector(output=_INTRADAY_OUTPUT, tags=["equities"])
async def alpha_vantage_intraday(
    params: AlphaVantageIntradayParams, *, api_key: str
) -> Result:
    """Fetch intraday OHLCV time series for a stock at 1/5/15/30/60 min intervals.

    outputsize='compact' returns the last 100 data points (default).
    outputsize='full' returns the full intraday time series for the current and
    previous trading day.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_INTRADAY",
        params={
            "symbol": params.symbol,
            "interval": params.interval,
            "outputsize": params.outputsize,
        },
        op_name="alpha_vantage_intraday",
    )

    ts_key = f"Time Series ({params.interval})"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No intraday data for {params.symbol} at {params.interval}",
        )

    rows = []
    for ts_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "timestamp": ts_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _INTRADAY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_intraday",
            params={
                "symbol": params.symbol,
                "interval": params.interval,
                "outputsize": params.outputsize,
            },
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Forex — Weekly & Monthly
# ---------------------------------------------------------------------------


class AlphaVantageFxWeeklyParams(BaseModel):
    """Weekly forex time series."""

    from_symbol: str = Field(..., description="Source currency code, e.g. 'EUR'")
    to_symbol: str = Field(..., description="Target currency code, e.g. 'USD'")


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"])
async def alpha_vantage_fx_weekly(
    params: AlphaVantageFxWeeklyParams, *, api_key: str
) -> Result:
    """Fetch weekly forex OHLC time series for a currency pair.

    Returns full history of weekly data. No volume data for forex.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="FX_WEEKLY",
        params={
            "from_symbol": params.from_symbol,
            "to_symbol": params.to_symbol,
        },
        op_name="alpha_vantage_fx_weekly",
    )

    ts_key = "Time Series FX (Weekly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No FX weekly data for {params.from_symbol}/{params.to_symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
            }
        )

    pair = f"{params.from_symbol}/{params.to_symbol}"
    df = pd.DataFrame(rows)
    return _FX_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_weekly",
            params={
                "from_symbol": params.from_symbol,
                "to_symbol": params.to_symbol,
            },
        ),
        params={"pair": pair},
    )


class AlphaVantageFxMonthlyParams(BaseModel):
    """Monthly forex time series."""

    from_symbol: str = Field(..., description="Source currency code, e.g. 'EUR'")
    to_symbol: str = Field(..., description="Target currency code, e.g. 'USD'")


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"])
async def alpha_vantage_fx_monthly(
    params: AlphaVantageFxMonthlyParams, *, api_key: str
) -> Result:
    """Fetch monthly forex OHLC time series for a currency pair.

    Returns full history of monthly data. No volume data for forex.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="FX_MONTHLY",
        params={
            "from_symbol": params.from_symbol,
            "to_symbol": params.to_symbol,
        },
        op_name="alpha_vantage_fx_monthly",
    )

    ts_key = "Time Series FX (Monthly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No FX monthly data for {params.from_symbol}/{params.to_symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
            }
        )

    pair = f"{params.from_symbol}/{params.to_symbol}"
    df = pd.DataFrame(rows)
    return _FX_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_monthly",
            params={
                "from_symbol": params.from_symbol,
                "to_symbol": params.to_symbol,
            },
        ),
        params={"pair": pair},
    )


# ---------------------------------------------------------------------------
# Crypto — Weekly & Monthly
# ---------------------------------------------------------------------------


class AlphaVantageCryptoWeeklyParams(BaseModel):
    """Weekly crypto time series."""

    symbol: str = Field(..., description="Crypto symbol, e.g. 'BTC', 'ETH'")
    market: str = Field(
        default="USD", description="Exchange market currency, e.g. 'USD', 'EUR'"
    )


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"])
async def alpha_vantage_crypto_weekly(
    params: AlphaVantageCryptoWeeklyParams, *, api_key: str
) -> Result:
    """Fetch weekly OHLCV time series for a cryptocurrency. Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="DIGITAL_CURRENCY_WEEKLY",
        params={"symbol": params.symbol, "market": params.market},
        op_name="alpha_vantage_crypto_weekly",
    )

    ts_key = "Time Series (Digital Currency Weekly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No crypto weekly data for {params.symbol}/{params.market}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_crypto_weekly",
            params={"symbol": params.symbol, "market": params.market},
        ),
        params={"symbol": params.symbol},
    )


class AlphaVantageCryptoMonthlyParams(BaseModel):
    """Monthly crypto time series."""

    symbol: str = Field(..., description="Crypto symbol, e.g. 'BTC', 'ETH'")
    market: str = Field(
        default="USD", description="Exchange market currency, e.g. 'USD', 'EUR'"
    )


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"])
async def alpha_vantage_crypto_monthly(
    params: AlphaVantageCryptoMonthlyParams, *, api_key: str
) -> Result:
    """Fetch monthly OHLCV time series for a cryptocurrency. Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="DIGITAL_CURRENCY_MONTHLY",
        params={"symbol": params.symbol, "market": params.market},
        op_name="alpha_vantage_crypto_monthly",
    )

    ts_key = "Time Series (Digital Currency Monthly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No crypto monthly data for {params.symbol}/{params.market}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_crypto_monthly",
            params={"symbol": params.symbol, "market": params.market},
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Company — ETF Profile
# ---------------------------------------------------------------------------


class AlphaVantageEtfProfileParams(BaseModel):
    """ETF profile including holdings and sector allocation."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="ETF ticker, e.g. 'SPY', 'QQQ'. Use alpha_vantage_search to find ETFs.",
    )


@connector(tags=["equities", "etf"], result_type="dict")
async def alpha_vantage_etf_profile(
    params: AlphaVantageEtfProfileParams, *, api_key: str
) -> Result:
    """Fetch ETF profile: net assets, expense ratio, portfolio turnover,
    dividend yield, inception date, top holdings (symbol, description, weight),
    and sector allocation. Note: aggressive rate limiting on free tier.
    Use alpha_vantage_search to resolve ETF symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="ETF_PROFILE",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_etf_profile",
    )

    if not isinstance(data, dict) or (not data.get("holdings") and not data.get("net_assets")):
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No ETF profile data for {params.symbol}",
        )

    return Result(
        data=data,
        provenance=Provenance(
            source="alpha_vantage_etf_profile", params={"symbol": params.symbol}
        ),
    )


# ---------------------------------------------------------------------------
# Calendars — Earnings Calendar & IPO Calendar (CSV endpoints)
# ---------------------------------------------------------------------------


async def _av_fetch_csv(
    http: HttpClient,
    *,
    function: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> pd.DataFrame:
    """Fetch Alpha Vantage CSV endpoints (calendars, listing status).

    Returns a parsed DataFrame. Handles the rate-limit edge case where the CSV
    body starts with 'Information' instead of column headers.
    """
    import io

    req_params: dict[str, Any] = {"function": function}
    if params:
        req_params.update(params)

    try:
        response = await http.request("GET", "/query", params=req_params)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        match status:
            case 401 | 403:
                raise UnauthorizedError(
                    provider=_PROVIDER,
                    message="Invalid or missing Alpha Vantage API key",
                ) from e
            case 429:
                raise RateLimitError(
                    provider=_PROVIDER,
                    retry_after=60.0,
                    message=f"Alpha Vantage rate limit hit on '{op_name}'",
                ) from e
            case _:
                raise ProviderError(
                    provider=_PROVIDER,
                    status_code=status,
                    message=f"Alpha Vantage API error {status} on '{op_name}'",
                ) from e

    text = response.text
    # Rate-limit responses come as CSV with "Information" as header
    if text.startswith("Information"):
        raise RateLimitError(
            provider=_PROVIDER,
            retry_after=60.0,
            message=f"Alpha Vantage rate limit on '{op_name}'",
        )

    df = pd.read_csv(io.StringIO(text))
    return df


_EARNINGS_CAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="reportDate", dtype="date", role=ColumnRole.DATA),
        Column(name="fiscalDateEnding", dtype="date", role=ColumnRole.DATA),
        Column(name="estimate", dtype="numeric", role=ColumnRole.DATA),
        Column(name="currency", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageEarningsCalendarParams(BaseModel):
    """Upcoming earnings release dates."""

    horizon: Literal["3month", "6month", "12month"] = Field(
        default="3month",
        description="Lookahead window: 3month, 6month, or 12month.",
    )
    symbol: str | None = Field(
        default=None,
        description="Optional ticker to filter by, e.g. 'IBM'. Omit for all companies.",
    )


@connector(output=_EARNINGS_CAL_OUTPUT, tags=["equities", "calendars"])
async def alpha_vantage_earnings_calendar(
    params: AlphaVantageEarningsCalendarParams, *, api_key: str
) -> Result:
    """Fetch upcoming earnings release dates. Returns company name, report date,
    fiscal date ending, EPS estimate, and currency. Filter by symbol or get
    all upcoming earnings within the horizon window.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {"horizon": params.horizon}
    if params.symbol:
        req_params["symbol"] = params.symbol

    df = await _av_fetch_csv(
        http,
        function="EARNINGS_CALENDAR",
        params=req_params,
        op_name="alpha_vantage_earnings_calendar",
    )

    if df.empty:
        raise EmptyDataError(
            provider=_PROVIDER,
            message="No upcoming earnings events found",
        )

    prov_params: dict[str, Any] = {"horizon": params.horizon}
    if params.symbol:
        prov_params["symbol"] = params.symbol

    return _EARNINGS_CAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_earnings_calendar", params=prov_params
        ),
        params=prov_params,
    )


_IPO_CAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="ipoDate", dtype="date", role=ColumnRole.DATA),
        Column(name="priceRangeLow", dtype="numeric", role=ColumnRole.DATA),
        Column(name="priceRangeHigh", dtype="numeric", role=ColumnRole.DATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageIpoCalendarParams(BaseModel):
    """Upcoming and recent IPOs."""

    pass


@connector(output=_IPO_CAL_OUTPUT, tags=["equities", "calendars"])
async def alpha_vantage_ipo_calendar(
    params: AlphaVantageIpoCalendarParams, *, api_key: str
) -> Result:
    """Fetch upcoming and recent IPOs: company name, expected IPO date,
    price range (low/high), currency, and exchange.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    df = await _av_fetch_csv(
        http,
        function="IPO_CALENDAR",
        op_name="alpha_vantage_ipo_calendar",
    )

    if df.empty:
        raise EmptyDataError(
            provider=_PROVIDER,
            message="No upcoming IPO events found",
        )

    return _IPO_CAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_ipo_calendar", params={}),
        params={},
    )


# ---------------------------------------------------------------------------
# Technical Indicators (unified)
# ---------------------------------------------------------------------------

_TECHNICAL_INDICATORS = (
    "SMA", "EMA", "WMA", "DEMA", "TEMA", "TRIMA", "KAMA", "MAMA", "VWAP",
    "T3", "RSI", "WILLR", "ADX", "ADXR", "APO", "PPO", "MOM", "BOP",
    "CCI", "CMO", "ROC", "ROCR", "AROON", "AROONOSC", "MFI", "TRIX",
    "ULTOSC", "DX", "MINUS_DI", "PLUS_DI", "MINUS_DM", "PLUS_DM",
    "BBANDS", "MIDPOINT", "MIDPRICE", "SAR", "TRANGE", "ATR", "NATR",
    "AD", "ADOSC", "OBV", "HT_TRENDLINE", "HT_SINE", "HT_TRENDMODE",
    "HT_DCPERIOD", "HT_DCPHASE", "HT_PHASOR", "STOCH", "STOCHF",
    "STOCHRSI", "MACD", "MACDEXT",
)

_TECHNICAL_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
    ]
)


class AlphaVantageTechnicalParams(BaseModel):
    """Technical indicator for a stock symbol."""

    symbol: Annotated[str, Namespace("alpha_vantage")] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    function: Literal[_TECHNICAL_INDICATORS] = Field(  # type: ignore[valid-type]
        ...,
        description=(
            "Indicator function. Common: SMA, EMA, RSI, MACD, BBANDS, STOCH, ADX, "
            "CCI, WILLR, MFI, OBV, ATR, VWAP, AROON, SAR, TRIX, APO, PPO."
        ),
    )
    interval: Literal["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"] = Field(
        default="daily",
        description="Time interval for the indicator.",
    )
    time_period: int = Field(
        default=20,
        ge=1,
        description="Number of data points for the calculation (e.g. 20 for SMA-20).",
    )
    series_type: Literal["close", "open", "high", "low"] = Field(
        default="close",
        description="Price type to use for the calculation.",
    )


@connector(output=_TECHNICAL_OUTPUT, tags=["equities", "technical"])
async def alpha_vantage_technical(
    params: AlphaVantageTechnicalParams, *, api_key: str
) -> Result:
    """Fetch a technical indicator for a stock. Supports 50+ indicators including
    SMA, EMA, RSI, MACD, Bollinger Bands, Stochastic, ADX, CCI, OBV, ATR, and more.
    The response columns vary by indicator (e.g. SMA returns 'SMA', BBANDS returns
    'Real Upper Band', 'Real Middle Band', 'Real Lower Band'). All values are numeric.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function=params.function,
        params={
            "symbol": params.symbol,
            "interval": params.interval,
            "time_period": params.time_period,
            "series_type": params.series_type,
        },
        op_name="alpha_vantage_technical",
    )

    # Technical Analysis key varies: "Technical Analysis: SMA", "Technical Analysis: RSI", etc.
    ta_key = f"Technical Analysis: {params.function}"
    time_series = data.get(ta_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No {params.function} data for {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        row: dict[str, Any] = {"date": date_str}
        row.update(values)
        rows.append(row)

    df = pd.DataFrame(rows)
    # Convert all indicator value columns to numeric
    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return _TECHNICAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_technical",
            params={
                "symbol": params.symbol,
                "function": params.function,
                "interval": params.interval,
                "time_period": params.time_period,
                "series_type": params.series_type,
            },
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Precious Metals — Spot Price & Historical
# ---------------------------------------------------------------------------

_METAL_SPOT_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage_metal"),
        Column(name="nominal", role=ColumnRole.TITLE),
        Column(name="price", dtype="numeric", role=ColumnRole.DATA),
        Column(name="timestamp", role=ColumnRole.METADATA),
    ]
)

_METAL_HISTORY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage_metal",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="price", dtype="numeric", role=ColumnRole.DATA),
    ]
)


class AlphaVantageMetalSpotParams(BaseModel):
    """Real-time spot price for gold or silver."""

    symbol: Literal["GOLD", "XAU", "SILVER", "XAG"] = Field(
        ...,
        description="Metal symbol: GOLD or XAU (gold), SILVER or XAG (silver).",
    )


@connector(output=_METAL_SPOT_OUTPUT, tags=["commodities"])
async def alpha_vantage_metal_spot(
    params: AlphaVantageMetalSpotParams, *, api_key: str
) -> Result:
    """Fetch real-time spot price for gold or silver. Returns current price and
    timestamp. Use GOLD/XAU for gold, SILVER/XAG for silver.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="GOLD_SILVER_SPOT",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_metal_spot",
    )

    if not isinstance(data, dict) or "price" not in data:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No spot price data for {params.symbol}",
        )

    row = {
        "symbol": params.symbol,
        "nominal": data.get("nominal", params.symbol),
        "price": data.get("price"),
        "timestamp": data.get("timestamp", ""),
    }
    df = pd.DataFrame([row])
    return _METAL_SPOT_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_metal_spot", params={"symbol": params.symbol}
        ),
        params={"symbol": params.symbol},
    )


class AlphaVantageMetalHistoryParams(BaseModel):
    """Historical prices for gold or silver."""

    symbol: Literal["GOLD", "XAU", "SILVER", "XAG"] = Field(
        ...,
        description="Metal symbol: GOLD or XAU (gold), SILVER or XAG (silver).",
    )
    interval: Literal["daily", "weekly", "monthly"] = Field(
        default="monthly",
        description="Data frequency: daily, weekly, or monthly.",
    )


@connector(output=_METAL_HISTORY_OUTPUT, tags=["commodities"])
async def alpha_vantage_metal_history(
    params: AlphaVantageMetalHistoryParams, *, api_key: str
) -> Result:
    """Fetch historical prices for gold or silver. Returns date and price.
    Note: uses 'price' field (not 'value') unlike other commodity endpoints.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="GOLD_SILVER_HISTORY",
        params={"symbol": params.symbol, "interval": params.interval},
        op_name="alpha_vantage_metal_history",
    )

    observations = data.get("data", [])
    if not observations:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No historical data for {params.symbol}",
        )

    rows = []
    for obs in observations:
        price = obs.get("price")
        if price == ".":
            price = None
        rows.append({"date": obs.get("date"), "price": price})

    df = pd.DataFrame(rows)
    return _METAL_HISTORY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_metal_history",
            params={"symbol": params.symbol, "interval": params.interval},
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Listing Status — Enumerator
# ---------------------------------------------------------------------------

_LISTING_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="assetType", role=ColumnRole.METADATA),
        Column(name="ipoDate", role=ColumnRole.METADATA),
        Column(name="status", role=ColumnRole.METADATA),
    ]
)


class AlphaVantageListingParams(BaseModel):
    """Parameters for enumerating listed securities."""

    state: Literal["active", "delisted"] = Field(
        default="active",
        description="'active' for current listings; 'delisted' for historical.",
    )


@enumerator(output=_LISTING_OUTPUT, tags=["equities"])
async def enumerate_alpha_vantage(
    params: AlphaVantageListingParams, *, api_key: str
) -> pd.DataFrame:
    """Enumerate all US-listed securities from Alpha Vantage for catalog indexing.

    Returns symbol, name, exchange, asset type (Stock/ETF), IPO date, and status.
    Use state='active' for current listings (default), 'delisted' for historical.
    """
    http = _make_http(api_key)
    df = await _av_fetch_csv(
        http,
        function="LISTING_STATUS",
        params={"state": params.state},
        op_name="enumerate_alpha_vantage",
    )

    if df.empty:
        return pd.DataFrame(
            columns=["symbol", "name", "exchange", "assetType", "ipoDate", "status"]
        )

    # Keep only the columns we care about
    keep = ["symbol", "name", "exchange", "assetType", "ipoDate", "status"]
    cols = [c for c in keep if c in df.columns]
    return df[cols]


# ---------------------------------------------------------------------------
# Connector collections
# ---------------------------------------------------------------------------

AV_FETCH_CONNECTORS = Connectors(
    [
        # Market data
        alpha_vantage_quote,
        alpha_vantage_daily,
        alpha_vantage_weekly,
        alpha_vantage_monthly,
        alpha_vantage_intraday,
        # Company fundamentals
        alpha_vantage_overview,
        alpha_vantage_income_statement,
        alpha_vantage_balance_sheet,
        alpha_vantage_cash_flow,
        alpha_vantage_earnings,
        alpha_vantage_etf_profile,
        # Calendars
        alpha_vantage_earnings_calendar,
        alpha_vantage_ipo_calendar,
        # Forex
        alpha_vantage_fx_rate,
        alpha_vantage_fx_daily,
        alpha_vantage_fx_weekly,
        alpha_vantage_fx_monthly,
        # Crypto
        alpha_vantage_crypto_daily,
        alpha_vantage_crypto_weekly,
        alpha_vantage_crypto_monthly,
        # Economic indicators
        alpha_vantage_econ,
        # Precious metals (real-time spot — not available via FRED)
        alpha_vantage_metal_spot,
        alpha_vantage_metal_history,
        # Alpha intelligence
        alpha_vantage_news,
        alpha_vantage_top_movers,
        # Technical indicators
        alpha_vantage_technical,
        # Options
        alpha_vantage_options,
    ]
)

FETCH_CONNECTORS = AV_FETCH_CONNECTORS

CONNECTORS = Connectors(
    [
        alpha_vantage_search,
        *AV_FETCH_CONNECTORS,
        enumerate_alpha_vantage,
    ]
)
