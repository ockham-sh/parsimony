"""FMP source: Financial Modeling Prep API connectors.

Provides 18 connectors covering the full FMP equity research surface:
utilities, core market data, fundamentals, events, signals, and market context.
Mirrors the 17-tool toolset from the YAML-based main branch, adapted to the
decorator-based connector framework.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

import httpx
import pandas as pd
from pydantic import BaseModel, Field

from ockham.connector import Connectors, Namespace, connector
from ockham.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from ockham.transport.http import HttpClient


def _make_http(api_key: str, base_url: str = "https://financialmodelingprep.com/stable") -> HttpClient:
    return HttpClient(base_url, query_params={"apikey": api_key})


# ---------------------------------------------------------------------------
# Shared fetch logic
# ---------------------------------------------------------------------------


async def _fmp_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any],
    op_name: str,
    output_config: OutputConfig | None = None,
) -> Result:
    """Shared FMP fetch: path template substitution, JSON extraction, Result building.

    Handles FMP-specific HTTP errors (401/402) with user-friendly messages that
    never expose the API key.
    """
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

    rendered = re.sub(r"\{[^}]+\}", "", rendered)

    try:
        response = await http.request("GET", f"/{rendered.lstrip('/')}", params=query_params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        match e.response.status_code:
            case 401:
                raise ValueError("Invalid or missing FMP API key") from None
            case 402:
                raise ValueError(
                    "Your FMP plan is not eligible for this data request"
                ) from None
            case _:
                raise ValueError(
                    f"FMP API error {e.response.status_code} on endpoint '{op_name}'"
                ) from None

    data = response.json()

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for key in ("historical", "data", "results"):
            if key in data and isinstance(data[key], list):
                df = pd.DataFrame(data[key])
                break
        else:
            df = pd.DataFrame([data])
    else:
        raise ValueError(f"Unexpected response type from FMP: {type(data)}")

    if df.empty:
        raise ValueError(f"No data returned from FMP endpoint '{op_name}'")

    prov = Provenance(source=op_name, params=dict(params))

    if output_config is not None:
        return output_config.build_table_result(df, provenance=prov, params=dict(params))
    return Result.from_dataframe(df, prov)


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class FmpSymbolParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock ticker symbol (e.g. AAPL)"
    )


class FmpSymbolsParams(BaseModel):
    """Comma-separated symbols for batch endpoints."""

    symbols: str = Field(
        ..., description="Comma-separated stock symbols (e.g. AAPL,MSFT,GOOGL)"
    )


class FmpSearchParams(BaseModel):
    query: str = Field(..., description="Company name fragment or partial ticker (e.g. 'Deutsche Bank' or 'DBK')")
    limit: int = Field(default=20, description="Maximum number of results (default 20)")
    exchange: str | None = Field(default=None, description="Restrict to exchange (e.g. NYSE, NASDAQ, XETRA)")


class FmpFinancialStatementParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock ticker symbol (e.g. AAPL)"
    )
    period: str = Field(default="annual", description="Reporting period (annual or quarter)")
    limit: int = Field(default=5, description="Maximum number of periods to return")


class FmpHistoricalPricesParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock ticker symbol (e.g. AAPL)"
    )
    frequency: str = Field(
        default="daily",
        description="Price frequency: daily, dividend_adjusted, 1min, 5min, 15min, 30min, 1hour, 4hour",
    )
    from_date: str | None = Field(default=None, alias="from", description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, alias="to", description="End date (YYYY-MM-DD)")

    model_config = {"populate_by_name": True}


class FmpTaxonomyParams(BaseModel):
    type: Literal["sectors", "industries", "exchanges", "symbols_with_financials"] = Field(
        ..., description="Taxonomy type: sectors, industries, exchanges, or symbols_with_financials"
    )


class FmpNewsParams(BaseModel):
    type: Literal["news", "press_releases"] = Field(
        ..., description="news for third-party articles, press_releases for official company IR"
    )
    symbols: str = Field(..., description="Comma-separated stock symbols (e.g. AAPL,MSFT)")
    from_date: str | None = Field(default=None, alias="from", description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, alias="to", description="End date (YYYY-MM-DD)")
    limit: int = Field(default=20, description="Max records (default 20, max 250)")
    page: int = Field(default=0, description="Page offset (0-indexed)")

    model_config = {"populate_by_name": True}


class FmpInsiderTradesParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    limit: int = Field(default=20, description="Max trades to return (default 20)")
    page: int = Field(default=0, description="Page offset (0-indexed)")


class FmpInstitutionalPositionsParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    year: str = Field(..., description="Reporting year (e.g. 2024)")
    quarter: str = Field(..., description="Reporting quarter (1, 2, 3, or 4)")


class FmpEarningsTranscriptParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    year: str = Field(..., description="Fiscal year (e.g. 2024)")
    quarter: str = Field(..., description="Fiscal quarter (1, 2, 3, or 4)")


class FmpCorporateHistoryParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    event_type: Literal["earnings", "dividends", "splits"] = Field(
        ..., description="Type of corporate event"
    )
    limit: int = Field(default=10, description="Max historical records (default 10)")


class FmpEventCalendarParams(BaseModel):
    event_type: Literal["earnings", "dividends", "splits"] = Field(
        ..., description="Calendar type: earnings, dividends, or splits"
    )
    from_date: str | None = Field(default=None, alias="from", description="Start date (YYYY-MM-DD, max 90-day range)")
    to_date: str | None = Field(default=None, alias="to", description="End date (YYYY-MM-DD)")

    model_config = {"populate_by_name": True}


class FmpAnalystEstimatesParams(BaseModel):
    symbol: Annotated[str, Namespace("fmp_symbols")] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    period: str = Field(default="annual", description="annual or quarter")
    limit: int = Field(default=4, description="Number of estimate periods (default 4)")


class FmpIndexConstituentsParams(BaseModel):
    index: Literal["SP500", "NASDAQ", "DOW_JONES"] = Field(
        ..., description="Index: SP500, NASDAQ, or DOW_JONES"
    )


class FmpMarketMoversParams(BaseModel):
    type: Literal["gainers", "losers", "most_actives"] = Field(
        ..., description="gainers, losers, or most_actives"
    )


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------


SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="name"),
        Column(name="currency"),
        Column(name="exchangeFullName"),
        Column(name="exchange"),
    ]
)

COMPANY_PROFILE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="companyName"),
        Column(name="price", dtype="numeric"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="beta", dtype="numeric"),
        Column(name="exchange"),
        Column(name="exchangeFullName"),
        Column(name="currency"),
        Column(name="sector"),
        Column(name="industry"),
        Column(name="country"),
        Column(name="fullTimeEmployees", dtype="numeric"),
        Column(name="ceo"),
        Column(name="description"),
        Column(name="website"),
        Column(name="ipoDate"),
        Column(name="isEtf", dtype="bool"),
        Column(name="isActivelyTrading", dtype="bool"),
        Column(name="isAdr", dtype="bool"),
        Column(name="isFund", dtype="bool"),
    ]
)

INCOME_STATEMENT_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", dtype="datetime"),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="reportedCurrency", role=ColumnRole.METADATA),
        Column(name="revenue", dtype="numeric"),
        Column(name="costOfRevenue", dtype="numeric"),
        Column(name="grossProfit", dtype="numeric"),
        Column(name="operatingExpenses", dtype="numeric"),
        Column(name="operatingIncome", dtype="numeric"),
        Column(name="ebitda", dtype="numeric"),
        Column(name="netIncome", dtype="numeric"),
        Column(name="eps", dtype="numeric"),
        Column(name="epsDiluted", dtype="numeric"),
    ]
)

BALANCE_SHEET_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", dtype="datetime"),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="totalAssets", dtype="numeric"),
        Column(name="totalLiabilities", dtype="numeric"),
        Column(name="totalStockholdersEquity", dtype="numeric"),
        Column(name="totalDebt", dtype="numeric"),
        Column(name="netDebt", dtype="numeric"),
        Column(name="cashAndCashEquivalents", dtype="numeric"),
        Column(name="*"),
    ]
)

CASH_FLOW_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", dtype="datetime"),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="reportedCurrency", role=ColumnRole.METADATA),
        Column(name="netIncome", dtype="numeric"),
        Column(name="operatingCashFlow", dtype="numeric"),
        Column(name="capitalExpenditure", dtype="numeric"),
        Column(name="freeCashFlow", dtype="numeric"),
        Column(name="netCashProvidedByOperatingActivities", dtype="numeric"),
        Column(name="netCashProvidedByInvestingActivities", dtype="numeric"),
        Column(name="netCashProvidedByFinancingActivities", dtype="numeric"),
        Column(name="netChangeInCash", dtype="numeric"),
        Column(name="*"),
    ]
)

HISTORICAL_PRICES_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="changePercent", dtype="numeric"),
        Column(name="vwap", dtype="numeric"),
    ]
)

STOCK_QUOTE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="name"),
        Column(name="price", dtype="numeric"),
        Column(name="changesPercentage", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="dayLow", dtype="numeric"),
        Column(name="dayHigh", dtype="numeric"),
        Column(name="yearLow", dtype="numeric"),
        Column(name="yearHigh", dtype="numeric"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="avgVolume", dtype="numeric"),
        Column(name="pe", dtype="numeric"),
        Column(name="eps", dtype="numeric"),
        Column(name="priceAvg50", dtype="numeric"),
        Column(name="priceAvg200", dtype="numeric"),
        Column(name="exchange"),
        Column(name="open", dtype="numeric"),
        Column(name="previousClose", dtype="numeric"),
    ]
)

PEERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="companyName"),
        Column(name="price", dtype="numeric"),
        Column(name="mktCap", dtype="numeric"),
    ]
)

NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="publishedDate", dtype="datetime"),
        Column(name="title"),
        Column(name="text"),
        Column(name="url"),
        Column(name="site"),
        Column(name="image"),
    ]
)

INSIDER_TRADES_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="filingDate", dtype="datetime"),
        Column(name="transactionDate", dtype="datetime"),
        Column(name="reportingName"),
        Column(name="typeOfOwner"),
        Column(name="transactionType"),
        Column(name="acquisitionOrDisposition"),
        Column(name="securitiesTransacted", dtype="numeric"),
        Column(name="price", dtype="numeric"),
        Column(name="securitiesOwned", dtype="numeric"),
        Column(name="formType"),
        Column(name="url"),
    ]
)

INSTITUTIONAL_POSITIONS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="date", dtype="date"),
        Column(name="investorsHolding", dtype="numeric"),
        Column(name="investorsHoldingChange", dtype="numeric"),
        Column(name="numberOf13Fshares", dtype="numeric"),
        Column(name="numberOf13FsharesChange", dtype="numeric"),
        Column(name="totalInvested", dtype="numeric"),
        Column(name="totalInvestedChange", dtype="numeric"),
        Column(name="ownershipPercent", dtype="numeric"),
        Column(name="ownershipPercentChange", dtype="numeric"),
        Column(name="newPositions", dtype="numeric"),
        Column(name="closedPositions", dtype="numeric"),
        Column(name="increasedPositions", dtype="numeric"),
        Column(name="reducedPositions", dtype="numeric"),
        Column(name="putCallRatio", dtype="numeric"),
    ]
)

EARNINGS_TRANSCRIPT_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="year", dtype="numeric"),
        Column(name="period"),
        Column(name="date", dtype="date"),
        Column(name="content"),
    ]
)

ANALYST_ESTIMATES_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="date", dtype="date"),
        Column(name="revenueLow", dtype="numeric"),
        Column(name="revenueAvg", dtype="numeric"),
        Column(name="revenueHigh", dtype="numeric"),
        Column(name="ebitdaLow", dtype="numeric"),
        Column(name="ebitdaAvg", dtype="numeric"),
        Column(name="ebitdaHigh", dtype="numeric"),
        Column(name="netIncomeLow", dtype="numeric"),
        Column(name="netIncomeAvg", dtype="numeric"),
        Column(name="netIncomeHigh", dtype="numeric"),
        Column(name="epsLow", dtype="numeric"),
        Column(name="epsAvg", dtype="numeric"),
        Column(name="epsHigh", dtype="numeric"),
        Column(name="numAnalystsRevenue", dtype="numeric"),
        Column(name="numAnalystsEps", dtype="numeric"),
    ]
)

INDEX_CONSTITUENTS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="name"),
        Column(name="sector"),
        Column(name="subSector"),
        Column(name="headQuarter"),
        Column(name="dateFirstAdded", dtype="date"),
        Column(name="cik"),
        Column(name="founded"),
    ]
)

MARKET_MOVERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="name"),
        Column(name="price", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="changesPercentage", dtype="numeric"),
        Column(name="exchange"),
    ]
)


# ---------------------------------------------------------------------------
# Dispatch maps for enum-style connectors
# ---------------------------------------------------------------------------

_TAXONOMY_DISPATCH: dict[str, str] = {
    "sectors": "available-sectors",
    "industries": "available-industries",
    "exchanges": "available-exchanges",
    "symbols_with_financials": "stock-list",
}

_NEWS_DISPATCH: dict[str, str] = {
    "news": "news/stock",
    "press_releases": "news/press-releases",
}

_CORPORATE_HISTORY_DISPATCH: dict[str, str] = {
    "earnings": "earnings",
    "dividends": "dividends",
    "splits": "splits",
}

_EVENT_CALENDAR_DISPATCH: dict[str, str] = {
    "earnings": "earnings-calendar",
    "dividends": "dividends-calendar",
    "splits": "splits-calendar",
}

_INDEX_DISPATCH: dict[str, str] = {
    "SP500": "sp500-constituent",
    "NASDAQ": "nasdaq-constituent",
    "DOW_JONES": "dowjones-constituent",
}

_MARKET_MOVERS_DISPATCH: dict[str, str] = {
    "gainers": "biggest-gainers",
    "losers": "biggest-losers",
    "most_actives": "most-actives",
}

_INTRADAY_FREQUENCIES = {"1min", "5min", "15min", "30min", "1hour", "4hour"}

_PRICES_PATH_MAP: dict[str, str] = {
    "daily": "historical-price-eod/full",
    "dividend_adjusted": "historical-price-eod/dividend-adjusted",
}


# ---------------------------------------------------------------------------
# Connectors — Utilities
# ---------------------------------------------------------------------------


@connector(output=SEARCH_OUTPUT, tags=["equity", "utility"])
async def fmp_search(
    params: FmpSearchParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Search for companies by name fragment or partial ticker. Returns matches ranked by relevance. Use to resolve a company name to its ticker symbol."""
    http = _make_http(api_key, base_url)
    p: dict[str, Any] = {"query": params.query, "limit": params.limit}
    if params.exchange:
        p["exchange"] = params.exchange
    return await _fmp_fetch(
        http, path="search-name", params=p,
        op_name="fmp_search", output_config=SEARCH_OUTPUT,
    )


@connector(tags=["equity", "utility"])
async def fmp_taxonomy(
    params: FmpTaxonomyParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Return valid values for a taxonomy type: sectors, industries, exchanges, or symbols_with_financials. Use before building screener filters to ensure field values are valid."""
    http = _make_http(api_key, base_url)
    path = _TAXONOMY_DISPATCH[params.type]
    return await _fmp_fetch(
        http, path=path, params={},
        op_name="fmp_taxonomy",
    )


# ---------------------------------------------------------------------------
# Connectors — Core market data
# ---------------------------------------------------------------------------


@connector(output=STOCK_QUOTE_OUTPUT, tags=["equity"])
async def fmp_quotes(
    params: FmpSymbolsParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch real-time quotes for one or more symbols in a single request. Returns price, change, 52-week range, market cap, volume, moving averages. Pass comma-separated symbols (e.g. AAPL,MSFT,GOOGL)."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="batch-quote", params={"symbols": params.symbols},
        op_name="fmp_quotes", output_config=STOCK_QUOTE_OUTPUT,
    )


@connector(output=HISTORICAL_PRICES_OUTPUT, tags=["equity"])
async def fmp_prices(
    params: FmpHistoricalPricesParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch price history for a symbol. Supports daily, dividend_adjusted, and intraday frequencies (1min, 5min, 15min, 30min, 1hour, 4hour). Daily returns full OHLCV + adjClose; intraday returns last ~5 days."""
    http = _make_http(api_key, base_url)
    freq = params.frequency

    if freq in _INTRADAY_FREQUENCIES:
        path = f"historical-chart/{freq}"
    else:
        path = _PRICES_PATH_MAP.get(freq, "historical-price-eod/full")

    p: dict[str, Any] = {"symbol": params.symbol}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date

    return await _fmp_fetch(
        http, path=path, params=p,
        op_name="fmp_prices", output_config=HISTORICAL_PRICES_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Fundamentals
# ---------------------------------------------------------------------------


@connector(output=COMPANY_PROFILE_OUTPUT, tags=["equity"])
async def fmp_company_profile(
    params: FmpSymbolParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch company profile: name, sector, industry, market cap, CEO, employees, website, ETF/ADR flags."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="profile", params={"symbol": params.symbol},
        op_name="fmp_company_profile", output_config=COMPANY_PROFILE_OUTPUT,
    )


@connector(output=PEERS_OUTPUT, tags=["equity"])
async def fmp_peers(
    params: FmpSymbolParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Return the peer group for a company — stocks in the same sector with comparable market cap on the same exchange."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="stock-peers", params={"symbol": params.symbol},
        op_name="fmp_peers", output_config=PEERS_OUTPUT,
    )


@connector(output=INCOME_STATEMENT_OUTPUT, tags=["equity"])
async def fmp_income_statements(
    params: FmpFinancialStatementParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch income statements: revenue, EBITDA, net income, EPS for multiple periods."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="income-statement", params=params.model_dump(),
        op_name="fmp_income_statements", output_config=INCOME_STATEMENT_OUTPUT,
    )


@connector(output=BALANCE_SHEET_OUTPUT, tags=["equity"])
async def fmp_balance_sheet_statements(
    params: FmpFinancialStatementParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch balance sheet: assets, liabilities, equity, debt, cash for multiple periods."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="balance-sheet-statement", params=params.model_dump(),
        op_name="fmp_balance_sheet_statements", output_config=BALANCE_SHEET_OUTPUT,
    )


@connector(output=CASH_FLOW_OUTPUT, tags=["equity"])
async def fmp_cash_flow_statements(
    params: FmpFinancialStatementParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch cash flow statement: operating, investing, financing activities, free cash flow for multiple periods."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="cash-flow-statement", params=params.model_dump(),
        op_name="fmp_cash_flow_statements", output_config=CASH_FLOW_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Events and catalysts
# ---------------------------------------------------------------------------


@connector(tags=["equity"])
async def fmp_corporate_history(
    params: FmpCorporateHistoryParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch historical corporate events for a symbol: earnings (EPS, revenue actual vs estimated), dividends, or splits."""
    http = _make_http(api_key, base_url)
    path = _CORPORATE_HISTORY_DISPATCH[params.event_type]
    return await _fmp_fetch(
        http, path=path, params={"symbol": params.symbol, "limit": params.limit},
        op_name="fmp_corporate_history",
    )


@connector(tags=["equity"])
async def fmp_event_calendar(
    params: FmpEventCalendarParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Return the market-wide calendar for earnings, dividends, or splits within a date window (max 90 days)."""
    http = _make_http(api_key, base_url)
    path = _EVENT_CALENDAR_DISPATCH[params.event_type]
    p: dict[str, Any] = {}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _fmp_fetch(
        http, path=path, params=p,
        op_name="fmp_event_calendar",
    )


@connector(output=ANALYST_ESTIMATES_OUTPUT, tags=["equity"])
async def fmp_analyst_estimates(
    params: FmpAnalystEstimatesParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch forward analyst consensus estimates: revenue, EBITDA, net income, EPS low/avg/high plus analyst coverage counts."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="analyst-estimates",
        params={"symbol": params.symbol, "period": params.period, "limit": params.limit},
        op_name="fmp_analyst_estimates", output_config=ANALYST_ESTIMATES_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Signals and context
# ---------------------------------------------------------------------------


@connector(output=NEWS_OUTPUT, tags=["equity"])
async def fmp_news(
    params: FmpNewsParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch stock news articles or official press releases for one or more symbols. Pass type='news' for third-party articles or 'press_releases' for company IR communications."""
    http = _make_http(api_key, base_url)
    path = _NEWS_DISPATCH[params.type]
    p: dict[str, Any] = {"symbols": params.symbols, "limit": params.limit, "page": params.page}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _fmp_fetch(
        http, path=path, params=p,
        op_name="fmp_news", output_config=NEWS_OUTPUT,
    )


@connector(output=INSIDER_TRADES_OUTPUT, tags=["equity"])
async def fmp_insider_trades(
    params: FmpInsiderTradesParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch insider trading activity (executive and director trades): transaction type, shares, price, insider name."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="insider-trading/search",
        params={"symbol": params.symbol, "limit": params.limit, "page": params.page},
        op_name="fmp_insider_trades", output_config=INSIDER_TRADES_OUTPUT,
    )


@connector(output=INSTITUTIONAL_POSITIONS_OUTPUT, tags=["equity"])
async def fmp_institutional_positions(
    params: FmpInstitutionalPositionsParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch quarterly institutional (13F) ownership snapshot: investor count, share changes, invested value, ownership %."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="institutional-ownership/symbol-positions-summary",
        params={"symbol": params.symbol, "year": params.year, "quarter": params.quarter},
        op_name="fmp_institutional_positions", output_config=INSTITUTIONAL_POSITIONS_OUTPUT,
    )


@connector(output=EARNINGS_TRANSCRIPT_OUTPUT, tags=["equity"])
async def fmp_earnings_transcript(
    params: FmpEarningsTranscriptParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Fetch the full text transcript of an earnings call for a symbol, year, and quarter."""
    http = _make_http(api_key, base_url)
    return await _fmp_fetch(
        http, path="earning-call-transcript",
        params={"symbol": params.symbol, "year": params.year, "quarter": params.quarter},
        op_name="fmp_earnings_transcript", output_config=EARNINGS_TRANSCRIPT_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Market context
# ---------------------------------------------------------------------------


@connector(output=INDEX_CONSTITUENTS_OUTPUT, tags=["equity"])
async def fmp_index_constituents(
    params: FmpIndexConstituentsParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Return current constituents of SP500, NASDAQ, or DOW_JONES: symbol, name, sector, headquarters."""
    http = _make_http(api_key, base_url)
    path = _INDEX_DISPATCH[params.index]
    return await _fmp_fetch(
        http, path=path, params={},
        op_name="fmp_index_constituents", output_config=INDEX_CONSTITUENTS_OUTPUT,
    )


@connector(output=MARKET_MOVERS_OUTPUT, tags=["equity"])
async def fmp_market_movers(
    params: FmpMarketMoversParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Return today's biggest market movers: gainers (highest % up), losers (biggest % down), or most_actives (highest volume)."""
    http = _make_http(api_key, base_url)
    path = _MARKET_MOVERS_DISPATCH[params.type]
    return await _fmp_fetch(
        http, path=path, params={},
        op_name="fmp_market_movers", output_config=MARKET_MOVERS_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

FMP_DISCOVERY_CONNECTORS = Connectors([fmp_search, fmp_taxonomy])

FMP_FETCH_CONNECTORS = Connectors(
    [
        # Core market data
        fmp_quotes,
        fmp_prices,
        # Fundamentals
        fmp_company_profile,
        fmp_peers,
        fmp_income_statements,
        fmp_balance_sheet_statements,
        fmp_cash_flow_statements,
        # Events and catalysts
        fmp_corporate_history,
        fmp_event_calendar,
        fmp_analyst_estimates,
        # Signals and context
        fmp_news,
        fmp_insider_trades,
        fmp_institutional_positions,
        fmp_earnings_transcript,
        # Market context
        fmp_index_constituents,
        fmp_market_movers,
    ]
)

CONNECTORS = FMP_DISCOVERY_CONNECTORS + FMP_FETCH_CONNECTORS
