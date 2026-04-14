"""Tiingo source: equities, crypto, forex — EOD, real-time, and fundamentals.

API docs: https://www.tiingo.com/documentation/general/overview
Authentication: ``Authorization: Token <key>`` request header.
Base URL: https://api.tiingo.com
Rate limit: free tier ~50 req/hour, 500 req/day. No rate-limit headers.

Provides 13 connectors:
  - Discovery: ticker search
  - Equities: EOD prices, IEX real-time quotes, company metadata
  - Crypto: historical prices, real-time top-of-book
  - Forex: historical prices, real-time top-of-book
  - Fundamentals: company meta, daily metrics, financial statements, definitions
  - News: company/market news (Power+ plan)
  - Enumerator: supported tickers list for catalog indexing

Free-tier restrictions: news (403), fundamentals daily/statements (404).
"""

from __future__ import annotations

from typing import Annotated, Any

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


ENV_VARS: dict[str, str] = {"api_key": "TIINGO_API_KEY"}

_BASE_URL = "https://api.tiingo.com"
_TIMEOUT = 15.0


# ---------------------------------------------------------------------------
# Transport helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        _BASE_URL,
        headers={"Authorization": f"Token {api_key}"},
        timeout=_TIMEOUT,
    )


async def _tiingo_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared Tiingo GET with typed error mapping. Returns parsed JSON body."""
    try:
        response = await http.request("GET", path, params=params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        match status:
            case 401:
                raise UnauthorizedError(
                    provider="tiingo",
                    message="Invalid or missing Tiingo API key",
                ) from e
            case 403:
                raise PaymentRequiredError(
                    provider="tiingo",
                    message=f"Tiingo endpoint '{op_name}' requires a higher-tier plan",
                ) from e
            case 429:
                raise RateLimitError(
                    provider="tiingo",
                    retry_after=60.0,
                    message=f"Tiingo rate limit hit on '{op_name}'",
                ) from e
            case _:
                raise ProviderError(
                    provider="tiingo",
                    status_code=status,
                    message=f"Tiingo API error {status} on '{op_name}'",
                ) from e

    return response.json()


# ---------------------------------------------------------------------------
# Discovery — search
# ---------------------------------------------------------------------------

_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="asset_type", role=ColumnRole.METADATA),
        Column(name="is_active", role=ColumnRole.METADATA, dtype="bool"),
        Column(name="country_code", role=ColumnRole.METADATA),
        Column(name="perma_ticker", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="open_figi", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)


class TiingoSearchParams(BaseModel):
    """Search Tiingo for stocks, ETFs, mutual funds, and crypto by name or ticker."""

    query: str = Field(..., min_length=1, description="Search term, e.g. 'apple' or 'AAPL'")
    limit: int = Field(default=25, ge=1, le=100, description="Max results to return (1-100)")


@connector(output=_SEARCH_OUTPUT, tags=["equities", "tool"])
async def tiingo_search(params: TiingoSearchParams, *, api_key: str) -> Result:
    """Search Tiingo for stocks, ETFs, mutual funds, and crypto by name or ticker.
    Returns ticker (the stable API identifier), name, assetType (Stock, ETF,
    Mutual Fund), isActive, and countryCode. Use ticker with tiingo_eod,
    tiingo_iex, tiingo_meta, or tiingo_fundamentals_meta for further data.

    Example: query='apple' → ticker='AAPL'; query='bitcoin' → ticker='BTCUSD'.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path="/tiingo/utilities/search",
        params={"query": params.query, "limit": params.limit},
        op_name="tiingo_search",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No tickers found for query: {params.query}",
        )

    rows = [
        {
            "ticker": r.get("ticker", ""),
            "name": r.get("name", ""),
            "asset_type": r.get("assetType", ""),
            "is_active": r.get("isActive", False),
            "country_code": r.get("countryCode", ""),
            "perma_ticker": r.get("permaTicker", ""),
            "open_figi": r.get("openFIGIComposite", ""),
        }
        for r in data
        if r.get("ticker")
    ]
    df = pd.DataFrame(rows)
    return _SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_search", params={"query": params.query}),
        params={"query": params.query},
    )


# ---------------------------------------------------------------------------
# Equities — EOD prices
# ---------------------------------------------------------------------------

_EOD_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, param_key="ticker", namespace="tiingo_ticker"),
        Column(name="date", role=ColumnRole.DATA, dtype="datetime"),
        Column(name="close", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="open", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="adj_close", dtype="numeric"),
        Column(name="adj_high", dtype="numeric"),
        Column(name="adj_low", dtype="numeric"),
        Column(name="adj_open", dtype="numeric"),
        Column(name="adj_volume", dtype="numeric"),
        Column(name="div_cash", dtype="numeric"),
        Column(name="split_factor", dtype="numeric"),
    ]
)


class TiingoEodParams(BaseModel):
    """Historical end-of-day prices for a stock."""

    ticker: Annotated[str, Namespace("tiingo_ticker")] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use tiingo_search to resolve tickers."
    )
    start_date: str | None = Field(
        default=None, description="Start date ISO 8601, e.g. '2024-01-01'."
    )
    end_date: str | None = Field(
        default=None, description="End date ISO 8601, e.g. '2024-12-31'."
    )


@connector(output=_EOD_OUTPUT, tags=["equities"])
async def tiingo_eod(params: TiingoEodParams, *, api_key: str) -> Result:
    """Fetch historical end-of-day OHLCV prices for a stock with split/dividend
    adjusted values. Returns date, open/high/low/close, volume, and adjusted
    counterparts (adjOpen, adjHigh, adjLow, adjClose, adjVolume), plus divCash
    and splitFactor. Free tier provides full history back to listing date.
    Use tiingo_search to resolve ticker symbols first.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {}
    if params.start_date:
        req["startDate"] = params.start_date
    if params.end_date:
        req["endDate"] = params.end_date

    data = await _tiingo_fetch(
        http,
        path=f"/tiingo/daily/{params.ticker}/prices",
        params=req,
        op_name="tiingo_eod",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No EOD data returned for ticker: {params.ticker}",
        )

    rows = [
        {
            "date": r.get("date"),
            "close": r.get("close"),
            "high": r.get("high"),
            "low": r.get("low"),
            "open": r.get("open"),
            "volume": r.get("volume"),
            "adj_close": r.get("adjClose"),
            "adj_high": r.get("adjHigh"),
            "adj_low": r.get("adjLow"),
            "adj_open": r.get("adjOpen"),
            "adj_volume": r.get("adjVolume"),
            "div_cash": r.get("divCash"),
            "split_factor": r.get("splitFactor"),
        }
        for r in data
    ]
    df = pd.DataFrame(rows)
    df["ticker"] = params.ticker
    return _EOD_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_eod", params={"ticker": params.ticker}),
        params={"ticker": params.ticker},
    )


# ---------------------------------------------------------------------------
# Equities — IEX real-time quotes
# ---------------------------------------------------------------------------

_IEX_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="timestamp", role=ColumnRole.METADATA, dtype="datetime"),
        Column(name="tngo_last", dtype="numeric"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="prev_close", dtype="numeric"),
        Column(name="mid", dtype="numeric"),
        Column(name="bid_price", dtype="numeric"),
        Column(name="ask_price", dtype="numeric"),
        Column(name="bid_size", dtype="numeric"),
        Column(name="ask_size", dtype="numeric"),
    ]
)


class TiingoIexParams(BaseModel):
    """Real-time IEX quote for one or more stock tickers."""

    tickers: str = Field(
        ...,
        description="Comma-separated tickers, e.g. 'AAPL' or 'AAPL,MSFT,TSLA'. Use tiingo_search to resolve.",
    )


@connector(output=_IEX_OUTPUT, tags=["equities"])
async def tiingo_iex(params: TiingoIexParams, *, api_key: str) -> Result:
    """Fetch real-time IEX quotes for one or more stocks. Returns Tiingo's
    composite last price (tngoLast), OHLV for the day, previous close,
    mid/bid/ask prices and sizes. Timestamp is ISO 8601 UTC.
    Free tier supported. Use tiingo_search to resolve tickers first.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path="/iex/",
        params={"tickers": params.tickers},
        op_name="tiingo_iex",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No IEX data returned for tickers: {params.tickers}",
        )

    rows = [
        {
            "ticker": r.get("ticker", ""),
            "timestamp": r.get("timestamp"),
            "tngo_last": r.get("tngoLast"),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "volume": r.get("volume"),
            "prev_close": r.get("prevClose"),
            "mid": r.get("mid"),
            "bid_price": r.get("bidPrice"),
            "ask_price": r.get("askPrice"),
            "bid_size": r.get("bidSize"),
            "ask_size": r.get("askSize"),
        }
        for r in data
        if r.get("ticker")
    ]
    df = pd.DataFrame(rows)
    return _IEX_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_iex", params={"tickers": params.tickers}),
        params={"tickers": params.tickers},
    )


# ---------------------------------------------------------------------------
# Equities — IEX historical intraday
# ---------------------------------------------------------------------------

_IEX_HIST_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, param_key="ticker", namespace="tiingo_ticker"),
        Column(name="date", role=ColumnRole.DATA, dtype="datetime"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
    ]
)


class TiingoIexHistParams(BaseModel):
    """Historical IEX intraday prices for a stock."""

    ticker: Annotated[str, Namespace("tiingo_ticker")] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use tiingo_search to resolve tickers."
    )
    start_date: str | None = Field(
        default=None, description="Start date ISO 8601, e.g. '2024-01-01'."
    )
    end_date: str | None = Field(
        default=None, description="End date ISO 8601, e.g. '2024-01-05'."
    )
    resample_freq: str = Field(
        default="1hour",
        description="Resample frequency: '1min', '5min', '15min', '30min', '1hour', '2hour', '4hour'.",
    )


@connector(output=_IEX_HIST_OUTPUT, tags=["equities"])
async def tiingo_iex_historical(params: TiingoIexHistParams, *, api_key: str) -> Result:
    """Fetch historical IEX intraday OHLC prices for a stock at a given
    frequency. Returns the most recent 2000 data points at the specified
    frequency — cannot request arbitrarily old data. Free tier supported.
    Use tiingo_search to resolve ticker symbols first.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"resampleFreq": params.resample_freq}
    if params.start_date:
        req["startDate"] = params.start_date
    if params.end_date:
        req["endDate"] = params.end_date

    data = await _tiingo_fetch(
        http,
        path=f"/iex/{params.ticker}/prices",
        params=req,
        op_name="tiingo_iex_historical",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No IEX intraday data for ticker: {params.ticker}",
        )

    rows = [
        {
            "date": r.get("date"),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
        }
        for r in data
    ]
    df = pd.DataFrame(rows)
    df["ticker"] = params.ticker
    return _IEX_HIST_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_iex_historical", params={"ticker": params.ticker}),
        params={"ticker": params.ticker},
    )


# ---------------------------------------------------------------------------
# Equities — company metadata
# ---------------------------------------------------------------------------


class TiingoMetaParams(BaseModel):
    """Company metadata for a stock ticker."""

    ticker: Annotated[str, Namespace("tiingo_ticker")] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use tiingo_search to resolve tickers."
    )


@connector(tags=["equities"], result_type="dict")
async def tiingo_meta(params: TiingoMetaParams, *, api_key: str) -> Result:
    """Fetch company metadata for a stock: name, description, exchange,
    listing start/end dates. Use tiingo_search to resolve ticker symbols.
    For sector/industry data use tiingo_fundamentals_meta.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path=f"/tiingo/daily/{params.ticker}",
        params={},
        op_name="tiingo_meta",
    )

    if not isinstance(data, dict) or not data.get("ticker"):
        raise EmptyDataError(
            provider="tiingo",
            message=f"No metadata returned for ticker: {params.ticker}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="tiingo_meta", params={"ticker": params.ticker}),
    )


# ---------------------------------------------------------------------------
# Fundamentals — company metadata (sector, industry, SIC)
# ---------------------------------------------------------------------------


class TiingoFundamentalsMetaParams(BaseModel):
    """Fundamentals metadata: sector, industry, SIC codes, and more."""

    tickers: str = Field(
        ...,
        description="Comma-separated tickers, e.g. 'AAPL' or 'AAPL,MSFT'. Use tiingo_search to resolve.",
    )


@connector(tags=["equities"], result_type="dict")
async def tiingo_fundamentals_meta(params: TiingoFundamentalsMetaParams, *, api_key: str) -> Result:
    """Fetch fundamentals metadata for one or more stocks: sector, industry,
    SIC code/sector/industry, reporting currency, location, company website,
    SEC filing link, ADR flag, and data freshness timestamps.
    Use tiingo_search to resolve tickers first.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path="/tiingo/fundamentals/meta",
        params={"tickers": params.tickers},
        op_name="tiingo_fundamentals_meta",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No fundamentals metadata for tickers: {params.tickers}",
        )

    # Single ticker → return dict; multiple → return list
    result_data = data[0] if len(data) == 1 else data
    return Result(
        data=result_data,
        provenance=Provenance(source="tiingo_fundamentals_meta", params={"tickers": params.tickers}),
    )


# ---------------------------------------------------------------------------
# Fundamentals — definitions (reference data)
# ---------------------------------------------------------------------------

_DEFINITIONS_OUTPUT = OutputConfig(
    columns=[
        Column(name="data_code", role=ColumnRole.KEY, namespace="tiingo_data_code"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="statement_type", role=ColumnRole.METADATA),
        Column(name="units", role=ColumnRole.METADATA),
    ]
)


class TiingoDefinitionsParams(BaseModel):
    """List all fundamental metric definitions (codes, names, units)."""

    pass


@connector(output=_DEFINITIONS_OUTPUT, tags=["equities"])
async def tiingo_fundamentals_definitions(params: TiingoDefinitionsParams, *, api_key: str) -> Result:
    """List all available fundamental metric definitions: dataCode (metric ID),
    name, description, statementType (overview/incomeStatement/balanceSheet/
    cashFlow), and units. Use dataCodes to interpret tiingo_fundamentals_daily
    and tiingo_fundamentals_statements responses.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path="/tiingo/fundamentals/definitions",
        params={},
        op_name="tiingo_fundamentals_definitions",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message="No fundamental definitions returned",
        )

    rows = [
        {
            "data_code": r.get("dataCode", ""),
            "name": r.get("name", ""),
            "description": r.get("description", ""),
            "statement_type": r.get("statementType", ""),
            "units": r.get("units", ""),
        }
        for r in data
        if r.get("dataCode")
    ]
    df = pd.DataFrame(rows)
    return _DEFINITIONS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_fundamentals_definitions", params={}),
        params={},
    )


# ---------------------------------------------------------------------------
# News (Power+ plan required)
# ---------------------------------------------------------------------------

_NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="published_date", role=ColumnRole.METADATA, dtype="datetime"),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="tickers", role=ColumnRole.METADATA),
        Column(name="tags", role=ColumnRole.METADATA),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="url", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)


class TiingoNewsParams(BaseModel):
    """News articles from Tiingo (requires Power+ plan)."""

    model_config = ConfigDict(populate_by_name=True)

    tickers: str | None = Field(
        default=None,
        description="Comma-separated tickers to filter, e.g. 'AAPL,MSFT'. Omit for all.",
    )
    source: str | None = Field(
        default=None, description="News source filter, e.g. 'bloomberg.com'."
    )
    start_date: str | None = Field(
        default=None, description="Start date ISO 8601, e.g. '2024-01-01'."
    )
    end_date: str | None = Field(
        default=None, description="End date ISO 8601, e.g. '2024-12-31'."
    )
    limit: int = Field(default=50, ge=1, le=100, description="Max articles (1-100)")


@connector(output=_NEWS_OUTPUT, tags=["equities", "news"])
async def tiingo_news(params: TiingoNewsParams, *, api_key: str) -> Result:
    """[Power+] Fetch news articles from Tiingo. Filter by tickers, source, and
    date range. Returns title, publishedDate, source, related tickers, tags,
    description, and URL. Requires Power+ plan (free tier returns 403).
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"limit": params.limit}
    if params.tickers:
        req["tickers"] = params.tickers
    if params.source:
        req["source"] = params.source
    if params.start_date:
        req["startDate"] = params.start_date
    if params.end_date:
        req["endDate"] = params.end_date

    data = await _tiingo_fetch(
        http, path="/tiingo/news", params=req, op_name="tiingo_news"
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message="No news articles returned",
        )

    rows = [
        {
            "id": article.get("id"),
            "title": article.get("title", ""),
            "published_date": article.get("publishedDate"),
            "source": article.get("source", ""),
            "tickers": ",".join(article.get("tickers", [])) if article.get("tickers") else "",
            "tags": ",".join(article.get("tags", [])) if article.get("tags") else "",
            "description": article.get("description", ""),
            "url": article.get("url", ""),
        }
        for article in data
    ]
    df = pd.DataFrame(rows)
    return _NEWS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_news", params={"tickers": params.tickers or "all"}),
        params={"tickers": params.tickers or "all"},
    )


# ---------------------------------------------------------------------------
# Crypto — historical prices
# ---------------------------------------------------------------------------

_CRYPTO_PRICES_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, param_key="tickers", namespace="tiingo_crypto"),
        Column(name="date", role=ColumnRole.DATA, dtype="datetime"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="volume_notional", dtype="numeric"),
        Column(name="trades_done", dtype="numeric"),
    ]
)


class TiingoCryptoPricesParams(BaseModel):
    """Historical crypto prices."""

    tickers: str = Field(
        ..., description="Crypto pair, e.g. 'btcusd' or 'ethusd'. Use lowercase."
    )
    start_date: str | None = Field(
        default=None, description="Start date ISO 8601, e.g. '2024-01-01'."
    )
    end_date: str | None = Field(
        default=None, description="End date ISO 8601, e.g. '2024-12-31'."
    )
    resample_freq: str = Field(
        default="1day",
        description="Resample frequency: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day'.",
    )


@connector(output=_CRYPTO_PRICES_OUTPUT, tags=["crypto"])
async def tiingo_crypto_prices(params: TiingoCryptoPricesParams, *, api_key: str) -> Result:
    """Fetch historical crypto OHLCV prices. Returns date, open, high, low,
    close, volume (in base currency), volumeNotional (in quote currency), and
    tradesDone. Supports multiple resample frequencies from 1min to 1day.
    Free tier supported. Pairs are lowercase, e.g. 'btcusd', 'ethusd'.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"tickers": params.tickers, "resampleFreq": params.resample_freq}
    if params.start_date:
        req["startDate"] = params.start_date
    if params.end_date:
        req["endDate"] = params.end_date

    data = await _tiingo_fetch(
        http,
        path="/tiingo/crypto/prices",
        params=req,
        op_name="tiingo_crypto_prices",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No crypto data returned for: {params.tickers}",
        )

    # Response is [{ticker, baseCurrency, quoteCurrency, priceData: [...]}]
    all_rows: list[dict] = []
    for entry in data:
        ticker = entry.get("ticker", params.tickers)
        for p in entry.get("priceData", []):
            all_rows.append(
                {
                    "ticker": ticker,
                    "date": p.get("date"),
                    "open": p.get("open"),
                    "high": p.get("high"),
                    "low": p.get("low"),
                    "close": p.get("close"),
                    "volume": p.get("volume"),
                    "volume_notional": p.get("volumeNotional"),
                    "trades_done": p.get("tradesDone"),
                }
            )

    if not all_rows:
        raise EmptyDataError(
            provider="tiingo",
            message=f"Empty price data for crypto pair: {params.tickers}",
        )

    df = pd.DataFrame(all_rows)
    return _CRYPTO_PRICES_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_crypto_prices", params={"tickers": params.tickers}),
        params={"tickers": params.tickers},
    )


# ---------------------------------------------------------------------------
# Crypto — real-time top-of-book
# ---------------------------------------------------------------------------

_CRYPTO_TOP_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_crypto"),
        Column(name="last_price", dtype="numeric"),
        Column(name="quote_timestamp", role=ColumnRole.METADATA, dtype="datetime"),
        Column(name="bid_price", dtype="numeric"),
        Column(name="ask_price", dtype="numeric"),
        Column(name="bid_size", dtype="numeric"),
        Column(name="ask_size", dtype="numeric"),
        Column(name="last_size_notional", dtype="numeric"),
        Column(name="last_exchange", role=ColumnRole.METADATA),
    ]
)


class TiingoCryptoTopParams(BaseModel):
    """Real-time crypto top-of-book quotes."""

    tickers: str = Field(
        ..., description="Comma-separated crypto pairs, e.g. 'btcusd' or 'btcusd,ethusd'. Lowercase."
    )


@connector(output=_CRYPTO_TOP_OUTPUT, tags=["crypto"])
async def tiingo_crypto_top(params: TiingoCryptoTopParams, *, api_key: str) -> Result:
    """Fetch real-time top-of-book quotes for crypto pairs: last price, bid/ask
    prices and sizes, last trade size (notional), and exchange. Free tier
    supported. Use lowercase pairs, e.g. 'btcusd', 'ethusd'.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path="/tiingo/crypto/top",
        params={"tickers": params.tickers},
        op_name="tiingo_crypto_top",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No crypto top data for: {params.tickers}",
        )

    # Response: [{ticker, baseCurrency, quoteCurrency, topOfBookData: [{...}]}]
    rows: list[dict] = []
    for entry in data:
        ticker = entry.get("ticker", "")
        for book in entry.get("topOfBookData", []):
            rows.append(
                {
                    "ticker": ticker,
                    "last_price": book.get("lastPrice"),
                    "quote_timestamp": book.get("quoteTimestamp"),
                    "bid_price": book.get("bidPrice"),
                    "ask_price": book.get("askPrice"),
                    "bid_size": book.get("bidSize"),
                    "ask_size": book.get("askSize"),
                    "last_size_notional": book.get("lastSizeNotional"),
                    "last_exchange": book.get("lastExchange", ""),
                }
            )

    if not rows:
        raise EmptyDataError(
            provider="tiingo",
            message=f"Empty top-of-book data for: {params.tickers}",
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_TOP_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_crypto_top", params={"tickers": params.tickers}),
        params={"tickers": params.tickers},
    )


# ---------------------------------------------------------------------------
# Forex — historical prices
# ---------------------------------------------------------------------------

_FX_PRICES_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, param_key="tickers", namespace="tiingo_fx"),
        Column(name="date", role=ColumnRole.DATA, dtype="datetime"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
    ]
)


class TiingoFxPricesParams(BaseModel):
    """Historical forex prices."""

    tickers: str = Field(
        ..., description="Forex pair, e.g. 'eurusd' or 'gbpjpy'. Use lowercase."
    )
    start_date: str | None = Field(
        default=None, description="Start date ISO 8601, e.g. '2024-01-01'."
    )
    end_date: str | None = Field(
        default=None, description="End date ISO 8601, e.g. '2024-12-31'."
    )
    resample_freq: str = Field(
        default="1day",
        description="Resample frequency: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day'.",
    )


@connector(output=_FX_PRICES_OUTPUT, tags=["forex"])
async def tiingo_fx_prices(params: TiingoFxPricesParams, *, api_key: str) -> Result:
    """Fetch historical forex OHLC prices. Returns date, open, high, low, close
    for a currency pair. Supports multiple resample frequencies from 1min to 1day.
    Free tier supported. Pairs are lowercase, e.g. 'eurusd', 'gbpjpy'.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"tickers": params.tickers, "resampleFreq": params.resample_freq}
    if params.start_date:
        req["startDate"] = params.start_date
    if params.end_date:
        req["endDate"] = params.end_date

    data = await _tiingo_fetch(
        http,
        path="/tiingo/fx/prices",
        params=req,
        op_name="tiingo_fx_prices",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No forex data returned for: {params.tickers}",
        )

    rows = [
        {
            "ticker": r.get("ticker", params.tickers),
            "date": r.get("date"),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
        }
        for r in data
    ]
    df = pd.DataFrame(rows)
    return _FX_PRICES_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_fx_prices", params={"tickers": params.tickers}),
        params={"tickers": params.tickers},
    )


# ---------------------------------------------------------------------------
# Forex — real-time top-of-book
# ---------------------------------------------------------------------------

_FX_TOP_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_fx"),
        Column(name="quote_timestamp", role=ColumnRole.METADATA, dtype="datetime"),
        Column(name="mid_price", dtype="numeric"),
        Column(name="bid_price", dtype="numeric"),
        Column(name="ask_price", dtype="numeric"),
        Column(name="bid_size", dtype="numeric"),
        Column(name="ask_size", dtype="numeric"),
    ]
)


class TiingoFxTopParams(BaseModel):
    """Real-time forex top-of-book quotes."""

    tickers: str = Field(
        ..., description="Comma-separated forex pairs, e.g. 'eurusd' or 'eurusd,gbpjpy'. Lowercase."
    )


@connector(output=_FX_TOP_OUTPUT, tags=["forex"])
async def tiingo_fx_top(params: TiingoFxTopParams, *, api_key: str) -> Result:
    """Fetch real-time top-of-book forex quotes: mid, bid/ask prices and sizes.
    Free tier supported. Pairs are lowercase, e.g. 'eurusd', 'gbpjpy'.
    """
    http = _make_http(api_key)
    data = await _tiingo_fetch(
        http,
        path="/tiingo/fx/top",
        params={"tickers": params.tickers},
        op_name="tiingo_fx_top",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="tiingo",
            message=f"No forex top data for: {params.tickers}",
        )

    rows = [
        {
            "ticker": r.get("ticker", ""),
            "quote_timestamp": r.get("quoteTimestamp"),
            "mid_price": r.get("midPrice"),
            "bid_price": r.get("bidPrice"),
            "ask_price": r.get("askPrice"),
            "bid_size": r.get("bidSize"),
            "ask_size": r.get("askSize"),
        }
        for r in data
        if r.get("ticker")
    ]
    df = pd.DataFrame(rows)
    return _FX_TOP_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="tiingo_fx_top", params={"tickers": params.tickers}),
        params={"tickers": params.tickers},
    )


# ---------------------------------------------------------------------------
# Enumerator — supported tickers for catalog indexing
# ---------------------------------------------------------------------------

_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="asset_type", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="is_active", role=ColumnRole.METADATA, dtype="bool"),
        Column(name="country_code", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
    ]
)


class TiingoEnumerateParams(BaseModel):
    """Parameters for enumerating Tiingo supported tickers."""

    pass


@enumerator(output=_ENUMERATE_OUTPUT, tags=["equities"])
async def enumerate_tiingo(params: TiingoEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate all supported tickers from Tiingo for catalog indexing.

    Downloads the supported_tickers.zip CSV from apimedia.tiingo.com — returns
    ~127 000 rows with ticker, exchange, asset type, start/end dates. The file
    is a static CDN snapshot; refresh at most once per day.
    """
    import csv
    import io
    import zipfile

    _MEDIA_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
    _COLS = ["ticker", "name", "asset_type", "exchange", "is_active", "country_code", "start_date", "end_date"]

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(_MEDIA_URL)
        resp.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    csv_name = zf.namelist()[0]
    text = zf.read(csv_name).decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    rows = [
        {
            "ticker": row.get("ticker", ""),
            "name": "",  # CSV has no name column; catalog enrichment uses tiingo_meta
            "asset_type": row.get("assetType", ""),
            "exchange": row.get("exchange", ""),
            "is_active": not bool(row.get("endDate", "")),  # endDate absent → still active
            "country_code": "",
            "start_date": row.get("startDate", ""),
            "end_date": row.get("endDate", ""),
        }
        for row in reader
        if row.get("ticker")
    ]

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=_COLS)


# ---------------------------------------------------------------------------
# Connector collections
# ---------------------------------------------------------------------------

TIINGO_FETCH_CONNECTORS = Connectors(
    [
        tiingo_eod,
        tiingo_iex,
        tiingo_iex_historical,
        tiingo_meta,
        tiingo_fundamentals_meta,
        tiingo_fundamentals_definitions,
        tiingo_news,
        tiingo_crypto_prices,
        tiingo_crypto_top,
        tiingo_fx_prices,
        tiingo_fx_top,
    ]
)

TIINGO_DISCOVERY_CONNECTORS = Connectors(
    [
        tiingo_search,
    ]
)

FETCH_CONNECTORS = TIINGO_FETCH_CONNECTORS
CONNECTORS = TIINGO_DISCOVERY_CONNECTORS + TIINGO_FETCH_CONNECTORS + Connectors([enumerate_tiingo])
