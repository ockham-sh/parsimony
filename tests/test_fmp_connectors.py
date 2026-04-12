"""Tests for all FMP connectors.

Two test layers:
1. **Unit tests** (mocked HTTP) — verify endpoint paths, parameter passing,
   response parsing, and error handling. Run always.
2. **Live integration tests** (real FMP API) — verify every connector returns
   data against the actual FMP stable API. Gated behind ``@pytest.mark.live``
   and require ``FMP_API_KEY`` env var. Run with: ``pytest -m live``

The live tests are the authoritative source of truth. If unit tests pass
but live tests fail, the unit tests are wrong.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from parsimony.result import Result

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

API_KEY = "test-key-123"

# Check if live tests should run
_LIVE_FMP_KEY = os.environ.get("FMP_API_KEY", "")
_HAS_LIVE_KEY = bool(_LIVE_FMP_KEY)

live = pytest.mark.skipif(not _HAS_LIVE_KEY, reason="FMP_API_KEY not set")


def _make_response(json_data: Any, status_code: int = 200) -> httpx.Response:
    """Build a fake httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"{status_code}",
            request=MagicMock(),
            response=resp,
        )
    return resp


def _patch_http(response: httpx.Response):
    return patch(
        "parsimony.transport.http.HttpClient.request",
        new_callable=AsyncMock,
        return_value=response,
    )


async def _call(connector_obj, **kwargs) -> Result:
    bound = connector_obj.bind_deps(api_key=API_KEY)
    return await bound(**kwargs)


async def _call_live(connector_obj, **kwargs) -> Result:
    bound = connector_obj.bind_deps(api_key=_LIVE_FMP_KEY)
    return await bound(**kwargs)


# ===========================================================================
# UNIT TESTS — mocked HTTP, verify paths & parsing
# ===========================================================================


class TestFmpSearch:
    @pytest.mark.asyncio
    async def test_calls_correct_endpoint(self) -> None:
        from parsimony.connectors.fmp import fmp_search

        data = [{"symbol": "AAPL", "name": "Apple Inc", "currency": "USD",
                 "exchangeFullName": "NASDAQ", "exchange": "NASDAQ"}]
        with _patch_http(_make_response(data)) as mock_req:
            result = await _call(fmp_search, query="Apple", limit=10)
        assert "/search-name" in mock_req.call_args.args[1]
        assert len(result.df) == 1


class TestFmpTaxonomy:
    @pytest.mark.asyncio
    async def test_sectors_path(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy

        with _patch_http(_make_response([{"sector": "Tech"}])) as m:
            await _call(fmp_taxonomy, type="sectors")
        assert "/available-sectors" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_industries_path(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy

        with _patch_http(_make_response([{"industry": "SW"}])) as m:
            await _call(fmp_taxonomy, type="industries")
        assert "/available-industries" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_exchanges_path(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy

        with _patch_http(_make_response([{"exchange": "NYSE"}])) as m:
            await _call(fmp_taxonomy, type="exchanges")
        assert "/available-exchanges" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_symbols_with_financials_uses_stock_list(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy

        with _patch_http(_make_response([{"symbol": "AAPL"}])) as m:
            await _call(fmp_taxonomy, type="symbols_with_financials")
        assert "/stock-list" in m.call_args.args[1]


class TestFmpQuotes:
    @pytest.mark.asyncio
    async def test_uses_batch_quote(self) -> None:
        from parsimony.connectors.fmp import fmp_quotes

        data = [{"symbol": "AAPL", "name": "Apple", "price": 180.0,
                 "changesPercentage": 0.8, "change": 1.5, "dayLow": 178.0,
                 "dayHigh": 181.0, "yearLow": 120.0, "yearHigh": 200.0,
                 "marketCap": 2.8e12, "volume": 5e7, "avgVolume": 5.5e7,
                 "pe": 28.5, "eps": 6.3, "priceAvg50": 175.0, "priceAvg200": 170.0,
                 "exchange": "NASDAQ", "open": 179.0, "previousClose": 178.5}]
        with _patch_http(_make_response(data)) as m:
            result = await _call(fmp_quotes, symbols="AAPL,MSFT")
        assert "/batch-quote" in m.call_args.args[1]


class TestFmpPrices:
    @pytest.mark.asyncio
    async def test_daily_path(self) -> None:
        from parsimony.connectors.fmp import fmp_prices

        data = {"historical": [{"date": "2024-01-02", "open": 180.0, "high": 182.0,
                 "low": 179.0, "close": 181.0, "volume": 5e7, "change": 1.0,
                 "changePercent": 0.55, "vwap": 180.5}]}
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_prices, symbol="AAPL", frequency="daily")
        assert "/historical-price-eod/full" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_intraday_path(self) -> None:
        from parsimony.connectors.fmp import fmp_prices

        data = [{"date": "2024-01-02 10:00:00", "open": 180.0, "high": 180.5,
                 "low": 179.5, "close": 180.2, "volume": 1e5}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_prices, symbol="AAPL", frequency="5min")
        assert "/historical-chart/5min" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_dividend_adjusted_path(self) -> None:
        from parsimony.connectors.fmp import fmp_prices

        data = {"historical": [{"date": "2024-01-02", "open": 180.0, "high": 182.0,
                 "low": 179.0, "close": 181.0, "volume": 5e7, "change": 1.0,
                 "changePercent": 0.55, "vwap": 180.5}]}
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_prices, symbol="AAPL", frequency="dividend_adjusted")
        assert "/historical-price-eod/dividend-adjusted" in m.call_args.args[1]


class TestFmpCompanyProfile:
    @pytest.mark.asyncio
    async def test_path_and_parsing(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile

        data = [{"symbol": "AAPL", "companyName": "Apple Inc", "price": 180.0,
                 "marketCap": 2.8e12, "beta": 1.2, "exchange": "NASDAQ",
                 "exchangeFullName": "NASDAQ", "currency": "USD", "sector": "Technology",
                 "industry": "Consumer Electronics", "country": "US",
                 "fullTimeEmployees": 161000, "ceo": "Tim Cook",
                 "description": "Apple designs...", "website": "https://apple.com",
                 "ipoDate": "1980-12-12", "isEtf": False, "isActivelyTrading": True,
                 "isAdr": False, "isFund": False}]
        with _patch_http(_make_response(data)) as m:
            result = await _call(fmp_company_profile, symbol="AAPL")
        assert "/profile" in m.call_args.args[1]
        assert result.df["companyName"].iloc[0] == "Apple Inc"


class TestFmpPeers:
    @pytest.mark.asyncio
    async def test_path(self) -> None:
        from parsimony.connectors.fmp import fmp_peers

        data = [{"symbol": "MSFT", "companyName": "Microsoft", "price": 380.0, "mktCap": 2.9e12}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_peers, symbol="AAPL")
        assert "/stock-peers" in m.call_args.args[1]


class TestFmpFinancialStatements:
    """All 3 financial statement connectors must use query-param style (not path-segment)."""

    @pytest.mark.asyncio
    async def test_income_statement_no_symbol_in_path(self) -> None:
        from parsimony.connectors.fmp import fmp_income_statements

        data = [{"date": "2024-09-30", "symbol": "AAPL", "reportedCurrency": "USD",
                 "revenue": 3.91e11, "costOfRevenue": 2.14e11, "grossProfit": 1.77e11,
                 "operatingExpenses": 5.5e10, "operatingIncome": 1.22e11,
                 "ebitda": 1.3e11, "netIncome": 9.7e10, "eps": 6.42, "epsDiluted": 6.32}]
        with _patch_http(_make_response(data)) as m:
            result = await _call(fmp_income_statements, symbol="AAPL", period="annual", limit=1)
        path = m.call_args.args[1]
        assert "/income-statement" in path
        assert "/AAPL" not in path  # symbol must NOT be in path
        # symbol must be in query params
        params = m.call_args.kwargs.get("params", {})
        assert params.get("symbol") == "AAPL"

    @pytest.mark.asyncio
    async def test_balance_sheet_no_symbol_in_path(self) -> None:
        from parsimony.connectors.fmp import fmp_balance_sheet_statements

        data = [{"date": "2024-09-30", "symbol": "AAPL", "totalAssets": 3.52e11,
                 "totalLiabilities": 2.9e11, "totalStockholdersEquity": 6.2e10,
                 "totalDebt": 1.11e11, "netDebt": 7.6e10, "cashAndCashEquivalents": 3.5e10}]
        with _patch_http(_make_response(data)) as m:
            result = await _call(fmp_balance_sheet_statements, symbol="AAPL")
        path = m.call_args.args[1]
        assert "/balance-sheet-statement" in path
        assert "/AAPL" not in path
        params = m.call_args.kwargs.get("params", {})
        assert params.get("symbol") == "AAPL"

    @pytest.mark.asyncio
    async def test_cash_flow_no_symbol_in_path(self) -> None:
        from parsimony.connectors.fmp import fmp_cash_flow_statements

        data = [{"date": "2024-09-30", "symbol": "AAPL", "reportedCurrency": "USD",
                 "netIncome": 9.7e10, "operatingCashFlow": 1.18e11,
                 "capitalExpenditure": -1.1e10, "freeCashFlow": 1.07e11,
                 "netCashProvidedByOperatingActivities": 1.18e11,
                 "netCashProvidedByInvestingActivities": -6e9,
                 "netCashProvidedByFinancingActivities": -1.08e11,
                 "netChangeInCash": 4e9}]
        with _patch_http(_make_response(data)) as m:
            result = await _call(fmp_cash_flow_statements, symbol="AAPL")
        path = m.call_args.args[1]
        assert "/cash-flow-statement" in path
        assert "/AAPL" not in path
        params = m.call_args.kwargs.get("params", {})
        assert params.get("symbol") == "AAPL"


class TestFmpCorporateHistory:
    @pytest.mark.asyncio
    async def test_earnings_path(self) -> None:
        from parsimony.connectors.fmp import fmp_corporate_history

        data = [{"date": "2024-10-31", "symbol": "AAPL", "eps": 1.64}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_corporate_history, symbol="AAPL", event_type="earnings")
        assert m.call_args.args[1].endswith("/earnings")

    @pytest.mark.asyncio
    async def test_dividends_path(self) -> None:
        from parsimony.connectors.fmp import fmp_corporate_history

        data = [{"date": "2024-11-01", "symbol": "AAPL", "dividend": 0.25}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_corporate_history, symbol="AAPL", event_type="dividends")
        assert m.call_args.args[1].endswith("/dividends")

    @pytest.mark.asyncio
    async def test_splits_uses_splits_not_stock_split(self) -> None:
        from parsimony.connectors.fmp import fmp_corporate_history

        data = [{"date": "2020-08-31", "symbol": "AAPL", "numerator": 4, "denominator": 1}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_corporate_history, symbol="AAPL", event_type="splits")
        assert m.call_args.args[1].endswith("/splits")
        assert "stock-split" not in m.call_args.args[1]


class TestFmpEventCalendar:
    @pytest.mark.asyncio
    async def test_earnings_calendar(self) -> None:
        from parsimony.connectors.fmp import fmp_event_calendar

        data = [{"date": "2024-10-31", "symbol": "AAPL", "eps": 1.64}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_event_calendar, event_type="earnings")
        assert "/earnings-calendar" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_dividends_calendar(self) -> None:
        from parsimony.connectors.fmp import fmp_event_calendar

        data = [{"date": "2024-11-01", "symbol": "AAPL", "dividend": 0.25}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_event_calendar, event_type="dividends")
        assert "/dividends-calendar" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_splits_calendar(self) -> None:
        from parsimony.connectors.fmp import fmp_event_calendar

        data = [{"date": "2024-08-31", "symbol": "TSLA", "numerator": 3}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_event_calendar, event_type="splits")
        assert "/splits-calendar" in m.call_args.args[1]


class TestFmpAnalystEstimates:
    @pytest.mark.asyncio
    async def test_path_and_required_period(self) -> None:
        from parsimony.connectors.fmp import fmp_analyst_estimates

        data = [{"symbol": "AAPL", "date": "2025-09-30", "revenueLow": 3.8e11,
                 "revenueAvg": 4e11, "revenueHigh": 4.2e11, "ebitdaLow": 1.2e11,
                 "ebitdaAvg": 1.3e11, "ebitdaHigh": 1.4e11, "netIncomeLow": 9e10,
                 "netIncomeAvg": 1e11, "netIncomeHigh": 1.1e11, "epsLow": 6.0,
                 "epsAvg": 6.5, "epsHigh": 7.0, "numAnalystsRevenue": 30,
                 "numAnalystsEps": 28}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_analyst_estimates, symbol="AAPL")
        assert "/analyst-estimates" in m.call_args.args[1]
        params = m.call_args.kwargs.get("params", {})
        assert "period" in params  # period is required by FMP API


class TestFmpNews:
    @pytest.mark.asyncio
    async def test_stock_news_path(self) -> None:
        from parsimony.connectors.fmp import fmp_news

        data = [{"symbol": "AAPL", "publishedDate": "2024-10-31T14:00:00",
                 "title": "Q4", "text": "Apple...", "url": "https://x.com",
                 "site": "Reuters", "image": None}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_news, type="news", symbols="AAPL")
        assert "/news/stock" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_press_releases_path(self) -> None:
        from parsimony.connectors.fmp import fmp_news

        data = [{"symbol": "AAPL", "publishedDate": "2024-10-31T14:00:00",
                 "title": "Q4", "text": "Apple...", "url": "https://x.com",
                 "site": "Apple IR", "image": None}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_news, type="press_releases", symbols="AAPL")
        assert "/news/press-releases" in m.call_args.args[1]


class TestFmpInsiderTrades:
    @pytest.mark.asyncio
    async def test_path(self) -> None:
        from parsimony.connectors.fmp import fmp_insider_trades

        data = [{"symbol": "AAPL", "filingDate": "2024-10-15",
                 "transactionDate": "2024-10-14", "reportingName": "Tim Cook",
                 "typeOfOwner": "officer", "transactionType": "S-Sale",
                 "acquisitionOrDisposition": "D", "securitiesTransacted": 50000,
                 "price": 180.0, "securitiesOwned": 3e6, "formType": "4", "url": "x"}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_insider_trades, symbol="AAPL")
        assert "/insider-trading/search" in m.call_args.args[1]


class TestFmpInstitutionalPositions:
    @pytest.mark.asyncio
    async def test_path(self) -> None:
        from parsimony.connectors.fmp import fmp_institutional_positions

        data = [{"symbol": "AAPL", "date": "2024-09-30", "investorsHolding": 5000,
                 "investorsHoldingChange": 50, "numberOf13Fshares": 1.5e10,
                 "numberOf13FsharesChange": 2e8, "totalInvested": 2.7e12,
                 "totalInvestedChange": 1e11, "ownershipPercent": 0.62,
                 "ownershipPercentChange": 0.01, "newPositions": 120,
                 "closedPositions": 80, "increasedPositions": 2500,
                 "reducedPositions": 1800, "putCallRatio": 0.75}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_institutional_positions, symbol="AAPL", year="2024", quarter="3")
        assert "/institutional-ownership/symbol-positions-summary" in m.call_args.args[1]


class TestFmpEarningsTranscript:
    @pytest.mark.asyncio
    async def test_path(self) -> None:
        from parsimony.connectors.fmp import fmp_earnings_transcript

        data = [{"symbol": "AAPL", "year": 2024, "period": "Q4",
                 "date": "2024-10-31", "content": "Good afternoon..."}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_earnings_transcript, symbol="AAPL", year="2024", quarter="4")
        assert "/earning-call-transcript" in m.call_args.args[1]


class TestFmpIndexConstituents:
    @pytest.mark.asyncio
    async def test_sp500(self) -> None:
        from parsimony.connectors.fmp import fmp_index_constituents

        data = [{"symbol": "AAPL", "name": "Apple", "sector": "Technology",
                 "subSector": "CE", "headQuarter": "CA", "dateFirstAdded": "1982-11-30",
                 "cik": "x", "founded": "1976"}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_index_constituents, index="SP500")
        assert "/sp500-constituent" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_nasdaq(self) -> None:
        from parsimony.connectors.fmp import fmp_index_constituents

        with _patch_http(_make_response([{"symbol": "MSFT", "name": "Microsoft",
                 "sector": "Tech", "subSector": "SW", "headQuarter": "WA",
                 "dateFirstAdded": None, "cik": "x", "founded": "1975"}])) as m:
            await _call(fmp_index_constituents, index="NASDAQ")
        assert "/nasdaq-constituent" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_dow_jones(self) -> None:
        from parsimony.connectors.fmp import fmp_index_constituents

        with _patch_http(_make_response([{"symbol": "AAPL", "name": "Apple",
                 "sector": "Tech", "subSector": "CE", "headQuarter": "CA",
                 "dateFirstAdded": None, "cik": "x", "founded": "1976"}])) as m:
            await _call(fmp_index_constituents, index="DOW_JONES")
        assert "/dowjones-constituent" in m.call_args.args[1]


class TestFmpMarketMovers:
    @pytest.mark.asyncio
    async def test_gainers(self) -> None:
        from parsimony.connectors.fmp import fmp_market_movers

        data = [{"symbol": "X", "name": "X", "price": 50.0, "change": 10.0,
                 "changesPercentage": 25.0, "exchange": "NASDAQ"}]
        with _patch_http(_make_response(data)) as m:
            await _call(fmp_market_movers, type="gainers")
        assert "/biggest-gainers" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_losers(self) -> None:
        from parsimony.connectors.fmp import fmp_market_movers

        with _patch_http(_make_response([{"symbol": "X", "name": "X", "price": 10.0,
                 "change": -5.0, "changesPercentage": -33.0, "exchange": "NYSE"}])) as m:
            await _call(fmp_market_movers, type="losers")
        assert "/biggest-losers" in m.call_args.args[1]

    @pytest.mark.asyncio
    async def test_most_actives(self) -> None:
        from parsimony.connectors.fmp import fmp_market_movers

        with _patch_http(_make_response([{"symbol": "NVDA", "name": "NVIDIA", "price": 800.0,
                 "change": 20.0, "changesPercentage": 2.5, "exchange": "NASDAQ"}])) as m:
            await _call(fmp_market_movers, type="most_actives")
        assert "/most-actives" in m.call_args.args[1]


# ===========================================================================
# ERROR HANDLING
# ===========================================================================


class TestFmpErrorHandling:
    @pytest.mark.asyncio
    async def test_401_friendly_message(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile

        with _patch_http(_make_response({}, status_code=401)):
            with pytest.raises(ValueError, match="Invalid or missing FMP API key"):
                await _call(fmp_company_profile, symbol="AAPL")

    @pytest.mark.asyncio
    async def test_402_plan_message(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile

        with _patch_http(_make_response({}, status_code=402)), pytest.raises(ValueError, match="not eligible"):
            await _call(fmp_company_profile, symbol="AAPL")

    @pytest.mark.asyncio
    async def test_500_generic_message(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile

        with _patch_http(_make_response({}, status_code=500)):
            with pytest.raises(ValueError, match="FMP API error 500"):
                await _call(fmp_company_profile, symbol="AAPL")

    @pytest.mark.asyncio
    async def test_api_key_never_exposed(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile

        with _patch_http(_make_response({}, status_code=401)):
            with pytest.raises(ValueError) as exc_info:
                await _call(fmp_company_profile, symbol="AAPL")
            assert API_KEY not in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_empty_response_raises(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile

        with _patch_http(_make_response([])), pytest.raises(ValueError, match="No data returned"):
            await _call(fmp_company_profile, symbol="INVALID")


# ===========================================================================
# SCREENER UNIT TESTS
# ===========================================================================


class TestFmpScreener:
    def _screener_data(self):
        return [
            {"symbol": "AAPL", "companyName": "Apple", "sector": "Technology",
             "industry": "CE", "country": "US", "exchange": "NASDAQ",
             "marketCap": 2.8e12, "price": 180.0, "beta": 1.2, "volume": 5e7,
             "lastAnnualDividend": 0.96, "isEtf": False, "isFund": False,
             "isActivelyTrading": True},
            {"symbol": "MSFT", "companyName": "Microsoft", "sector": "Technology",
             "industry": "SW", "country": "US", "exchange": "NASDAQ",
             "marketCap": 2.9e12, "price": 380.0, "beta": 0.9, "volume": 3e7,
             "lastAnnualDividend": 2.72, "isEtf": False, "isFund": False,
             "isActivelyTrading": True},
        ]

    def _metrics(self, sym):
        return [{"symbol": sym, "enterpriseValueTTM": 2.8e12,
                 "returnOnEquityTTM": 0.15, "freeCashFlowYieldTTM": 0.035}]

    def _ratios(self, sym):
        return [{"symbol": sym, "priceToEarningsRatioTTM": 28.5,
                 "grossProfitMarginTTM": 0.45, "dividendYieldTTM": 0.005}]

    def _mock_request(self, method, path, params=None, **kw):
        if "company-screener" in path:
            return _make_response(self._screener_data())
        elif "key-metrics-ttm" in path:
            sym = (params or {}).get("symbol", "AAPL")
            return _make_response(self._metrics(sym))
        elif "ratios-ttm" in path:
            sym = (params or {}).get("symbol", "AAPL")
            return _make_response(self._ratios(sym))
        return _make_response([{"symbol": "X"}])

    @pytest.mark.asyncio
    async def test_basic_screener(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        with patch("parsimony.transport.http.HttpClient.request",
                   side_effect=self._mock_request):
            result = await _call(fmp_screener, sector="Technology", limit=10)
        assert len(result.df) == 2

    @pytest.mark.asyncio
    async def test_where_clause(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        with patch("parsimony.transport.http.HttpClient.request",
                   side_effect=self._mock_request):
            result = await _call(fmp_screener, where_clause="price > 200", limit=50)
        assert all(result.df["price"] > 200)

    @pytest.mark.asyncio
    async def test_sort_and_limit(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        with patch("parsimony.transport.http.HttpClient.request",
                   side_effect=self._mock_request):
            result = await _call(fmp_screener, sort_by="marketCap", sort_order="desc", limit=1)
        assert len(result.df) == 1

    @pytest.mark.asyncio
    async def test_fields_selection(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        with patch("parsimony.transport.http.HttpClient.request",
                   side_effect=self._mock_request):
            result = await _call(fmp_screener, fields=["companyName", "marketCap"], limit=10)
        assert set(result.df.columns) == {"symbol", "companyName", "marketCap"}

    @pytest.mark.asyncio
    async def test_skips_enrichment_when_not_needed(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        endpoints: list[str] = []

        def tracking_mock(method, path, params=None, **kw):
            endpoints.append(path)
            return self._mock_request(method, path, params, **kw)

        with patch("parsimony.transport.http.HttpClient.request",
                   side_effect=tracking_mock):
            await _call(fmp_screener, fields=["companyName", "marketCap"], limit=10)

        assert any("company-screener" in p for p in endpoints)
        assert not any("key-metrics-ttm" in p for p in endpoints)
        assert not any("ratios-ttm" in p for p in endpoints)

    @pytest.mark.asyncio
    async def test_invalid_where_clause(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        with patch("parsimony.transport.http.HttpClient.request",
                   side_effect=self._mock_request), pytest.raises(ValueError, match="Invalid where_clause"):
            await _call(fmp_screener, where_clause="nonExistent > 5", limit=10)

    @pytest.mark.asyncio
    async def test_empty_screener_raises(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener

        with _patch_http(_make_response([])), pytest.raises(ValueError, match="no rows"):
            await _call(fmp_screener, sector="Nonexistent", limit=10)


# ===========================================================================
# COLLECTION INTEGRITY
# ===========================================================================


class TestCollectionIntegrity:
    def test_fmp_has_18_connectors(self) -> None:
        from parsimony.connectors.fmp import CONNECTORS
        assert len(CONNECTORS) == 18

    def test_screener_has_1_connector(self) -> None:
        from parsimony.connectors.fmp_screener import CONNECTORS
        assert len(CONNECTORS) == 1

    def test_all_have_descriptions(self) -> None:
        from parsimony.connectors.fmp import CONNECTORS as FMP
        from parsimony.connectors.fmp_screener import CONNECTORS as SCREENER
        for c in list(FMP) + list(SCREENER):
            assert c.description and len(c.description) > 10, f"{c.name} missing description"

    def test_all_bindable(self) -> None:
        from parsimony.connectors.fmp import CONNECTORS as FMP
        from parsimony.connectors.fmp_screener import CONNECTORS as SCREENER
        for c in list(FMP) + list(SCREENER):
            bound = c.bind_deps(api_key="test")
            assert not bound.dep_names, f"{c.name} has unbound deps"

    def test_no_duplicate_names(self) -> None:
        from parsimony.connectors.fmp import CONNECTORS as FMP
        from parsimony.connectors.fmp_screener import CONNECTORS as SCREENER
        names = [c.name for c in list(FMP) + list(SCREENER)]
        assert len(names) == len(set(names))

    def test_factory_includes_all_19(self) -> None:
        os.environ.setdefault("FRED_API_KEY", "test")
        os.environ.setdefault("FMP_API_KEY", "test")
        from parsimony.connectors import build_connectors_from_env
        connectors = build_connectors_from_env()
        fmp = sorted(c.name for c in connectors if c.name.startswith("fmp_"))
        assert len(fmp) == 19
        assert "fmp_screener" in fmp


# ===========================================================================
# LIVE INTEGRATION TESTS — run with: pytest -m live
# Requires FMP_API_KEY env var.
# These are the authoritative tests. If they fail, the code is wrong.
# ===========================================================================


@live
class TestLiveFmpConnectors:
    """Every connector tested against the real FMP stable API."""

    @pytest.mark.asyncio
    async def test_search(self) -> None:
        from parsimony.connectors.fmp import fmp_search
        result = await _call_live(fmp_search, query="Apple", limit=3)
        assert len(result.df) >= 1
        assert "symbol" in result.df.columns

    @pytest.mark.asyncio
    async def test_taxonomy_sectors(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy
        result = await _call_live(fmp_taxonomy, type="sectors")
        assert len(result.df) >= 5

    @pytest.mark.asyncio
    async def test_taxonomy_industries(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy
        result = await _call_live(fmp_taxonomy, type="industries")
        assert len(result.df) >= 10

    @pytest.mark.asyncio
    async def test_taxonomy_exchanges(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy
        result = await _call_live(fmp_taxonomy, type="exchanges")
        assert len(result.df) >= 5

    @pytest.mark.asyncio
    async def test_taxonomy_symbols_with_financials(self) -> None:
        from parsimony.connectors.fmp import fmp_taxonomy
        result = await _call_live(fmp_taxonomy, type="symbols_with_financials")
        assert len(result.df) >= 100

    @pytest.mark.asyncio
    async def test_quotes(self) -> None:
        from parsimony.connectors.fmp import fmp_quotes
        result = await _call_live(fmp_quotes, symbols="AAPL,MSFT")
        assert len(result.df) == 2
        assert set(result.df["symbol"].tolist()) == {"AAPL", "MSFT"}

    @pytest.mark.asyncio
    async def test_prices_daily(self) -> None:
        from parsimony.connectors.fmp import fmp_prices
        result = await _call_live(fmp_prices, symbol="AAPL", frequency="daily",
                                  **{"from": "2024-01-02", "to": "2024-01-05"})
        assert len(result.df) >= 1
        assert "close" in result.df.columns

    @pytest.mark.asyncio
    async def test_prices_intraday(self) -> None:
        from parsimony.connectors.fmp import fmp_prices
        result = await _call_live(fmp_prices, symbol="AAPL", frequency="5min")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_prices_dividend_adjusted(self) -> None:
        from parsimony.connectors.fmp import fmp_prices
        result = await _call_live(fmp_prices, symbol="AAPL", frequency="dividend_adjusted",
                                  **{"from": "2024-01-02", "to": "2024-01-05"})
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_company_profile(self) -> None:
        from parsimony.connectors.fmp import fmp_company_profile
        result = await _call_live(fmp_company_profile, symbol="AAPL")
        df = result.df
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["companyName"].iloc[0] == "Apple Inc."

    @pytest.mark.asyncio
    async def test_peers(self) -> None:
        from parsimony.connectors.fmp import fmp_peers
        result = await _call_live(fmp_peers, symbol="AAPL")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_income_statements(self) -> None:
        from parsimony.connectors.fmp import fmp_income_statements
        result = await _call_live(fmp_income_statements, symbol="AAPL", period="annual", limit=2)
        assert len(result.df) == 2
        assert "revenue" in result.df.columns

    @pytest.mark.asyncio
    async def test_balance_sheet_statements(self) -> None:
        from parsimony.connectors.fmp import fmp_balance_sheet_statements
        result = await _call_live(fmp_balance_sheet_statements, symbol="AAPL", period="annual", limit=2)
        assert len(result.df) == 2
        assert "totalAssets" in result.df.columns

    @pytest.mark.asyncio
    async def test_cash_flow_statements(self) -> None:
        from parsimony.connectors.fmp import fmp_cash_flow_statements
        result = await _call_live(fmp_cash_flow_statements, symbol="AAPL", period="annual", limit=2)
        assert len(result.df) == 2
        assert "freeCashFlow" in result.df.columns

    @pytest.mark.asyncio
    async def test_corporate_history_earnings(self) -> None:
        from parsimony.connectors.fmp import fmp_corporate_history
        result = await _call_live(fmp_corporate_history, symbol="AAPL", event_type="earnings", limit=3)
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_corporate_history_dividends(self) -> None:
        from parsimony.connectors.fmp import fmp_corporate_history
        result = await _call_live(fmp_corporate_history, symbol="AAPL", event_type="dividends", limit=3)
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_corporate_history_splits(self) -> None:
        from parsimony.connectors.fmp import fmp_corporate_history
        result = await _call_live(fmp_corporate_history, symbol="AAPL", event_type="splits", limit=3)
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_event_calendar_earnings(self) -> None:
        from parsimony.connectors.fmp import fmp_event_calendar
        result = await _call_live(fmp_event_calendar, event_type="earnings")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_event_calendar_dividends(self) -> None:
        from parsimony.connectors.fmp import fmp_event_calendar
        result = await _call_live(fmp_event_calendar, event_type="dividends")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_event_calendar_splits(self) -> None:
        from parsimony.connectors.fmp import fmp_event_calendar
        result = await _call_live(fmp_event_calendar, event_type="splits")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_analyst_estimates(self) -> None:
        from parsimony.connectors.fmp import fmp_analyst_estimates
        result = await _call_live(fmp_analyst_estimates, symbol="AAPL", period="annual", limit=2)
        assert len(result.df) >= 1
        assert "epsAvg" in result.df.columns

    @pytest.mark.asyncio
    async def test_news(self) -> None:
        from parsimony.connectors.fmp import fmp_news
        result = await _call_live(fmp_news, type="news", symbols="AAPL", limit=3)
        assert len(result.df) >= 1
        assert "title" in result.df.columns

    @pytest.mark.asyncio
    async def test_press_releases(self) -> None:
        from parsimony.connectors.fmp import fmp_news
        result = await _call_live(fmp_news, type="press_releases", symbols="AAPL", limit=3)
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_insider_trades(self) -> None:
        from parsimony.connectors.fmp import fmp_insider_trades
        result = await _call_live(fmp_insider_trades, symbol="AAPL", limit=5)
        assert len(result.df) >= 1
        assert "reportingName" in result.df.columns

    @pytest.mark.asyncio
    async def test_institutional_positions(self) -> None:
        from parsimony.connectors.fmp import fmp_institutional_positions
        result = await _call_live(fmp_institutional_positions, symbol="AAPL", year="2024", quarter="3")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_earnings_transcript(self) -> None:
        from parsimony.connectors.fmp import fmp_earnings_transcript
        result = await _call_live(fmp_earnings_transcript, symbol="AAPL", year="2024", quarter="4")
        assert len(result.df) >= 1
        assert "content" in result.df.columns
        assert len(result.df["content"].iloc[0]) > 100  # transcript should be substantial

    @pytest.mark.asyncio
    async def test_index_constituents_sp500(self) -> None:
        from parsimony.connectors.fmp import fmp_index_constituents
        result = await _call_live(fmp_index_constituents, index="SP500")
        assert len(result.df) >= 400  # S&P 500 should have ~503

    @pytest.mark.asyncio
    async def test_index_constituents_nasdaq(self) -> None:
        from parsimony.connectors.fmp import fmp_index_constituents
        result = await _call_live(fmp_index_constituents, index="NASDAQ")
        assert len(result.df) >= 50

    @pytest.mark.asyncio
    async def test_index_constituents_dow_jones(self) -> None:
        from parsimony.connectors.fmp import fmp_index_constituents
        result = await _call_live(fmp_index_constituents, index="DOW_JONES")
        assert len(result.df) >= 25

    @pytest.mark.asyncio
    async def test_market_movers_gainers(self) -> None:
        from parsimony.connectors.fmp import fmp_market_movers
        result = await _call_live(fmp_market_movers, type="gainers")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_market_movers_losers(self) -> None:
        from parsimony.connectors.fmp import fmp_market_movers
        result = await _call_live(fmp_market_movers, type="losers")
        assert len(result.df) >= 1

    @pytest.mark.asyncio
    async def test_market_movers_most_actives(self) -> None:
        from parsimony.connectors.fmp import fmp_market_movers
        result = await _call_live(fmp_market_movers, type="most_actives")
        assert len(result.df) >= 1


@live
class TestLiveFmpScreener:
    @pytest.mark.asyncio
    async def test_basic_screener(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener
        result = await _call_live(fmp_screener, sector="Technology", country="US", limit=5)
        assert len(result.df) == 5
        assert "symbol" in result.df.columns

    @pytest.mark.asyncio
    async def test_screener_with_enrichment(self) -> None:
        from parsimony.connectors.fmp_screener import fmp_screener
        result = await _call_live(
            fmp_screener, sector="Technology", country="US",
            fields=["companyName", "marketCap", "priceToEarningsRatioTTM", "returnOnEquityTTM"],
            sort_by="marketCap", sort_order="desc", limit=3,
        )
        assert len(result.df) == 3
        assert "priceToEarningsRatioTTM" in result.df.columns
