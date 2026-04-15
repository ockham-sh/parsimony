"""Tests for all EODHD connectors.

Two test layers:
1. **Unit tests** (mocked HTTP) — verify endpoint paths, parameter passing,
   response parsing, and error handling. Run always.
2. **Live integration tests** (real EODHD API) — gated behind ``@pytest.mark.live``
   and require ``EODHD_API_KEY`` env var. Run with: ``pytest -m live``
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

_LIVE_KEY = os.environ.get("EODHD_API_KEY", "")
_HAS_LIVE_KEY = bool(_LIVE_KEY)

live = pytest.mark.skipif(not _HAS_LIVE_KEY, reason="EODHD_API_KEY not set")


def _make_response(json_data: Any, status_code: int = 200) -> httpx.Response:
    """Build a fake httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = {}
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
    bound = connector_obj.bind_deps(api_key=_LIVE_KEY)
    return await bound(**kwargs)


# ---------------------------------------------------------------------------
# Minimal response stubs
# ---------------------------------------------------------------------------

_EOD_ROW = {
    "date": "2024-01-02",
    "open": 185.0,
    "high": 187.0,
    "low": 184.0,
    "close": 186.0,
    "adjusted_close": 186.0,
    "volume": 55000000,
}

_LIVE_ROW = {
    "code": "AAPL.US",
    "timestamp": 1704200000,
    "open": 185.0,
    "high": 187.0,
    "low": 184.0,
    "close": 186.0,
    "volume": 55000000,
    "previousClose": 184.5,
    "change": 1.5,
    "change_p": 0.81,
}

_INTRADAY_ROW = {
    "timestamp": 1704200000,
    "datetime": "2024-01-02 10:00:00",
    "open": 185.0,
    "high": 185.5,
    "low": 184.8,
    "close": 185.2,
    "volume": 100000,
}

_BULK_ROW = {
    "code": "AAPL",
    "name": "Apple Inc",
    "exchange_short_name": "US",
    "date": "2024-01-02",
    "open": 185.0,
    "high": 187.0,
    "low": 184.0,
    "close": 186.0,
    "adjusted_close": 186.0,
    "volume": 55000000,
}

_DIVIDENDS_ROW = {
    "date": "2024-02-09",
    "declarationDate": "2024-02-01",
    "recordDate": "2024-02-12",
    "paymentDate": "2024-02-15",
    "period": "Quarterly",
    "value": 0.24,
    "unadjustedValue": 0.24,
    "currency": "USD",
}

_SPLITS_ROW = {
    "date": "2020-08-31",
    "split": "4/1",
}

_SEARCH_ROW = {
    "Code": "AAPL",
    "Exchange": "US",
    "Name": "Apple Inc",
    "Type": "Common Stock",
    "Country": "USA",
    "Currency": "USD",
    "ISIN": "US0378331005",
    "previousClose": 186.0,
    "previousCloseDate": "2024-01-02",
}

_EXCHANGE_ROW = {
    "Name": "US",
    "Code": "US",
    "OperatingMIC": "XNAS",
    "Country": "USA",
    "Currency": "USD",
    "CountryISO2": "US",
    "CountryISO3": "USA",
}

_EXCHANGE_SYMBOL_ROW = {
    "Code": "AAPL",
    "Name": "Apple Inc",
    "Country": "USA",
    "Exchange": "US",
    "Currency": "USD",
    "Type": "Common Stock",
    "Isin": "US0378331005",
}

_NEWS_ROW = {
    "date": "2024-01-02T10:00:00+00:00",
    "title": "Apple beats estimates",
    "content": "Apple Inc...",
    "link": "https://example.com/article",
    "symbols": ["AAPL.US"],
    "tags": ["earnings"],
}

_MACRO_ROW = {
    "Date": "2024-01-01",
    "Value": 2.1,
    "Period": "annual",
    "LastUpdated": "2024-06-15",
}

_TECHNICAL_ROW = {
    "date": "2024-01-02",
    "open": 185.0,
    "high": 187.0,
    "low": 184.0,
    "close": 186.0,
    "volume": 55000000,
    "sma": 180.5,
}

_INSIDER_ROW = {
    "date": "2024-01-05",
    "code": "AAPL.US",
    "type": "S-Sale",
    "value": 5000000.0,
    "count": 25000,
    "transaction": "Sale",
    "filingDate": "2024-01-07",
    "ownerName": "Tim Cook",
    "ownerType": "officer",
    "formType": "4",
    "url": "https://www.sec.gov/cgi-bin/browse-edgar",
}

_SCREENER_ROW = {
    "code": "AAPL.US",
    "name": "Apple Inc",
    "exchange": "US",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "market_capitalization": 2800000000000.0,
    "pe": 28.5,
    "eps": 6.32,
}


# ===========================================================================
# _to_bracket_params — pure unit tests
# ===========================================================================


class TestToBracketParams:
    def test_filter_prefix_rewritten(self) -> None:
        from parsimony.connectors.eodhd import _to_bracket_params

        result = _to_bracket_params({"filter_market_cap": 1000})
        assert result == {"filter[market_cap]": 1000}

    def test_page_prefix_rewritten(self) -> None:
        from parsimony.connectors.eodhd import _to_bracket_params

        result = _to_bracket_params({"page_offset": 10})
        assert result == {"page[offset]": 10}

    def test_none_values_dropped(self) -> None:
        from parsimony.connectors.eodhd import _to_bracket_params

        result = _to_bracket_params({"ticker": "AAPL.US", "from": None})
        assert result == {"ticker": "AAPL.US"}
        assert "from" not in result

    def test_plain_keys_pass_through(self) -> None:
        from parsimony.connectors.eodhd import _to_bracket_params

        result = _to_bracket_params({"limit": 50, "offset": 0})
        assert result == {"limit": 50, "offset": 0}

    def test_mixed_keys(self) -> None:
        from parsimony.connectors.eodhd import _to_bracket_params

        result = _to_bracket_params({"ticker": "AAPL.US", "filter_pe": 30, "page_number": 1, "missing": None})
        assert result == {
            "ticker": "AAPL.US",
            "filter[pe]": 30,
            "page[number]": 1,
        }

    def test_does_not_mutate_input(self) -> None:
        from parsimony.connectors.eodhd import _to_bracket_params

        original = {"filter_pe": 30, "ticker": "AAPL.US"}
        _to_bracket_params(original)
        assert original == {"filter_pe": 30, "ticker": "AAPL.US"}


# ===========================================================================
# Market Data
# ===========================================================================


class TestEodhdEod:
    async def test_path_contains_ticker(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod

        with _patch_http(_make_response([_EOD_ROW])) as m:
            await _call(eodhd_eod, ticker="AAPL.US")
        assert "AAPL.US" in m.call_args.args[1]

    async def test_result_has_close_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod

        with _patch_http(_make_response([_EOD_ROW])):
            result = await _call(eodhd_eod, ticker="AAPL.US")
        assert "close" in result.df.columns

    async def test_from_to_passed_as_query_params(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod

        with _patch_http(_make_response([_EOD_ROW])) as m:
            await _call(eodhd_eod, ticker="AAPL.US", from_date="2024-01-01", to_date="2024-12-31")
        params = m.call_args.kwargs.get("params", {})
        assert params.get("from") == "2024-01-01"
        assert params.get("to") == "2024-12-31"

    async def test_empty_list_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response([])), pytest.raises(EmptyDataError):
            await _call(eodhd_eod, ticker="INVALID.XX")

    async def test_401_raises_unauthorized(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import UnauthorizedError

        with _patch_http(_make_response({}, status_code=401)), pytest.raises(UnauthorizedError):
            await _call(eodhd_eod, ticker="AAPL.US")

    async def test_402_raises_payment_required(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import PaymentRequiredError

        with _patch_http(_make_response({}, status_code=402)), pytest.raises(PaymentRequiredError):
            await _call(eodhd_eod, ticker="AAPL.US")

    async def test_500_raises_provider_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import ProviderError

        with _patch_http(_make_response({}, status_code=500)), pytest.raises(ProviderError, match="500"):
            await _call(eodhd_eod, ticker="AAPL.US")

    async def test_api_key_not_in_error_message(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import UnauthorizedError

        with _patch_http(_make_response({}, status_code=401)), pytest.raises(UnauthorizedError) as exc:
            await _call(eodhd_eod, ticker="AAPL.US")
        assert API_KEY not in str(exc.value)


class TestEodhdLive:
    async def test_path_contains_real_time(self) -> None:
        from parsimony.connectors.eodhd import eodhd_live

        with _patch_http(_make_response([_LIVE_ROW])) as m:
            await _call(eodhd_live, ticker="AAPL.US")
        assert "real-time" in m.call_args.args[1]

    async def test_result_has_change_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_live

        with _patch_http(_make_response([_LIVE_ROW])):
            result = await _call(eodhd_live, ticker="AAPL.US")
        assert "change" in result.df.columns

    async def test_empty_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_live
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response([])), pytest.raises(EmptyDataError):
            await _call(eodhd_live, ticker="INVALID.XX")


class TestEodhdIntraday:
    async def test_path_contains_intraday(self) -> None:
        from parsimony.connectors.eodhd import eodhd_intraday

        with _patch_http(_make_response([_INTRADAY_ROW])) as m:
            await _call(eodhd_intraday, ticker="AAPL.US", interval="5m")
        assert "intraday" in m.call_args.args[1]

    async def test_interval_in_query_params(self) -> None:
        from parsimony.connectors.eodhd import eodhd_intraday

        with _patch_http(_make_response([_INTRADAY_ROW])) as m:
            await _call(eodhd_intraday, ticker="AAPL.US", interval="1h")
        params = m.call_args.kwargs.get("params", {})
        assert params.get("interval") == "1h"


class TestEodhdBulkEod:
    async def test_path_contains_bulk_last_day(self) -> None:
        from parsimony.connectors.eodhd import eodhd_bulk_eod

        with _patch_http(_make_response([_BULK_ROW])) as m:
            await _call(eodhd_bulk_eod, exchange="US")
        assert "bulk_last_day" in m.call_args.args[1]

    async def test_exchange_in_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_bulk_eod

        with _patch_http(_make_response([_BULK_ROW])) as m:
            await _call(eodhd_bulk_eod, exchange="LSE")
        assert "LSE" in m.call_args.args[1]


# ===========================================================================
# Corporate Actions
# ===========================================================================


class TestEodhdDividends:
    async def test_path_contains_div(self) -> None:
        from parsimony.connectors.eodhd import eodhd_dividends

        with _patch_http(_make_response([_DIVIDENDS_ROW])) as m:
            await _call(eodhd_dividends, ticker="AAPL.US")
        assert "/div/" in m.call_args.args[1]

    async def test_result_has_value_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_dividends

        with _patch_http(_make_response([_DIVIDENDS_ROW])):
            result = await _call(eodhd_dividends, ticker="AAPL.US")
        assert "value" in result.df.columns

    async def test_empty_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_dividends
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response([])), pytest.raises(EmptyDataError):
            await _call(eodhd_dividends, ticker="INVALID.XX")


class TestEodhdSplits:
    async def test_path_contains_splits(self) -> None:
        from parsimony.connectors.eodhd import eodhd_splits

        with _patch_http(_make_response([_SPLITS_ROW])) as m:
            await _call(eodhd_splits, ticker="AAPL.US")
        assert "/splits/" in m.call_args.args[1]

    async def test_result_has_split_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_splits

        with _patch_http(_make_response([_SPLITS_ROW])):
            result = await _call(eodhd_splits, ticker="AAPL.US")
        assert "split" in result.df.columns


# ===========================================================================
# Reference
# ===========================================================================


class TestEodhdSearch:
    async def test_path_contains_search(self) -> None:
        from parsimony.connectors.eodhd import eodhd_search

        with _patch_http(_make_response([_SEARCH_ROW])) as m:
            await _call(eodhd_search, query="Apple")
        assert "/search/" in m.call_args.args[1]

    async def test_query_in_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_search

        with _patch_http(_make_response([_SEARCH_ROW])) as m:
            await _call(eodhd_search, query="Apple")
        assert "Apple" in m.call_args.args[1]

    async def test_result_has_name_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_search

        with _patch_http(_make_response([_SEARCH_ROW])):
            result = await _call(eodhd_search, query="Apple")
        assert "Name" in result.df.columns

    async def test_empty_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_search
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response([])), pytest.raises(EmptyDataError):
            await _call(eodhd_search, query="zzz-no-match")


class TestEodhdExchanges:
    async def test_path_contains_exchanges_list(self) -> None:
        from parsimony.connectors.eodhd import EodhdExchangesParams, eodhd_exchanges

        with _patch_http(_make_response([_EXCHANGE_ROW])) as m:
            bound = eodhd_exchanges.bind_deps(api_key=API_KEY)
            await bound(params=EodhdExchangesParams())
        assert "exchanges-list" in m.call_args.args[1]

    async def test_result_has_code_column(self) -> None:
        from parsimony.connectors.eodhd import EodhdExchangesParams, eodhd_exchanges

        with _patch_http(_make_response([_EXCHANGE_ROW])):
            bound = eodhd_exchanges.bind_deps(api_key=API_KEY)
            result = await bound(params=EodhdExchangesParams())
        assert "Code" in result.df.columns


class TestEodhdExchangeSymbols:
    async def test_path_contains_exchange_symbol_list(self) -> None:
        from parsimony.connectors.eodhd import eodhd_exchange_symbols

        with _patch_http(_make_response([_EXCHANGE_SYMBOL_ROW])) as m:
            await _call(eodhd_exchange_symbols, exchange="US")
        assert "exchange-symbol-list" in m.call_args.args[1]

    async def test_exchange_in_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_exchange_symbols

        with _patch_http(_make_response([_EXCHANGE_SYMBOL_ROW])) as m:
            await _call(eodhd_exchange_symbols, exchange="LSE")
        assert "LSE" in m.call_args.args[1]


# ===========================================================================
# Fundamentals (raw=True)
# ===========================================================================


class TestEodhdFundamentals:
    async def test_path_contains_fundamentals(self) -> None:
        from parsimony.connectors.eodhd import eodhd_fundamentals

        with _patch_http(_make_response({"General": {"Code": "AAPL"}})) as m:
            await _call(eodhd_fundamentals, ticker="AAPL.US")
        assert "/fundamentals/" in m.call_args.args[1]

    async def test_returns_raw_dict(self) -> None:
        from parsimony.connectors.eodhd import eodhd_fundamentals

        payload = {"General": {"Code": "AAPL", "Name": "Apple Inc"}}
        with _patch_http(_make_response(payload)):
            result = await _call(eodhd_fundamentals, ticker="AAPL.US")
        assert result.data == payload

    async def test_401_raises_unauthorized(self) -> None:
        from parsimony.connectors.eodhd import eodhd_fundamentals
        from parsimony.errors import UnauthorizedError

        with _patch_http(_make_response({}, status_code=401)), pytest.raises(UnauthorizedError):
            await _call(eodhd_fundamentals, ticker="AAPL.US")


# ===========================================================================
# Calendar — dispatch dict
# ===========================================================================


class TestEodhdCalendar:
    async def test_earnings_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_calendar

        data = {
            "earnings": [
                {
                    "code": "AAPL.US",
                    "report_date": "2024-02-01",
                    "before_after_market": "BMO",
                    "currency": "USD",
                    "actual": 2.18,
                    "estimate": 2.12,
                    "difference": 0.06,
                    "percent": 2.83,
                }
            ]
        }
        with _patch_http(_make_response(data)) as m:
            await _call(eodhd_calendar, type="earnings")
        assert "calendar/earnings" in m.call_args.args[1]

    async def test_ipo_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_calendar

        data = {
            "ipos": [
                {
                    "code": "NEWCO.US",
                    "name": "NewCo Inc",
                    "exchange": "US",
                    "currency": "USD",
                    "start_date": "2024-02-15",
                    "filing_date": "2024-01-20",
                    "amended_date": None,
                    "price_from": 18.0,
                    "price_to": 20.0,
                    "offer_price": 19.0,
                    "shares": 10000000,
                    "deal_size": 190000000.0,
                }
            ]
        }
        with _patch_http(_make_response(data)) as m:
            await _call(eodhd_calendar, type="ipo")
        assert "calendar/ipo" in m.call_args.args[1]

    async def test_trends_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_calendar

        data = {
            "trends": [
                {
                    "code": "AAPL.US",
                    "date": "2024-01-01",
                    "period": "0m",
                    "strong_buy": 15,
                    "buy": 10,
                    "hold": 5,
                    "sell": 2,
                    "strong_sell": 1,
                }
            ]
        }
        with _patch_http(_make_response(data)) as m:
            await _call(eodhd_calendar, type="trends")
        assert "calendar/trends" in m.call_args.args[1]

    async def test_earnings_result_unwrapped(self) -> None:
        from parsimony.connectors.eodhd import eodhd_calendar

        data = {
            "earnings": [
                {
                    "code": "AAPL.US",
                    "report_date": "2024-02-01",
                    "before_after_market": "BMO",
                    "currency": "USD",
                    "actual": 2.18,
                    "estimate": 2.12,
                    "difference": 0.06,
                    "percent": 2.83,
                }
            ]
        }
        with _patch_http(_make_response(data)):
            result = await _call(eodhd_calendar, type="earnings")
        assert "code" in result.df.columns


# ===========================================================================
# News
# ===========================================================================


class TestEodhdNews:
    async def test_path_is_news(self) -> None:
        from parsimony.connectors.eodhd import EodhdNewsParams, eodhd_news

        with _patch_http(_make_response([_NEWS_ROW])) as m:
            bound = eodhd_news.bind_deps(api_key=API_KEY)
            await bound(params=EodhdNewsParams())
        assert "/news" in m.call_args.args[1]

    async def test_ticker_mapped_to_s_param(self) -> None:
        from parsimony.connectors.eodhd import eodhd_news

        with _patch_http(_make_response([_NEWS_ROW])) as m:
            await _call(eodhd_news, ticker="AAPL.US")
        params = m.call_args.kwargs.get("params", {})
        assert params.get("s") == "AAPL.US"

    async def test_no_ticker_omits_s_param(self) -> None:
        from parsimony.connectors.eodhd import EodhdNewsParams, eodhd_news

        with _patch_http(_make_response([_NEWS_ROW])) as m:
            bound = eodhd_news.bind_deps(api_key=API_KEY)
            await bound(params=EodhdNewsParams())
        params = m.call_args.kwargs.get("params", {})
        assert "s" not in params

    async def test_result_has_title_column(self) -> None:
        from parsimony.connectors.eodhd import EodhdNewsParams, eodhd_news

        with _patch_http(_make_response([_NEWS_ROW])):
            bound = eodhd_news.bind_deps(api_key=API_KEY)
            result = await bound(params=EodhdNewsParams())
        assert "title" in result.df.columns


# ===========================================================================
# Macro
# ===========================================================================


class TestEodhdMacro:
    async def test_path_contains_macro_indicator(self) -> None:
        from parsimony.connectors.eodhd import eodhd_macro

        with _patch_http(_make_response([_MACRO_ROW])) as m:
            await _call(eodhd_macro, country="USA", indicator="inflation_consumer_prices_annual")
        assert "macro-indicator" in m.call_args.args[1]

    async def test_country_in_path(self) -> None:
        from parsimony.connectors.eodhd import eodhd_macro

        with _patch_http(_make_response([_MACRO_ROW])) as m:
            await _call(eodhd_macro, country="USA", indicator="inflation_consumer_prices_annual")
        assert "USA" in m.call_args.args[1]

    async def test_indicator_as_query_param(self) -> None:
        from parsimony.connectors.eodhd import eodhd_macro

        with _patch_http(_make_response([_MACRO_ROW])) as m:
            await _call(eodhd_macro, country="USA", indicator="gdp_current_usd")
        params = m.call_args.kwargs.get("params", {})
        assert params.get("indicator") == "gdp_current_usd"

    async def test_result_has_value_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_macro

        with _patch_http(_make_response([_MACRO_ROW])):
            result = await _call(eodhd_macro, country="USA", indicator="gdp_current_usd")
        assert "Value" in result.df.columns

    async def test_empty_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_macro
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response([])), pytest.raises(EmptyDataError):
            await _call(eodhd_macro, country="ZZZ", indicator="gdp_current_usd")


class TestEodhdMacroBulk:
    async def test_path_contains_macro_indicator(self) -> None:
        from parsimony.connectors.eodhd import eodhd_macro_bulk

        with _patch_http(_make_response([_MACRO_ROW])) as m:
            await _call(eodhd_macro_bulk, country="USA")
        assert "macro-indicator" in m.call_args.args[1]


# ===========================================================================
# Technical
# ===========================================================================


class TestEodhdTechnical:
    async def test_path_contains_technicals(self) -> None:
        from parsimony.connectors.eodhd import eodhd_technical

        with _patch_http(_make_response([_TECHNICAL_ROW])) as m:
            await _call(eodhd_technical, ticker="AAPL.US", function="sma")
        assert "technicals" in m.call_args.args[1]

    async def test_function_in_query_params(self) -> None:
        from parsimony.connectors.eodhd import eodhd_technical

        with _patch_http(_make_response([_TECHNICAL_ROW])) as m:
            await _call(eodhd_technical, ticker="AAPL.US", function="ema")
        params = m.call_args.kwargs.get("params", {})
        assert params.get("function") == "ema"

    async def test_period_in_query_params(self) -> None:
        from parsimony.connectors.eodhd import eodhd_technical

        with _patch_http(_make_response([_TECHNICAL_ROW])) as m:
            await _call(eodhd_technical, ticker="AAPL.US", function="rsi", period=14)
        params = m.call_args.kwargs.get("params", {})
        assert params.get("period") == 14

    async def test_empty_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_technical
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response([])), pytest.raises(EmptyDataError):
            await _call(eodhd_technical, ticker="INVALID.XX", function="sma")


# ===========================================================================
# Insider Transactions
# ===========================================================================


class TestEodhdInsider:
    async def test_path_contains_insider_transactions(self) -> None:
        from parsimony.connectors.eodhd import eodhd_insider

        with _patch_http(_make_response([_INSIDER_ROW])) as m:
            await _call(eodhd_insider, ticker="AAPL.US")
        assert "insider-transactions" in m.call_args.args[1]

    async def test_result_has_type_column(self) -> None:
        from parsimony.connectors.eodhd import eodhd_insider

        with _patch_http(_make_response([_INSIDER_ROW])):
            result = await _call(eodhd_insider, ticker="AAPL.US")
        assert "type" in result.df.columns

    async def test_without_ticker_omits_code_param(self) -> None:
        from parsimony.connectors.eodhd import EodhdInsiderParams, eodhd_insider

        with _patch_http(_make_response([_INSIDER_ROW])) as m:
            bound = eodhd_insider.bind_deps(api_key=API_KEY)
            await bound(params=EodhdInsiderParams())
        params = m.call_args.kwargs.get("params", {})
        assert "code" not in params


# ===========================================================================
# Screener
# ===========================================================================


class TestEodhdScreener:
    async def test_path_is_screener(self) -> None:
        from parsimony.connectors.eodhd import EodhdScreenerParams, eodhd_screener

        with _patch_http(_make_response({"data": [_SCREENER_ROW]})) as m:
            bound = eodhd_screener.bind_deps(api_key=API_KEY)
            await bound(params=EodhdScreenerParams())
        assert "/screener" in m.call_args.args[1]

    async def test_filters_serialised_as_json(self) -> None:
        import json

        from parsimony.connectors.eodhd import eodhd_screener

        with _patch_http(_make_response({"data": [_SCREENER_ROW]})) as m:
            await _call(eodhd_screener, filters=[("market_capitalization", ">", "1000000000")])
        params = m.call_args.kwargs.get("params", {})
        assert "filters" in params
        decoded = json.loads(params["filters"])
        assert decoded == [["market_capitalization", ">", "1000000000"]]

    async def test_result_has_code_column(self) -> None:
        from parsimony.connectors.eodhd import EodhdScreenerParams, eodhd_screener

        with _patch_http(_make_response({"data": [_SCREENER_ROW]})):
            bound = eodhd_screener.bind_deps(api_key=API_KEY)
            result = await bound(params=EodhdScreenerParams())
        assert "code" in result.df.columns

    async def test_empty_raises_empty_data_error(self) -> None:
        from parsimony.connectors.eodhd import EodhdScreenerParams, eodhd_screener
        from parsimony.errors import EmptyDataError

        with _patch_http(_make_response({"data": []})), pytest.raises(EmptyDataError):
            bound = eodhd_screener.bind_deps(api_key=API_KEY)
            await bound(params=EodhdScreenerParams())


# ===========================================================================
# Shared error handling — 429 Rate Limit
# ===========================================================================


class TestEodhdRateLimit:
    async def test_429_raises_rate_limit_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import RateLimitError

        resp = _make_response({}, status_code=429)
        resp.headers = {"Retry-After": "30"}
        with _patch_http(resp), pytest.raises(RateLimitError) as exc:
            await _call(eodhd_eod, ticker="AAPL.US")
        assert exc.value.retry_after == 30.0

    async def test_429_fallback_retry_after(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import RateLimitError

        resp = _make_response({}, status_code=429)
        resp.headers = {}
        with _patch_http(resp), pytest.raises(RateLimitError) as exc:
            await _call(eodhd_eod, ticker="AAPL.US")
        assert exc.value.retry_after == 60.0


# ===========================================================================
# Body-level error detection
# ===========================================================================


class TestEodhdBodyError:
    async def test_body_error_field_raises_provider_error(self) -> None:
        from parsimony.connectors.eodhd import eodhd_eod
        from parsimony.errors import ProviderError

        with _patch_http(_make_response({"error": "Invalid ticker symbol"})), pytest.raises(
            ProviderError, match="Invalid ticker symbol"
        ):
            await _call(eodhd_eod, ticker="BADTICKER")


# ===========================================================================
# Export collections
# ===========================================================================


class TestExportCollections:
    def test_connectors_includes_all(self) -> None:
        from parsimony.connectors.eodhd import CONNECTORS

        names = {c.name for c in CONNECTORS}

        # Discovery connectors present
        assert "eodhd_search" in names
        assert "eodhd_exchanges" in names
        assert "eodhd_news" in names
        assert "eodhd_screener" in names

        # Fetch connectors present
        assert "eodhd_eod" in names
        assert "eodhd_live" in names
        assert "eodhd_intraday" in names

    def test_expected_connector_count(self) -> None:
        from parsimony.connectors.eodhd import CONNECTORS

        assert len(CONNECTORS) == 17
