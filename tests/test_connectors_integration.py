"""Integration tests for :class:`~parsimony.connector.Connectors` composition and execution."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, connector
from parsimony.result import Provenance, Result


class MockParams(BaseModel):
    series_id: str = Field(..., min_length=1)


@connector()
async def mock_fetch(params: MockParams, *, fail: bool = False) -> Result:
    """Fetch mock data."""
    if fail:
        raise ValueError("Simulated fetch failure")
    df = pd.DataFrame({"date": pd.to_datetime(["2024-01-01", "2024-02-01"]), "value": [100.0, 200.0]})
    return Result.from_dataframe(df, Provenance(source="mock", params=params.model_dump()))


MOCK_CONNECTORS = Connectors([mock_fetch])


class TestConnectorsExecution:
    @pytest.fixture
    def connectors_with_mock(self) -> Connectors:
        return MOCK_CONNECTORS

    @pytest.mark.asyncio
    async def test_call_success(self, connectors_with_mock: Connectors) -> None:
        result = await connectors_with_mock["mock_fetch"](series_id="GDPC1")
        assert len(result.data) == 2
        assert result.provenance.source == "mock_fetch"

    @pytest.mark.asyncio
    async def test_missing_raises(self, connectors_with_mock: Connectors) -> None:
        with pytest.raises(KeyError, match="No connector 'no_such'"):
            await connectors_with_mock["no_such"]()


class TestPreboundComposition:
    def test_fred_bind_names(self) -> None:
        from parsimony_fred import CONNECTORS as FRED

        wired = FRED.bind_deps(api_key="test-key")
        assert "fred_fetch" in wired.names()

    def test_fred_connectors(self) -> None:
        from parsimony_fred import CONNECTORS as FRED

        c = FRED.bind_deps(api_key="test-key")
        names = set(c.names())
        assert "fred_search" in names
        assert "fred_fetch" in names

    def test_fmp_connectors(self) -> None:
        from parsimony.connectors.fmp import CONNECTORS as FMP

        c = FMP.bind_deps(api_key="test-key")
        names = set(c.names())
        assert "fmp_income_statements" in names
        assert "fmp_company_profile" in names
        assert "fmp_quotes" in names
        assert "fmp_prices" in names
        assert "fmp_screener" not in names  # screener is in separate module
        # Verify new connectors are present
        assert "fmp_search" in names
        assert "fmp_peers" in names
        assert "fmp_cash_flow_statements" in names
        assert "fmp_news" in names
        assert "fmp_insider_trades" in names
        assert "fmp_analyst_estimates" in names
        assert "fmp_index_constituents" in names
        assert "fmp_market_movers" in names

    def test_alpha_vantage_connectors(self) -> None:
        from parsimony.connectors.alpha_vantage import CONNECTORS as AV

        c = AV.bind_deps(api_key="test-key")
        names = set(c.names())
        # Discovery
        assert "alpha_vantage_search" in names
        # Market data — equities
        assert "alpha_vantage_quote" in names
        assert "alpha_vantage_daily" in names
        assert "alpha_vantage_weekly" in names
        assert "alpha_vantage_monthly" in names
        assert "alpha_vantage_intraday" in names
        # Company fundamentals
        assert "alpha_vantage_overview" in names
        assert "alpha_vantage_income_statement" in names
        assert "alpha_vantage_balance_sheet" in names
        assert "alpha_vantage_cash_flow" in names
        assert "alpha_vantage_earnings" in names
        assert "alpha_vantage_etf_profile" in names
        # Calendars
        assert "alpha_vantage_earnings_calendar" in names
        assert "alpha_vantage_ipo_calendar" in names
        # Forex
        assert "alpha_vantage_fx_rate" in names
        assert "alpha_vantage_fx_daily" in names
        assert "alpha_vantage_fx_weekly" in names
        assert "alpha_vantage_fx_monthly" in names
        # Crypto
        assert "alpha_vantage_crypto_daily" in names
        assert "alpha_vantage_crypto_weekly" in names
        assert "alpha_vantage_crypto_monthly" in names
        # Economic indicators
        assert "alpha_vantage_econ" in names
        # Precious metals
        assert "alpha_vantage_metal_spot" in names
        assert "alpha_vantage_metal_history" in names
        # Alpha intelligence
        assert "alpha_vantage_news" in names
        assert "alpha_vantage_top_movers" in names
        # Technical indicators
        assert "alpha_vantage_technical" in names
        # Options
        assert "alpha_vantage_options" in names
        # Enumerator
        assert "enumerate_alpha_vantage" in names
