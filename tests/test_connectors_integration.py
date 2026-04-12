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
    df = pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01", "2024-02-01"]), "value": [100.0, 200.0]}
    )
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
        assert result.provenance.source == "mock"

    @pytest.mark.asyncio
    async def test_missing_raises(self, connectors_with_mock: Connectors) -> None:
        with pytest.raises(KeyError, match="No connector 'no_such'"):
            await connectors_with_mock["no_such"]()


class TestPreboundComposition:
    def test_fred_bind_names(self) -> None:
        from parsimony.connectors.fred import CONNECTORS as FRED

        wired = FRED.bind_deps(api_key="test-key")
        assert "fred_fetch" in wired.names()

    def test_fred_connectors(self) -> None:
        from parsimony.connectors.fred import CONNECTORS as FRED

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
