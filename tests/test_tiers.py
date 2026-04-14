"""Tests for typed connector exceptions and HTTP error mapping."""

from __future__ import annotations

import pytest

from parsimony.connector import (
    ConnectorError,
    PaymentRequiredError,
    RateLimitError,
)


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_payment_required_error_is_connector_error(self) -> None:
        assert issubclass(PaymentRequiredError, ConnectorError)

    def test_rate_limit_error_is_connector_error(self) -> None:
        assert issubclass(RateLimitError, ConnectorError)

    def test_payment_required_error_fields(self) -> None:
        e = PaymentRequiredError("fmp")
        assert e.provider == "fmp"
        assert "fmp" in str(e)

    def test_payment_required_error_custom_message(self) -> None:
        e = PaymentRequiredError("fmp", message="Custom msg")
        assert str(e) == "Custom msg"

    def test_rate_limit_burst(self) -> None:
        e = RateLimitError("fmp", 5.0)
        assert e.quota_exhausted is False
        assert e.retry_after == 5.0
        assert "retry" in str(e).lower()

    def test_rate_limit_quota_exhausted(self) -> None:
        e = RateLimitError("fmp", 3600.0, quota_exhausted=True)
        assert e.quota_exhausted is True
        assert "quota" in str(e).lower()

    def test_catch_connector_error_catches_payment_required(self) -> None:
        with pytest.raises(ConnectorError):
            raise PaymentRequiredError("fmp")

    def test_catch_connector_error_catches_rate_limit_error(self) -> None:
        with pytest.raises(ConnectorError):
            raise RateLimitError("fmp", 5.0)


# ---------------------------------------------------------------------------
# HTTP-level error tests (FMP 402)
# ---------------------------------------------------------------------------


class TestFmpPaymentRequiredHttp:
    """Test that FMP HTTP 402 raises PaymentRequiredError."""

    @pytest.mark.asyncio
    async def test_fmp_402_raises_payment_required(self) -> None:
        from unittest.mock import AsyncMock, patch

        import httpx

        from parsimony.connectors.fmp import fmp_company_profile

        mock_response = httpx.Response(402, request=httpx.Request("GET", "https://example.com"))

        with patch(
            "parsimony.connectors.fmp.HttpClient.request",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            bound = fmp_company_profile.bind_deps(api_key="test-key")
            with pytest.raises(PaymentRequiredError) as exc_info:
                await bound(symbol="AAPL")
            assert exc_info.value.provider == "fmp"


class TestEodhdPaymentRequiredHttp:
    """Test that EODHD HTTP 402 raises PaymentRequiredError."""

    @pytest.mark.asyncio
    async def test_eodhd_402_raises_payment_required(self) -> None:
        from unittest.mock import AsyncMock, patch

        import httpx

        from parsimony.connectors.eodhd import eodhd_eod

        mock_response = httpx.Response(402, request=httpx.Request("GET", "https://example.com"))

        with patch(
            "parsimony.connectors.eodhd.HttpClient.request",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            bound = eodhd_eod.bind_deps(api_key="test-key")
            with pytest.raises(PaymentRequiredError) as exc_info:
                await bound(ticker="AAPL.US")
            assert exc_info.value.provider == "eodhd"


# ---------------------------------------------------------------------------
# Integration tests: typed errors through connector call paths
# ---------------------------------------------------------------------------


class TestTypedErrorsThroughCallPath:
    """Test that typed exceptions propagate correctly through full connector calls."""

    @pytest.mark.asyncio
    async def test_fmp_401_raises_unauthorized(self) -> None:
        from unittest.mock import AsyncMock, patch

        import httpx

        from parsimony.connector import UnauthorizedError
        from parsimony.connectors.fmp import fmp_company_profile

        mock_response = httpx.Response(401, request=httpx.Request("GET", "https://example.com"))
        with patch("parsimony.connectors.fmp.HttpClient.request", new_callable=AsyncMock, return_value=mock_response):
            bound = fmp_company_profile.bind_deps(api_key="bad-key")
            with pytest.raises(UnauthorizedError) as exc_info:
                await bound(symbol="AAPL")
            assert exc_info.value.provider == "fmp"

    @pytest.mark.asyncio
    async def test_fmp_500_raises_provider_error(self) -> None:
        from unittest.mock import AsyncMock, patch

        import httpx

        from parsimony.connector import ProviderError
        from parsimony.connectors.fmp import fmp_company_profile

        mock_response = httpx.Response(500, request=httpx.Request("GET", "https://example.com"))
        with patch("parsimony.connectors.fmp.HttpClient.request", new_callable=AsyncMock, return_value=mock_response):
            bound = fmp_company_profile.bind_deps(api_key="test-key")
            with pytest.raises(ProviderError) as exc_info:
                await bound(symbol="AAPL")
            assert exc_info.value.provider == "fmp"
            assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_fmp_empty_raises_empty_data_error(self) -> None:
        from unittest.mock import AsyncMock, patch

        import httpx

        from parsimony.connector import EmptyDataError
        from parsimony.connectors.fmp import fmp_company_profile

        mock_response = httpx.Response(200, json=[], request=httpx.Request("GET", "https://example.com"))
        with patch("parsimony.connectors.fmp.HttpClient.request", new_callable=AsyncMock, return_value=mock_response):
            bound = fmp_company_profile.bind_deps(api_key="test-key")
            with pytest.raises(EmptyDataError) as exc_info:
                await bound(symbol="AAPL")
            assert exc_info.value.provider == "fmp"

    @pytest.mark.asyncio
    async def test_eodhd_401_raises_unauthorized(self) -> None:
        from unittest.mock import AsyncMock, patch

        import httpx

        from parsimony.connector import UnauthorizedError
        from parsimony.connectors.eodhd import eodhd_eod

        mock_response = httpx.Response(401, request=httpx.Request("GET", "https://example.com"))
        with patch("parsimony.connectors.eodhd.HttpClient.request", new_callable=AsyncMock, return_value=mock_response):
            bound = eodhd_eod.bind_deps(api_key="bad-key")
            with pytest.raises(UnauthorizedError) as exc_info:
                await bound(ticker="AAPL.US")
            assert exc_info.value.provider == "eodhd"

    def test_all_errors_catchable_via_connector_error(self) -> None:
        """All typed exceptions are catchable via the base ConnectorError."""
        from parsimony.connector import (
            ConnectorError,
            EmptyDataError,
            ParseError,
            ProviderError,
            UnauthorizedError,
        )

        for cls, args in [
            (UnauthorizedError, ("fmp",)),
            (ProviderError, ("fmp", 500)),
            (EmptyDataError, ("fmp",)),
            (ParseError, ("fmp",)),
            (PaymentRequiredError, ("fmp",)),
        ]:
            with pytest.raises(ConnectorError):
                raise cls(*args)
