"""Locked-string tests for the agent-facing kernel default messages.

The MCP bridge (``parsimony-mcp``) and the terminal sandbox renderer
(``parsimony-agents``) both consume ``str(exc)`` for ``ConnectorError``
subclasses as their canonical agent-facing string. This file is the
single point that locks those strings down. A reword should be a
deliberate kernel change with an LLM eval pass — not a casual edit.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)

# ---------------------------------------------------------------------------
# UnauthorizedError
# ---------------------------------------------------------------------------


class TestUnauthorizedError:
    def test_default_with_env_var_names_the_variable(self) -> None:
        exc = UnauthorizedError("fred", env_var="FRED_API_KEY")
        text = str(exc)
        assert "fred" in text
        assert "FRED_API_KEY" in text
        assert "DO NOT retry with different arguments" in text
        assert exc.env_var == "FRED_API_KEY"
        assert exc.provider == "fred"

    def test_default_without_env_var_falls_back_to_generic_directive(self) -> None:
        exc = UnauthorizedError("fred")
        text = str(exc)
        assert "fred" in text
        assert "credentials missing or invalid" in text
        assert "DO NOT retry with different arguments" in text
        assert exc.env_var is None

    def test_message_override_wins(self) -> None:
        exc = UnauthorizedError("fred", message="custom override", env_var="FRED_API_KEY")
        assert str(exc) == "custom override"
        # env_var attribute still preserved for programmatic readers.
        assert exc.env_var == "FRED_API_KEY"


# ---------------------------------------------------------------------------
# PaymentRequiredError
# ---------------------------------------------------------------------------


class TestPaymentRequiredError:
    def test_default_directs_to_alternative(self) -> None:
        exc = PaymentRequiredError("premium")
        text = str(exc)
        assert "premium" in text
        assert "not included in your plan" in text
        assert "DO NOT retry" in text
        assert "different connector" in text

    def test_message_override_wins(self) -> None:
        exc = PaymentRequiredError("premium", message="error_code=10005: historical-data restriction")
        assert str(exc) == "error_code=10005: historical-data restriction"


# ---------------------------------------------------------------------------
# RateLimitError
# ---------------------------------------------------------------------------


class TestRateLimitError:
    def test_burst_default_includes_retry_after_and_directive(self) -> None:
        exc = RateLimitError("fred", retry_after=30.0)
        text = str(exc)
        assert "fred" in text
        assert "retry after 30s" in text
        assert "DO NOT retry this tool immediately" in text
        assert "pick a different connector" in text
        assert exc.retry_after == 30.0
        assert exc.quota_exhausted is False

    def test_quota_exhausted_default_says_billing_period(self) -> None:
        exc = RateLimitError("fred", retry_after=0.0, quota_exhausted=True)
        text = str(exc)
        assert "fred" in text
        assert "quota exhausted" in text
        assert "DO NOT retry" in text
        assert "billing" in text
        assert exc.quota_exhausted is True

    def test_retry_after_over_24h_rejects_likely_epoch(self) -> None:
        # The constructor itself raises ValueError; the trailing ``raise`` is
        # never reached on the happy path. We still write it so CodeQL's
        # py/unused-exception-object rule sees the object being used — and
        # so a regression that stops raising ValueError surfaces as a
        # different test failure rather than a silent pass.
        with pytest.raises(ValueError, match="Unix epoch"):
            raise RateLimitError("fred", retry_after=1_700_000_000.0)

    def test_message_override_wins(self) -> None:
        exc = RateLimitError("fred", retry_after=10.0, message="custom prose")
        assert str(exc) == "custom prose"
        assert exc.retry_after == 10.0


# ---------------------------------------------------------------------------
# ProviderError
# ---------------------------------------------------------------------------


class TestProviderError:
    def test_408_default_says_timeout(self) -> None:
        exc = ProviderError("fmp", status_code=408)
        text = str(exc)
        assert "fmp" in text
        assert "timed out" in text
        assert "DO NOT immediately retry" in text
        assert exc.status_code == 408

    def test_5xx_default_says_likely_transient(self) -> None:
        exc = ProviderError("fmp", status_code=503)
        text = str(exc)
        assert "fmp" in text
        assert "HTTP 503" in text
        assert "likely" in text and "transient" in text
        assert "do not loop" in text

    def test_500_default_treated_as_server_error(self) -> None:
        exc = ProviderError("fmp", status_code=500)
        text = str(exc)
        assert "HTTP 500" in text
        assert "transient" in text

    def test_4xx_default_says_request_rejected(self) -> None:
        exc = ProviderError("fmp", status_code=404)
        text = str(exc)
        assert "fmp" in text
        assert "HTTP 404" in text
        assert "rejected" in text
        assert "DO NOT retry with the same parameters" in text

    def test_message_override_wins(self) -> None:
        exc = ProviderError("fmp", status_code=500, message="custom 5xx context")
        assert str(exc) == "custom 5xx context"
        assert exc.status_code == 500


# ---------------------------------------------------------------------------
# EmptyDataError
# ---------------------------------------------------------------------------


class TestEmptyDataError:
    def test_default_invites_param_adjustment_no_do_not_retry(self) -> None:
        exc = EmptyDataError("fred")
        text = str(exc)
        assert "fred" in text
        assert "no data returned" in text
        assert "Adjust parameters" in text
        # Empty is a valid outcome — agent SHOULD be free to retry with
        # different params; no DO NOT retry directive.
        assert "DO NOT" not in text

    def test_query_params_attribute_preserved(self) -> None:
        exc = EmptyDataError("fred", query_params={"series_id": "UNRATE"})
        assert exc.query_params == {"series_id": "UNRATE"}

    def test_message_override_wins(self) -> None:
        exc = EmptyDataError("fred", message="No rows for UNRATE")
        assert str(exc) == "No rows for UNRATE"


# ---------------------------------------------------------------------------
# ParseError
# ---------------------------------------------------------------------------


class TestParseError:
    def test_default_calls_out_connector_bug_and_blocks_retry(self) -> None:
        exc = ParseError("fmp")
        text = str(exc)
        assert "fmp" in text
        assert "could not be parsed" in text
        assert "schema drift" in text
        assert "retrying will not help" in text
        assert "DO NOT retry" in text

    def test_message_override_wins(self) -> None:
        exc = ParseError("fmp", message="Unexpected response type from FMP: dict")
        assert str(exc) == "Unexpected response type from FMP: dict"


# ---------------------------------------------------------------------------
# Bare ConnectorError
# ---------------------------------------------------------------------------


class TestConnectorError:
    def test_bare_connector_error_keeps_author_message(self) -> None:
        exc = ConnectorError("flow_id must be non-empty", provider="sdmx")
        assert str(exc) == "flow_id must be non-empty"
        assert exc.provider == "sdmx"


# ---------------------------------------------------------------------------
# Connector.__call__ env_var hand-off
# ---------------------------------------------------------------------------


class _Params(BaseModel):
    x: str


@connector(env={"api_key": "TEST_KEY"})
async def _needy(params: _Params, *, api_key: str) -> str:
    """Test connector requiring TEST_KEY."""
    return f"{params.x}:{api_key}"


@connector(env={"api_key": "PRIMARY_KEY", "secret": "SECONDARY_KEY"})
async def _multi_needy(params: _Params, *, api_key: str, secret: str) -> str:
    """Test connector requiring two env vars."""
    return f"{params.x}:{api_key}:{secret}"


def test_unbound_connector_raises_with_env_var_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_KEY", raising=False)
    coll = Connectors([_needy]).bind_env()
    with pytest.raises(UnauthorizedError) as excinfo:
        asyncio.run(coll["_needy"](x="hello"))
    exc = excinfo.value
    assert exc.env_var == "TEST_KEY"
    text = str(exc)
    assert "TEST_KEY" in text
    assert "DO NOT retry with different arguments" in text


def test_unbound_connector_with_multiple_env_vars_lists_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PRIMARY_KEY", raising=False)
    monkeypatch.delenv("SECONDARY_KEY", raising=False)
    coll = Connectors([_multi_needy]).bind_env()
    with pytest.raises(UnauthorizedError) as excinfo:
        asyncio.run(coll["_multi_needy"](x="hello"))
    exc = excinfo.value
    text = str(exc)
    # First var by sorted order is PRIMARY_KEY.
    assert exc.env_var == "PRIMARY_KEY"
    assert "PRIMARY_KEY" in text
    assert "SECONDARY_KEY" in text
    assert "DO NOT retry" in text
