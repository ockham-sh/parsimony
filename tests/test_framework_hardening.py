"""Regression tests for framework hardening (PLAN-framework-hardening.md).

Tests go through the public connector call path — not private functions —
to ensure the full pipeline is exercised.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest

from parsimony.connector import connector
from parsimony.errors import ParseError, RateLimitError
from parsimony.result import Column, ColumnRole, OutputSpec, Result

# ---------------------------------------------------------------------------
# OutputSpec is passive: the framework never coerces, validates, or raises
# ParseError over a connector's returned values. dtype handling is
# connector-owned now — these are regression guards for that contract.
# ---------------------------------------------------------------------------


def test_output_spec_does_not_coerce_or_validate_returned_values() -> None:
    """Values a connector returns pass through byte-for-byte, whatever OutputSpec declares."""
    output = OutputSpec(columns=[Column(name="ts", role=ColumnRole.DATA)])

    @connector(output=output, description="test connector")
    def _inner() -> pd.DataFrame:
        return pd.DataFrame({"ts": ["2024-01-01", "not-a-date", "n/a"]})

    result = _inner()
    assert isinstance(result, Result)
    assert list(result.frame["ts"]) == ["2024-01-01", "not-a-date", "n/a"]
    assert result.frame["ts"].dtype == object


def test_output_spec_has_no_dtype_field_to_declare() -> None:
    assert not hasattr(Column(name="x"), "dtype")


# ---------------------------------------------------------------------------
# Connector-raised ValueError must NOT be swallowed as ParseError
# ---------------------------------------------------------------------------


def test_connector_valueerror_not_wrapped_as_parse_error() -> None:
    """A ValueError from the connector function itself must propagate as-is, not as ParseError."""

    @connector(description="raises ValueError")
    def _bad() -> pd.DataFrame:
        raise ValueError("bad input from connector logic")

    with pytest.raises(ValueError, match="bad input from connector logic"):
        _bad()

    # Confirm it's NOT a ParseError (ParseError is a ConnectorError, not a ValueError)
    try:
        _bad()
    except ParseError:
        pytest.fail("connector ValueError must not be wrapped as ParseError")
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# Task 2 — HttpClient follows redirects
# ---------------------------------------------------------------------------


def test_http_client_follows_redirects() -> None:
    """HttpClient must follow a 302 redirect to the final URL."""
    from parsimony.transport import HttpClient

    redirect_response = httpx.Response(
        302,
        headers={"location": "http://example.com/final"},
        request=httpx.Request("GET", "http://example.com/original"),
    )
    final_response = httpx.Response(
        200,
        content=b'{"ok": true}',
        request=httpx.Request("GET", "http://example.com/final"),
    )

    transport = httpx.MockTransport(
        lambda request: redirect_response if "original" in str(request.url) else final_response
    )

    client = HttpClient("http://example.com", provider="example", _transport=transport)
    response = client.request("GET", "/original", op_name="op")

    assert response.status_code == 200
    assert len(response.history) > 0
    assert str(response.url) == "http://example.com/final"


def test_http_client_follow_redirects_default_true() -> None:
    """HttpClient.follow_redirects defaults to True."""
    from parsimony.transport import HttpClient

    c = HttpClient("http://example.com", provider="example")
    assert c._follow_redirects is True
    assert c._max_redirects == 5


# ---------------------------------------------------------------------------
# Task 3 — RateLimitError.retry_after epoch guard
# ---------------------------------------------------------------------------


def test_rate_limit_error_epoch_raises_value_error() -> None:
    """RateLimitError with a Unix epoch timestamp must raise ValueError at construction."""
    with pytest.raises(ValueError, match="Unix epoch timestamp"):
        RateLimitError(provider="test", retry_after=1_700_000_000)


def test_rate_limit_error_valid_duration_does_not_raise() -> None:
    """RateLimitError with a valid duration (≤86400s) must not raise."""
    err = RateLimitError(provider="test", retry_after=60.0)
    assert err.retry_after == 60.0
    assert err.provider == "test"
