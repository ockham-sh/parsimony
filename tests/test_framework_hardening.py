"""Regression tests for framework hardening (PLAN-framework-hardening.md).

Tests go through the public connector call path — not private functions —
to ensure the full pipeline is exercised.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.connector import connector
from parsimony.errors import ParseError, RateLimitError
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


class _NoParams(BaseModel):
    pass


# ---------------------------------------------------------------------------
# Helpers: minimal in-process connectors for coercion testing
# ---------------------------------------------------------------------------

def _make_connector(df: pd.DataFrame, dtype: str, col_name: str = "value") -> object:
    """Build a minimal @connector that returns *df* through an OutputConfig with the given dtype."""
    output = OutputConfig(
        columns=[
            Column(name=col_name, dtype=dtype, role=ColumnRole.DATA),
        ]
    )

    @connector(output=output, description="test connector")
    async def _inner(params: _NoParams) -> pd.DataFrame:
        return df

    return _inner


# ---------------------------------------------------------------------------
# Task 1a — timestamp dtype: ISO strings produce all-NaT → ParseError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timestamp_iso_strings_raise_parse_error() -> None:
    """Connector returning ISO date strings with dtype='timestamp' must raise ParseError."""
    df = pd.DataFrame({"ts": ["2024-01-01", "2024-06-15", "2024-12-31"]})
    conn = _make_connector(df, dtype="timestamp", col_name="ts")

    with pytest.raises(ParseError) as exc_info:
        await conn(_NoParams())

    assert "timestamp" in str(exc_info.value).lower()
    assert "ts" in str(exc_info.value)
    # Hunt watchpoint: no raw sample values in the message
    assert "2024-01-01" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Task 1b — numeric dtype: non-numeric strings produce all-NaN → ParseError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_numeric_non_numeric_strings_raise_parse_error() -> None:
    """Connector returning non-numeric strings with dtype='numeric' must raise ParseError."""
    df = pd.DataFrame({"price": ["n/a", "N/A", "--"]})
    conn = _make_connector(df, dtype="numeric", col_name="price")

    with pytest.raises(ParseError) as exc_info:
        await conn(_NoParams())

    assert "numeric" in str(exc_info.value).lower()
    assert "price" in str(exc_info.value)
    # Hunt watchpoint: no raw sample values in the message
    assert "n/a" not in str(exc_info.value).lower() or "NaN" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Task 1c — astype fallback: unsupported dtype string → ParseError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unsupported_dtype_raises_parse_error() -> None:
    """Connector declaring an unknown dtype string must raise ParseError, not TypeError."""
    df = pd.DataFrame({"col": [1, 2, 3]})
    conn = _make_connector(df, dtype="not_a_real_dtype", col_name="col")

    with pytest.raises(ParseError) as exc_info:
        await conn(_NoParams())

    assert "not_a_real_dtype" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Connector-raised ValueError must NOT be swallowed as ParseError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connector_valueerror_not_wrapped_as_parse_error() -> None:
    """A ValueError from the connector function itself must propagate as-is, not as ParseError."""

    @connector(description="raises ValueError")
    async def _bad(params: _NoParams) -> pd.DataFrame:
        raise ValueError("bad input from connector logic")

    with pytest.raises(ValueError, match="bad input from connector logic"):
        await _bad(_NoParams())

    # Confirm it's NOT a ParseError (ParseError is a ConnectorError, not a ValueError)
    try:
        await _bad(_NoParams())
    except ParseError:
        pytest.fail("connector ValueError must not be wrapped as ParseError")
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# Task 2 — HttpClient follows redirects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_client_follows_redirects() -> None:
    """HttpClient must follow a 302 redirect to the final URL."""
    from parsimony.transport.http import HttpClient

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

    client = HttpClient("http://example.com", _transport=transport)
    response = await client.request("GET", "/original")

    assert response.status_code == 200
    assert len(response.history) > 0
    assert str(response.url) == "http://example.com/final"


@pytest.mark.asyncio
async def test_http_client_follow_redirects_default_true() -> None:
    """HttpClient.follow_redirects defaults to True."""
    from parsimony.transport.http import HttpClient

    c = HttpClient("http://example.com")
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


# ---------------------------------------------------------------------------
# Task 1 happy paths — coercion success pipeline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timestamp_epoch_integers_produce_datetime_column() -> None:
    """Connector returning unix epoch integers with dtype='timestamp' must produce a datetime column."""
    df = pd.DataFrame({"ts": [0, 86400, 1_700_000_000]})
    conn = _make_connector(df, dtype="timestamp", col_name="ts")

    result = await conn(_NoParams())

    assert isinstance(result, Result)
    assert pd.api.types.is_datetime64_any_dtype(result.df["ts"])
    assert result.df["ts"].notna().all()


@pytest.mark.asyncio
async def test_numeric_strings_produce_float_column() -> None:
    """Connector returning numeric strings with dtype='numeric' must produce a float column."""
    df = pd.DataFrame({"price": ["1.5", "3.14", "0.0"]})
    conn = _make_connector(df, dtype="numeric", col_name="price")

    result = await conn(_NoParams())

    assert isinstance(result, Result)
    assert pd.api.types.is_float_dtype(result.df["price"])
    assert result.df["price"].notna().all()
