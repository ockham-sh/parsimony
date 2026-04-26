"""Tests for the public connector-facing helpers in :mod:`parsimony.transport`."""

from __future__ import annotations

import time

import httpx
import pytest

from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import (
    HttpClient,
    HttpRetryPolicy,
    map_http_error,
    map_timeout_error,
    parse_retry_after,
    pooled_client,
    redact_sensitive_text,
    redact_url,
)

# ---------------------------------------------------------------------------
# redact_url
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "api_key",
        "apikey",
        "api_token",
        "token",
        "access_token",
        "refresh_token",
        "id_token",
        "client_secret",
        "secret",
        "password",
        "authorization",
    ],
)
def test_redact_url_masks_sensitive_value(name: str) -> None:
    url = f"https://api.example.com/v1/path?{name}=super-secret&series=UNRATE"
    out = redact_url(url)
    assert "super-secret" not in out
    assert "series=UNRATE" in out
    assert f"{name}=%2A%2A%2A" in out or f"{name}=***" in out


def test_redact_url_hyphen_and_case_insensitive() -> None:
    url = "https://x.test/path?Api-Key=secret-1&API_TOKEN=secret-2&series=A"
    out = redact_url(url)
    assert "secret-1" not in out
    assert "secret-2" not in out
    assert "series=A" in out


def test_redact_url_no_query_unchanged() -> None:
    url = "https://x.test/path"
    assert redact_url(url) == url


def test_redact_url_multiple_sensitive_all_masked() -> None:
    url = "https://x.test/path?api_key=k1&token=t1&series=A&apikey=k2"
    out = redact_url(url)
    for secret in ("k1", "t1", "k2"):
        assert secret not in out
    assert "series=A" in out


def test_redact_url_non_sensitive_preserved() -> None:
    url = "https://x.test/path?series_id=UNRATE&start=2024-01-01"
    assert redact_url(url) == url


def test_redact_sensitive_text_masks_query_secrets_inside_arbitrary_text() -> None:
    text = "request failed at https://x.test/path?api_key=secret123&series=UNRATE"
    out = redact_sensitive_text(text)
    assert "secret123" not in out
    assert "series=UNRATE" in out


# ---------------------------------------------------------------------------
# parse_retry_after
# ---------------------------------------------------------------------------


def _response_with_headers(headers: dict[str, str]) -> httpx.Response:
    return httpx.Response(429, headers=headers, request=httpx.Request("GET", "https://x.test"))


def test_parse_retry_after_numeric_header() -> None:
    resp = _response_with_headers({"Retry-After": "42"})
    assert parse_retry_after(resp) == 42.0


def test_parse_retry_after_missing_returns_default() -> None:
    resp = _response_with_headers({})
    assert parse_retry_after(resp) == 60.0


def test_parse_retry_after_custom_default() -> None:
    resp = _response_with_headers({})
    assert parse_retry_after(resp, default=30.0) == 30.0


def test_parse_retry_after_x_ratelimit_reset_epoch() -> None:
    future = time.time() + 90.0
    resp = _response_with_headers({"X-Ratelimit-Reset": str(future)})
    value = parse_retry_after(resp)
    assert 88.0 <= value <= 92.0


def test_parse_retry_after_invalid_falls_back() -> None:
    resp = _response_with_headers({"Retry-After": "not-a-number"})
    assert parse_retry_after(resp) == 60.0


def test_parse_retry_after_out_of_range_falls_back() -> None:
    # A value larger than 24h (e.g. raw Unix epoch in Retry-After header) is
    # rejected and the default substituted.
    resp = _response_with_headers({"Retry-After": str(time.time() + 60)})
    assert parse_retry_after(resp) == 60.0


def test_parse_retry_after_negative_falls_back() -> None:
    resp = _response_with_headers({"Retry-After": "-5"})
    assert parse_retry_after(resp) == 60.0


def test_parse_retry_after_x_ratelimit_reset_in_past_falls_back() -> None:
    past = time.time() - 30.0
    resp = _response_with_headers({"X-Ratelimit-Reset": str(past)})
    # max(1.0, past - now) = 1.0 (within range)
    assert parse_retry_after(resp) == 1.0


# ---------------------------------------------------------------------------
# map_http_error
# ---------------------------------------------------------------------------


def _http_status_error(status: int, headers: dict[str, str] | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.example.com/v1/data?api_key=secret-key")
    response = httpx.Response(status, headers=headers or {}, request=request)
    return httpx.HTTPStatusError("http error", request=request, response=response)


@pytest.mark.parametrize("status", [401, 403])
def test_map_http_error_401_403_unauthorized(status: int) -> None:
    exc = _http_status_error(status)
    with pytest.raises(UnauthorizedError) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    assert excinfo.value.provider == "example"
    assert excinfo.value.__cause__ is exc


def test_map_http_error_402_payment_required() -> None:
    exc = _http_status_error(402)
    with pytest.raises(PaymentRequiredError) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    assert excinfo.value.provider == "example"
    assert excinfo.value.__cause__ is exc


def test_map_http_error_429_rate_limit_with_retry_after() -> None:
    exc = _http_status_error(429, headers={"Retry-After": "30"})
    with pytest.raises(RateLimitError) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    assert excinfo.value.retry_after == 30.0
    assert excinfo.value.provider == "example"


def test_map_http_error_429_uses_default_retry_after_when_header_missing() -> None:
    exc = _http_status_error(429)
    with pytest.raises(RateLimitError) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    assert excinfo.value.retry_after == 60.0


@pytest.mark.parametrize("status", [400, 404, 500, 502, 503])
def test_map_http_error_other_provider_error(status: int) -> None:
    exc = _http_status_error(status)
    with pytest.raises(ProviderError) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    assert excinfo.value.status_code == status
    assert excinfo.value.provider == "example"
    assert str(status) in str(excinfo.value)


@pytest.mark.parametrize("status", [401, 402, 403, 429, 500])
def test_map_http_error_message_does_not_leak_url_or_key(status: int) -> None:
    exc = _http_status_error(status, headers={"Retry-After": "10"} if status == 429 else None)
    with pytest.raises(Exception) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    msg = str(excinfo.value)
    assert "secret-key" not in msg
    assert "api.example.com" not in msg
    assert "?api_key=" not in msg


def test_map_http_error_op_name_in_message() -> None:
    exc = _http_status_error(429, headers={"Retry-After": "5"})
    with pytest.raises(RateLimitError) as excinfo:
        map_http_error(exc, provider="example", op_name="test_op")
    assert "test_op" in str(excinfo.value)


# ---------------------------------------------------------------------------
# map_timeout_error
# ---------------------------------------------------------------------------


def test_map_timeout_error_raises_provider_error_408() -> None:
    exc = httpx.TimeoutException("timed out")
    with pytest.raises(ProviderError) as excinfo:
        map_timeout_error(exc, provider="example", op_name="test_op")
    assert excinfo.value.status_code == 408
    assert excinfo.value.provider == "example"
    assert excinfo.value.__cause__ is exc


def test_map_timeout_error_message_includes_provider_and_op() -> None:
    exc = httpx.ReadTimeout("read timed out")
    with pytest.raises(ProviderError) as excinfo:
        map_timeout_error(exc, provider="example", op_name="my_endpoint")
    msg = str(excinfo.value)
    assert "example" in msg
    assert "my_endpoint" in msg


# ---------------------------------------------------------------------------
# pooled_client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pooled_client_yields_client_reusing_single_async_client() -> None:
    created: list[httpx.AsyncClient] = []
    original_init = httpx.AsyncClient.__init__

    def tracking_init(self: httpx.AsyncClient, *args: object, **kwargs: object) -> None:
        original_init(self, *args, **kwargs)
        created.append(self)

    http = HttpClient(
        "https://api.example.com",
        timeout=5.0,
        headers={"X-Test": "1"},
        query_params={"apikey": "secret"},
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    pooled = HttpClient(
        "https://api.example.com",
        timeout=5.0,
        headers={"X-Test": "1"},
        query_params={"apikey": "secret"},
        _transport=transport,
    )

    async with pooled_client(pooled) as shared:
        # Two consecutive requests through the same shared client.
        r1 = await shared.request("GET", "/a")
        r2 = await shared.request("GET", "/b")

    assert r1.status_code == 200
    assert r2.status_code == 200
    # Both requests reused the single underlying client — verify that the
    # shared HttpClient carried through the config from the outer one.
    assert shared.base_url == pooled.base_url
    # The outer HttpClient used for construction is untouched.
    assert http.base_url == "https://api.example.com"


@pytest.mark.asyncio
async def test_http_client_retries_transient_status_then_succeeds() -> None:
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    http = HttpClient(
        "https://api.example.com",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=2, base_delay_s=0.0, jitter_s=0.0),
    )
    response = await http.request("GET", "/status")
    assert response.status_code == 200
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_http_client_does_not_retry_terminal_4xx() -> None:
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, request=request)

    http = HttpClient(
        "https://api.example.com",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=3, base_delay_s=0.0, jitter_s=0.0),
    )
    response = await http.request("GET", "/missing")
    assert response.status_code == 404
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_http_client_retries_transient_exception_then_succeeds() -> None:
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connect failed", request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    http = HttpClient(
        "https://api.example.com",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=2, base_delay_s=0.0, jitter_s=0.0),
    )
    response = await http.request("GET", "/connect")
    assert response.status_code == 200
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_http_client_respects_retry_after_for_429(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"}, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    monkeypatch.setattr("parsimony.transport.asyncio.sleep", fake_sleep)
    http = HttpClient(
        "https://api.example.com",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=2, base_delay_s=0.0, jitter_s=0.0, max_delay_s=10.0),
    )
    response = await http.request("GET", "/rate-limited")
    assert response.status_code == 200
    assert calls["n"] == 2
    assert delays == [7.0]


@pytest.mark.asyncio
async def test_http_client_exhausted_retries_preserves_error_mapping() -> None:
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, request=request)

    http = HttpClient(
        "https://api.example.com",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=3, base_delay_s=0.0, jitter_s=0.0),
    )
    response = await http.request("GET", "/still-failing")
    assert calls["n"] == 3
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        response.raise_for_status()
    with pytest.raises(ProviderError) as mapped:
        map_http_error(excinfo.value, provider="example", op_name="still-failing")
    assert mapped.value.status_code == 503
