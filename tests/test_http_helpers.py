"""Tests for the public connector-facing helpers in :mod:`parsimony.transport`."""

from __future__ import annotations

import time
import traceback
from contextlib import suppress

import httpx
import pytest

from parsimony.errors import (
    ConnectorError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import (
    HttpClient,
    HttpRetryPolicy,
    check_status,
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
# check_status — typed errors from the status code (never from an exception)
# ---------------------------------------------------------------------------


def _status_response(status: int, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, headers=headers or {}, request=httpx.Request("GET", "https://x.test"))


@pytest.mark.parametrize("status", [200, 201, 204, 299])
def test_check_status_2xx_passes_through(status: int) -> None:
    assert check_status(_status_response(status), provider="example", op_name="op") is None


@pytest.mark.parametrize("status", [401, 403])
def test_check_status_401_403_unauthorized(status: int) -> None:
    with pytest.raises(UnauthorizedError) as excinfo:
        check_status(_status_response(status), provider="example", op_name="op")
    assert excinfo.value.provider == "example"
    # Raised fresh from the status — no chained httpx exception.
    assert excinfo.value.__cause__ is None


def test_check_status_401_names_env_var_when_given() -> None:
    with pytest.raises(UnauthorizedError) as excinfo:
        check_status(_status_response(401), provider="example", op_name="op", env_var="EXAMPLE_API_KEY")
    assert "EXAMPLE_API_KEY" in str(excinfo.value)


def test_check_status_402_payment_required() -> None:
    with pytest.raises(PaymentRequiredError) as excinfo:
        check_status(_status_response(402), provider="example", op_name="op")
    assert excinfo.value.provider == "example"


def test_check_status_429_rate_limit_with_retry_after() -> None:
    with pytest.raises(RateLimitError) as excinfo:
        check_status(_status_response(429, headers={"Retry-After": "30"}), provider="example", op_name="op")
    assert excinfo.value.retry_after == 30.0
    assert excinfo.value.provider == "example"


def test_check_status_429_uses_default_retry_after_when_header_missing() -> None:
    with pytest.raises(RateLimitError) as excinfo:
        check_status(_status_response(429), provider="example", op_name="op")
    assert excinfo.value.retry_after == 60.0


@pytest.mark.parametrize("status", [400, 404, 500, 502, 503])
def test_check_status_other_provider_error(status: int) -> None:
    with pytest.raises(ProviderError) as excinfo:
        check_status(_status_response(status), provider="example", op_name="op")
    assert excinfo.value.status_code == status
    assert excinfo.value.provider == "example"
    assert str(status) in str(excinfo.value)


def test_check_status_op_name_in_message() -> None:
    with pytest.raises(RateLimitError) as excinfo:
        check_status(_status_response(429, headers={"Retry-After": "5"}), provider="example", op_name="my_op")
    assert "my_op" in str(excinfo.value)


class _DuckResponse:
    """A minimal non-httpx response — proves ``check_status`` is transport-agnostic."""

    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}


@pytest.mark.parametrize(
    ("status", "expected"),
    [(401, UnauthorizedError), (402, PaymentRequiredError), (429, RateLimitError), (500, ProviderError)],
)
def test_check_status_duck_typed_response(status: int, expected: type[ConnectorError]) -> None:
    resp = _DuckResponse(status, headers={"Retry-After": "9"} if status == 429 else None)
    with pytest.raises(expected):
        check_status(resp, provider="curlish", op_name="op")


# ---------------------------------------------------------------------------
# credential leakage — the transport never hands back a key-bearing httpx object
#
# check_status maps from the status, so its errors have no chained httpx cause
# to leak. The only key-bearing httpx object left is a transport-failure
# exception, which HttpClient.request maps and scrubs internally, and the
# request URL carried on any returned response is scrubbed too — so even a
# caller that calls raise_for_status() itself cannot leak the key.
# ---------------------------------------------------------------------------

_LIVE_KEY = "LIVE-KEY-MUST-NOT-LEAK-42"


def _all_chain_surfaces(exc: BaseException) -> str:
    """Every string a caller could reach from *exc* and its cause/context chain.

    Covers the two vectors F1 identifies: ``str(link)`` (what a traceback or
    ``logging.exception`` prints) and ``str(link.request.url)`` (explicit
    attribute access), for each link in the chain.
    """
    seen: set[int] = set()
    parts: list[str] = []
    node: BaseException | None = exc
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        parts.append(str(node))
        request = getattr(node, "request", None)
        if request is not None:
            with suppress(RuntimeError):
                parts.append(str(request.url))
        node = node.__cause__ or node.__context__
    return "\n".join(parts)


def _keyed_client(handler: object, **policy_kwargs: object) -> HttpClient:
    """A client whose default query params carry a live-looking key."""
    return HttpClient(
        "https://api.example.com",
        provider="example",
        query_params={"api_token": _LIVE_KEY},
        _transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
        retry_policy=HttpRetryPolicy(max_attempts=1, base_delay_s=0.0, jitter_s=0.0, **policy_kwargs),  # type: ignore[arg-type]
    )


def test_request_transport_failure_maps_to_provider_error_503_scrubbed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    http = _keyed_client(handler)
    with pytest.raises(ProviderError) as excinfo:
        http.request("GET", "/data", op_name="fetch")
    assert excinfo.value.status_code == 503
    assert _LIVE_KEY not in _all_chain_surfaces(excinfo.value)


def test_request_timeout_maps_to_provider_error_408_scrubbed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    http = _keyed_client(handler)
    with pytest.raises(ProviderError) as excinfo:
        http.request("GET", "/data", op_name="fetch")
    assert excinfo.value.status_code == 408
    assert _LIVE_KEY not in _all_chain_surfaces(excinfo.value)


def test_request_transport_failure_traceback_is_secret_free() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    http = _keyed_client(handler)
    tb = ""
    try:
        http.request("GET", "/data", op_name="fetch")
    except ProviderError as typed:
        tb = "".join(traceback.format_exception(type(typed), typed, typed.__traceback__))
    assert tb and _LIVE_KEY not in tb


def test_request_returned_response_has_scrubbed_request_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True}, request=request)

    http = _keyed_client(handler)
    response = http.request("GET", "/data", op_name="fetch")
    assert _LIVE_KEY not in str(response.request.url)


def test_request_returned_response_raise_for_status_does_not_leak() -> None:
    """Even a caller that (against guidance) calls raise_for_status() cannot leak."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    http = _keyed_client(handler)
    response = http.request("GET", "/missing", op_name="fetch")
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        response.raise_for_status()
    assert _LIVE_KEY not in _all_chain_surfaces(excinfo.value)


# ---------------------------------------------------------------------------
# pooled_client
# ---------------------------------------------------------------------------


def test_pooled_client_yields_client_reusing_single_sync_client(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[httpx.Client] = []
    original_init = httpx.Client.__init__

    def tracking_init(self: httpx.Client, *args: object, **kwargs: object) -> None:
        original_init(self, *args, **kwargs)
        created.append(self)

    monkeypatch.setattr(httpx.Client, "__init__", tracking_init)

    http = HttpClient(
        "https://api.example.com",
        provider="example",
        timeout=5.0,
        headers={"X-Test": "1"},
        query_params={"apikey": "secret"},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    pooled = HttpClient(
        "https://api.example.com",
        provider="example",
        timeout=5.0,
        headers={"X-Test": "1"},
        query_params={"apikey": "secret"},
        _transport=transport,
    )

    with pooled_client(pooled) as shared:
        r1 = shared.request("GET", "/a", op_name="probe")
        r2 = shared.request("GET", "/b", op_name="probe")

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert len(created) == 1
    assert shared.base_url == pooled.base_url
    assert shared.provider == "example"
    assert http.base_url == "https://api.example.com"


def test_http_client_retries_transient_status_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    http = HttpClient(
        "https://api.example.com",
        provider="example",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=2, base_delay_s=0.0, jitter_s=0.0),
    )
    response = http.request("GET", "/status", op_name="op")
    assert response.status_code == 200
    assert calls["n"] == 2


def test_http_client_does_not_retry_terminal_4xx() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, request=request)

    http = HttpClient(
        "https://api.example.com",
        provider="example",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=3, base_delay_s=0.0, jitter_s=0.0),
    )
    response = http.request("GET", "/missing", op_name="op")
    assert response.status_code == 404
    assert calls["n"] == 1


def test_http_client_retries_transient_exception_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connect failed", request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    http = HttpClient(
        "https://api.example.com",
        provider="example",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=2, base_delay_s=0.0, jitter_s=0.0),
    )
    response = http.request("GET", "/connect", op_name="op")
    assert response.status_code == 200
    assert calls["n"] == 2


def test_http_client_does_not_retry_429() -> None:
    """429 surfaces immediately so callers can raise RateLimitError without hidden retries."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "7"}, request=request)

    http = HttpClient(
        "https://api.example.com",
        provider="example",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=3, base_delay_s=0.0, jitter_s=0.0, max_delay_s=10.0),
    )
    response = http.request("GET", "/rate-limited", op_name="op")
    assert response.status_code == 429
    assert calls["n"] == 1


def test_http_client_exhausted_retries_returns_response_for_check_status() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, request=request)

    http = HttpClient(
        "https://api.example.com",
        provider="example",
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=3, base_delay_s=0.0, jitter_s=0.0),
    )
    response = http.request("GET", "/still-failing", op_name="still-failing")
    assert calls["n"] == 3
    assert response.status_code == 503
    with pytest.raises(ProviderError) as mapped:
        check_status(response, provider="example", op_name="still-failing")
    assert mapped.value.status_code == 503
