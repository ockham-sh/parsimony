"""Tests for parsimony.transport.helpers (fetch_json/text/csv + client factories).

The transport-layer primitives (redaction, status mapping, HttpClient retries)
are covered in test_http_helpers.py. This file covers the thin connector-facing
helpers module, which the README documents as the recommended way to build an
HTTP connector.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from parsimony.errors import (
    EmptyDataError,
    ParseError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import HttpClient, HttpRetryPolicy
from parsimony.transport.helpers import (
    fetch_csv,
    fetch_json,
    fetch_text,
    make_api_key_client,
    make_http_client,
    require_key,
)

Handler = Callable[[httpx.Request], httpx.Response]


def _http(handler: Handler, *, provider: str = "acme", retry: bool = False, **client_kw: object) -> HttpClient:
    """Build an HttpClient wired to a mock transport (no retries by default)."""
    return HttpClient(
        "https://api.example.com",
        provider=provider,
        _transport=httpx.MockTransport(handler),
        retry_policy=HttpRetryPolicy(max_attempts=3, base_delay_s=0.0, jitter_s=0.0) if retry else None,
        **client_kw,  # type: ignore[arg-type]
    )


def test_require_key_returns_arg() -> None:
    assert require_key("sekret", env_var="ACME_KEY", provider="acme") == "sekret"


def test_require_key_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACME_KEY", "from-env")
    assert require_key("", env_var="ACME_KEY", provider="acme") == "from-env"


def test_require_key_strips_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACME_KEY", "  padded  ")
    assert require_key("", env_var="ACME_KEY", provider="acme") == "padded"
    assert require_key("  arg  ", env_var="ACME_KEY", provider="acme") == "arg"


def test_require_key_blank_and_whitespace_count_as_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACME_KEY", "   ")
    with pytest.raises(UnauthorizedError) as exc:
        require_key("", env_var="ACME_KEY", provider="acme")
    assert exc.value.env_var == "ACME_KEY"
    with pytest.raises(UnauthorizedError):
        require_key("  \t  ", env_var="ACME_KEY", provider="acme")


def test_make_http_client_normalises_base_url() -> None:
    client = make_http_client(
        "https://api.example.com/",
        provider="acme",
        query_params={"format": "json"},
        headers={"X-Test": "1"},
        timeout=9.0,
    )
    assert client.base_url == "https://api.example.com"
    assert client.provider == "acme"


def test_make_api_key_client_sends_key_as_default_query_param() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["apikey"] = request.url.params.get("apikey", "")
        return httpx.Response(200, json={"ok": True}, request=request)

    client = make_api_key_client("https://api.example.com", provider="acme", api_key="secret-key")
    with httpx.Client(transport=httpx.MockTransport(handler)) as sync_client:
        shared = client.with_shared_client(sync_client)
        resp = shared.request("GET", "/series", op_name="series")
    assert resp.status_code == 200
    assert captured["apikey"] == "secret-key"


def test_fetch_json_happy_path_returns_parsed_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"observations": [1, 2, 3]}, request=request)

    body = fetch_json(_http(handler), path="series/GDP", op_name="series")
    assert body == {"observations": [1, 2, 3]}


def test_fetch_json_filters_none_params() -> None:
    seen: dict[str, list[str]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["keys"] = sorted(request.url.params.keys())
        return httpx.Response(200, json={"ok": True}, request=request)

    fetch_json(
        _http(handler),
        path="data",
        params={"a": "1", "b": None, "c": "3"},
        op_name="data",
    )
    assert seen["keys"] == ["a", "c"]


def test_fetch_json_non_json_body_raises_parse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>upstream error page</html>", request=request)

    with pytest.raises(ParseError) as exc:
        fetch_json(_http(handler), path="series", op_name="series")
    # The undecodable body must not leak into the message.
    assert "upstream error page" not in str(exc.value)
    assert exc.value.provider == "acme"


def test_fetch_json_401_maps_to_unauthorized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad key"}, request=request)

    with pytest.raises(UnauthorizedError):
        fetch_json(_http(handler), path="series", op_name="series")


def test_fetch_json_429_maps_to_rate_limit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "30"}, request=request)

    with pytest.raises(RateLimitError) as exc:
        fetch_json(_http(handler), path="series", op_name="series")
    assert exc.value.retry_after == 30.0


def test_fetch_json_500_maps_to_provider_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, request=request)

    with pytest.raises(ProviderError) as exc:
        fetch_json(_http(handler), path="series", op_name="series")
    assert exc.value.status_code == 500


def test_fetch_text_returns_body_and_maps_errors() -> None:
    def ok(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="raw,body\n1,2", request=request)

    assert fetch_text(_http(ok), path="x", op_name="x") == "raw,body\n1,2"

    def unauthorized(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, request=request)

    with pytest.raises(UnauthorizedError):
        fetch_text(_http(unauthorized), path="x", op_name="x")


def test_fetch_csv_parses_dataframe() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="date,value\n2020-01-01,1.5\n2020-02-01,2.5", request=request)

    df = fetch_csv(_http(handler), path="series", op_name="series")
    assert list(df.columns) == ["date", "value"]
    assert df["value"].tolist() == [1.5, 2.5]


def test_fetch_csv_passes_read_csv_kwargs() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="a;b\n1;2", request=request)

    df = fetch_csv(_http(handler), path="x", op_name="x", sep=";")
    assert list(df.columns) == ["a", "b"]


def test_fetch_csv_empty_body_raises_empty_data_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="", request=request)

    with pytest.raises(EmptyDataError):
        fetch_csv(_http(handler), path="x", op_name="x")


def test_fetch_csv_maps_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "12"}, request=request)

    with pytest.raises(RateLimitError) as exc:
        fetch_csv(_http(handler), path="x", op_name="x")
    assert exc.value.retry_after == 12.0


# A non-timeout transport failure (connection refused, DNS failure, protocol
# error) must surface as a typed ProviderError, never a raw httpx exception —
# the shared _get path covers all three helpers, so each is checked.


def _connect_error_handler(message: str = "connection refused") -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(message, request=request)

    return handler


def test_fetch_json_transport_error_maps_to_provider_error() -> None:
    with pytest.raises(ProviderError) as exc:
        fetch_json(_http(_connect_error_handler()), path="series", op_name="series")
    assert exc.value.status_code == 503
    assert exc.value.provider == "acme"
    # The transport error string must not leak into the message.
    assert "connection refused" not in str(exc.value)


def test_fetch_text_transport_error_maps_to_provider_error() -> None:
    with pytest.raises(ProviderError) as exc:
        fetch_text(_http(_connect_error_handler()), path="x", op_name="x")
    assert exc.value.status_code == 503


def test_fetch_csv_transport_error_maps_to_provider_error() -> None:
    with pytest.raises(ProviderError) as exc:
        fetch_csv(_http(_connect_error_handler()), path="x", op_name="x")
    assert exc.value.status_code == 503


def test_fetch_json_protocol_error_maps_to_provider_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError("server disconnected", request=request)

    with pytest.raises(ProviderError) as exc:
        fetch_json(_http(handler), path="series", op_name="series")
    assert exc.value.status_code == 503
