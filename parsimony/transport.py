"""Transport utilities for connector packages.

Each section below covers one transport layer.  New sections can be added here
as the kernel adds support for additional protocols.

.. rubric:: HTTP

* :func:`redact_url` — mask sensitive query-param values before logging or
  embedding a URL in an exception message.
* :func:`parse_retry_after` — extract retry-after seconds from a 429 response.
* :func:`map_http_error` — translate ``httpx.HTTPStatusError`` into a typed
  :mod:`parsimony.errors` exception.
* :func:`map_timeout_error` — translate ``httpx.TimeoutException`` into a
  typed :class:`~parsimony.errors.ProviderError` (status 408).
* :func:`pooled_client` — async context manager that yields an
  :class:`HttpClient` backed by a single pooled ``httpx.AsyncClient``, for
  enumerator loops and fan-out fetches.
* :class:`HttpClient` — async HTTP client with base URL, default
  headers/query params, and redacted logging.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, NoReturn
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)

logger = logging.getLogger(__name__)

# ── HTTP ──────────────────────────────────────────────────────────────────────

_SENSITIVE_QUERY_PARAM_NAMES: frozenset[str] = frozenset(
    {
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
    }
)

_REDACTED_VALUE = "***"

_DEFAULT_RATE_LIMIT_RETRY_AFTER: float = 60.0


def _redact_params_for_logging(params: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy safe to emit in structured logs (secrets stripped)."""
    redacted: dict[str, Any] = {}
    for name, value in params.items():
        normalized = str(name).lower().replace("-", "_")
        if normalized in _SENSITIVE_QUERY_PARAM_NAMES or normalized.endswith("_token"):
            redacted[name] = "***REDACTED***"
        else:
            redacted[name] = value
    return redacted


def _safe_redirect_url(url: httpx.URL) -> str:
    """Return ``scheme://host/path`` with all query params stripped."""
    return f"{url.scheme}://{url.host}{url.path}"


def redact_url(url: str) -> str:
    """Return *url* with sensitive query-param values masked.

    Use before logging a request URL or embedding one in an exception message.
    Sensitive parameter names are matched against
    :data:`_SENSITIVE_QUERY_PARAM_NAMES` (case-insensitive, hyphen→underscore
    normalised). Non-sensitive params are preserved as-is. URLs without a
    query string are returned unchanged.
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    redacted = [
        (k, _REDACTED_VALUE if str(k).lower().replace("-", "_") in _SENSITIVE_QUERY_PARAM_NAMES else v)
        for k, v in pairs
    ]
    return urlunsplit(parts._replace(query=urlencode(redacted)))


def parse_retry_after(response: httpx.Response, *, default: float = _DEFAULT_RATE_LIMIT_RETRY_AFTER) -> float:
    """Extract retry-after seconds from a 429 response.

    Order of attempts:

    1. ``Retry-After`` header parsed as a numeric (seconds).
    2. ``X-Ratelimit-Reset`` header parsed as a Unix epoch timestamp; the
       returned value is ``max(1.0, reset - now)``.
    3. ``default``.

    Result is clamped to ``(0, 86400]`` (the kernel's
    :class:`~parsimony.errors.RateLimitError` rejects values larger than
    24 hours as likely-mis-encoded epochs).
    """
    header = response.headers.get("Retry-After", "").strip()
    if header:
        try:
            value = float(header)
            if 0 < value <= 86_400:
                return value
        except ValueError:
            pass
    epoch_header = response.headers.get("X-Ratelimit-Reset", "").strip()
    if epoch_header:
        try:
            reset = float(epoch_header)
            value = max(1.0, reset - time.time())
            if 0 < value <= 86_400:
                return value
        except ValueError:
            pass
    return default


def map_http_error(exc: httpx.HTTPStatusError, *, provider: str, op_name: str) -> NoReturn:
    """Translate an :class:`httpx.HTTPStatusError` into a typed connector error.

    Mapping (matches the kernel's :mod:`parsimony.errors` hierarchy):

    * 401, 403 → :class:`~parsimony.errors.UnauthorizedError`
    * 402      → :class:`~parsimony.errors.PaymentRequiredError`
    * 429      → :class:`~parsimony.errors.RateLimitError` with
      ``retry_after`` from :func:`parse_retry_after`
    * else     → :class:`~parsimony.errors.ProviderError` carrying the status

    The original exception is chained via ``raise ... from exc`` so the
    traceback retains it. Messages do not embed the request URL — callers
    that want a URL in the message must redact via :func:`redact_url` first.
    """
    status = exc.response.status_code
    if status in (401, 403):
        raise UnauthorizedError(
            provider=provider,
            message=f"Invalid or missing {provider} API credentials",
        ) from exc
    if status == 402:
        raise PaymentRequiredError(
            provider=provider,
            message=f"Your {provider} plan is not eligible for this data request",
        ) from exc
    if status == 429:
        raise RateLimitError(
            provider=provider,
            retry_after=parse_retry_after(exc.response),
            message=f"{provider} rate limit reached on endpoint '{op_name}'",
        ) from exc
    raise ProviderError(
        provider=provider,
        status_code=status,
        message=f"{provider} API error {status} on endpoint '{op_name}'",
    ) from exc


def map_timeout_error(exc: httpx.TimeoutException, *, provider: str, op_name: str) -> NoReturn:
    """Translate an :class:`httpx.TimeoutException` into a typed connector error.

    Raises :class:`~parsimony.errors.ProviderError` with ``status_code=408``
    (the HTTP semantic for "request timeout") so downstream callers can treat
    it uniformly with other transport failures. The original exception is
    chained via ``raise ... from exc`` for traceback visibility.
    """
    raise ProviderError(
        provider=provider,
        status_code=408,
        message=f"{provider} request timed out on endpoint '{op_name}'",
    ) from exc


class HttpClient:
    """Async HTTP client with base URL, default headers/query params, and redacted logging.

    By default each request creates a short-lived ``httpx.AsyncClient`` so that
    TCP connections are never shared across different ``asyncio.run()`` event
    loops.  Pass ``shared_client=`` to reuse a single client for connection
    pooling within one async call (e.g. screener fan-out).
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        verify_ssl: bool = True,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        follow_redirects: bool = True,
        max_redirects: int = 5,
        _transport: httpx.AsyncBaseTransport | None = None,
        shared_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._default_headers = dict(headers or {})
        self._default_query_params = dict(query_params or {})
        self._follow_redirects = follow_redirects
        self._max_redirects = max_redirects
        self._transport = _transport
        self._shared_client = shared_client

    @property
    def base_url(self) -> str:
        return self._base_url

    def with_shared_client(self, client: httpx.AsyncClient) -> HttpClient:
        """Return a new HttpClient that reuses *client* for connection pooling."""
        return HttpClient(
            self._base_url,
            timeout=self._timeout,
            verify_ssl=self._verify_ssl,
            headers=self._default_headers or None,
            query_params=self._default_query_params or None,
            follow_redirects=self._follow_redirects,
            max_redirects=self._max_redirects,
            _transport=self._transport,
            shared_client=client,
        )

    async def aclose(self) -> None:
        """No-op retained for backward compatibility."""

    def _client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "timeout": self._timeout,
            "headers": self._default_headers,
            "verify": self._verify_ssl,
            "follow_redirects": self._follow_redirects,
            "max_redirects": self._max_redirects,
        }
        if self._transport is not None:
            kwargs["transport"] = self._transport
        return kwargs

    async def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> httpx.Response:
        url = f"{self._base_url}/{path.lstrip('/')}"
        request_params = {**self._default_query_params, **(params or {})}
        request_headers = {**self._default_headers, **(headers or {})}

        logger.info(
            "%s %s",
            method,
            path,
            extra={
                "http_method": method,
                "http_url": url,
                "http_path": path,
                "http_params": _redact_params_for_logging(request_params),
            },
        )

        if self._shared_client is not None:
            response = await self._shared_client.request(
                method=method,
                url=url,
                params=request_params,
                json=json,
                headers=request_headers,
            )
        else:
            async with httpx.AsyncClient(**self._client_kwargs()) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    params=request_params,
                    json=json,
                    headers=request_headers,
                )

        if response.history:
            final_url = _safe_redirect_url(response.url)
            logger.info(
                "Followed %d redirect(s) to %s",
                len(response.history),
                final_url,
                extra={
                    "http_redirect_hops": len(response.history),
                    "http_redirect_target": final_url,
                },
            )

        logger.info(
            "Response %s",
            response.status_code,
            extra={
                "http_method": method,
                "http_url": url,
                "http_status_code": response.status_code,
                "http_response_size": len(response.content) if response.content else 0,
            },
        )
        return response


@asynccontextmanager
async def pooled_client(http: HttpClient) -> AsyncIterator[HttpClient]:
    """Yield an :class:`HttpClient` backed by a single pooled ``httpx.AsyncClient``.

    Use when a single logical operation issues many requests (enumerator
    loops, screener fan-out) and TCP/TLS state should be reused across them.
    The returned client inherits the configured base URL, default headers,
    default query params, timeout, TLS settings, and transport of *http*.

    Example::

        async with pooled_client(http) as shared:
            for key in keys:
                response = await shared.request("GET", f"/data/{key}")
    """
    async with httpx.AsyncClient(**http._client_kwargs()) as shared:
        yield http.with_shared_client(shared)


__all__ = [
    "HttpClient",
    "map_http_error",
    "map_timeout_error",
    "parse_retry_after",
    "pooled_client",
    "redact_url",
]
