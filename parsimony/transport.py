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

import asyncio
import logging
import random
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
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
_URL_RE = re.compile(r"https?://[^\s'\"<>]+")


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


def redact_sensitive_text(text: str) -> str:
    """Redact URL query secrets from arbitrary text."""
    if not text:
        return text
    return _URL_RE.sub(lambda m: redact_url(m.group(0)), text)


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


@dataclass(frozen=True)
class HttpRetryPolicy:
    """Transient retry policy for :class:`HttpClient`."""

    max_attempts: int = 3
    base_delay_s: float = 0.25
    max_delay_s: float = 8.0
    jitter_s: float = 0.1
    retryable_methods: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
    retryable_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    def validate(self) -> HttpRetryPolicy:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.base_delay_s < 0:
            raise ValueError(f"base_delay_s must be >= 0, got {self.base_delay_s}")
        if self.max_delay_s <= 0:
            raise ValueError(f"max_delay_s must be > 0, got {self.max_delay_s}")
        if self.jitter_s < 0:
            raise ValueError(f"jitter_s must be >= 0, got {self.jitter_s}")
        return self

    def should_retry_method(self, method: str) -> bool:
        return method.upper() in self.retryable_methods

    def backoff_seconds(self, attempt: int, *, retry_after: float | None = None) -> float:
        if retry_after is not None:
            return float(min(max(retry_after, 0.0), self.max_delay_s))
        exp = self.base_delay_s * (2 ** max(0, attempt - 1))
        jitter = float(random.uniform(0.0, self.jitter_s)) if self.jitter_s > 0 else 0.0
        return float(min(exp + jitter, self.max_delay_s))


DEFAULT_HTTP_RETRY_POLICY = HttpRetryPolicy().validate()


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
        retry_policy: HttpRetryPolicy | None = DEFAULT_HTTP_RETRY_POLICY,
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
        self._retry_policy = retry_policy.validate() if retry_policy is not None else None

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
            retry_policy=self._retry_policy,
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

        method_upper = method.upper()
        policy = self._retry_policy
        max_attempts = policy.max_attempts if policy and policy.should_retry_method(method_upper) else 1

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._request_once(
                    method=method,
                    url=url,
                    params=request_params,
                    json=json,
                    headers=request_headers,
                )
            except Exception as exc:
                if not self._is_retryable_exception(exc, policy=policy) or attempt >= max_attempts:
                    raise
                assert policy is not None
                delay = policy.backoff_seconds(attempt)
                logger.warning(
                    "Transient HTTP exception (%s). Retrying in %.2fs (attempt %d/%d)",
                    type(exc).__name__,
                    delay,
                    attempt + 1,
                    max_attempts,
                )
                await asyncio.sleep(delay)
                continue

            if self._should_retry_response(response, policy=policy, method=method_upper) and attempt < max_attempts:
                assert policy is not None
                retry_after = parse_retry_after(response) if response.status_code == 429 else None
                delay = policy.backoff_seconds(attempt, retry_after=retry_after)
                logger.warning(
                    "Transient HTTP status %d. Retrying in %.2fs (attempt %d/%d)",
                    response.status_code,
                    delay,
                    attempt + 1,
                    max_attempts,
                )
                await asyncio.sleep(delay)
                continue
            break

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

    async def _request_once(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any],
        json: dict[str, Any] | None,
        headers: dict[str, Any],
    ) -> httpx.Response:
        if self._shared_client is not None:
            return await self._shared_client.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=headers,
            )
        async with httpx.AsyncClient(**self._client_kwargs()) as client:
            return await client.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=headers,
            )

    @staticmethod
    def _is_retryable_exception(exc: Exception, *, policy: HttpRetryPolicy | None) -> bool:
        if policy is None:
            return False
        return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError))

    @staticmethod
    def _should_retry_response(response: httpx.Response, *, policy: HttpRetryPolicy | None, method: str) -> bool:
        if policy is None or not policy.should_retry_method(method):
            return False
        return response.status_code in policy.retryable_statuses


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
    "DEFAULT_HTTP_RETRY_POLICY",
    "HttpClient",
    "HttpRetryPolicy",
    "map_http_error",
    "map_timeout_error",
    "parse_retry_after",
    "pooled_client",
    "redact_sensitive_text",
    "redact_url",
]
