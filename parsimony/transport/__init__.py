"""Transport utilities for connector packages.

Each section below covers one transport layer.  New sections can be added here
as the kernel adds support for additional protocols.

.. rubric:: HTTP

* :func:`redact_url` — mask sensitive query-param values before logging or
  embedding a URL in an exception message.
* :func:`parse_retry_after` — extract retry-after seconds from a 429 response.
* :func:`check_status` — raise a typed :mod:`parsimony.errors` exception for a
  non-2xx response, decided from the status code (never from a raised
  ``httpx.HTTPStatusError``). Transport-agnostic: duck-types on
  ``status_code``/``headers``.
* :func:`pooled_client` — sync context manager that yields an
  :class:`HttpClient` backed by a single pooled ``httpx.Client``, for
  enumerator loops and fan-out fetches.
* :class:`HttpClient` — sync HTTP client with base URL, default
  headers/query params, and redacted logging.
"""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
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
        "registrationkey",
    }
)

_REDACTED_VALUE = "***"

_DEFAULT_RATE_LIMIT_RETRY_AFTER: float = 60.0
_URL_RE = re.compile(r"https?://[^\s'\"<>]+")


def redact_params_for_logging(params: dict[str, Any]) -> dict[str, Any]:
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


def _redact_httpx_error(exc: httpx.HTTPError) -> None:
    """Strip query-string credentials from *exc* in place before it is chained.

    An ``httpx`` error becomes the ``__cause__``/``__context__`` of the typed
    error we raise, so its secrets reach every traceback, ``logging.exception``,
    and ``.request.url`` access. ``raise_for_status`` embeds the full request
    URL in the message; transport errors carry it on ``.request.url`` (which
    ``.response.url`` aliases). Redacting the message and that one URL object
    closes both vectors while keeping the chained error intact for debugging.
    """
    exc.args = tuple(redact_sensitive_text(a) if isinstance(a, str) else a for a in exc.args)
    try:
        request = exc.request
    except RuntimeError:
        # httpx exposes .request as a property that raises when no request was
        # attached (e.g. a bare timeout constructed without one). Nothing to scrub.
        return
    request.url = httpx.URL(redact_url(str(request.url)))


def _raise_transport_error(exc: httpx.HTTPError, *, provider: str, op_name: str) -> NoReturn:
    """Translate a transport-level ``httpx`` failure into a typed connector error.

    A *transport* failure is one that never produced a response: a timeout, a
    connection refused, a DNS failure, a read/write error, or a protocol error.
    :class:`~parsimony.transport.HttpClient.request` calls this from inside its
    retry loop so no raw ``httpx`` exception ever escapes the transport — the
    only key-bearing ``httpx`` object left in the system is scrubbed here before
    it becomes a chained ``__cause__``.

    * a :class:`httpx.TimeoutException` → :class:`~parsimony.errors.ProviderError`
      with ``status_code=408`` (the HTTP semantic for "request timeout");
    * anything else → :class:`~parsimony.errors.ProviderError` with
      ``status_code=503`` ("the provider could not be reached" — transient,
      treat like a 5xx). Only the exception *type name* is embedded.

    The original exception is chained via ``raise ... from exc`` for traceback
    visibility, after :func:`_redact_httpx_error` strips any credential from it.
    """
    _redact_httpx_error(exc)
    if isinstance(exc, httpx.TimeoutException):
        raise ProviderError(
            provider=provider,
            status_code=408,
            message=f"{provider} request timed out on endpoint '{op_name}'",
        ) from exc
    raise ProviderError(
        provider=provider,
        status_code=503,
        message=f"{provider} could not be reached on endpoint '{op_name}' ({type(exc).__name__})",
    ) from exc


def check_status(response: Any, *, provider: str, op_name: str, env_var: str | None = None) -> None:
    """Raise a typed connector error for a non-2xx *response*, decided from its status.

    This maps **from the status code**, never from a raised
    ``httpx.HTTPStatusError`` — so nothing here constructs an exception that
    embeds the request URL, and there is no chained cause to leak the
    query-string credential. A 2xx passes through (returns ``None``).

    Mapping (matches the kernel's :mod:`parsimony.errors` hierarchy):

    * 401, 403 → :class:`~parsimony.errors.UnauthorizedError`
    * 402      → :class:`~parsimony.errors.PaymentRequiredError`
    * 429      → :class:`~parsimony.errors.RateLimitError` with ``retry_after``
      from :func:`parse_retry_after`
    * other non-2xx → :class:`~parsimony.errors.ProviderError` carrying the status

    A provider whose statuses don't fit that table (e.g. a 403 that can mean
    either a plan restriction or a rolling quota) handles the special case with
    an ordinary ``if`` on the response *before* calling this, then defers every
    other non-2xx here.

    *response* is duck-typed: only ``.status_code`` and ``.headers`` (via
    :func:`parse_retry_after`) are read, so a ``curl_cffi`` response works as
    well as an ``httpx`` one.
    """
    status = response.status_code
    if 200 <= status < 300:
        return
    if status in (401, 403):
        raise UnauthorizedError(provider=provider, env_var=env_var)
    if status == 402:
        raise PaymentRequiredError(
            provider=provider,
            message=f"Your {provider} plan is not eligible for this data request",
        )
    if status == 429:
        raise RateLimitError(
            provider=provider,
            retry_after=parse_retry_after(response),
            message=f"{provider} rate limit reached on endpoint '{op_name}'",
        )
    raise ProviderError(
        provider=provider,
        status_code=status,
        message=f"{provider} API error {status} on endpoint '{op_name}'",
    )


@dataclass(frozen=True)
class HttpRetryPolicy:
    """Transient retry policy for :class:`HttpClient`."""

    max_attempts: int = 3
    base_delay_s: float = 0.25
    max_delay_s: float = 8.0
    jitter_s: float = 0.1
    retryable_methods: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
    retryable_statuses: frozenset[int] = frozenset({500, 502, 503, 504})

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
    """Sync HTTP client with base URL, default headers/query params, and redacted logging.

    By default each request creates a short-lived ``httpx.Client``.  Pass
    ``shared_client=`` to reuse a single client for connection pooling within
    one logical operation (e.g. enumerator fan-out).
    """

    def __init__(
        self,
        base_url: str,
        *,
        provider: str,
        timeout: float = 30.0,
        verify_ssl: bool = True,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        follow_redirects: bool = True,
        max_redirects: int = 5,
        _transport: httpx.BaseTransport | None = None,
        shared_client: httpx.Client | None = None,
        retry_policy: HttpRetryPolicy | None = DEFAULT_HTTP_RETRY_POLICY,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._provider = provider
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

    @property
    def provider(self) -> str:
        """The provider slug used to tag typed errors raised for this client."""
        return self._provider

    def with_shared_client(self, client: httpx.Client) -> HttpClient:
        """Return a new HttpClient that reuses *client* for connection pooling."""
        return HttpClient(
            self._base_url,
            provider=self._provider,
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

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        *,
        op_name: str,
    ) -> httpx.Response:
        url = f"{self._base_url}/{path.lstrip('/')}"
        request_params = {**self._default_query_params, **(params or {})}
        request_headers = {**self._default_headers, **(headers or {})}

        # Debug, not info: this fires twice per HTTP call, and a paginated fetch
        # makes hundreds of calls. At INFO it buries the events a caller actually
        # turned logging on to see — a catalog download, a model load — under
        # per-request chatter. That is the same failure mode as the progress bar
        # this package suppresses elsewhere, one level up. Per-request tracing is
        # a debugging tool, so it lives at the level named for debugging, which is
        # also where httpx, urllib3 and requests put theirs.
        logger.debug(
            "%s %s",
            method,
            path,
            extra={
                "http_method": method,
                "http_url": redact_url(url),
                "http_path": path,
                "http_params": redact_params_for_logging(request_params),
            },
        )

        method_upper = method.upper()
        policy = self._retry_policy
        max_attempts = policy.max_attempts if policy and policy.should_retry_method(method_upper) else 1

        for attempt in range(1, max_attempts + 1):
            try:
                response = self._request_once(
                    method=method,
                    url=url,
                    params=request_params,
                    json=json,
                    headers=request_headers,
                )
            except Exception as exc:
                if not self._is_retryable_exception(exc, policy=policy) or attempt >= max_attempts:
                    if isinstance(exc, httpx.HTTPError):
                        _raise_transport_error(exc, provider=self._provider, op_name=op_name)
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
                time.sleep(delay)
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
                time.sleep(delay)
                continue
            break

        if response.history:
            final_url = _safe_redirect_url(response.url)
            logger.debug(
                "Followed %d redirect(s) to %s",
                len(response.history),
                final_url,
                extra={
                    "http_redirect_hops": len(response.history),
                    "http_redirect_target": final_url,
                },
            )

        logger.debug(
            "Response %s",
            response.status_code,
            extra={
                "http_method": method,
                "http_url": redact_url(url),
                "http_status_code": response.status_code,
                "http_response_size": len(response.content) if response.content else 0,
            },
        )

        # Scrub the request URL carried on the returned response so no
        # key-bearing httpx object escapes the transport — even a caller that
        # (against guidance) calls ``response.raise_for_status()`` itself gets a
        # redacted URL in the resulting HTTPStatusError.
        with suppress(RuntimeError):
            response.request.url = httpx.URL(redact_url(str(response.request.url)))
        return response

    def _request_once(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any],
        json: dict[str, Any] | None,
        headers: dict[str, Any],
    ) -> httpx.Response:
        if self._shared_client is not None:
            return self._shared_client.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=headers,
            )
        with httpx.Client(**self._client_kwargs()) as client:
            return client.request(
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


@contextmanager
def pooled_client(http: HttpClient) -> Iterator[HttpClient]:
    """Yield an :class:`HttpClient` backed by a single pooled ``httpx.Client``.

    Use when a single logical operation issues many requests (enumerator
    loops, screener fan-out) and TCP/TLS state should be reused across them.
    The returned client inherits the configured base URL, default headers,
    default query params, timeout, TLS settings, and transport of *http*.

    Example::

        with pooled_client(http) as shared:
            for key in keys:
                response = shared.request("GET", f"/data/{key}")
    """
    with httpx.Client(**http._client_kwargs()) as shared:
        yield http.with_shared_client(shared)


__all__ = [
    "DEFAULT_HTTP_RETRY_POLICY",
    "HttpClient",
    "HttpRetryPolicy",
    "check_status",
    "parse_retry_after",
    "pooled_client",
    "redact_params_for_logging",
    "redact_sensitive_text",
    "redact_url",
]
