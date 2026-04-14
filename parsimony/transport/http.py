"""Framework HTTP client: generic async REST helper with no product dependencies."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Query param names whose values must never appear in logs (lowercase, underscores).
_SENSITIVE_QUERY_PARAM_NAMES: frozenset[str] = frozenset(
    {
        "api_key",
        "apikey",
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


class HttpClient:
    """
    Async HTTP client with base URL, default headers/query params, and redacted logging.

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
