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


class HttpClient:
    """
    Async HTTP client with base URL, default headers/query params, and redacted logging.

    Each request creates a short-lived ``httpx.AsyncClient`` so that TCP
    connections are never shared across different ``asyncio.run()`` event loops.
    This avoids ``RuntimeError: TCPTransport closed`` when successive synchronous
    entrypoints spin up isolated event loops.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        verify_ssl: bool = True,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._default_headers = dict(headers or {})
        self._default_query_params = dict(query_params or {})

    @property
    def base_url(self) -> str:
        return self._base_url

    async def aclose(self) -> None:
        """No-op retained for backward compatibility."""

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

        async with httpx.AsyncClient(
            timeout=self._timeout,
            headers=self._default_headers,
            verify=self._verify_ssl,
        ) as client:
            response = await client.request(
                method=method,
                url=url,
                params=request_params,
                json=json,
                headers=request_headers,
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
