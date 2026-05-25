"""Shared HTTP helpers for connector packages."""

from __future__ import annotations

from typing import Any

import httpx

from parsimony.transport import HttpClient, map_http_error, map_timeout_error


async def fetch_json(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    provider: str,
    op_name: str,
) -> Any:
    """GET *path*, map httpx errors to kernel types, return parsed JSON."""
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = await http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=provider, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=provider, op_name=op_name)
    return response.json()


def make_http_client(
    base_url: str,
    *,
    query_params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> HttpClient:
    """Construct a configured :class:`HttpClient` for a provider base URL."""
    return HttpClient(
        base_url,
        query_params=query_params or {},
        headers=headers or {},
        timeout=timeout,
    )


def make_api_key_client(
    base_url: str,
    *,
    api_key: str,
    api_key_param: str = "apikey",
    timeout: float = 15.0,
) -> HttpClient:
    """Construct an :class:`HttpClient` with a default API-key query parameter."""
    return make_http_client(
        base_url,
        query_params={api_key_param: api_key},
        timeout=timeout,
    )
