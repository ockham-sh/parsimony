"""Shared HTTP helpers for connector packages."""

from __future__ import annotations

import os
from typing import Any

import httpx

from parsimony.errors import ParseError, UnauthorizedError
from parsimony.transport import HttpClient, map_http_error, map_timeout_error


def require_key(arg: str, *, env_var: str, provider: str) -> str:
    """Resolve an API key from *arg* or *env_var*, or raise :class:`UnauthorizedError`.

    Connector packages use this for the common ``api_key or os.environ[...]``
    pattern without duplicating the fail-fast logic in every provider.
    """
    key = arg or os.environ.get(env_var, "")
    if not key:
        raise UnauthorizedError(provider, env_var=env_var)
    return key


def fetch_json(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    provider: str,
    op_name: str,
    env_var: str | None = None,
) -> Any:
    """GET *path*, map httpx errors to kernel types, return parsed JSON."""
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=provider, op_name=op_name, env_var=env_var)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=provider, op_name=op_name)
    try:
        return response.json()
    except ValueError as exc:
        raise ParseError(provider=provider) from exc


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
