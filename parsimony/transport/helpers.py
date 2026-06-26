"""Shared HTTP helpers for connector packages.

``fetch_json`` / ``fetch_text`` / ``fetch_csv`` are thin GET wrappers that share
one request path: map ``httpx`` errors to the :mod:`parsimony.errors` taxonomy
and parse the body into the shape the connector wants. A body that cannot be
parsed in the requested shape surfaces as a typed :class:`~parsimony.errors.ParseError`
(never a raw ``json``/``pandas`` exception), so every fetch failure is something
the agent loop can act on.
"""

from __future__ import annotations

import io
import json
import os
from typing import Any

import httpx
import pandas as pd

from parsimony.errors import EmptyDataError, ParseError, UnauthorizedError
from parsimony.transport import HttpClient, map_http_error, map_timeout_error, map_transport_error


def require_key(arg: str, *, env_var: str, provider: str) -> str:
    """Resolve an API key from *arg* or *env_var*, or raise :class:`UnauthorizedError`.

    Connector packages use this for the common ``api_key or os.environ[...]``
    pattern without duplicating the fail-fast logic in every provider.
    """
    key = arg or os.environ.get(env_var, "")
    if not key:
        raise UnauthorizedError(provider, env_var=env_var)
    return key


def _get(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None,
    provider: str,
    op_name: str,
    env_var: str | None = None,
) -> httpx.Response:
    """GET *path*, dropping ``None`` params and mapping httpx errors to the taxonomy.

    Every ``httpx`` failure mode is mapped to a typed
    :class:`~parsimony.errors.ConnectorError`: a non-2xx status via
    :func:`~parsimony.transport.map_http_error`, a timeout via
    :func:`~parsimony.transport.map_timeout_error`, and any other transport
    failure (connection refused, DNS, protocol error) via
    :func:`~parsimony.transport.map_transport_error`. No raw ``httpx`` exception
    escapes this helper. ``TimeoutException`` is a ``TransportError`` subclass,
    so it must be caught before the broader transport handler.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=provider, op_name=op_name, env_var=env_var)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=provider, op_name=op_name)
    except httpx.TransportError as exc:
        map_transport_error(exc, provider=provider, op_name=op_name)
    return response


def fetch_json(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    provider: str,
    op_name: str,
    env_var: str | None = None,
) -> Any:
    """GET *path*, map httpx errors to kernel types, return parsed JSON.

    A 200 with a non-JSON body (e.g. an HTML error page) surfaces as the
    typed :class:`~parsimony.errors.ParseError`, not a raw
    ``json.JSONDecodeError`` — so the agent loop sees an actionable taxonomy
    error like every other connector failure. The undecodable body is not
    embedded in the message (it may be large or carry injected content).
    """
    response = _get(http, path=path, params=params, provider=provider, op_name=op_name, env_var=env_var)
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise ParseError(provider, f"{provider}: '{op_name}' returned a non-JSON response body") from exc


def fetch_text(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    provider: str,
    op_name: str,
    env_var: str | None = None,
) -> str:
    """GET *path*, map httpx errors to kernel types, return the response body as text."""
    response = _get(http, path=path, params=params, provider=provider, op_name=op_name, env_var=env_var)
    return response.text


def fetch_csv(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    provider: str,
    op_name: str,
    env_var: str | None = None,
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """GET *path*, map httpx errors, parse the CSV body into a :class:`~pandas.DataFrame`.

    Extra keyword args pass straight through to :func:`pandas.read_csv` (``sep=``,
    ``skiprows=``, ``dtype=``, …). A body pandas cannot parse as CSV surfaces as
    :class:`~parsimony.errors.ParseError`; a body with no parseable rows/columns
    surfaces as :class:`~parsimony.errors.EmptyDataError` — never a raw pandas
    exception. The unparseable body is not embedded in the message.
    """
    response = _get(http, path=path, params=params, provider=provider, op_name=op_name, env_var=env_var)
    try:
        return pd.read_csv(io.StringIO(response.text), **read_csv_kwargs)
    except pd.errors.EmptyDataError as exc:
        raise EmptyDataError(provider, query_params=params) from exc
    except (pd.errors.ParserError, ValueError) as exc:
        raise ParseError(provider, f"{provider}: '{op_name}' returned a body that is not valid CSV") from exc


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
