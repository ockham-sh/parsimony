"""Shared HTTP helpers for connector packages.

``fetch_json`` / ``fetch_text`` / ``fetch_csv`` are thin GET wrappers that share
one request path: run the request (transport failures already mapped to the
:mod:`parsimony.errors` taxonomy inside :meth:`HttpClient.request`), raise a
typed error for any non-2xx status via :func:`~parsimony.transport.check_status`,
then parse the body into the shape the connector wants. A body that cannot be
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
from parsimony.transport import HttpClient, check_status


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
    op_name: str,
    env_var: str | None = None,
) -> httpx.Response:
    """GET *path*, dropping ``None`` params and mapping errors to the taxonomy.

    Transport failures (timeout, connection refused, DNS, protocol error) are
    already mapped to a typed :class:`~parsimony.errors.ProviderError` inside
    :meth:`HttpClient.request` — no raw ``httpx`` exception reaches here. A
    non-2xx status is turned into the matching
    :class:`~parsimony.errors.ConnectorError` by
    :func:`~parsimony.transport.check_status`, decided from the status code (so
    nothing constructs a URL-bearing ``HTTPStatusError``). The provider slug is
    read from the client.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    check_status(response, provider=http.provider, op_name=op_name, env_var=env_var)
    return response


def fetch_json(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
    env_var: str | None = None,
) -> Any:
    """GET *path*, map errors to kernel types, return parsed JSON.

    A 200 with a non-JSON body (e.g. an HTML error page) surfaces as the
    typed :class:`~parsimony.errors.ParseError`, not a raw
    ``json.JSONDecodeError`` — so the agent loop sees an actionable taxonomy
    error like every other connector failure. The undecodable body is not
    embedded in the message (it may be large or carry injected content).
    """
    provider = http.provider
    response = _get(http, path=path, params=params, op_name=op_name, env_var=env_var)
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise ParseError(provider, f"{provider}: '{op_name}' returned a non-JSON response body") from exc


def fetch_text(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
    env_var: str | None = None,
) -> str:
    """GET *path*, map errors to kernel types, return the response body as text."""
    response = _get(http, path=path, params=params, op_name=op_name, env_var=env_var)
    return response.text


def fetch_csv(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
    env_var: str | None = None,
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """GET *path*, map errors, parse the CSV body into a :class:`~pandas.DataFrame`.

    Extra keyword args pass straight through to :func:`pandas.read_csv` (``sep=``,
    ``skiprows=``, ``dtype=``, …). A body pandas cannot parse as CSV surfaces as
    :class:`~parsimony.errors.ParseError`; a body with no parseable rows/columns
    surfaces as :class:`~parsimony.errors.EmptyDataError` — never a raw pandas
    exception. The unparseable body is not embedded in the message.
    """
    provider = http.provider
    response = _get(http, path=path, params=params, op_name=op_name, env_var=env_var)
    try:
        return pd.read_csv(io.StringIO(response.text), **read_csv_kwargs)
    except pd.errors.EmptyDataError as exc:
        raise EmptyDataError(provider, query_params=params) from exc
    except (pd.errors.ParserError, ValueError) as exc:
        raise ParseError(provider, f"{provider}: '{op_name}' returned a body that is not valid CSV") from exc


def make_http_client(
    base_url: str,
    *,
    provider: str,
    query_params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> HttpClient:
    """Construct a configured :class:`HttpClient` for a provider base URL.

    *provider* is the slug stamped on every typed error the client raises
    (via :func:`~parsimony.transport.check_status` or a transport failure).
    """
    return HttpClient(
        base_url,
        provider=provider,
        query_params=query_params or {},
        headers=headers or {},
        timeout=timeout,
    )


def make_api_key_client(
    base_url: str,
    *,
    provider: str,
    api_key: str,
    api_key_param: str = "apikey",
    timeout: float = 15.0,
) -> HttpClient:
    """Construct an :class:`HttpClient` with a default API-key query parameter."""
    return make_http_client(
        base_url,
        provider=provider,
        query_params={api_key_param: api_key},
        timeout=timeout,
    )
