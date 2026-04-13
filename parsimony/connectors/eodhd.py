"""EODHD source: REST API with path interpolation and filter/page param mapping.

[TO BE REFACTORED!] THIS IS AN EXAMPLE OF BAD IMPLEMENTATION
eodhd follows a single generic path-based connector and it should follow the FMP pattern which is typed
connectors per endpoint.
"""

from __future__ import annotations

from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from parsimony.connector import Connectors, PaymentRequiredError, UnauthorizedError, connector
from parsimony.result import Provenance, Result
from parsimony.transport.http import HttpClient
from parsimony.transport.json_helpers import interpolate_path, json_to_df


ENV_VARS: dict[str, str] = {"api_key": "EODHD_API_KEY"}


class EodhdFetchParams(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: Literal["GET", "POST"] = "GET"
    path: str = Field(..., min_length=1, description="API path, may contain {placeholders}")


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        "https://eodhd.com/api",
        query_params={"api_token": api_key, "fmt": "json"},
    )


@connector(tags=["eodhd"])
async def eodhd_fetch(params: EodhdFetchParams, *, api_key: str) -> Result:
    """EOD Historical Data REST API (path, method, filter_*, page_* query params)."""
    http = _make_http(api_key)
    raw = params.model_dump()
    raw.update(params.model_extra or {})
    method = str(raw.pop("method", "GET")).upper()
    path = raw.pop("path")

    rendered_path, request_params = interpolate_path(path, raw)

    transformed: dict[str, Any] = {}
    for k, v in request_params.items():
        if v is None:
            continue
        if k.startswith("filter_"):
            transformed[f"filter[{k[7:]}]"] = v
        elif k.startswith("page_"):
            transformed[f"page[{k[5:]}]"] = v
        else:
            transformed[k] = v

    try:
        response = await http.request(method, rendered_path, params=transformed)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise UnauthorizedError(provider="eodhd", message="Invalid EODHD API token") from e
        if e.response.status_code == 402:
            raise PaymentRequiredError(
                provider="eodhd",
                message="Your EODHD plan is not eligible for this data request",
            ) from e
        raise

    data = response.json()
    if isinstance(data, dict) and any(
        k in data and isinstance(data[k], list) for k in ("earnings", "ipos", "splits", "data")
    ):
        for key in ("earnings", "ipos", "splits", "data"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break

    df = json_to_df(data)
    return Result.from_dataframe(
        df,
        Provenance(source="eodhd", params=params.model_dump()),
    )


CONNECTORS = Connectors([eodhd_fetch])
