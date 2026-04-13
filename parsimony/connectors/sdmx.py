"""SDMX source: typed connectors for discovery, DSD, codelists, series keys, and fetch."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote

import pandas as pd
import sdmx as sdmx_lib
from pydantic import BaseModel, Field, field_validator
from requests.exceptions import HTTPError

from parsimony.catalog.models import code_token as _code_token
from parsimony.catalog.models import normalize_code
from parsimony.connector import Connectors, EmptyDataError, ParseError, ProviderError, connector
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
    SemanticTableResult,
)


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class SdmxFetchParams(BaseModel):
    """Parameters for fetching SDMX series data."""

    dataset_key: str = Field(..., description="Dataset identifier with agency prefix (e.g. ECB-YC)")
    series_key: str = Field(..., description="Dot-separated dimension values identifying the series")
    start_period: str | None = Field(default=None, description="Start period filter (e.g. 2020-01)")
    end_period: str | None = Field(default=None, description="End period filter (e.g. 2024-12)")

    @field_validator("dataset_key")
    @classmethod
    def _dataset_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("dataset_key must be non-empty")
        if "-" not in stripped:
            raise ValueError("dataset_key must include agency prefix, e.g. 'ECB-YC'")
        return stripped

    @field_validator("series_key")
    @classmethod
    def _series_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("series_key must be non-empty")
        return stripped


class SdmxListDatasetsParams(BaseModel):
    """Parameters for listing SDMX dataflows for an agency."""

    agency: str = Field(
        ...,
        description="SDMX source id (e.g. ECB, ESTAT, IMF_DATA, WB_WDI)",
    )

    @field_validator("agency")
    @classmethod
    def _agency(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("agency must be non-empty")
        return stripped


class SdmxDsdParams(BaseModel):
    """Parameters for inspecting a dataset's Data Structure Definition."""

    dataset_key: str = Field(..., description="Dataset identifier with agency prefix (e.g. ECB-YC)")

    @field_validator("dataset_key")
    @classmethod
    def _dataset_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("dataset_key must be non-empty")
        if "-" not in stripped:
            raise ValueError("dataset_key must include agency prefix, e.g. 'ECB-YC'")
        return stripped


class SdmxCodelistParams(BaseModel):
    """Parameters for enumerating codes for one dimension."""

    dataset_key: str = Field(..., description="Dataset identifier with agency prefix (e.g. ECB-YC)")
    dimension: str = Field(..., description="Dimension id (e.g. FREQ, REF_AREA)")

    @field_validator("dataset_key")
    @classmethod
    def _dataset_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("dataset_key must be non-empty")
        if "-" not in stripped:
            raise ValueError("dataset_key must include agency prefix, e.g. 'ECB-YC'")
        return stripped

    @field_validator("dimension")
    @classmethod
    def _dimension(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("dimension must be non-empty")
        return stripped


class SdmxSeriesKeysParams(BaseModel):
    """Parameters for listing available series keys for a dataset."""

    dataset_key: str = Field(..., description="Dataset identifier with agency prefix (e.g. ECB-YC)")
    key: str | None = Field(
        default=None,
        description=(
            "SDMX key string in DSD positional order, e.g. 'B.U2.SR_1Y+SR_2Y.*.*'. "
            "Each position maps to a dimension; '+' separates alternative codes (OR); "
            "'*' is a wildcard (all codes). Takes precedence over 'filters' when set."
        ),
    )
    filters: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Named dimension filters (dimension_id -> allowed codes). Ignored when 'key' is set.",
    )

    @field_validator("dataset_key")
    @classmethod
    def _dataset_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("dataset_key must be non-empty")
        if "-" not in stripped:
            raise ValueError("dataset_key must include agency prefix, e.g. 'ECB-YC'")
        return stripped


class SdmxDatasetCodelistsParams(BaseModel):
    """Parameters for enumerating all dimension codelists for one SDMX dataset."""

    dataset_key: str = Field(..., description="Dataset identifier with agency prefix (e.g. ECB-YC)")

    @field_validator("dataset_key")
    @classmethod
    def _dataset_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("dataset_key must be non-empty")
        if "-" not in stripped:
            raise ValueError("dataset_key must include agency prefix, e.g. 'ECB-YC'")
        return stripped


# ---------------------------------------------------------------------------
# Output config factories
# ---------------------------------------------------------------------------


def _sdmx_fetch_output(namespace: str, dimension_ids: list[str]) -> OutputConfig:
    """Tabular schema for one dataset fetch; includes identity/metadata + observations."""
    cols: list[Column] = [
        Column(
            name="series_key",
            role=ColumnRole.KEY,
            param_key="series_key",
            namespace=namespace,
        ),
        Column(name="title", role=ColumnRole.TITLE),
    ]
    for dim_id in dimension_ids:
        cols.append(Column(name=dim_id, role=ColumnRole.METADATA))
    cols.extend(
        [
            Column(name="TIME_PERIOD", dtype="datetime", role=ColumnRole.DATA),
            Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        ]
    )
    return OutputConfig(columns=cols)


def _sdmx_list_datasets_output(namespace: str) -> OutputConfig:
    return OutputConfig(
        columns=[
            Column(name="dataset_id", role=ColumnRole.KEY, namespace=namespace),
            Column(name="name", role=ColumnRole.TITLE),
        ]
    )


def _sdmx_codelist_output(namespace: str) -> OutputConfig:
    return OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace=namespace),
            Column(name="name", role=ColumnRole.TITLE),
        ]
    )


def _sdmx_series_keys_output(
    namespace: str,
    dimension_ids: list[str],
) -> OutputConfig:
    cols: list[Column] = [
        Column(name="series_key", role=ColumnRole.KEY, namespace=namespace, param_key="series_key"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="dataset_key", role=ColumnRole.METADATA),
    ]
    for dim_id in dimension_ids:
        cols.append(Column(name=dim_id, role=ColumnRole.METADATA))
    return OutputConfig(columns=cols)


# ---------------------------------------------------------------------------
# Namespace helpers (public; used by catalog indexing)
# ---------------------------------------------------------------------------


def institution_source_from_dataset_key(dataset_key: str) -> str:
    agency, _ = dataset_key.split("-", 1)
    a = agency.strip().upper()
    if a == "ESTAT":
        return "eurostat"
    return agency.strip().lower()


def sdmx_namespace_from_dataset_key(dataset_key: str) -> str:
    """Catalog namespace for one SDMX dataset (``sdmx_<tokenized_dataset>``)."""
    return normalize_code(f"sdmx_{_code_token(dataset_key)}")


def sdmx_agency_namespace(agency_id: str) -> str:
    """Catalog namespace for listing datasets under one agency."""
    return normalize_code(f"sdmx_{_code_token(agency_id)}_datasets")


def sdmx_codelist_namespace(
    maintainer_id: str,
    codelist_id: str,
) -> str:
    """Catalog namespace for one SDMX codelist identity."""
    return normalize_code(f"sdmx_{_code_token(maintainer_id)}_{_code_token(codelist_id)}")


@contextmanager
def _sdmx_client(agency_id: str):
    """SDMX client with session cleanup and World Bank URL fix for bad reference links."""
    client = sdmx_lib.Client(agency_id)
    original_send = client.session.send

    def _patched_send(request: Any, **kwargs: Any) -> Any:
        url = getattr(request, "url", "") or ""
        if "dataapi.worldbank.org" in url:
            request.url = url.replace("dataapi.worldbank.org", "api.worldbank.org").replace(
                "http://", "https://"
            )
        return original_send(request, **kwargs)

    client.session.send = _patched_send  # type: ignore[method-assign]
    try:
        yield client
    finally:
        if hasattr(client, "session") and hasattr(client.session, "close"):
            client.session.close()


def _build_sdmx_dataset_url(agency_id: str, dataset_id: str) -> str | None:
    normalized = agency_id.upper()
    encoded = quote(dataset_id, safe="")
    if normalized == "ECB":
        return f"https://data.ecb.europa.eu/data/datasets/{encoded}"
    if normalized == "ESTAT":
        return (
            f"https://ec.europa.eu/eurostat/databrowser/view/{encoded}"
            "/default/table?lang=en"
        )
    if normalized in ("IMF", "IMF_DATA"):
        return f"https://data.imf.org/?sk={encoded}"
    return None


def _get_concept_name(concept: Any) -> str:
    if not concept or not hasattr(concept, "name"):
        return ""
    locs = getattr(concept.name, "localizations", {}) or {}
    if not locs:
        return str(concept.name)
    return str(locs.get("en", next(iter(locs.values()), str(concept.name))))


def _fetch_dsd(
    client: Any,
    dataset_id: str,
) -> tuple[Any, Any, Any]:
    """Fetch dataflow + DSD; try bundled references first."""
    try:
        msg = client.dataflow(resource_id=dataset_id, params={"references": "descendants"}, force=True)
    except Exception:
        msg = client.dataflow(resource_id=dataset_id, force=True)
    dataflow = msg.dataflow[dataset_id]
    structure_id = dataflow.structure.id
    if hasattr(msg, "structure") and structure_id in msg.structure:
        dsd = msg.structure[structure_id]
    else:
        msg_struct = client.datastructure(resource_id=structure_id, force=True)
        dsd = msg_struct.structure[structure_id]
    return dataflow, dsd, msg


def _get_dimension_codelist(dim: Any, msg: Any) -> list[tuple[str, str]]:
    """Return list of (code_id, english_label) for a dimension's codelist."""
    codelist = _get_dimension_codelist_object(dim, msg)
    if codelist is None:
        return []
    result: list[tuple[str, str]] = []
    for code in codelist:
        if hasattr(code, "name") and code.name is not None:
            locs = getattr(code.name, "localizations", {}) or {}
            name = str(locs.get("en", str(code.name)))
        else:
            name = str(code.id)
        result.append((str(code.id), name))
    return result


def _get_dimension_codelist_object(dim: Any, msg: Any) -> Any | None:
    """Resolve the codelist object referenced by a dimension."""
    enumerated = None
    if dim.local_representation and dim.local_representation.enumerated:
        enumerated = dim.local_representation.enumerated
    elif dim.concept_identity:
        core_rep = getattr(dim.concept_identity, "core_representation", None)
        if core_rep and core_rep.enumerated:
            enumerated = core_rep.enumerated
    if enumerated is None:
        return None
    cl_id = getattr(enumerated, "id", None)
    if not cl_id:
        return None
    return getattr(msg, "codelist", {}).get(cl_id)


def _get_dimension_codelist_identity(dim: Any, msg: Any) -> tuple[str, str, str | None] | None:
    """Return (maintainer_id, codelist_id, version) for a dimension's codelist."""
    codelist = _get_dimension_codelist_object(dim, msg)
    if codelist is None:
        return None
    codelist_id = getattr(codelist, "id", None)
    if not codelist_id:
        return None
    maintainer = getattr(getattr(codelist, "maintainer", None), "id", None)
    version = getattr(codelist, "version", None)
    maintainer_id = str(maintainer or "unknown")
    version_str = str(version).strip() if version is not None else None
    return maintainer_id, str(codelist_id), version_str or None


def _dimension_label_maps(dsd: Any, msg: Any) -> dict[str, dict[str, str]]:
    """Resolve codelist labels per non-time dimension."""
    label_maps: dict[str, dict[str, str]] = {}
    for dim in _non_time_dimensions(dsd):
        pairs = _get_dimension_codelist(dim, msg)
        if not pairs:
            continue
        label_maps[str(dim.id)] = {str(code): str(label) for code, label in pairs}
    return label_maps


def _format_code_with_label(code: str, label: str | None) -> str:
    code_clean = str(code).strip()
    if not code_clean:
        return ""
    if label is None:
        return code_clean
    label_clean = str(label).strip()
    if not label_clean:
        return code_clean
    if label_clean.lower() == code_clean.lower():
        return code_clean
    return f"{code_clean} ({label_clean})"


def _build_sdmx_title(
    row: pd.Series,
    dim_ids: list[str],
    label_maps: dict[str, dict[str, str]],
) -> str:
    parts: list[str] = []
    for dim_id in dim_ids:
        code = str(row.get(dim_id, "")).strip()
        if not code:
            continue
        label = str(label_maps.get(dim_id, {}).get(code, "")).strip()
        parts.append(label or code)
    return " - ".join(parts)


def _parse_sdmx_key(key: str, dim_ids: list[str]) -> dict[str, list[str]]:
    """Parse an SDMX key string into a named filter dict using the ordered dimension list.

    Each '.'-separated position maps to the corresponding dimension id. '+' within a
    position means OR. '*' means unconstrained (skipped). Raises ValueError when the
    number of positions does not match the number of dimensions.
    """
    parts = key.split(".")
    if len(parts) != len(dim_ids):
        raise ValueError(
            f"SDMX key has {len(parts)} position(s) but the DSD has {len(dim_ids)} "
            f"dimension(s) ({', '.join(dim_ids)}). Key: {key!r}"
        )
    result: dict[str, list[str]] = {}
    for dim_id, part in zip(dim_ids, parts):
        if part and part != "*":
            codes = [c.strip() for c in part.split("+") if c.strip()]
            if codes:
                result[dim_id] = codes
    return result


def _non_time_dimensions(dsd: Any) -> list[Any]:
    out: list[Any] = []
    for idx, dim in enumerate(dsd.dimensions):
        is_time = getattr(dim, "id", None) == "TIME_PERIOD"
        if not is_time:
            out.append((idx, dim))
    return [d for _, d in out]


def _ordered_non_time_dimension_ids(dsd: Any) -> list[str]:
    """Return non-time dimension ids in DSD positional order."""
    return [str(dim.id) for dim in _non_time_dimensions(dsd)]


def _resolve_series_dimension_ids(
    columns: list[str],
    dsd_dim_ids: list[str],
) -> list[str]:
    """Return series-dimension ids in DSD order, rejecting mismatches."""
    missing = [dim_id for dim_id in dsd_dim_ids if dim_id not in columns]
    if missing:
        raise ValueError(
            "Unable to align SDMX result columns to DSD order; missing dimension column(s) "
            f"{missing}. Available columns: {columns}"
        )
    return list(dsd_dim_ids)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(tags=["sdmx"])
async def sdmx_fetch(params: SdmxFetchParams) -> Result:
    """Fetch SDMX series data by dataset_key (e.g. ECB-YC) and series_key.

    Returns time series with series_key/title/dimension metadata plus TIME_PERIOD/value.
    """
    dataset_key = params.dataset_key
    series_key = params.series_key
    start_period = params.start_period
    end_period = params.end_period

    agency_id, dataset_id = dataset_key.split("-", 1)

    with _sdmx_client(agency_id) as client:
        try:
            _dataflow, dsd, dsd_msg = _fetch_dsd(client, dataset_id)
            msg = await asyncio.to_thread(
                client.get,
                resource_type="data",
                resource_id=dataset_id,
                key=series_key,
                params={"startPeriod": start_period, "endPeriod": end_period},
            )
        except HTTPError as exc:
            text = getattr(exc.response, "text", str(exc))
            raise ProviderError(
                provider="sdmx",
                status_code=getattr(exc.response, "status_code", 0),
                message=(
                    f"Upstream request failed ({exc.response.status_code}): {text}. "
                    "Validate the series_key against the codelists using sdmx_codelist or sdmx_series_keys."
                ),
            ) from exc
        except Exception as exc:
            raise ProviderError(
                provider="sdmx",
                status_code=0,
                message=f"Failed to fetch {dataset_id}: {exc}",
            ) from exc

    raw = sdmx_lib.to_pandas(msg.data)
    if isinstance(raw, pd.Series):
        df = raw.rename("value").to_frame().reset_index()
    else:
        df = pd.DataFrame(raw).reset_index()
    if df.empty:
        raise EmptyDataError(provider="sdmx", message="No data returned for requested series.")

    if "value" not in df.columns:
        value_columns = [col for col in df.columns if col not in {"TIME_PERIOD"}]
        if len(value_columns) != 1:
            raise ParseError(provider="sdmx", message="Unable to determine SDMX value column")
        df = df.rename(columns={value_columns[0]: "value"})

    dim_ids = _resolve_series_dimension_ids(
        list(df.columns),
        _ordered_non_time_dimension_ids(dsd),
    )
    if not dim_ids:
        raise ParseError(provider="sdmx", message="Unable to determine SDMX series dimensions for series_key")
    for dim_id in dim_ids:
        df[dim_id] = df[dim_id].astype("string").fillna("")
    df["series_key"] = df[dim_ids].agg(".".join, axis=1)

    label_maps = _dimension_label_maps(dsd, dsd_msg)
    df["title"] = df.apply(
        lambda row: _build_sdmx_title(row, dim_ids, label_maps),
        axis=1,
    )
    empty_title_mask = df["title"].astype(str).str.strip() == ""
    if empty_title_mask.any():
        df.loc[empty_title_mask, "title"] = df.loc[empty_title_mask, "series_key"]
    for dim_id in dim_ids:
        labels = label_maps.get(dim_id, {})
        df[dim_id] = df[dim_id].map(
            lambda code, _lbl=labels: _format_code_with_label(str(code), _lbl.get(str(code)))
        )
    long_df = df[["series_key", "title", *dim_ids, "TIME_PERIOD", "value"]]

    additional_metadata: list[dict[str, str]] = []
    series_url = _build_sdmx_dataset_url(agency_id, dataset_id)
    if series_url:
        additional_metadata.append({"name": "series_url", "value": series_url})

    prov = Provenance(
        source="sdmx",
        params={
            "dataset_key": dataset_key,
            "series_key": series_key,
            "start_period": start_period,
            "end_period": end_period,
        },
        properties={"metadata": additional_metadata} if additional_metadata else {},
    )
    ns = sdmx_namespace_from_dataset_key(dataset_key)
    return _sdmx_fetch_output(ns, dim_ids).build_table_result(
        long_df,
        provenance=prov,
        params=params.model_dump(),
    )


@connector(tags=["sdmx", "tool"])
async def sdmx_list_datasets(params: SdmxListDatasetsParams) -> Result:
    """List all dataflows (datasets) for an SDMX agency.

    Returns dataset_id and English name per row. Indexable for catalog discovery.
    """
    agency = params.agency

    def _run() -> pd.DataFrame:
        with _sdmx_client(agency) as client:
            msg = client.dataflow(force=True)
        rows: list[dict[str, str]] = []
        for flow_id, df_def in msg.dataflow.items():
            name = ""
            if hasattr(df_def, "name") and df_def.name is not None:
                locs = getattr(df_def.name, "localizations", {}) or {}
                name = str(locs.get("en", str(df_def.name)))
            rows.append({"dataset_id": str(flow_id), "name": name or str(flow_id)})
        return pd.DataFrame(rows)

    df = await asyncio.to_thread(_run)
    if df.empty:
        raise EmptyDataError(provider="sdmx", message=f"No dataflows returned for agency {agency!r}.")
    ns = sdmx_agency_namespace(agency)
    prov = Provenance(source="sdmx_list_datasets", params={"agency": agency})
    return _sdmx_list_datasets_output(ns).build_table_result(df, provenance=prov, params=params.model_dump())


@connector(tags=["sdmx", "tool"])
async def sdmx_dsd(params: SdmxDsdParams) -> Result:
    """Inspect DSD: one row per non-time dimension with concept name and codelist size.

    Structural introspection only (plain Result, not for catalog indexing).
    """
    dataset_key = params.dataset_key
    agency_id, dataset_id = dataset_key.split("-", 1)

    def _run() -> tuple[pd.DataFrame, str]:
        with _sdmx_client(agency_id) as client:
            _dataflow, dsd, msg = _fetch_dsd(client, dataset_id)
        dims = _non_time_dimensions(dsd)
        template = ".".join(d.id for d in dims)
        out_rows: list[dict[str, Any]] = []
        for pos, dim in enumerate(dims):
            codes = _get_dimension_codelist(dim, msg)
            concept_name = _get_concept_name(getattr(dim, "concept_identity", None))
            out_rows.append(
                {
                    "position": pos,
                    "dimension_id": dim.id,
                    "concept_name": concept_name or dim.id,
                    "codelist_size": len(codes),
                }
            )
        return pd.DataFrame(out_rows), template

    df, template = await asyncio.to_thread(_run)
    if df.empty:
        raise EmptyDataError(provider="sdmx", message=f"No non-time dimensions in DSD for {dataset_key!r}.")
    prov = Provenance(
        source="sdmx_dsd",
        params={"dataset_key": dataset_key},
        properties={"template": template, "dataset_key": dataset_key},
    )
    return Result.from_dataframe(df, provenance=prov)


@connector(tags=["sdmx", "tool"])
async def sdmx_codelist(params: SdmxCodelistParams) -> Result:
    """Enumerate all codes for one dimension (full DSD codelist, not availability-filtered)."""
    dataset_key = params.dataset_key
    dimension = params.dimension
    agency_id, dataset_id = dataset_key.split("-", 1)

    def _run() -> tuple[pd.DataFrame, str, str, str | None]:
        with _sdmx_client(agency_id) as client:
            _dataflow, dsd, msg = _fetch_dsd(client, dataset_id)
        for dim in dsd.dimensions:
            if dim.id == dimension:
                identity = _get_dimension_codelist_identity(dim, msg)
                if identity is None:
                    raise EmptyDataError(
                        provider="sdmx",
                        message=f"Dimension {dimension!r} has no resolvable codelist identity for {dataset_key!r}.",
                    )
                maintainer_id, codelist_id, version = identity
                pairs = _get_dimension_codelist(dim, msg)
                if not pairs:
                    raise EmptyDataError(
                        provider="sdmx",
                        message=f"Dimension {dimension!r} has no resolvable codelist for {dataset_key!r}.",
                    )
                return (
                    pd.DataFrame([{"code": c, "name": n} for c, n in pairs]),
                    maintainer_id,
                    codelist_id,
                    version,
                )
        raise EmptyDataError(provider="sdmx", message=f"Unknown dimension {dimension!r} for dataset {dataset_key!r}.")

    df, maintainer_id, codelist_id, version = await asyncio.to_thread(_run)
    ns = sdmx_codelist_namespace(maintainer_id, codelist_id)
    prov = Provenance(
        source="sdmx_codelist",
        params={
            "dataset_key": dataset_key,
            "dimension": dimension,
            "codelist_maintainer": maintainer_id,
            "codelist_id": codelist_id,
            "codelist_version": version,
        },
    )
    return _sdmx_codelist_output(ns).build_table_result(df, provenance=prov, params=params.model_dump())


@connector(tags=["sdmx", "tool"])
async def sdmx_series_keys(params: SdmxSeriesKeysParams) -> Result:
    """List available series keys for a dataset via the SDMX API.

    Uses ``client.series_keys`` (standard path). For some agencies (e.g. complex World Bank
    flows) results may be incomplete; prefer narrowing with ``filters`` or validating fetches.

    Returns series_key, human-readable title, ``dataset_key`` (for catalog fetch round-trip),
    and one metadata column per non-time dimension.
    """
    dataset_key = params.dataset_key
    agency_id, dataset_id = dataset_key.split("-", 1)

    def _run() -> tuple[pd.DataFrame, list[str]]:
        with _sdmx_client(agency_id) as client:
            _dataflow, dsd, dsd_msg = _fetch_dsd(client, dataset_id)
            raw = client.series_keys(dataset_id)
        if isinstance(raw, dict):
            series_list = list(raw.values())
        else:
            series_list = list(raw)
        if not series_list:
            return pd.DataFrame(), []
        sk_df = sdmx_lib.to_pandas(series_list).astype("string")
        dim_ids = _resolve_series_dimension_ids(
            list(sk_df.columns),
            _ordered_non_time_dimension_ids(dsd),
        )
        for dim_id in dim_ids:
            sk_df[dim_id] = sk_df[dim_id].fillna("")
        sk_df["series_key"] = sk_df[dim_ids].agg(".".join, axis=1)
        active_filters = (
            _parse_sdmx_key(params.key, dim_ids) if params.key else params.filters
        )
        for fk, allowed in active_filters.items():
            if fk not in sk_df.columns:
                raise ValueError(f"Filter key {fk!r} is not a dimension column in series keys.")
            allow_set = {str(x).strip() for x in allowed if str(x).strip()}
            sk_df = sk_df[sk_df[fk].isin(allow_set)]

        label_maps = _dimension_label_maps(dsd, dsd_msg)
        sk_df["title"] = sk_df.apply(
            lambda row: _build_sdmx_title(row, dim_ids, label_maps),
            axis=1,
        )
        empty_title_mask = sk_df["title"].astype(str).str.strip() == ""
        if empty_title_mask.any():
            sk_df.loc[empty_title_mask, "title"] = sk_df.loc[empty_title_mask, "series_key"]
        for dim_id in dim_ids:
            labels = label_maps.get(dim_id, {})
            sk_df[dim_id] = sk_df[dim_id].map(
                lambda code, _lbl=labels: _format_code_with_label(str(code), _lbl.get(str(code)))
            )
        sk_df["dataset_key"] = dataset_key
        cols = ["series_key", "title", "dataset_key"] + dim_ids
        return sk_df[cols], dim_ids

    df, dim_ids = await asyncio.to_thread(_run)
    if df.empty:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No series keys returned for {dataset_key!r} (after filters). "
                "Try different filters or another dataset."
            ),
        )
    ns = sdmx_namespace_from_dataset_key(dataset_key)
    prov = Provenance(
        source="sdmx_series_keys",
        params={"dataset_key": dataset_key, "key": params.key, "filters": dict(params.filters)},
    )
    out_cfg = _sdmx_series_keys_output(ns, dim_ids)
    return out_cfg.build_table_result(df, provenance=prov, params=params.model_dump())


def _build_dataset_codelists_tables_sync(dataset_key: str) -> list[SemanticTableResult]:
    """Resolve DSD and build one catalog-indexable table per non-time codelist dimension."""
    agency_id, dataset_id = dataset_key.split("-", 1)
    with _sdmx_client(agency_id) as client:
        _dataflow, dsd, msg = _fetch_dsd(client, dataset_id)

    out: list[SemanticTableResult] = []
    for dim in dsd.dimensions:
        dim_id = str(getattr(dim, "id", "")).strip()
        if not dim_id or dim_id == "TIME_PERIOD":
            continue
        identity = _get_dimension_codelist_identity(dim, msg)
        if identity is None:
            continue
        maintainer_id, codelist_id, version = identity
        pairs = _get_dimension_codelist(dim, msg)
        if not pairs:
            continue
        df = pd.DataFrame([{"code": code, "name": name} for code, name in pairs])
        ns = sdmx_codelist_namespace(maintainer_id, codelist_id)
        prov = Provenance(
            source="sdmx_codelist",
            params={
                "dataset_key": dataset_key,
                "dimension": dim_id,
                "codelist_maintainer": maintainer_id,
                "codelist_id": codelist_id,
                "codelist_version": version,
            },
        )
        table = _sdmx_codelist_output(ns).build_table_result(
            df,
            provenance=prov,
            params={
                "dataset_key": dataset_key,
                "dimension": dim_id,
                "codelist_maintainer": maintainer_id,
                "codelist_id": codelist_id,
                "codelist_version": version,
            },
        )
        out.append(table)
    if not out:
        raise EmptyDataError(provider="sdmx", message=f"No codelist dimensions found for {dataset_key!r}")
    return out


async def enumerate_sdmx_dataset_codelists(
    params: SdmxDatasetCodelistsParams,
) -> list[SemanticTableResult]:
    """Return one :class:`~parsimony.result.SemanticTableResult` per dimension codelist.

    Each result uses the same schema as :func:`sdmx_codelist` (KEY ``namespace`` from
    :func:`sdmx_codelist_namespace`). Suitable for :meth:`~parsimony.catalog.catalog.Catalog.index_result`.
    """
    return await asyncio.to_thread(_build_dataset_codelists_tables_sync, params.dataset_key)


SDMX_FETCH_CONNECTORS = Connectors([sdmx_fetch])

SDMX_DISCOVERY_CONNECTORS = Connectors(
    [sdmx_list_datasets, sdmx_dsd, sdmx_codelist, sdmx_series_keys]
)

CONNECTORS = SDMX_FETCH_CONNECTORS + SDMX_DISCOVERY_CONNECTORS
