"""Catalog snapshot validation at load time and for release gates."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from parsimony.catalog.storage import (
    ENTRIES_FILENAME,
    INDEXES_DIRNAME,
    BackendMeta,
    CatalogMeta,
    read_meta,
)

SUPPORTED_BACKEND_KINDS: frozenset[str] = frozenset({"memory", "parquet"})
SUPPORTED_INDEX_KINDS: frozenset[str] = frozenset({"vector", "bm25", "hybrid", "dis_max"})


class CatalogValidationError(ValueError):
    """Raised when a catalog snapshot fails structural validation."""


def _canonical_json(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def manifest_contract_payload(meta: CatalogMeta) -> dict[str, Any]:
    """Stable manifest fields that define backend semantics (excludes volatile build timestamps)."""

    backend = meta.backend.model_dump()
    payload: dict[str, Any] = {
        "schema_version": meta.schema_version,
        "name": meta.name,
        "namespaces": list(meta.namespaces),
        "entry_count": meta.entry_count,
        "index_fields": dict(sorted(meta.index_fields.items())),
        "default_field": meta.default_field,
        "backend": backend,
    }
    if meta.sdmx is not None:
        payload["sdmx"] = meta.sdmx
    return payload


def compute_manifest_contract_sha256(meta: CatalogMeta) -> str:
    return hashlib.sha256(_canonical_json(manifest_contract_payload(meta))).hexdigest()


def _resolve_rows_path(catalog_dir: Path, rows_filename: str) -> Path:
    rows_path = Path(rows_filename)
    if rows_path.is_absolute() or ".." in rows_path.parts:
        raise CatalogValidationError(f"backend.rows_filename must be a relative catalog path, got {rows_filename!r}")
    resolved = (catalog_dir / rows_path).resolve()
    if catalog_dir.resolve() not in resolved.parents and resolved != catalog_dir.resolve():
        raise CatalogValidationError(f"backend.rows_filename escapes catalog directory: {rows_filename!r}")
    return resolved


def _parquet_column_names(path: Path) -> set[str]:
    return set(pq.read_schema(path).names)


def _validate_parquet_backend(catalog_dir: Path, backend: BackendMeta) -> None:
    rows_path = _resolve_rows_path(catalog_dir, backend.rows_filename)
    if not rows_path.is_file():
        raise CatalogValidationError(f"Parquet backend rows file missing: {rows_path}")

    columns = _parquet_column_names(rows_path)
    required = {backend.code_column, backend.title_column}
    missing = sorted(required - columns)
    if missing:
        raise CatalogValidationError(f"Parquet backend missing required columns {missing} in {rows_path.name}")

    for field in backend.field_links:
        if field not in columns:
            raise CatalogValidationError(f"field_links source column {field!r} missing from {rows_path.name}")
        linked = backend.field_links[field]
        if linked not in columns:
            raise CatalogValidationError(f"field_links target column {linked!r} missing from {rows_path.name}")


def _validate_memory_backend(catalog_dir: Path, backend: BackendMeta) -> None:
    rows_name = backend.rows_filename or ENTRIES_FILENAME
    rows_path = _resolve_rows_path(catalog_dir, rows_name)
    if not rows_path.is_file():
        raise CatalogValidationError(f"Memory backend rows file missing: {rows_path}")
    columns = _parquet_column_names(rows_path)
    required = {"namespace", "code", "title", "metadata_json"}
    missing = sorted(required - columns)
    if missing:
        raise CatalogValidationError(f"Memory backend entries missing columns {missing}")


def _validate_indexes(catalog_dir: Path, index_fields: dict[str, str]) -> None:
    indexes_dir = catalog_dir / INDEXES_DIRNAME
    if not indexes_dir.is_dir():
        raise CatalogValidationError(f"Catalog snapshot missing indexes directory: {indexes_dir}")

    for field, kind in index_fields.items():
        if kind not in SUPPORTED_INDEX_KINDS:
            raise CatalogValidationError(f"Unsupported index kind {kind!r} for field {field!r}")
        field_dir = indexes_dir / field
        if not field_dir.is_dir():
            raise CatalogValidationError(f"Index directory missing for field {field!r}: {field_dir}")

    for child in indexes_dir.iterdir():
        if child.is_dir() and child.name not in index_fields:
            raise CatalogValidationError(f"Unexpected index directory {child.name!r} (not listed in meta.index_fields)")


def validate_catalog_meta(meta: CatalogMeta) -> None:
    if meta.backend.kind not in SUPPORTED_BACKEND_KINDS:
        raise CatalogValidationError(
            f"Unsupported backend.kind {meta.backend.kind!r}; expected one of {sorted(SUPPORTED_BACKEND_KINDS)}"
        )


def validate_catalog_snapshot(catalog_dir: Path, *, meta: CatalogMeta | None = None) -> CatalogMeta:
    """Validate catalog directory structure and return parsed meta."""

    src = Path(catalog_dir)
    if not src.is_dir():
        raise CatalogValidationError(f"Catalog directory does not exist: {src}")

    parsed = meta or read_meta(src)
    validate_catalog_meta(parsed)

    if parsed.build.manifest_contract_sha256:
        expected = compute_manifest_contract_sha256(parsed)
        if parsed.build.manifest_contract_sha256 != expected:
            raise CatalogValidationError(
                "Catalog manifest contract digest mismatch:\n"
                f"  expected: {expected}\n"
                f"  actual:   {parsed.build.manifest_contract_sha256}"
            )

    if parsed.backend.kind == "parquet":
        _validate_parquet_backend(src, parsed.backend)
    else:
        _validate_memory_backend(src, parsed.backend)

    _validate_indexes(src, parsed.index_fields)
    return parsed


class ReleaseManifestEntry(BaseModel):
    """One catalog in a published catalog release."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    catalog_kind: Literal["datasets", "codelist", "series"]
    agency: str | None = None
    flow_id: str | None = None
    entry_count: int = Field(ge=0)
    content_sha256: str
    manifest_contract_sha256: str
    path: str


class ReleaseManifest(BaseModel):
    """Top-level release manifest for publication readiness."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    release_id: str
    built_at: str
    package_commit_sha: str | None = None
    validator_version: str
    agencies: list[str]
    dataset_catalogs: list[str]
    codelist_count: int = Field(ge=0)
    structure_marker_count: int = Field(ge=0)
    series_catalog_count: int = Field(ge=0)
    skipped_flows: list[dict[str, str]] = Field(default_factory=list)
    catalogs: list[ReleaseManifestEntry] = Field(default_factory=list)
