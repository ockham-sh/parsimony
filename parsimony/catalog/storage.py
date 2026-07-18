"""Catalog snapshot storage constants and manifest I/O."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from parsimony.entity import Entity

SCHEMA_VERSION = 1
META_FILENAME = "meta.json"
ENTRIES_FILENAME = "entries.parquet"
INDEXES_DIRNAME = "indexes"
VALUES_FILENAME = "values.parquet"
POSTINGS_FILENAME = "postings.parquet"
VECTORS_FILENAME = "vectors.faiss"


class BuildInfo(BaseModel):
    """Provenance for a published snapshot."""

    built_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    parsimony_version: str | None = None
    builder: str | None = Field(
        default=None,
        description="Free-form identifier of the script or job that built this catalog.",
    )
    content_sha256: str = Field(
        default="",
        description="Integrity digest of all files in the catalog except meta.json",
    )
    manifest_contract_sha256: str = Field(
        default="",
        description="Digest of normalized manifest contract fields (backend semantics, indexes, extensions).",
    )


class BackendMeta(BaseModel):
    """Row storage configuration for a catalog snapshot."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["memory", "parquet"] = "memory"
    rows_filename: str = ENTRIES_FILENAME
    namespace: str | None = None
    code_column: str = "code"
    title_column: str = "title"
    field_links: dict[str, str] = Field(default_factory=dict)


class CatalogMeta(BaseModel):
    """Catalog snapshot manifest."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = 1
    name: str
    namespaces: list[str]
    entry_count: int = Field(ge=0)
    index_fields: dict[str, str] = Field(default_factory=dict)
    default_field: str | None = None
    backend: BackendMeta = Field(default_factory=BackendMeta)
    build: BuildInfo = Field(default_factory=BuildInfo)


def read_meta(path: str | Path) -> CatalogMeta:
    """Read ``meta.json`` from *path*."""

    return CatalogMeta.model_validate_json((Path(path) / META_FILENAME).read_text())


# Hub dataset repos carry README / .gitattributes beside snapshot payloads; exclude
# them from the integrity digest so Catalog.load("hf://…") matches local saves.
_SNAPSHOT_NON_PAYLOAD_NAMES = frozenset({"meta.json", "README.md", ".gitattributes"})


def _compute_content_sha256(directory: Path) -> str:
    import hashlib

    lines: list[str] = []
    for p in sorted(directory.rglob("*")):
        if not p.is_file():
            continue
        relpath = p.relative_to(directory).as_posix()
        if p.name in _SNAPSHOT_NON_PAYLOAD_NAMES or relpath.startswith(".cache/"):
            continue
        file_hash = hashlib.sha256()
        with open(p, "rb") as f:
            while chunk := f.read(65536):
                file_hash.update(chunk)
        lines.append(f"{relpath}:{file_hash.hexdigest()}\n")

    lines.sort()
    concatenated = "".join(lines).encode("utf-8")
    return hashlib.sha256(concatenated).hexdigest()


def _read_parquet(target: Path) -> list[Entity]:
    table = pq.read_table(target)
    rows = table.to_pylist()
    return [
        Entity(
            namespace=row["namespace"],
            code=row["code"],
            title=row["title"],
            metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
        )
        for row in rows
    ]
