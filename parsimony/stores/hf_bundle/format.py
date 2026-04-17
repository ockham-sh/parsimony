"""Bundle wire format: manifest schema, Parquet schema, allowed filenames.

Single source of truth shared by :mod:`parsimony.stores.hf_bundle.store`
(reader) and :mod:`parsimony.stores.hf_bundle.builder` (writer). Any drift
between the two sides must be a single-file diff here.

The bundle layout (one HuggingFace repo per namespace) is::

    parsimony-dev/<namespace>/
        entries.parquet   # catalog rows, row_id dense 0..N-1 matches FAISS pos
        index.faiss       # IndexHNSWFlat over L2-normalized embeddings
        manifest.json     # BundleManifest (this file)
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Final

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# Filenames (the only ones the client snapshot-downloads)
# ---------------------------------------------------------------------------

ENTRIES_FILENAME: Final[str] = "entries.parquet"
INDEX_FILENAME: Final[str] = "index.faiss"
MANIFEST_FILENAME: Final[str] = "manifest.json"

BUNDLE_FILENAMES: Final[frozenset[str]] = frozenset({ENTRIES_FILENAME, INDEX_FILENAME, MANIFEST_FILENAME})

# ---------------------------------------------------------------------------
# Parquet schema (entries.parquet)
# ---------------------------------------------------------------------------

ENTRIES_PARQUET_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("namespace", pa.string(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=True),
        pa.field("tags", pa.list_(pa.string()), nullable=False),
        pa.field("metadata", pa.string(), nullable=False),
        pa.field("properties", pa.string(), nullable=False),
        pa.field("observable_id", pa.string(), nullable=True),
        pa.field("row_id", pa.int64(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# FAISS index build params — constants, not manifest fields. The bundle is
# hard-coded to IndexHNSWFlat; if we ever ship a different index type, the
# format version bump happens here, not per-bundle.
# ---------------------------------------------------------------------------

FAISS_HNSW_M: Final[int] = 16
FAISS_HNSW_EF_CONSTRUCTION: Final[int] = 200
FAISS_HNSW_EF_SEARCH_DEFAULT: Final[int] = 64

# ---------------------------------------------------------------------------
# HF repo naming convention
# ---------------------------------------------------------------------------

HF_ORG: Final[str] = "parsimony-dev"


def hf_repo_id(namespace: str) -> str:
    """Canonical HF repo id for a catalog namespace bundle."""
    return f"{HF_ORG}/{namespace}"


# ---------------------------------------------------------------------------
# Allowed embedding-model identifiers
# ---------------------------------------------------------------------------
#
# The prefix allowlist rejects obviously hostile third-party repos at
# manifest-validation and provider-construction time. The 40-char commit SHA
# on ``embedding_model_revision`` pins the specific version.

ALLOWED_MODEL_ID_PREFIXES: Final[tuple[str, ...]] = (
    "sentence-transformers/",
    f"{HF_ORG}/",
)

_SHA_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")


# ---------------------------------------------------------------------------
# Size caps on downloaded artifacts
# ---------------------------------------------------------------------------

MAX_MANIFEST_BYTES: Final[int] = 64 * 1024  # 64 KiB
MAX_PARQUET_BYTES: Final[int] = 512 * 1024 * 1024  # 512 MiB
MAX_INDEX_BYTES: Final[int] = 1024 * 1024 * 1024  # 1 GiB


# ---------------------------------------------------------------------------
# BundleManifest (manifest.json)
# ---------------------------------------------------------------------------


class BundleManifest(BaseModel):
    """Wire contract for a catalog bundle's ``manifest.json``.

    Minimal fields — only what changes per bundle or is load-bearing for the
    reader. FAISS index type, HNSW build params, Parquet schema version, and
    bundle format version are encoded in code on both sides.

    Strict mode (``extra='forbid'``) so a tampered manifest with unknown
    fields fails loudly.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    namespace: str = Field(..., min_length=1)
    built_at: datetime
    entry_count: int = Field(..., ge=0)

    embedding_model: str = Field(..., min_length=1)
    embedding_model_revision: str = Field(..., min_length=1)
    embedding_dim: int = Field(..., gt=0)

    # Only the search-time knob that actually varies. Build-time params
    # (ef_construction, M) are constants in code.
    faiss_hnsw_ef_search: int = Field(default=FAISS_HNSW_EF_SEARCH_DEFAULT, gt=0)

    entries_sha256: str
    index_sha256: str

    builder_git_sha: str | None = None

    @field_validator("embedding_model")
    @classmethod
    def _check_model_allowlist(cls, value: str) -> str:
        if not any(value.startswith(p) for p in ALLOWED_MODEL_ID_PREFIXES):
            raise ValueError(
                f"embedding_model {value!r} is not under an allowed prefix (allowed: {ALLOWED_MODEL_ID_PREFIXES})"
            )
        return value

    @field_validator("embedding_model_revision")
    @classmethod
    def _check_model_revision(cls, value: str) -> str:
        if not _SHA_RE.match(value):
            raise ValueError(f"embedding_model_revision must be a full 40-char commit SHA, got {value!r}")
        return value

    @field_validator("entries_sha256", "index_sha256")
    @classmethod
    def _check_sha256(cls, value: str) -> str:
        if not _SHA256_RE.match(value):
            raise ValueError("sha256 field must be 64 lowercase hex characters")
        return value

    @field_validator("built_at")
    @classmethod
    def _require_tz(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("built_at must be timezone-aware (UTC)")
        return value.astimezone(UTC)


__all__ = [
    "ALLOWED_MODEL_ID_PREFIXES",
    "BUNDLE_FILENAMES",
    "BundleManifest",
    "ENTRIES_FILENAME",
    "ENTRIES_PARQUET_SCHEMA",
    "FAISS_HNSW_EF_CONSTRUCTION",
    "FAISS_HNSW_EF_SEARCH_DEFAULT",
    "FAISS_HNSW_M",
    "HF_ORG",
    "INDEX_FILENAME",
    "MANIFEST_FILENAME",
    "MAX_INDEX_BYTES",
    "MAX_MANIFEST_BYTES",
    "MAX_PARQUET_BYTES",
    "hf_repo_id",
]
