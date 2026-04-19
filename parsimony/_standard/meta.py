"""On-disk layout for the standard catalog snapshot.

A snapshot is three files in a single directory::

    <catalog>/
      meta.json          # CatalogMeta, JSON-serialized
      entries.parquet    # SeriesEntry rows + embedding column
      embeddings.faiss   # FAISS index built from those embeddings

BM25 is not persisted; it is rebuilt in memory at load time from the entries'
``embedding_text``. The on-disk format avoids pickled Python objects to stay
forward-compatible and safe to read from public hubs.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from parsimony.catalog.embedder_info import EmbedderInfo

SCHEMA_VERSION = 1

META_FILENAME = "meta.json"
ENTRIES_FILENAME = "entries.parquet"
INDEX_FILENAME = "embeddings.faiss"


class BuildInfo(BaseModel):
    """Provenance for a published snapshot."""

    built_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    parsimony_version: str | None = None
    builder: str | None = Field(
        default=None,
        description="Free-form identifier of the script or job that built this catalog.",
    )


class CatalogMeta(BaseModel):
    """Catalog snapshot manifest (``meta.json``)."""

    schema_version: int = Field(default=SCHEMA_VERSION)
    name: str = Field(
        description=(
            "Catalog name (lowercase snake_case). Identifies the snapshot; conventionally matches the HF "
            "repo suffix (e.g. 'fred' for hf://ockham/catalog-fred)."
        ),
    )
    namespaces: list[str] = Field(
        description=(
            "Distinct entry namespaces in entries.parquet (lowercase snake_case). May contain one or "
            "many — entries are self-describing."
        ),
    )
    entry_count: int = Field(ge=0)
    embedder: EmbedderInfo
    build: BuildInfo = Field(default_factory=BuildInfo)
