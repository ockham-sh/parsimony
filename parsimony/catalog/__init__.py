"""Series catalog framework (namespace-keyed core)."""

from __future__ import annotations

from typing import Any

from parsimony.catalog.models import (
    EmbeddingProvider,
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    code_token,
    normalize_code,
    normalize_series_catalog_row,
    series_match_from_entry,
)

__all__ = [
    "Catalog",
    "CatalogStore",
    "EmbeddingProvider",
    "IndexResult",
    "SeriesEntry",
    "SeriesMatch",
    "build_embedding_text",
    "code_token",
    "normalize_code",
    "normalize_series_catalog_row",
    "series_match_from_entry",
]


def __getattr__(name: str) -> Any:
    if name == "Catalog":
        from parsimony.catalog.catalog import Catalog

        return Catalog
    if name == "build_embedding_text":
        from parsimony.catalog.catalog import build_embedding_text

        return build_embedding_text
    if name == "CatalogStore":
        # Lazy to break the catalog <-> stores circular import when the
        # stores package is the entry point (e.g., the bundle builder CLI).
        from parsimony.stores.catalog_store import CatalogStore

        return CatalogStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
