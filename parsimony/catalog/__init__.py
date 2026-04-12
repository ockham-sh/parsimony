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
from parsimony.stores.catalog_store import CatalogStore

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
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
