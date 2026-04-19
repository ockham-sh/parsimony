"""Series catalog framework (namespace-keyed core)."""

from __future__ import annotations

from parsimony.catalog.catalog import Catalog, build_embedding_text
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
