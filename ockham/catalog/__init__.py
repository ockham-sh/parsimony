"""Series catalog framework (namespace-keyed core)."""

from __future__ import annotations

from typing import Any

from ockham.catalog.embeddings import EmbeddingProvider
from ockham.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    code_token,
    normalize_code,
    normalize_series_catalog_row,
    series_match_from_entry,
)
from ockham.catalog.series_pipeline import build_embedding_text
from ockham.catalog.store import CatalogStore

__all__ = [
    "CatalogStore",
    "EmbeddingProvider",
    "IndexResult",
    "SeriesCatalog",
    "SeriesEntry",
    "SeriesMatch",
    "build_embedding_text",
    "code_token",
    "normalize_code",
    "normalize_series_catalog_row",
    "series_match_from_entry",
]


def __getattr__(name: str) -> Any:
    if name == "SeriesCatalog":
        from ockham.catalog.catalog import SeriesCatalog

        return SeriesCatalog
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
