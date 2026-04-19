"""Series catalog framework (namespace-keyed core)."""

from __future__ import annotations

from parsimony.catalog.catalog import BaseCatalog
from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    code_token,
    normalize_code,
    normalize_entity_code,
    series_match_from_entry,
)

__all__ = [
    "BaseCatalog",
    "EmbedderInfo",
    "IndexResult",
    "SeriesEntry",
    "SeriesMatch",
    "catalog_key",
    "code_token",
    "normalize_code",
    "normalize_entity_code",
    "series_match_from_entry",
]
