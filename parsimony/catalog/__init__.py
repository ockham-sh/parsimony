"""Series catalog framework (namespace-keyed core).

The public surface of this module is deliberately small: the ABC
(:class:`BaseCatalog`), the value types (:class:`SeriesEntry`,
:class:`SeriesMatch`, :class:`IndexResult`, :class:`EmbedderInfo`), and the
normalization helpers. The canonical concrete implementation lives at
:class:`parsimony.Catalog` — which is loaded lazily via the root package so
that ``import parsimony`` does not pull FAISS, BM25, or sentence-transformers.
"""

from __future__ import annotations

from parsimony.catalog.catalog import BaseCatalog, entries_from_table_result
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
    "entries_from_table_result",
    "normalize_code",
    "normalize_entity_code",
    "series_match_from_entry",
]
