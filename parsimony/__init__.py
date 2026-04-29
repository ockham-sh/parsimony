"""Parsimony — typed connectors and a hybrid-search catalog for financial data.

Flat module layout. Heavy symbols (:class:`Catalog` and its FAISS /
sentence-transformers / huggingface-hub stack) load lazily on first access
via :pep:`562` so that ``import parsimony`` stays cheap.

* :class:`Connectors` is an immutable collection of :class:`Connector` objects;
  callers use ``await connectors[name](**kwargs)``. Each connector validates
  its input through a Pydantic param model.
* :class:`CatalogBackend` is the structural contract every catalog matches.
  :class:`Catalog` is the canonical implementation (Parquet rows + FAISS
  vectors + BM25 keywords + RRF) and is loaded lazily.
* Connector plugins are discovered through the ``parsimony.providers``
  entry-point group via :mod:`parsimony.discover`. Catalog publishing reads
  ``CATALOGS`` / optional ``RESOLVE_CATALOG`` on the plugin module — see
  :func:`parsimony.publish.publish`.
"""

from __future__ import annotations

import importlib
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from parsimony import cache as cache  # re-export so ``from parsimony import cache`` works
from parsimony import discover as discover  # re-export so ``from parsimony import discover`` works
from parsimony.connector import (
    Connector,
    Connectors,
    ResultCallback,
    connector,
    enumerator,
    loader,
)
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.stores import InMemoryDataStore, LoadResult

try:
    __version__ = version("parsimony-core")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"


__all__ = [
    # --- Connector primitives ---
    "Connector",
    "Connectors",
    "ResultCallback",
    "connector",
    "enumerator",
    "loader",
    # --- Result system ---
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    # --- Catalog (lazy) ---
    "Catalog",
    "CatalogBackend",
    "CatalogCache",
    "EmbedderInfo",
    "EmbeddingProvider",
    "FragmentEmbeddingCache",
    "IndexResult",
    "LiteLLMEmbeddingProvider",
    "OnnxEmbedder",
    "SentenceTransformerEmbedder",
    "SeriesEntry",
    "SeriesMatch",
    "catalog_key",
    "code_token",
    "normalize_code",
    "normalize_entity_code",
    "parse_catalog_url",
    "series_match_from_entry",
    # --- Data persistence ---
    "InMemoryDataStore",
    "LoadResult",
    # --- Errors ---
    "ConnectorError",
    "EmptyDataError",
    "ParseError",
    "PaymentRequiredError",
    "ProviderError",
    "RateLimitError",
    "UnauthorizedError",
]


# Heavy symbols — loaded lazily via PEP 562 so ``import parsimony`` does not
# pull torch / faiss / huggingface-hub. Keys are public attribute names; values
# are ``(module, attribute)``.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Catalog": ("parsimony.catalog", "Catalog"),
    "CatalogBackend": ("parsimony.catalog", "CatalogBackend"),
    "CatalogCache": ("parsimony.catalog", "CatalogCache"),
    "EmbedderInfo": ("parsimony.embedder", "EmbedderInfo"),
    "EmbeddingProvider": ("parsimony.embedder", "EmbeddingProvider"),
    "FragmentEmbeddingCache": ("parsimony.embedder", "FragmentEmbeddingCache"),
    "IndexResult": ("parsimony.catalog", "IndexResult"),
    "LiteLLMEmbeddingProvider": ("parsimony.embedder", "LiteLLMEmbeddingProvider"),
    "OnnxEmbedder": ("parsimony.embedder", "OnnxEmbedder"),
    "SentenceTransformerEmbedder": ("parsimony.embedder", "SentenceTransformerEmbedder"),
    "SeriesEntry": ("parsimony.catalog", "SeriesEntry"),
    "SeriesMatch": ("parsimony.catalog", "SeriesMatch"),
    "catalog_key": ("parsimony.catalog", "catalog_key"),
    "code_token": ("parsimony.catalog", "code_token"),
    "normalize_code": ("parsimony.catalog", "normalize_code"),
    "normalize_entity_code": ("parsimony.catalog", "normalize_entity_code"),
    "parse_catalog_url": ("parsimony.catalog", "parse_catalog_url"),
    "series_match_from_entry": ("parsimony.catalog", "series_match_from_entry"),
}


def __getattr__(name: str) -> Any:
    spec = _LAZY_IMPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module 'parsimony' has no attribute {name!r}")
    return getattr(importlib.import_module(spec[0]), spec[1])
