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
  entry-point group. Catalog publishing reads ``CATALOGS`` / optional
  ``RESOLVE_CATALOG`` on the plugin module — see :func:`parsimony.publish.publish`.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any

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
    "EmbedderInfo",
    "EmbeddingProvider",
    "IndexResult",
    "LiteLLMEmbeddingProvider",
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
    # --- Convenience ---
    "client",
]


# Heavy symbols — loaded lazily via PEP 562 so ``import parsimony`` does not
# pull torch / faiss / huggingface-hub. Keys are the public attribute names;
# values are ``(module, attribute)``.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Catalog": ("parsimony.catalog", "Catalog"),
    "CatalogBackend": ("parsimony.catalog", "CatalogBackend"),
    "EmbedderInfo": ("parsimony.embedder", "EmbedderInfo"),
    "EmbeddingProvider": ("parsimony.embedder", "EmbeddingProvider"),
    "IndexResult": ("parsimony.catalog", "IndexResult"),
    "LiteLLMEmbeddingProvider": ("parsimony.embedder", "LiteLLMEmbeddingProvider"),
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


_client_cache: Any = None


def __getattr__(name: str) -> Any:
    global _client_cache

    if name == "client":
        if _client_cache is None:
            from parsimony.discovery import build_connectors_from_env

            _client_cache = build_connectors_from_env()
        return _client_cache

    spec = _LAZY_IMPORTS.get(name)
    if spec is not None:
        import importlib

        module = importlib.import_module(spec[0])
        return getattr(module, spec[1])

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
