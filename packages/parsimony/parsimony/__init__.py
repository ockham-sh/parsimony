"""Parsimony — typed connectors and a hybrid-search catalog for financial data.

Public surface lives at the top level. Heavy symbols (``Catalog`` and its FAISS
/ sentence-transformers / huggingface-hub stack) load lazily on first access
via :pep:`562` so that ``import parsimony`` stays cheap.

* :class:`Connectors` is an immutable collection of :class:`Connector` objects;
  callers use ``await connectors[name](**kwargs)``. Each connector validates
  its input through a Pydantic param model.
* :class:`BaseCatalog` is the catalog ABC. :class:`Catalog` is the canonical
  implementation (Parquet rows + FAISS vectors + BM25 keywords + RRF) and is
  loaded lazily.
* Connector plugins are discovered through the ``parsimony.providers``
  entry-point group (see :mod:`parsimony.plugins`). The catalog has no plugin
  axis: custom backends subclass :class:`BaseCatalog` directly.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any

try:
    __version__ = version("parsimony")
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
    "Namespace",
    # --- Result system ---
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    "SemanticTableResult",
    # --- Catalog ---
    "BaseCatalog",
    "Catalog",
    "EmbedderInfo",
    "EmbeddingProvider",
    "IndexResult",
    "LiteLLMEmbeddingProvider",
    "SentenceTransformerEmbedder",
    "SeriesEntry",
    "SeriesMatch",
    # --- Data persistence ---
    "DataStore",
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
    # --- Utilities ---
    "code_token",
    "normalize_code",
    "series_match_from_entry",
    # --- Convenience ---
    "client",
]


_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    # Connector primitives
    "Connector": ("parsimony.connector", "Connector"),
    "Connectors": ("parsimony.connector", "Connectors"),
    "ResultCallback": ("parsimony.connector", "ResultCallback"),
    "connector": ("parsimony.connector", "connector"),
    "enumerator": ("parsimony.connector", "enumerator"),
    "loader": ("parsimony.connector", "loader"),
    "Namespace": ("parsimony.connector", "Namespace"),
    # Result system
    "Column": ("parsimony.result", "Column"),
    "ColumnRole": ("parsimony.result", "ColumnRole"),
    "OutputConfig": ("parsimony.result", "OutputConfig"),
    "Provenance": ("parsimony.result", "Provenance"),
    "Result": ("parsimony.result", "Result"),
    "SemanticTableResult": ("parsimony.result", "SemanticTableResult"),
    # Catalog (lightweight)
    "BaseCatalog": ("parsimony.catalog.catalog", "BaseCatalog"),
    "EmbedderInfo": ("parsimony.catalog.embedder_info", "EmbedderInfo"),
    "IndexResult": ("parsimony.catalog.models", "IndexResult"),
    "SeriesEntry": ("parsimony.catalog.models", "SeriesEntry"),
    "SeriesMatch": ("parsimony.catalog.models", "SeriesMatch"),
    "code_token": ("parsimony.catalog.models", "code_token"),
    "normalize_code": ("parsimony.catalog.models", "normalize_code"),
    "series_match_from_entry": ("parsimony.catalog.models", "series_match_from_entry"),
    # Catalog (heavy: FAISS, sentence-transformers, huggingface-hub, litellm)
    "Catalog": ("parsimony._standard", "Catalog"),
    "EmbeddingProvider": ("parsimony._standard.embedder", "EmbeddingProvider"),
    "SentenceTransformerEmbedder": ("parsimony._standard.embedder", "SentenceTransformerEmbedder"),
    "LiteLLMEmbeddingProvider": ("parsimony._standard.embedder", "LiteLLMEmbeddingProvider"),
    # Data persistence
    "DataStore": ("parsimony.stores.data_store", "DataStore"),
    "InMemoryDataStore": ("parsimony.stores.memory_data", "InMemoryDataStore"),
    "LoadResult": ("parsimony.stores.data_store", "LoadResult"),
    # Errors
    "ConnectorError": ("parsimony.errors", "ConnectorError"),
    "PaymentRequiredError": ("parsimony.errors", "PaymentRequiredError"),
    "RateLimitError": ("parsimony.errors", "RateLimitError"),
    "UnauthorizedError": ("parsimony.errors", "UnauthorizedError"),
    "ProviderError": ("parsimony.errors", "ProviderError"),
    "EmptyDataError": ("parsimony.errors", "EmptyDataError"),
    "ParseError": ("parsimony.errors", "ParseError"),
}

_client_cache: Any = None


def __getattr__(name: str) -> Any:
    global _client_cache

    if name == "client":
        if _client_cache is None:
            from parsimony.connectors import build_connectors_from_env

            _client_cache = build_connectors_from_env(lenient=True)
        return _client_cache

    spec = _LAZY_IMPORTS.get(name)
    if spec is not None:
        import importlib

        module = importlib.import_module(spec[0])
        return getattr(module, spec[1])

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
