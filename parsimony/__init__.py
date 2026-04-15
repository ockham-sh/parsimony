"""Parsimony — detachable catalog + connector framework.

Public surface (import from ``parsimony`` directly). Heavy symbols that pull in
optional stacks use lazy loading via :func:`__getattr__` to keep core imports light.

**Contracts**

* :class:`~parsimony.connector.Connectors` is an immutable collection of
  bound :class:`~parsimony.connector.Connector` instances; callers use
  ``await connectors[name](**kwargs)`` or ``connectors.get(name)``.
  Each connector exposes a typed Pydantic param model at the boundary.
  Post-fetch hooks attach per connector via :meth:`~parsimony.connector.Connector.with_callback`
  or to every connector in a collection via :meth:`~parsimony.connector.Connectors.with_callback`.
* :func:`~parsimony.connector.connector` infers each connector's name and description from the
  function name and docstring; pass ``output=`` for tabular :class:`~parsimony.result.OutputConfig`.
* :func:`~parsimony.connector.enumerator` builds the same :class:`~parsimony.connector.Connector`
  type with a catalog-oriented schema (no DATA columns; KEY with ``namespace=...``; one TITLE).
* :func:`~parsimony.connector.loader` builds the same :class:`~parsimony.connector.Connector`
  type with a data-oriented schema (KEY with ``namespace=...`` and DATA columns only; no TITLE/METADATA).
  :class:`~parsimony.data_store.DataStore` persists observations via :meth:`~parsimony.data_store.DataStore.load_result`.
* :class:`~parsimony.catalog.catalog.Catalog` orchestrates store and optional
  embeddings for indexing (:meth:`~parsimony.catalog.catalog.Catalog.ingest`).
  :meth:`~parsimony.catalog.store.CatalogStore.search` is implementation-defined on the store.
  Catalog identity is ``(namespace, code)``; ``code`` is the connector-native identifier for that namespace.
  :meth:`~parsimony.catalog.catalog.Catalog.index_result` builds :class:`SeriesEntry` rows from a
  :class:`~parsimony.result.SemanticTableResult`. The catalog namespace comes from ``namespace=...`` on the
  KEY column in :class:`~parsimony.result.OutputConfig`. Use ``conn.with_callback(catalog.index_result)``
  for auto-indexing after fetch.
* Optional :class:`~parsimony.connector.Namespace` metadata on a param field
  documents which catalog namespace supplies valid values for that field.
* :class:`~parsimony.catalog.models.SeriesEntry.observable_id` is reserved
  for a future knowledge-layer link; the framework does not interpret it yet.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any

try:
    __version__ = version("parsimony")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

__all__ = [
    # Core abstractions
    "Connector",
    "Connectors",
    "ResultCallback",
    "connector",
    "enumerator",
    "loader",
    "Namespace",
    # Result system
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    "SemanticTableResult",
    # Catalog
    "Catalog",
    "CatalogStore",
    "EmbeddingProvider",
    "IndexResult",
    "SeriesEntry",
    "SeriesMatch",
    "SQLiteCatalogStore",
    "InMemoryDataStore",
    "DataStore",
    "LoadResult",
    "LiteLLMEmbeddingProvider",
    "build_embedding_text",
    "code_token",
    "normalize_code",
    "normalize_series_catalog_row",
    "series_match_from_entry",
    # Errors
    "ConnectorError",
    "PaymentRequiredError",
    "RateLimitError",
    "UnauthorizedError",
    "ProviderError",
    "EmptyDataError",
    "ParseError",
    # Convenience
    "client",
]


# Lazy-loaded symbols: maps name → (module_path, attribute_name).
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    # Core abstractions
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
    # Catalog models
    "EmbeddingProvider": ("parsimony.catalog.models", "EmbeddingProvider"),
    "IndexResult": ("parsimony.catalog.models", "IndexResult"),
    "SeriesEntry": ("parsimony.catalog.models", "SeriesEntry"),
    "SeriesMatch": ("parsimony.catalog.models", "SeriesMatch"),
    "normalize_code": ("parsimony.catalog.models", "normalize_code"),
    "normalize_series_catalog_row": ("parsimony.catalog.models", "normalize_series_catalog_row"),
    "series_match_from_entry": ("parsimony.catalog.models", "series_match_from_entry"),
    "code_token": ("parsimony.catalog.models", "code_token"),
    # Catalog
    "Catalog": ("parsimony.catalog.catalog", "Catalog"),
    "CatalogStore": ("parsimony.stores.catalog_store", "CatalogStore"),
    "LiteLLMEmbeddingProvider": ("parsimony.embeddings.litellm", "LiteLLMEmbeddingProvider"),
    "SQLiteCatalogStore": ("parsimony.stores.sqlite_catalog", "SQLiteCatalogStore"),
    "InMemoryDataStore": ("parsimony.stores.memory_data", "InMemoryDataStore"),
    "DataStore": ("parsimony.stores.data_store", "DataStore"),
    "LoadResult": ("parsimony.stores.data_store", "LoadResult"),
    "build_embedding_text": ("parsimony.catalog.catalog", "build_embedding_text"),
    # Errors
    "ConnectorError": ("parsimony.errors", "ConnectorError"),
    "PaymentRequiredError": ("parsimony.errors", "PaymentRequiredError"),
    "RateLimitError": ("parsimony.errors", "RateLimitError"),
    "UnauthorizedError": ("parsimony.errors", "UnauthorizedError"),
    "ProviderError": ("parsimony.errors", "ProviderError"),
    "EmptyDataError": ("parsimony.errors", "EmptyDataError"),
    "ParseError": ("parsimony.errors", "ParseError"),
}

# Cache for the convenience client singleton.
_client_cache: Any = None


def __getattr__(name: str) -> Any:
    global _client_cache

    # Convenience: `from parsimony import client` builds a ready-to-use Connectors
    # collection with API keys from environment variables (cached after first access).
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
