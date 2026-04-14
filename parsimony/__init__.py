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

from typing import Any

from parsimony.catalog.models import (
    EmbeddingProvider,
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
    normalize_series_catalog_row,
    series_match_from_entry,
)
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
    SemanticTableResult,
)
from parsimony.stores.catalog_store import CatalogStore

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
    # Catalog
    "Catalog": ("parsimony.catalog.catalog", "Catalog"),
    "LiteLLMEmbeddingProvider": ("parsimony.embeddings.litellm", "LiteLLMEmbeddingProvider"),
    "SQLiteCatalogStore": ("parsimony.stores.sqlite_catalog", "SQLiteCatalogStore"),
    "InMemoryDataStore": ("parsimony.stores.memory_data", "InMemoryDataStore"),
    "DataStore": ("parsimony.stores.data_store", "DataStore"),
    "LoadResult": ("parsimony.stores.data_store", "LoadResult"),
    "code_token": ("parsimony.catalog.models", "code_token"),
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


def __getattr__(name: str) -> Any:
    # Convenience: `from parsimony import client` builds a ready-to-use Connectors
    # collection with API keys from environment variables.
    if name == "client":
        from parsimony.connectors import build_connectors_from_env

        return build_connectors_from_env()

    spec = _LAZY_IMPORTS.get(name)
    if spec is not None:
        import importlib

        module = importlib.import_module(spec[0])
        return getattr(module, spec[1])

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
