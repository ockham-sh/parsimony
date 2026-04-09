"""Ockham Data — detachable catalog + connector framework.

Public surface (import from ``ockham`` directly). Heavy symbols that pull in
optional stacks use lazy loading via :func:`__getattr__` to keep core imports light.

**Contracts**

* :class:`~ockham.connector.Connectors` is an immutable collection of
  bound :class:`~ockham.connector.Connector` instances; callers use
  ``await connectors[name](**kwargs)`` or ``connectors.get(name)``.
  Each connector exposes a typed Pydantic param model at the boundary.
  Post-fetch hooks attach per connector via :meth:`~ockham.connector.Connector.with_callback`
  or to every connector in a collection via :meth:`~ockham.connector.Connectors.with_callback`.
* :func:`~ockham.connector.connector` infers each connector's name and description from the
  function name and docstring; pass ``output=`` for tabular :class:`~ockham.result.OutputConfig`.
* :func:`~ockham.connector.enumerator` builds the same :class:`~ockham.connector.Connector`
  type with a catalog-oriented schema (no DATA columns; KEY with ``namespace=...``; one TITLE).
* :func:`~ockham.connector.loader` builds the same :class:`~ockham.connector.Connector`
  type with a data-oriented schema (KEY with ``namespace=...`` and DATA columns only; no TITLE/METADATA).
  :class:`~ockham.data_store.DataStore` persists observations via :meth:`~ockham.data_store.DataStore.load_result`.
* :class:`~ockham.catalog.catalog.Catalog` orchestrates store and optional
  embeddings for indexing (:meth:`~ockham.catalog.catalog.Catalog.ingest`).
  :meth:`~ockham.catalog.store.CatalogStore.search` is implementation-defined on the store.
  Catalog identity is ``(namespace, code)``; ``code`` is the connector-native identifier for that namespace.
  :meth:`~ockham.catalog.catalog.Catalog.index_result` builds :class:`SeriesEntry` rows from a
  :class:`~ockham.result.SemanticTableResult`. The catalog namespace comes from ``namespace=...`` on the
  KEY column in :class:`~ockham.result.OutputConfig`. Use ``conn.with_callback(catalog.index_result)``
  for auto-indexing after fetch.
* Optional :class:`~ockham.connector.Namespace` metadata on a param field
  documents which catalog namespace supplies valid values for that field.
* :class:`~ockham.catalog.models.SeriesEntry.observable_id` is reserved
  for a future knowledge-layer link; the framework does not interpret it yet.
"""

from __future__ import annotations

from typing import Any

from ockham.catalog.models import (
    EmbeddingProvider,
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
    normalize_series_catalog_row,
    series_match_from_entry,
)
from ockham.stores.catalog_store import CatalogStore
from ockham.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
    SemanticTableResult,
)

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
]


def __getattr__(name: str) -> Any:
    # Core abstractions
    if name == "Connector":
        from ockham.connector import Connector

        return Connector
    if name == "Connectors":
        from ockham.connector import Connectors

        return Connectors
    if name == "ResultCallback":
        from ockham.connector import ResultCallback

        return ResultCallback
    if name == "connector":
        from ockham.connector import connector

        return connector
    if name == "enumerator":
        from ockham.connector import enumerator

        return enumerator
    if name == "loader":
        from ockham.connector import loader

        return loader
    if name == "Namespace":
        from ockham.connector import Namespace

        return Namespace
    # Catalog
    if name == "Catalog":
        from ockham.catalog.catalog import Catalog

        return Catalog
    if name == "LiteLLMEmbeddingProvider":
        from ockham.embeddings.litellm import LiteLLMEmbeddingProvider

        return LiteLLMEmbeddingProvider
    if name == "SQLiteCatalogStore":
        from ockham.stores.sqlite_catalog import SQLiteCatalogStore

        return SQLiteCatalogStore
    if name == "InMemoryDataStore":
        from ockham.stores.memory_data import InMemoryDataStore

        return InMemoryDataStore
    if name == "DataStore":
        from ockham.stores.data_store import DataStore

        return DataStore
    if name == "LoadResult":
        from ockham.stores.data_store import LoadResult

        return LoadResult
    if name == "code_token":
        from ockham.catalog.models import code_token

        return code_token
    if name == "build_embedding_text":
        from ockham.catalog.catalog import build_embedding_text

        return build_embedding_text
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
