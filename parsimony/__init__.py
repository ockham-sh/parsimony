"""Parsimony — detachable catalog + connector framework.

**Contracts**

* :class:`~parsimony.connector.Connectors` is an immutable collection of
  bound :class:`~parsimony.connector.Connector` instances; callers use
  ``await connectors[name](**kwargs)`` or ``connectors.get(name)``.
  Each connector exposes a typed Pydantic param model at the boundary.
* :func:`~parsimony.connector.connector` infers each connector's name and
  description from the function name and docstring; pass ``output=`` for
  tabular :class:`~parsimony.result.OutputConfig`.
* :func:`~parsimony.connector.enumerator` builds the same
  :class:`~parsimony.connector.Connector` type with a catalog-oriented schema
  (no DATA columns; KEY with ``namespace=...``; one TITLE).
* :func:`~parsimony.connector.loader` builds the same
  :class:`~parsimony.connector.Connector` type with a data-oriented schema
  (KEY with ``namespace=...`` and DATA columns only; no TITLE/METADATA).
  :class:`~parsimony.stores.data_store.InMemoryDataStore` persists observations via
  :meth:`~parsimony.stores.data_store.InMemoryDataStore.load_result`.
* :class:`~parsimony.catalog.catalog.Catalog` orchestrates store and optional
  embeddings for indexing. Catalog identity is ``(namespace, code)``.
* Catalog helpers (:class:`SeriesEntry`, :class:`SeriesMatch`, normalization
  utilities) live in :mod:`parsimony.catalog.models` — import from there.

Optional providers under the ``[search]`` extra (``SQLiteCatalogStore``,
``LiteLLMEmbeddingProvider``) are not re-exported from the root namespace —
import them directly from their modules.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any

from parsimony.catalog.catalog import Catalog
from parsimony.connector import (
    Connector,
    Connectors,
    Namespace,
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
    SemanticTableResult,
)
from parsimony.stores.catalog_store import CatalogStore
from parsimony.stores.data_store import DataStore, InMemoryDataStore, LoadResult

try:
    __version__ = version("parsimony")
except PackageNotFoundError:
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
    "Namespace",
    # --- Result system ---
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    "SemanticTableResult",
    # --- Catalog ---
    "Catalog",
    "CatalogStore",
    "SQLiteCatalogStore",
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
    # --- Convenience ---
    "client",
]


# Convenience: `from parsimony import client` builds a ready-to-use Connectors
# collection with API keys from environment variables (cached after first access).
_client_cache: Any = None


def __getattr__(name: str) -> Any:
    global _client_cache

    if name == "client":
        if _client_cache is None:
            from parsimony.discovery import build_connectors_from_env

            _client_cache = build_connectors_from_env()
        return _client_cache

    # SQLiteCatalogStore needs sqlite-vec from the [search] extra; fail at
    # access time (with a real ImportError) rather than at package import.
    if name == "SQLiteCatalogStore":
        from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

        return SQLiteCatalogStore

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
