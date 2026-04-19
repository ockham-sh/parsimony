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
  entry-point group (see :mod:`parsimony.discovery`). The catalog has no
  plugin axis: custom backends subclass :class:`BaseCatalog` directly.
* ``CONTRACT_VERSION`` is the plugin ABI pin; see ``docs/contract.md``.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any

from parsimony.catalog.catalog import BaseCatalog
from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    code_token,
    normalize_code,
    series_match_from_entry,
)
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
from parsimony.stores.data_store import DataStore, InMemoryDataStore, LoadResult

try:
    __version__ = version("parsimony-core")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

#: Plugin contract ABI version. Bumped only when a stable symbol breaks; see
#: ``docs/contract.md`` §2.
CONTRACT_VERSION = "1"

__all__ = [
    # --- Meta ---
    "CONTRACT_VERSION",
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
    "catalog_key",
    "code_token",
    "normalize_code",
    "series_match_from_entry",
    # --- Convenience ---
    "client",
]


# Heavy symbols — loaded lazily via PEP 562 so ``import parsimony`` does not
# pull torch / faiss / huggingface-hub. Keys are the public attribute names;
# values are ``(module, attribute)``.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Catalog": ("parsimony._standard", "Catalog"),
    "EmbeddingProvider": ("parsimony._standard.embedder", "EmbeddingProvider"),
    "SentenceTransformerEmbedder": ("parsimony._standard.embedder", "SentenceTransformerEmbedder"),
    "LiteLLMEmbeddingProvider": ("parsimony._standard.embedder", "LiteLLMEmbeddingProvider"),
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
