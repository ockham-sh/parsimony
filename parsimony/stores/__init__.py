"""Store abstractions and implementations.

Lazy ``__getattr__`` so importing the stores package is cheap:

- ``CatalogStore`` / ``DataStore`` — the ABCs, no heavy deps.
- ``HFBundleCatalogStore`` — defers pyarrow / faiss / huggingface_hub until
  first access.
- ``SQLiteCatalogStore`` — defers sqlalchemy / aiosqlite until first access.

Matches the convention in :mod:`parsimony.catalog` and :mod:`parsimony.embeddings`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from parsimony.stores.catalog_store import CatalogStore
from parsimony.stores.data_store import DataStore

if TYPE_CHECKING:
    from parsimony.stores.hf_bundle.store import HFBundleCatalogStore
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

__all__ = ["CatalogStore", "DataStore", "HFBundleCatalogStore", "SQLiteCatalogStore"]


def __getattr__(name: str) -> Any:
    if name == "HFBundleCatalogStore":
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        return HFBundleCatalogStore
    if name == "SQLiteCatalogStore":
        from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

        return SQLiteCatalogStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
