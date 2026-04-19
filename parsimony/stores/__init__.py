"""Store abstractions and implementations.

Lazy ``__getattr__`` so importing the stores package is cheap. Sub-modules
with heavy deps (``SQLiteCatalogStore`` / ``HFBundleCatalogStore``) defer
sqlalchemy / faiss / huggingface_hub / pyarrow until first access.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from parsimony.stores.catalog_store import CatalogStore
from parsimony.stores.data_store import DataStore, InMemoryDataStore, LoadResult

if TYPE_CHECKING:
    from parsimony.stores.hf_bundle import HFBundleCatalogStore
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

__all__ = [
    "CatalogStore",
    "DataStore",
    "HFBundleCatalogStore",
    "InMemoryDataStore",
    "LoadResult",
    "SQLiteCatalogStore",
]


def __getattr__(name: str) -> Any:
    if name == "SQLiteCatalogStore":
        from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

        return SQLiteCatalogStore
    if name == "HFBundleCatalogStore":
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        return HFBundleCatalogStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
