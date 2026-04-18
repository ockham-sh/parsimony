"""Store abstractions and implementations.

Lazy ``__getattr__`` so importing the stores package is cheap:

- ``CatalogStore`` / ``DataStore`` — the ABCs, no heavy deps.
- ``SQLiteCatalogStore`` — defers sqlalchemy / aiosqlite until first access.
- ``HFBundleCatalogStore`` — defers faiss / huggingface_hub / pyarrow until
  first access. Reads Parquet + FAISS bundles published to the HF Hub.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from parsimony.stores.catalog_store import CatalogStore
from parsimony.stores.data_store import DataStore

if TYPE_CHECKING:
    from parsimony.stores.hf_bundle import HFBundleCatalogStore
    from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

__all__ = ["CatalogStore", "DataStore", "HFBundleCatalogStore", "SQLiteCatalogStore"]


def __getattr__(name: str) -> Any:
    if name == "SQLiteCatalogStore":
        from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

        return SQLiteCatalogStore
    if name == "HFBundleCatalogStore":
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        return HFBundleCatalogStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
