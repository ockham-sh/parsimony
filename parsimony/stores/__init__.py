"""Data-store abstractions for observation persistence.

The catalog is no longer a ``Store`` concern — the canonical
:class:`parsimony.Catalog` owns its own persistence. This module now holds
only the data-oriented store used by ``@loader`` connectors.
"""

from __future__ import annotations

from parsimony.stores.data_store import DataStore, InMemoryDataStore, LoadResult

__all__ = ["DataStore", "InMemoryDataStore", "LoadResult"]
