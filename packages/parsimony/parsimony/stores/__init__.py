"""Observation data persistence (catalog persistence lives on :class:`BaseCatalog`)."""

from parsimony.stores.data_store import DataStore, LoadResult
from parsimony.stores.memory_data import InMemoryDataStore

__all__ = ["DataStore", "InMemoryDataStore", "LoadResult"]
