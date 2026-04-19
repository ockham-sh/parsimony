"""In-memory data store for development, tests, and local tooling."""

from __future__ import annotations

import pandas as pd

from parsimony.catalog.models import catalog_key
from parsimony.stores.data_store import DataStore


class InMemoryDataStore(DataStore):
    """Process-local observation store: dict-backed (namespace, code) → DataFrame."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], pd.DataFrame] = {}

    async def upsert(self, namespace: str, code: str, df: pd.DataFrame) -> None:
        k = catalog_key(namespace, code)
        self._rows[k] = df.copy()

    async def get(self, namespace: str, code: str) -> pd.DataFrame | None:
        k = catalog_key(namespace, code)
        stored = self._rows.get(k)
        if stored is None:
            return None
        return stored.copy()

    async def delete(self, namespace: str, code: str) -> None:
        k = catalog_key(namespace, code)
        self._rows.pop(k, None)

    async def exists(self, keys: list[tuple[str, str]]) -> set[tuple[str, str]]:
        out: set[tuple[str, str]] = set()
        for ns, c in keys:
            k = catalog_key(ns, c)
            if k in self._rows:
                out.add(k)
        return out
