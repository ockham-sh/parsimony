"""InMemoryDataStore: observation tables keyed by (namespace, code).

Single concrete implementation today, hence the explicit name. When a
second implementation lands (SQLite, Parquet, …), extract a ``DataStore``
Protocol from the public method set — Python's structural typing keeps
that cheap and lets the Protocol reclaim the generic name.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import pandas as pd
from pydantic import BaseModel

from parsimony.entity import entity_key
from parsimony.result import Result

logger = logging.getLogger(__name__)


class LoadResult(BaseModel):
    """Statistics from a data load run."""

    total: int = 0
    loaded: int = 0
    skipped: int = 0
    errors: int = 0


class InMemoryDataStore:
    """Process-local observation store: dict-backed (namespace, code) → DataFrame."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], pd.DataFrame] = {}

    def upsert(self, namespace: str, code: str, df: pd.DataFrame) -> None:
        """Insert or replace observation data for one entity."""
        k = entity_key(namespace, code)
        self._rows[k] = df.copy()

    def get(self, namespace: str, code: str) -> pd.DataFrame | None:
        """Retrieve stored observations, or None if not loaded."""
        k = entity_key(namespace, code)
        stored = self._rows.get(k)
        if stored is None:
            return None
        return stored.copy()

    def delete(self, namespace: str, code: str) -> None:
        """Remove stored observations for one entity."""
        k = entity_key(namespace, code)
        self._rows.pop(k, None)

    def exists(self, keys: Sequence[tuple[str, str]]) -> set[tuple[str, str]]:
        """Return the subset of (namespace, code) pairs that have stored data."""
        out: set[tuple[str, str]] = set()
        for ns, c in keys:
            k = entity_key(ns, c)
            if k in self._rows:
                out.add(k)
        return out

    def load_result(
        self,
        table: Result,
        *,
        force: bool = False,
    ) -> LoadResult:
        """Persist each entity's DATA rows from *table*'s entity projection.

        With ``force=False``, skip entities already present in the store. With
        ``force=True``, upsert all entities. Delegates identity and grouping
        entirely to :attr:`Result.data` — no second grouping pass here.
        """
        result = LoadResult()
        data = table.data
        result.total = len(data)
        if not data:
            return result

        keys = list(data)
        existing: set[tuple[str, str]] = set() if force else self.exists(keys)

        for (ns, code), frame in data.items():
            if not force and (ns, code) in existing:
                result.skipped += 1
                continue
            try:
                self.upsert(ns, code, frame)
                result.loaded += 1
            except (OSError, RuntimeError, ValueError, TypeError) as exc:
                logger.warning("InMemoryDataStore upsert failed for (%s, %s): %s", ns, code, exc)
                result.errors += 1
        return result


__all__ = ["InMemoryDataStore", "LoadResult"]
