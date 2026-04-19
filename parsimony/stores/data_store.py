"""InMemoryDataStore: observation tables keyed by (namespace, code).

Single concrete implementation today, hence the explicit name. When a
second implementation lands (SQLite, Parquet, …), extract a ``DataStore``
Protocol from the public method set — Python's structural typing keeps
that cheap and lets the Protocol reclaim the generic name.

The ``DataStore`` alias below is kept for one cleanup cycle so downstream
plugins importing ``from parsimony import DataStore`` keep working.
"""

from __future__ import annotations

import logging

import pandas as pd
from pydantic import BaseModel

from parsimony.catalog.models import catalog_key, normalize_code, normalize_entity_code
from parsimony.result import ColumnRole, SemanticTableResult

logger = logging.getLogger(__name__)


class LoadResult(BaseModel):
    """Statistics from a data load run."""

    total: int = 0
    loaded: int = 0
    skipped: int = 0
    errors: int = 0


def _data_from_table_result(table: SemanticTableResult) -> list[tuple[str, str, pd.DataFrame]]:
    """Extract (namespace, code, data_frame) per distinct KEY value.

    Namespace comes from the KEY column's ``namespace=...``. The returned DataFrame contains
    only DATA columns; KEY is consumed for identity.
    """
    if not isinstance(table.data, (pd.DataFrame, pd.Series)):
        raise TypeError(f"load expected tabular data, got {type(table.data).__name__}")
    df = table.df
    if df.empty:
        return []

    cols = table.output_schema.columns
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(
            "SemanticTableResult must have exactly one KEY column in output_schema for "
            f"data loading, found {len(key_cols)}"
        )
    key_col = key_cols[0]
    if not key_col.namespace:
        raise ValueError("KEY column must declare namespace=... on the schema for DataStore.load_result")
    key_name = key_col.name
    if key_name not in df.columns:
        raise ValueError(f"SemanticTableResult missing KEY column {key_name!r}. Available: {list(df.columns)}")

    data_names = [c.name for c in cols if c.role == ColumnRole.DATA]
    if not data_names:
        raise ValueError("SemanticTableResult must declare at least one DATA column in output_schema for data loading")
    for dn in data_names:
        if dn not in df.columns:
            raise ValueError(f"SemanticTableResult missing DATA column {dn!r}. Available: {list(df.columns)}")

    ns = normalize_code(key_col.namespace)
    raw_codes = df[key_name].dropna().unique()
    out: list[tuple[str, str, pd.DataFrame]] = []
    for raw_code in raw_codes:
        code = normalize_entity_code(str(raw_code))
        mask = df[key_name] == raw_code
        sub = df.loc[mask, data_names].copy()
        out.append((ns, code, sub))
    return out


class InMemoryDataStore:
    """Process-local observation store: dict-backed (namespace, code) → DataFrame."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], pd.DataFrame] = {}

    async def upsert(self, namespace: str, code: str, df: pd.DataFrame) -> None:
        """Insert or replace observation data for one entity."""
        k = catalog_key(namespace, code)
        self._rows[k] = df.copy()

    async def get(self, namespace: str, code: str) -> pd.DataFrame | None:
        """Retrieve stored observations, or None if not loaded."""
        k = catalog_key(namespace, code)
        stored = self._rows.get(k)
        if stored is None:
            return None
        return stored.copy()

    async def delete(self, namespace: str, code: str) -> None:
        """Remove stored observations for one entity."""
        k = catalog_key(namespace, code)
        self._rows.pop(k, None)

    async def exists(self, keys: list[tuple[str, str]]) -> set[tuple[str, str]]:
        """Return the subset of (namespace, code) pairs that have stored data."""
        out: set[tuple[str, str]] = set()
        for ns, c in keys:
            k = catalog_key(ns, c)
            if k in self._rows:
                out.add(k)
        return out

    async def load_result(
        self,
        table: SemanticTableResult,
        *,
        force: bool = False,
    ) -> LoadResult:
        """Extract DATA columns from *table* and persist each entity.

        With ``force=False``, skip entities already present in the store. With ``force=True``,
        upsert all entities.
        """
        result = LoadResult()
        rows = _data_from_table_result(table)
        result.total = len(rows)
        if not rows:
            return result

        keys = [(ns, code) for ns, code, _ in rows]
        if force:
            existing: set[tuple[str, str]] = set()
        else:
            existing = await self.exists(keys)

        for ns, code, sub_df in rows:
            k = (normalize_code(ns), normalize_entity_code(code))
            if not force and k in existing:
                result.skipped += 1
                continue
            try:
                await self.upsert(ns, code, sub_df)
                result.loaded += 1
            except (OSError, RuntimeError, ValueError, TypeError) as exc:
                logger.warning("InMemoryDataStore upsert failed for (%s, %s): %s", ns, code, exc)
                result.errors += 1
        return result


# Transitional alias. When a second implementation arrives, replace this
# line with a `DataStore` Protocol extracted from `InMemoryDataStore`'s
# public methods.
DataStore = InMemoryDataStore
