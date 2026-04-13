"""DataStore: persistence for observation tables keyed by (namespace, code)."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import pandas as pd
from pydantic import BaseModel

from parsimony.catalog.models import normalize_code, normalize_entity_code
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
        raise TypeError(
            f"load expected tabular data, got {type(table.data).__name__}"
        )
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
        raise ValueError(
            "KEY column must declare namespace=... on the schema for DataStore.load_result"
        )
    key_name = key_col.name
    if key_name not in df.columns:
        raise ValueError(
            f"SemanticTableResult missing KEY column {key_name!r}. Available: {list(df.columns)}"
        )

    data_names = [c.name for c in cols if c.role == ColumnRole.DATA]
    if not data_names:
        raise ValueError(
            "SemanticTableResult must declare at least one DATA column in output_schema "
            "for data loading"
        )
    for dn in data_names:
        if dn not in df.columns:
            raise ValueError(
                f"SemanticTableResult missing DATA column {dn!r}. Available: {list(df.columns)}"
            )

    ns = normalize_code(key_col.namespace)
    raw_codes = df[key_name].dropna().unique()
    out: list[tuple[str, str, pd.DataFrame]] = []
    for raw_code in raw_codes:
        code = normalize_entity_code(str(raw_code))
        mask = df[key_name] == raw_code
        sub = df.loc[mask, data_names].copy()
        out.append((ns, code, sub))
    return out


class DataStore(ABC):
    """Persistence for observation data, keyed by (namespace, code)."""

    @abstractmethod
    async def upsert(self, namespace: str, code: str, df: pd.DataFrame) -> None:
        """Insert or replace observation data for one entity."""

    @abstractmethod
    async def get(self, namespace: str, code: str) -> pd.DataFrame | None:
        """Retrieve stored observations, or None if not loaded."""

    @abstractmethod
    async def delete(self, namespace: str, code: str) -> None:
        """Remove stored observations for one entity."""

    @abstractmethod
    async def exists(self, keys: list[tuple[str, str]]) -> set[tuple[str, str]]:
        """Return the subset of (namespace, code) pairs that have stored data."""

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
            except Exception:
                logger.exception("DataStore upsert failed for (%s, %s)", ns, code)
                result.errors += 1
        return result
