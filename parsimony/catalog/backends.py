"""Catalog row backends: in-memory entities and lazy parquet tables."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds

from parsimony.catalog.contracts import CatalogBackendConfig, FilterSpec
from parsimony.entity import Entity, field_value
from parsimony.errors import InvalidParameterError

DEFAULT_BATCH_SIZE = 10_000


def _row_matches_filter(row: dict[str, Any], filter_spec: FilterSpec) -> bool:
    for col, allowed in filter_spec.items():
        if not allowed:
            continue
        raw = row.get(col)
        if raw is None:
            return False
        if str(raw) not in {str(v) for v in allowed}:
            return False
    return True


class InMemoryRowBackend:
    """Eager backend over :class:`~parsimony.entity.Entity` rows."""

    def __init__(
        self,
        entities: list[Entity],
        *,
        config: CatalogBackendConfig | None = None,
    ) -> None:
        self._entities = entities
        self._config = config or CatalogBackendConfig()

    def column_names(self) -> list[str]:
        cols = {"namespace", "code", "title"}
        for entity in self._entities:
            cols.update(entity.metadata.keys())
        return sorted(cols)

    def count(self) -> int:
        return len(self._entities)

    def match_namespace(self, namespace: str) -> bool:
        if self._config.namespace is not None:
            return namespace == self._config.namespace
        return any(entity.namespace == namespace for entity in self._entities)

    def entity_at(self, index: int) -> Entity:
        return self._entities[index]

    def iter_rows(
        self,
        *,
        filter_spec: FilterSpec | None = None,
        columns: Sequence[str] | None = None,
        any_of: Mapping[str, Sequence[str]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        del columns
        allowed = {col: {str(v) for v in vals} for col, vals in (any_of or {}).items()}
        for entity in self._entities:
            row = self._entity_to_row(entity)
            if filter_spec is not None and not _row_matches_filter(row, filter_spec):
                continue
            if allowed and not any(str(row.get(col)) in vals for col, vals in allowed.items()):
                continue
            yield row

    def _entity_to_row(self, entity: Entity) -> dict[str, Any]:
        row: dict[str, Any] = {
            "namespace": entity.namespace,
            "code": entity.code,
            "title": entity.title,
        }
        row.update(entity.metadata)
        return row


class ParquetRowBackend:
    """Lazy backend over a flat parquet table."""

    def __init__(
        self,
        parquet_path: Path,
        *,
        config: CatalogBackendConfig,
    ) -> None:
        if not parquet_path.is_file():
            raise FileNotFoundError(f"Parquet rows file not found: {parquet_path}")
        self._path = parquet_path
        self._config = config
        self._dataset = ds.dataset(str(parquet_path), format="parquet")
        self._schema_names = set(self._dataset.schema.names)

    def column_names(self) -> list[str]:
        return list(self._dataset.schema.names)

    def count(self) -> int:
        return int(self._dataset.count_rows())

    def match_namespace(self, namespace: str) -> bool:
        if self._config.namespace is not None:
            return namespace == self._config.namespace
        return True

    def iter_rows(
        self,
        *,
        filter_spec: FilterSpec | None = None,
        columns: Sequence[str] | None = None,
        any_of: Mapping[str, Sequence[str]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        expr = _build_parquet_filter(filter_spec, schema_names=self._schema_names)
        or_expr = _build_any_of_filter(any_of, schema_names=self._schema_names)
        if any_of and or_expr is None:
            # Every candidate column is absent from this parquet: no row can match.
            return
        if or_expr is not None:
            expr = or_expr if expr is None else expr & or_expr
        scanner = self._dataset.scanner(
            filter=expr,
            columns=list(columns) if columns else None,
            batch_size=DEFAULT_BATCH_SIZE,
        )
        for batch in scanner.to_batches():
            yield from batch.to_pylist()


def _build_any_of_filter(
    any_of: Mapping[str, Sequence[str]] | None,
    *,
    schema_names: set[str],
) -> ds.Expression | None:
    """OR filter: a row qualifies when ANY listed column holds one of its values."""
    if not any_of:
        return None
    exprs: list[ds.Expression] = []
    for col, values in any_of.items():
        if col not in schema_names or not values:
            continue
        exprs.append(ds.field(col).isin([str(v) for v in values]))
    if not exprs:
        return None
    combined = exprs[0]
    for expr in exprs[1:]:
        combined = combined | expr
    return combined


def _build_parquet_filter(
    filter_spec: FilterSpec | None,
    *,
    schema_names: set[str],
) -> ds.Expression | None:
    if not filter_spec:
        return None
    exprs: list[ds.Expression] = []
    for col, values in filter_spec.items():
        if col not in schema_names:
            raise InvalidParameterError("catalog", f"Unknown parquet filter column {col!r}")
        if not values:
            continue
        exprs.append(ds.field(col).isin([str(v) for v in values]))
    if not exprs:
        return None
    combined = exprs[0]
    for expr in exprs[1:]:
        combined = combined & expr
    return combined


def entity_from_row(row: dict[str, Any], *, config: CatalogBackendConfig) -> Entity:
    """Materialize one :class:`~parsimony.entity.Entity` from a backend row."""
    namespace = str(row.get("namespace", config.namespace or "default"))
    code = str(row.get(config.code_column, row.get("code", ""))).strip()
    title = str(row.get(config.title_column, row.get("title", code))).strip() or code
    reserved = {
        "namespace",
        config.code_column,
        config.title_column,
        "code",
        "title",
        "metadata_json",
    }
    metadata: dict[str, Any] = {}
    for key, value in row.items():
        if key in reserved or value is None:
            continue
        metadata[key] = value
    return Entity(namespace=namespace, code=code, title=title, metadata=metadata)


def entity_matches_filter(entity: Entity, filter_spec: FilterSpec) -> bool:
    for col, allowed in filter_spec.items():
        if not allowed:
            continue
        val = field_value(entity, col)
        if val is None or str(val) not in {str(v) for v in allowed}:
            return False
    return True


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "InMemoryRowBackend",
    "ParquetRowBackend",
    "entity_from_row",
    "entity_matches_filter",
]
