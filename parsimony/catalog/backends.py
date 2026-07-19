"""Row storage for value-indexed (parquet-backed) catalogs.

A catalog searches in one of two layouts:

- **Row-indexed** (the default): the indexed entities are the rows.
  :class:`~parsimony.catalog.Catalog` grades its own entity list directly; no
  backend object exists.
- **Value-indexed** (parquet): the indexed entities are codelist *members* —
  distinct dimension values such as ``geo:DE`` "Germany" — and each parquet
  row is a *composition* of members (a series key is a tuple of them). Search
  matches members in the indexes first, then streams the rows composed with
  them from :class:`ParquetRowBackend`.

The row population of a value-indexed catalog is usually far larger than its
member population (tens of thousands of series over a handful of codelist
values per dimension); that asymmetry is why rows live in a lazy parquet scan
instead of in memory. Two caveats to the member model: a builder may also
index per-row text (e.g. a title column) as if its distinct values were
members — such an index scales with the rows, not the codelists — and the
row→member link is latent, carried by column-naming conventions and
``field_links`` rather than by the type system.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds

from parsimony.catalog.contracts import CatalogBackendConfig, FilterSpec
from parsimony.entity import Entity, field_value
from parsimony.errors import InvalidParameterError

DEFAULT_BATCH_SIZE = 10_000


class ParquetRowBackend:
    """Row store over a flat parquet file that is never itself indexed.

    Answers the second step of a value-indexed search: given the member values
    matched in the indexes, ``iter_rows(any_of=...)`` streams the rows composed
    with any of them, pushing the disjunction down into the parquet scan.
    """

    def __init__(self, parquet_path: Path) -> None:
        if not parquet_path.is_file():
            raise FileNotFoundError(f"Parquet rows file not found: {parquet_path}")
        self.path = parquet_path
        self._dataset = ds.dataset(str(parquet_path), format="parquet")
        self._schema_names = set(self._dataset.schema.names)

    def column_names(self) -> list[str]:
        return list(self._dataset.schema.names)

    def count(self) -> int:
        return int(self._dataset.count_rows())

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
    "ParquetRowBackend",
    "entity_from_row",
    "entity_matches_filter",
]
