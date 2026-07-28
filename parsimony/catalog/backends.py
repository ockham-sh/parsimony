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

from parsimony.catalog.contracts import CatalogBackendConfig
from parsimony.catalog.filters import Filter
from parsimony.entity import Entity

DEFAULT_BATCH_SIZE = 10_000


class ParquetRowBackend:
    """Row store over a flat parquet file that is never itself indexed.

    Answers the second step of a value-indexed search: given the member values
    matched in the indexes, ``iter_rows(expression=...)`` streams the rows
    composed with any of them, pushing the predicate down into the parquet scan.
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
        expression: Filter | None = None,
        columns: Sequence[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream rows matching the pushed-down predicate, in one scan.

        *expression* is the whole filter contract — one tree, compiled once to
        Arrow, so however many equality, membership, AND and OR terms the caller
        composed, the rows are read exactly once.
        """
        scanner = self._dataset.scanner(
            filter=None if expression is None else expression.to_arrow(self._schema_names),
            columns=list(columns) if columns else None,
            batch_size=DEFAULT_BATCH_SIZE,
        )
        for batch in scanner.to_batches():
            yield from batch.to_pylist()


def row_identity(row: Mapping[str, Any], *, config: CatalogBackendConfig) -> tuple[str, str]:
    """The ``(namespace, code)`` identity of a backend row, without materializing it.

    Ranking needs identity for every candidate row but a validated
    :class:`~parsimony.entity.Entity` only for the winners, so this is the one
    implementation both paths share.
    """
    namespace = str(row.get("namespace", config.namespace or "default"))
    code = str(row.get(config.code_column, row.get("code", ""))).strip()
    return namespace, code


def entity_from_row(row: dict[str, Any], *, config: CatalogBackendConfig) -> Entity:
    """Materialize one :class:`~parsimony.entity.Entity` from a backend row."""
    namespace, code = row_identity(row, config=config)
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


def entity_row(entity: Entity) -> dict[str, Any]:
    """Flatten one entity into a row mapping a :class:`Filter` can evaluate.

    Entity fields win over same-named metadata keys, matching
    :func:`parsimony.entity.field_value`. Nested metadata values remain nested
    for display; structured filters require scalar cells.
    """
    row: dict[str, Any] = dict(entity.metadata)
    row["namespace"] = entity.namespace
    row["code"] = entity.code
    row["title"] = entity.title
    return row


def entity_field_names(entities: Sequence[Entity]) -> frozenset[str]:
    """Every field name addressable on *entities* (identity fields + metadata keys)."""
    names = {"namespace", "code", "title"}
    for entity in entities:
        names.update(entity.metadata.keys())
    return frozenset(names)


__all__ = [
    "ParquetRowBackend",
    "entity_field_names",
    "entity_from_row",
    "entity_row",
    "row_identity",
]
