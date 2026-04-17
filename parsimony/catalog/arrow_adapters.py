"""Pure transforms between :class:`SeriesEntry` and the bundle Parquet schema.

These functions are the single round-trip contract shared by the bundle
builder (writer) and :class:`~parsimony.stores.hf_bundle.store.HFBundleCatalogStore`
(reader). The canonical invariant is that Parquet ``row_id`` is dense
``0..N-1`` and matches FAISS vector position ``i`` for every row ``i``.

Two reader entry points with explicit semantics:

- :func:`arrow_table_to_entries` — full-table load. Validates schema AND
  that ``row_id`` is dense 0..N-1. Used at bundle-load time.
- :func:`arrow_rows_to_entries` — point-lookup subset (from ``Table.take``
  or ``filter``). Validates schema only; row_id is expected to be arbitrary.

No I/O, no FAISS, no HF — these are pure structural transforms.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from parsimony.catalog.models import SeriesEntry
from parsimony.stores.hf_bundle.errors import BundleIntegrityError
from parsimony.stores.hf_bundle.format import ENTRIES_PARQUET_SCHEMA


def entries_to_arrow_table(entries: list[SeriesEntry]) -> pa.Table:
    """Serialize catalog rows to an Arrow table matching :data:`ENTRIES_PARQUET_SCHEMA`.

    Assigns dense ``row_id`` ``0..N-1`` in the order given, which becomes the
    FAISS vector position for downstream indexing. Callers that want a
    specific FAISS row order must order *entries* before calling.

    ``metadata`` and ``properties`` are serialized to JSON strings so the
    Parquet schema stays stable across entries with heterogeneous keys.
    """
    namespaces: list[str] = []
    codes: list[str] = []
    titles: list[str] = []
    descriptions: list[str | None] = []
    tags: list[list[str]] = []
    metadata_json: list[str] = []
    properties_json: list[str] = []
    observable_ids: list[str | None] = []
    row_ids: list[int] = []

    for i, e in enumerate(entries):
        namespaces.append(e.namespace)
        codes.append(e.code)
        titles.append(e.title)
        descriptions.append(e.description)
        tags.append(list(e.tags))
        metadata_json.append(json.dumps(e.metadata, sort_keys=True, default=_json_default))
        properties_json.append(json.dumps(e.properties, sort_keys=True, default=_json_default))
        observable_ids.append(e.observable_id)
        row_ids.append(i)

    return pa.table(
        {
            "namespace": namespaces,
            "code": codes,
            "title": titles,
            "description": descriptions,
            "tags": tags,
            "metadata": metadata_json,
            "properties": properties_json,
            "observable_id": observable_ids,
            "row_id": row_ids,
        },
        schema=ENTRIES_PARQUET_SCHEMA,
    )


def arrow_table_to_entries(
    table: pa.Table,
    *,
    namespace: str | None = None,
) -> list[SeriesEntry]:
    """Full-bundle load: schema validation + dense ``row_id`` 0..N-1 check.

    Validates ``row_id`` matches FAISS vector position — the invariant that
    the whole bundle rests on. Use this once per bundle load; use
    :func:`arrow_rows_to_entries` for point-lookup subsets.
    """
    _validate_schema(table)
    table = table.sort_by("row_id")
    row_ids = table.column("row_id").to_pylist()
    for i, rid in enumerate(row_ids):
        if rid != i:
            raise BundleIntegrityError(
                f"row_id at position {i} is {rid}, expected {i}; "
                f"Parquet row_id must be dense 0..{len(row_ids) - 1} and match FAISS positions",
                namespace=namespace,
            )
    return _hydrate_rows(table, namespace=namespace)


def arrow_rows_to_entries(
    table: pa.Table,
    *,
    namespace: str | None = None,
) -> list[SeriesEntry]:
    """Point-lookup hydration: schema validation only, ``row_id`` unchecked.

    Use for subsets produced by :meth:`pyarrow.Table.take` or
    :meth:`pyarrow.Table.filter`, where row order is arbitrary by design.
    """
    _validate_schema(table)
    return _hydrate_rows(table, namespace=namespace)


def _hydrate_rows(table: pa.Table, *, namespace: str | None) -> list[SeriesEntry]:
    namespaces = table.column("namespace").to_pylist()
    if namespace is not None:
        for i, ns in enumerate(namespaces):
            if ns != namespace:
                raise BundleIntegrityError(
                    f"Row {i} has namespace={ns!r} but bundle namespace is {namespace!r}",
                    namespace=namespace,
                )

    codes = table.column("code").to_pylist()
    titles = table.column("title").to_pylist()
    descriptions = table.column("description").to_pylist()
    tags_col = table.column("tags").to_pylist()
    metadata_col = table.column("metadata").to_pylist()
    properties_col = table.column("properties").to_pylist()
    observable_ids = table.column("observable_id").to_pylist()

    entries: list[SeriesEntry] = []
    for i in range(len(namespaces)):
        entries.append(
            SeriesEntry(
                namespace=namespaces[i],
                code=codes[i],
                title=titles[i],
                description=descriptions[i],
                tags=list(tags_col[i] or []),
                metadata=json.loads(metadata_col[i] or "{}"),
                properties=json.loads(properties_col[i] or "{}"),
                observable_id=observable_ids[i],
            )
        )
    return entries


def _validate_schema(table: pa.Table) -> None:
    expected = {f.name: f.type for f in ENTRIES_PARQUET_SCHEMA}
    actual = {f.name: f.type for f in table.schema}
    missing = set(expected) - set(actual)
    if missing:
        raise BundleIntegrityError(f"Parquet table is missing required columns: {sorted(missing)}")
    for name, expected_type in expected.items():
        if actual[name] != expected_type:
            raise BundleIntegrityError(f"Parquet column {name!r} has type {actual[name]!s}, expected {expected_type!s}")


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


__all__ = [
    "arrow_rows_to_entries",
    "arrow_table_to_entries",
    "entries_to_arrow_table",
]
