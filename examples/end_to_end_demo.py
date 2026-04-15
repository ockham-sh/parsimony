"""End-to-end walkthrough: connectors, Result vs SemanticTableResult, catalog indexing.

Run from the repo root::

    python examples/end_to_end_demo.py

This script is self-contained (in-memory catalog store, no API keys). It shows:

1. **Raw tabular return** — DataFrame with no ``output=`` → :class:`~parsimony.result.Result`
   with ``output_schema is None``.
2. **Declared schema on the connector** — ``@connector(output=OutputConfig(...))`` →
   :class:`~parsimony.result.SemanticTableResult` (same raw columns, semantic roles applied).
3. **Late schema** — :meth:`~parsimony.result.Result.to_table` on a raw
   :class:`~parsimony.result.Result` (equivalent to connector ``output=`` for tabular data).
4. **Catalog** — :meth:`~parsimony.catalog.catalog.Catalog.index_result` reads
   ``namespace`` from the KEY column; use ``embed=False`` without an embedding provider.
5. **Post-fetch hook** — :meth:`~parsimony.connector.Connector.with_callback` or
   :meth:`~parsimony.connector.Connectors.with_callback` to auto-index
   :class:`~parsimony.result.SemanticTableResult` rows.
6. **Dry run / tags** — ``dry_run=True`` previews dedupe counts (no writes); ``extra_tags`` on ingest.

Unmapped DataFrame columns are merged as DATA when a schema is applied (connector ``output`` or
``to_table``).
"""

from __future__ import annotations

import asyncio

import pandas as pd
from pydantic import BaseModel, Field

from parsimony.catalog.catalog import Catalog
from parsimony.connector import Connector, Connectors, connector
from parsimony.result import Column, ColumnRole, OutputConfig, Result, SemanticTableResult
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

# ---------------------------------------------------------------------------
# Shared demo identity: KEY column must declare namespace for catalog indexing.
# ---------------------------------------------------------------------------

DEMO_NAMESPACE = "demo_e2e"

DEMO_TABLE_SCHEMA = OutputConfig(
    columns=[
        Column(
            name="code",
            role=ColumnRole.KEY,
            dtype="auto",
            namespace=DEMO_NAMESPACE,
        ),
        Column(name="title", role=ColumnRole.TITLE, dtype="auto"),
        Column(name="region", role=ColumnRole.METADATA, dtype="auto"),
    ]
)


class DemoFetchParams(BaseModel):
    """Typed boundary for demo connectors (no generic dict params)."""

    label: str = Field(default="demo", description="Echoed in provenance only.")


def _sample_dataframe() -> pd.DataFrame:
    """Two series rows; ``notes`` is unmapped until schema merge (becomes DATA)."""
    return pd.DataFrame(
        {
            "code": ["ALPHA", "BETA"],
            "title": ["Alpha series", "Beta series"],
            "region": ["US", "EU"],
            "notes": ["first", "second"],
        }
    )


# ---------------------------------------------------------------------------
# Connectors: three ways to get raw vs typed tabular output
# ---------------------------------------------------------------------------


@connector()
async def demo_fetch_raw(params: DemoFetchParams) -> pd.DataFrame:
    """Returns a DataFrame; framework wraps as raw Result (no output schema)."""
    _ = params
    return _sample_dataframe()


@connector(output=DEMO_TABLE_SCHEMA)
async def demo_fetch_typed(params: DemoFetchParams) -> pd.DataFrame:
    """Same DataFrame with output= → SemanticTableResult with KEY namespace for indexing."""
    _ = params
    return _sample_dataframe()


@connector()
async def demo_fetch_for_late(params: DemoFetchParams) -> pd.DataFrame:
    """Raw Result; caller applies to_table() for SemanticTableResult."""
    _ = params
    return _sample_dataframe()


def _print_unmapped_columns(table: SemanticTableResult) -> None:
    data_cols = [c.name for c in table.data_columns]
    print(f"  DATA columns (includes auto-merged unmapped): {data_cols}")


async def _section_raw_vs_typed() -> None:
    print("\n--- 1) Raw DataFrame → Result (output_schema is None) ---\n")
    raw_conn: Connector = demo_fetch_raw
    raw_res = await raw_conn(label="demo")
    assert raw_res.output_schema is None
    print(f"  type: {type(raw_res).__name__}")
    print(f"  output_schema: {raw_res.output_schema}")
    print(f"  df columns: {list(raw_res.df.columns)}")

    print("\n--- 2) Same data via @connector(output=...) → SemanticTableResult ---\n")
    typed_conn: Connector = demo_fetch_typed
    typed = await typed_conn(label="demo")
    assert isinstance(typed, SemanticTableResult)
    print(f"  type: {type(typed).__name__}")
    key_ns = next(c.namespace for c in typed.output_schema.columns if c.role == ColumnRole.KEY)
    print(f"  KEY column namespace (for catalog): {key_ns!r}")
    _print_unmapped_columns(typed)

    print("\n--- 3) Late mapping: raw Result, then .to_table(OutputConfig) ---\n")
    late_raw = await demo_fetch_for_late(label="demo")
    late_table = late_raw.to_table(DEMO_TABLE_SCHEMA)
    assert isinstance(late_table, SemanticTableResult)
    print(f"  after to_table: {type(late_table).__name__}")
    _print_unmapped_columns(late_table)


async def _section_catalog_and_callbacks() -> None:
    store = SQLiteCatalogStore(":memory:")
    catalog = Catalog(store, embeddings=None)

    typed_conn: Connector = demo_fetch_typed

    print("\n--- 4) Manual index_result(table, embed=False) ---\n")
    table = await typed_conn(label="demo")
    assert isinstance(table, SemanticTableResult)
    idx = await catalog.index_result(table, embed=False)
    print(f"  IndexResult: total={idx.total}, indexed={idx.indexed}, skipped={idx.skipped}")

    listed, total = await catalog.list_entries(namespace=DEMO_NAMESPACE, limit=10)
    print(f"  list_entries: total={total}, codes={[e.code for e in listed]}")

    print("\n--- 5) dry_run=True (dedupe preview; no store writes) ---\n")
    dry = await catalog.index_result(table, embed=False, dry_run=True)
    print(f"  IndexResult: total={dry.total}, indexed={dry.indexed}, skipped={dry.skipped} (dry_run)")

    print("\n--- 6) extra_tags on ingest (fresh store so rows are not skipped) ---\n")
    store_tagged = SQLiteCatalogStore(":memory:")
    catalog_tagged = Catalog(store_tagged, embeddings=None)
    tagged = await catalog_tagged.index_result(
        table,
        embed=False,
        extra_tags=["e2e_demo", "batch-1"],
    )
    print(f"  indexed with extra_tags: {tagged.indexed}")
    got = await store_tagged.get(DEMO_NAMESPACE, "ALPHA")
    assert got is not None
    print(f"  sample tags on entry: {got.tags}")

    print("\n--- 7) with_callback: auto-index after fetch ---\n")
    store2 = SQLiteCatalogStore(":memory:")
    catalog2 = Catalog(store2, embeddings=None)

    async def auto_index(result: Result) -> None:
        if isinstance(result, SemanticTableResult):
            await catalog2.index_result(result, embed=False)

    hooked: Connector = demo_fetch_typed.with_callback(auto_index)
    await hooked(label="demo")
    listed2, total2 = await catalog2.list_entries(namespace=DEMO_NAMESPACE, limit=10)
    print(f"  after callback: total={total2}, codes={[e.code for e in listed2]}")

    print("\n--- 8) Connectors.with_callback on a collection ---\n")
    store3 = SQLiteCatalogStore(":memory:")
    catalog3 = Catalog(store3, embeddings=None)

    async def auto_index3(result: Result) -> None:
        if isinstance(result, SemanticTableResult):
            await catalog3.index_result(result, embed=False)

    bundle = Connectors([demo_fetch_typed, demo_fetch_raw]).with_callback(auto_index3)
    await bundle["demo_fetch_typed"](label="demo")
    await bundle["demo_fetch_raw"](label="demo")
    listed3, total3 = await catalog3.list_entries(namespace=DEMO_NAMESPACE, limit=10)
    print(
        "  typed connector indexed; raw connector skipped in callback: "
        f"total={total3}, codes={[e.code for e in listed3]}"
    )

    print("\n--- 9) Text search (no embedding provider) ---\n")
    matches = await catalog.search("Beta", limit=5)
    print(f"  search('Beta'): {[m.code for m in matches]}")


async def main() -> None:
    await _section_raw_vs_typed()
    await _section_catalog_and_callbacks()
    print("\nDone.\n")


if __name__ == "__main__":
    asyncio.run(main())
