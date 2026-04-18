"""Catalog workflow: enumerate, index, and search FRED series.

Demonstrates the full catalog lifecycle:
1. Enumerate all series in FRED Release 10 (Employment Situation).
2. Index the result into a Catalog backed by SQLiteCatalogStore.
3. Search the catalog by text query.
4. List entries and namespaces.

Setup:
    pip install parsimony-core
    export FRED_API_KEY="your-key-here"

Expected output:
    IndexResult showing how many series were indexed, followed by search
    matches for "unemployment rate" with namespace/code/title.

Run:
    python examples/catalog_workflow.py
"""

from __future__ import annotations

import asyncio
import os

from parsimony_fred import enumerate_fred_release

from parsimony import Catalog, SQLiteCatalogStore


async def main() -> None:
    api_key = os.environ["FRED_API_KEY"]

    # --- 1. Enumerate series in the Employment Situation release ---
    enumerator = enumerate_fred_release.bind_deps(api_key=api_key)
    result = await enumerator(release_id=10)
    print(f"Enumerated {len(result.df)} series from FRED Release 10")
    print(result.df[["series_id", "title"]].head(5).to_string(index=False))
    print()

    # --- 2. Create a catalog and index the enumeration result ---
    store = SQLiteCatalogStore(":memory:")
    catalog = Catalog(store, embeddings=None)

    # embed=False because we have no embedding provider; text search still works.
    index_summary = await catalog.index_result(result, embed=False)
    print(f"Indexed: {index_summary.indexed}, skipped: {index_summary.skipped}")
    print()

    # --- 3. Preview with dry_run before a second pass ---
    dry = await catalog.index_result(result, embed=False, dry_run=True)
    print(f"Dry run: would index {dry.indexed}, skip {dry.skipped}")
    print()

    # --- 4. Search the catalog ---
    matches = await catalog.search("unemployment rate", limit=5, namespaces=["fred"])
    print("--- Search: 'unemployment rate' ---")
    for m in matches:
        print(f"  {m.namespace}/{m.code}: {m.title}")
    print()

    # --- 5. List namespaces and entries ---
    namespaces = await catalog.list_namespaces()
    print(f"Namespaces: {namespaces}")

    entries, total = await catalog.list_entries(namespace="fred", limit=5)
    print(f"Entries in 'fred' namespace: {total} total, showing first {len(entries)}")
    for e in entries:
        print(f"  {e.code}: {e.title}")


if __name__ == "__main__":
    asyncio.run(main())
