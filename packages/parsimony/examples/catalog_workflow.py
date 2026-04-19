"""Catalog workflow: enumerate, index, and search FRED series.

Demonstrates the full catalog lifecycle:
1. Enumerate all series in FRED Release 10 (Employment Situation).
2. Index the result into a :class:`parsimony.Catalog`.
3. Search the catalog by text query.
4. List entries and namespaces.
5. Save the catalog to disk and reload it.

Setup:
    pip install 'parsimony-core[standard]'
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
import tempfile
from pathlib import Path

from parsimony import Catalog
from parsimony.connectors.fred import enumerate_fred_release


async def main() -> None:
    api_key = os.environ["FRED_API_KEY"]

    enumerator = enumerate_fred_release.bind_deps(api_key=api_key)
    result = await enumerator(release_id=10)
    print(f"Enumerated {len(result.df)} series from FRED Release 10")
    print(result.df[["series_id", "title"]].head(5).to_string(index=False))
    print()

    catalog = Catalog("fred")
    index_summary = await catalog.index_result(result)
    print(f"Indexed: {index_summary.indexed}, skipped: {index_summary.skipped}")
    print()

    dry = await catalog.index_result(result, dry_run=True)
    print(f"Dry run: would index {dry.indexed}, skip {dry.skipped}")
    print()

    matches = await catalog.search("unemployment rate", limit=5)
    print("--- Search: 'unemployment rate' ---")
    for m in matches:
        print(f"  {m.namespace}/{m.code}: {m.title}")
    print()

    namespaces = await catalog.list_namespaces()
    print(f"Namespaces: {namespaces}")

    entries, total = await catalog.list(namespace="fred", limit=5)
    print(f"Entries in 'fred' namespace: {total} total, showing first {len(entries)}")
    for e in entries:
        print(f"  {e.code}: {e.title}")
    print()

    with tempfile.TemporaryDirectory() as tmp:
        snapshot = Path(tmp) / "fred_release_10"
        await catalog.push(f"file://{snapshot}")
        reloaded = await Catalog.from_url(f"file://{snapshot}")
        print(f"Reloaded {len(reloaded)} entries from {snapshot}")


if __name__ == "__main__":
    asyncio.run(main())
