"""Build a SQLite catalog by running enumerators against live APIs.

Runs each provider's ``@enumerator`` connector to discover series from the
actual data sources, then indexes the results into a
:class:`~ockham.stores.sqlite.SQLiteCatalogStore`.

Usage::

    python -m ockham.catalog.builder output.db
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from ockham.catalog.catalog import SeriesCatalog, _entries_from_table_result
from ockham.stores.sqlite import SQLiteCatalogStore


async def build_catalog(db_path: Path, *, env: dict[str, str] | None = None) -> int:
    """Run all available enumerators and populate a SQLite catalog.

    Returns the total number of entries indexed.
    """
    import os

    _env = env if env is not None else dict(os.environ)

    store = SQLiteCatalogStore(db_path)
    catalog = SeriesCatalog(store, embeddings=None)
    total = 0

    enumerators = _collect_enumerators(_env)

    for name, conn, params in enumerators:
        try:
            print(f"  {name}...", end="", flush=True)  # noqa: T201
            result = await conn(params)
            entries = _entries_from_table_result(result)
            if entries:
                idx_result = await catalog.ingest(entries, embed=False, force=True)
                print(f" {idx_result.indexed:,} entries")  # noqa: T201
                total += idx_result.indexed
            else:
                print(" 0 entries")  # noqa: T201
        except Exception as exc:
            print(f" FAILED: {exc}")  # noqa: T201

    await store.close()
    return total


def _collect_enumerators(env: dict[str, str]) -> list[tuple[str, object, object]]:
    """Collect all enumerator connectors with their default params."""
    enumerators: list[tuple[str, object, object]] = []

    # Treasury (no auth)
    try:
        from ockham.connectors.treasury import TreasuryEnumerateParams, enumerate_treasury

        enumerators.append(("treasury", enumerate_treasury, TreasuryEnumerateParams()))
    except ImportError:
        pass

    # Riksbank (optional API key)
    try:
        from ockham.connectors.riksbank import RiksbankEnumerateParams, enumerate_riksbank

        conn = enumerate_riksbank.bind_deps(api_key=env.get("RIKSBANK_API_KEY", ""))
        enumerators.append(("riksbank", conn, RiksbankEnumerateParams()))
    except ImportError:
        pass

    # SNB (no auth)
    try:
        from ockham.connectors.snb import SnbEnumerateParams, enumerate_snb

        enumerators.append(("snb", enumerate_snb, SnbEnumerateParams()))
    except ImportError:
        pass

    # EIA (requires API key)
    eia_key = env.get("EIA_API_KEY")
    if eia_key:
        try:
            from ockham.connectors.eia import EiaEnumerateParams, enumerate_eia

            conn = enumerate_eia.bind_deps(api_key=eia_key)
            enumerators.append(("eia", conn, EiaEnumerateParams()))
        except ImportError:
            pass

    # BOE (no auth)
    try:
        from ockham.connectors.boe import BoeEnumerateParams, enumerate_boe

        enumerators.append(("boe", enumerate_boe, BoeEnumerateParams()))
    except ImportError:
        pass

    # RBA (no auth, curl_cffi optional)
    try:
        from ockham.connectors.rba import RbaEnumerateParams, enumerate_rba

        enumerators.append(("rba", enumerate_rba, RbaEnumerateParams()))
    except ImportError:
        pass

    # Destatis (guest creds)
    try:
        from ockham.connectors.destatis import DestatisEnumerateParams, enumerate_destatis

        conn = enumerate_destatis.bind_deps(
            username=env.get("DESTATIS_USERNAME", "GAST"),
            password=env.get("DESTATIS_PASSWORD", "GAST"),
        )
        enumerators.append(("destatis", conn, DestatisEnumerateParams()))
    except ImportError:
        pass

    return enumerators


# ---------------------------------------------------------------------------
# Compare (TEMPORARY — remove once enumerators are validated)
# ---------------------------------------------------------------------------


async def compare_catalog(db_path: Path, csv_dir: Path) -> None:
    """Compare a built SQLite catalog against legacy CSV index files.

    TEMPORARY: used during migration to validate enumerator coverage
    against the plotwise_mcp CSV indexes.  Remove once all enumerators
    are confirmed to match or exceed legacy coverage.

    Prints per-namespace coverage: live count, legacy count, missing, extra.
    """
    import csv as csv_mod
    import json

    store = SQLiteCatalogStore(db_path)
    namespaces = await store.list_namespaces()

    csv_files = {f.stem: f for f in csv_dir.glob("*.csv")}

    print(f"\n{'Namespace':<20} {'Live':>8} {'Legacy':>8} {'Missing':>8} {'Extra':>8}")  # noqa: T201
    print("-" * 60)  # noqa: T201

    for ns in sorted(set(namespaces) | set(csv_files.keys())):
        live_entries, _ = await store.list(namespace=ns, limit=1_000_000)
        live_codes = {e.code for e in live_entries}

        legacy_codes: set[str] = set()
        if ns in csv_files:
            with csv_files[ns].open(newline="", encoding="utf-8") as f:
                for row in csv_mod.DictReader(f):
                    code = row.get("series_id", "").strip()
                    if code:
                        legacy_codes.add(code)

        missing = legacy_codes - live_codes
        extra = live_codes - legacy_codes

        print(  # noqa: T201
            f"{ns:<20} {len(live_codes):>8,} {len(legacy_codes):>8,} "
            f"{len(missing):>8,} {len(extra):>8,}"
        )
        if missing:
            samples = sorted(missing)[:5]
            print(f"  sample missing: {', '.join(samples)}")  # noqa: T201

    await store.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    if len(sys.argv) < 2:
        print(  # noqa: T201
            "Usage:\n"
            "  python -m ockham.catalog.builder build <output.db>\n"
            "  python -m ockham.catalog.builder compare <catalog.db> <legacy_csv_dir>  (temporary)"
        )
        sys.exit(1)

    command = sys.argv[1]

    if command == "build":
        if len(sys.argv) < 3:
            print("Usage: python -m ockham.catalog.builder build <output.db>")  # noqa: T201
            sys.exit(1)
        db_path = Path(sys.argv[2])
        print(f"Building catalog from live enumerators -> {db_path}")  # noqa: T201
        total = asyncio.run(build_catalog(db_path))
        print(f"\nDone: {total:,} total entries indexed")  # noqa: T201

    elif command == "compare":
        # TEMPORARY — remove once enumerators are validated
        if len(sys.argv) < 4:
            print("Usage: python -m ockham.catalog.builder compare <catalog.db> <legacy_csv_dir>")  # noqa: T201
            sys.exit(1)
        db_path = Path(sys.argv[2])
        csv_dir = Path(sys.argv[3])
        if not db_path.exists():
            print(f"Error: {db_path} does not exist")  # noqa: T201
            sys.exit(1)
        if not csv_dir.is_dir():
            print(f"Error: {csv_dir} is not a directory")  # noqa: T201
            sys.exit(1)
        print(f"Comparing {db_path} against legacy CSVs in {csv_dir}")  # noqa: T201
        asyncio.run(compare_catalog(db_path, csv_dir))

    else:
        print(f"Unknown command: {command}")  # noqa: T201
        sys.exit(1)


if __name__ == "__main__":
    main()
