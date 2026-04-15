"""Build a SQLite catalog by running enumerators against live APIs.

Selects connectors tagged ``"enumerator"`` from the provider registry
and indexes the results into a
:class:`~parsimony.stores.sqlite.SQLiteCatalogStore`.

Usage::

    python -m parsimony.catalog.builder output.db
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path

from parsimony.catalog.catalog import Catalog, _entries_from_table_result
from parsimony.connector import Connector
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


def _collect_enumerators(env: dict[str, str]) -> list[tuple[str, Connector, object]]:
    """Discover all enumerator connectors from the provider registry.

    Iterates :data:`~parsimony.connectors.PROVIDERS`, imports each module,
    and selects connectors tagged ``"enumerator"`` (applied automatically
    by the :func:`~parsimony.connector.enumerator` decorator).
    Dependencies are bound from *env*; providers with missing required
    deps are silently skipped.
    """
    from parsimony.connectors import PROVIDERS

    enumerators: list[tuple[str, Connector, object]] = []

    for spec in PROVIDERS:
        try:
            module = importlib.import_module(spec.module)
        except ImportError:
            continue

        env_vars: dict[str, str] = getattr(module, "ENV_VARS", {})

        for conn in module.CONNECTORS:
            if "enumerator" not in conn.tags:
                continue

            # Resolve dependencies from env
            deps: dict[str, str] = {}
            skip = False
            for dep_name, env_var in env_vars.items():
                value = env.get(env_var, "")
                if value:
                    deps[dep_name] = value
                elif dep_name in conn.dep_names:
                    skip = True
                    break
            if skip:
                continue

            bound = conn.bind_deps(**deps) if deps else conn
            enumerators.append((conn.name, bound, conn.param_type()))

    return enumerators


async def build_catalog(db_path: Path, *, env: dict[str, str] | None = None) -> int:
    """Run all available enumerators and populate a SQLite catalog.

    Returns the total number of entries indexed.
    """
    import os

    _env = env if env is not None else dict(os.environ)

    store = SQLiteCatalogStore(db_path)
    catalog = Catalog(store, embeddings=None)
    total = 0

    enumerators = _collect_enumerators(_env)

    for name, conn, params in enumerators:
        try:
            print(f"  {name}...", end="", flush=True)  # noqa: T201
            result = await conn(params)  # type: ignore[arg-type]
            entries = _entries_from_table_result(result)  # type: ignore[arg-type]
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m parsimony.catalog.builder build <output.db>")  # noqa: T201
        sys.exit(1)

    command = sys.argv[1]

    if command == "build":
        if len(sys.argv) < 3:
            print("Usage: python -m parsimony.catalog.builder build <output.db>")  # noqa: T201
            sys.exit(1)
        db_path = Path(sys.argv[2])
        print(f"Building catalog from live enumerators -> {db_path}")  # noqa: T201
        total = asyncio.run(build_catalog(db_path))
        print(f"\nDone: {total:,} total entries indexed")  # noqa: T201

    else:
        print(f"Unknown command: {command}")  # noqa: T201
        sys.exit(1)


if __name__ == "__main__":
    main()
