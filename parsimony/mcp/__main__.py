"""Entry point: python -m parsimony.mcp

Starts the MCP server over stdio, exposing all connectors tagged "tool".
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import mcp.server.stdio
import platformdirs

from parsimony.catalog.catalog import Catalog
from parsimony.connectors import build_connectors_from_env
from parsimony.connectors.catalog_search import CONNECTORS as CATALOG_CONNECTORS
from parsimony.mcp.server import create_server
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


async def main() -> None:
    all_connectors = build_connectors_from_env(lenient=True)

    # --- Catalog wiring ---
    db_dir = Path(platformdirs.user_cache_dir("parsimony"))
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "catalogs.db"

    store = SQLiteCatalogStore(db_path=db_path)
    catalog = Catalog(store=store, connectors=all_connectors)

    # Pre-populate from GitHub pre-built catalogs (TTL-cached)
    await catalog.warm()

    catalog_connectors = CATALOG_CONNECTORS.bind_deps(catalog=catalog)
    all_connectors = all_connectors + catalog_connectors

    # --- Server ---
    server = create_server(all_connectors)
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
