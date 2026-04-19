"""Entry point: ``parsimony-mcp`` (or ``python -m parsimony_mcp``).

Starts the MCP server over stdio, exposing all connectors tagged "tool"
that the env-driven factory produces.
"""

from __future__ import annotations

import asyncio

import mcp.server.stdio
from parsimony.connectors import build_connectors_from_env

from parsimony_mcp.server import create_server


async def _run() -> None:
    all_connectors = build_connectors_from_env()
    tool_connectors = all_connectors.filter(tags=["tool"])
    server = create_server(tool_connectors)
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main() -> None:
    """Console-script entry point."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
