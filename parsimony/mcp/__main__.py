"""Entry point: python -m parsimony.mcp

Starts the MCP server over stdio, exposing all connectors tagged "tool".
"""

from __future__ import annotations

import asyncio

import mcp.server.stdio

from parsimony.discovery import build_connectors_from_env
from parsimony.mcp.server import create_server


async def main() -> None:
    all_connectors = build_connectors_from_env()
    tool_connectors = all_connectors.filter(tags=["tool"])
    server = create_server(tool_connectors)
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
