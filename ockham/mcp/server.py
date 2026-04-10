"""Build an MCP Server from an ockham Connectors collection."""

from __future__ import annotations

from mcp.server.lowlevel.server import Server
from mcp.types import TextContent, Tool

from ockham.connector import Connector, Connectors
from ockham.mcp.bridge import connector_to_tool, result_to_content


def create_server(connectors: Connectors) -> Server:
    """Build an MCP Server wired to the given connectors.

    The server's ``instructions`` are dynamically generated from the connectors'
    ``to_llm()`` descriptions, giving the connected agent full context on what
    tools are available and how to use them.
    """
    instructions = connectors.to_llm(context="mcp")
    server = Server("ockham-data", instructions=instructions)
    tool_map: dict[str, Connector] = {c.name: c for c in connectors}

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [connector_to_tool(c) for c in connectors]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        conn = tool_map.get(name)
        if conn is None:
            raise ValueError(f"Unknown tool: {name}")
        result = await conn(**arguments)
        return result_to_content(result)

    return server
