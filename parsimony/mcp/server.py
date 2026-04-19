"""Build an MCP Server from a parsimony Connectors collection."""

from __future__ import annotations

import logging

from mcp.server.lowlevel.server import Server
from mcp.types import TextContent, Tool
from pydantic import ValidationError

from parsimony.connector import Connector, Connectors
from parsimony.errors import ConnectorError
from parsimony.mcp.bridge import connector_to_tool, result_to_content

logger = logging.getLogger(__name__)


# Host-owned prose: the MCP server frames these connectors as discovery
# tools (compact context-friendly output) with bulk retrieval deferred to
# code execution. Changing the prompt wording here ships with the MCP
# server release, not a kernel release.
_MCP_HEADER = (
    "\n# Parsimony — financial data discovery tools\n"
    "\n"
    "These MCP tools search and discover data. They return compact, "
    "context-friendly results — metadata, listings, search matches — "
    "not bulk datasets. For bulk retrieval, write and execute a Python "
    "script:\n"
    "```python\n"
    "from parsimony import client\n"
    "result = await client['fred_fetch'](series_id='UNRATE')\n"
    "df = result.data  # pandas DataFrame\n"
    "```\n"
    "\n"
    "After discovering data with MCP tools, always execute the fetch — "
    "do not just suggest code.\n"
    "\n"
    "Workflow: discover (MCP tool) → fetch and execute (client) → analyze.\n"
    "For SDMX: list_datasets → dsd → codelist → series_keys → fetch.\n"
)


def _error_content(message: str) -> list[TextContent]:
    return [TextContent(type="text", text=message)]


def create_server(connectors: Connectors) -> Server:
    """Build an MCP Server wired to the given connectors.

    The server's ``instructions`` are dynamically generated from the connectors'
    ``to_llm()`` descriptions, giving the connected agent full context on what
    tools are available and how to use them.
    """
    instructions = connectors.to_llm(header=_MCP_HEADER, heading="Tools")
    server = Server("parsimony-data", instructions=instructions)
    tool_map: dict[str, Connector] = {c.name: c for c in connectors}

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [connector_to_tool(c) for c in connectors]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        conn = tool_map.get(name)
        if conn is None:
            available = sorted(tool_map.keys())
            return _error_content(f"Unknown tool: {name!r}. Available tools: {available}")
        try:
            result = await conn(**arguments)
        except ValidationError as exc:
            return _error_content(f"Invalid parameters for {name}: {exc}")
        except ConnectorError as exc:
            logger.warning("Connector error in MCP call_tool(%s): %s", name, exc)
            return _error_content(str(exc))
        except Exception:
            # Unhandled programmer bug or upstream library leak: log the
            # full traceback so the dev can diagnose, but keep the stdio
            # session alive — one misbehaving connector must not tear
            # down every other tool on the server. BaseException
            # (KeyboardInterrupt, SystemExit) propagates so Ctrl+C works.
            logger.exception("Unhandled error in MCP call_tool(%s)", name)
            return _error_content(f"Internal error in {name}; see server logs")
        return result_to_content(result)

    return server
