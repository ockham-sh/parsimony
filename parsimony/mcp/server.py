"""Build an MCP Server from a parsimony Connectors collection."""

from __future__ import annotations

import logging

from mcp.server.lowlevel.server import Server
from mcp.types import TextContent, Tool
from pydantic import ValidationError

from parsimony.connector import Connector, Connectors
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.mcp.bridge import connector_to_tool, result_to_content

logger = logging.getLogger(__name__)


def _error_content(message: str) -> list[TextContent]:
    return [TextContent(type="text", text=message)]


_MCP_PREAMBLE = (
    "Use tools for search and discovery. "
    "For bulk data fetching, write and execute Python code using "
    "the connectors listed below — do not just suggest code.\n"
)


def create_server(connectors: Connectors) -> Server:
    """Build an MCP Server wired to the given connectors.

    Receives the **full** connector collection.  Tool-tagged connectors are
    registered as native MCP tools (via ``list_tools``); all other connectors
    are described in the server's ``instructions`` (via ``to_llm()``) so the
    agent knows what is available through ``client["name"](...)``.
    """
    instructions = _MCP_PREAMBLE + connectors.to_llm()
    tool_connectors = connectors.filter(tags=[Connectors.TOOL_TAG])
    server = Server("parsimony-data", instructions=instructions)
    tool_map: dict[str, Connector] = {c.name: c for c in tool_connectors}

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [connector_to_tool(c) for c in tool_connectors]

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
        except UnauthorizedError as exc:
            return _error_content(
                f"Authentication error for {exc.provider}: {exc}. Check that the API key is correctly configured."
            )
        except PaymentRequiredError as exc:
            return _error_content(
                f"Plan restriction for {exc.provider}: {exc}. This endpoint requires a higher-tier API plan."
            )
        except RateLimitError as exc:
            msg = f"Rate limit hit for {exc.provider}."
            if exc.quota_exhausted:
                msg += " Quota exhausted for the billing period — do not retry."
            else:
                msg += f" Retry after {exc.retry_after:.0f} seconds."
            return _error_content(msg)
        except EmptyDataError as exc:
            return _error_content(f"No data returned: {exc}")
        except ConnectorError as exc:
            logger.warning("Connector error in MCP call_tool(%s): %s", name, exc)
            return _error_content(f"Error from {exc.provider}: {exc}")
        return result_to_content(result)

    return server
