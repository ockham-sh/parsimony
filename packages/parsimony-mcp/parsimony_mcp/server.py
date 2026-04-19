"""Build an MCP Server from a parsimony Connectors collection."""

from __future__ import annotations

import logging

from mcp.server.lowlevel.server import Server
from mcp.types import TextContent, Tool
from parsimony.connector import Connector, Connectors
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
from pydantic import ValidationError

from parsimony_mcp.bridge import connector_to_tool, result_to_content

logger = logging.getLogger(__name__)


def _error_content(message: str) -> list[TextContent]:
    return [TextContent(type="text", text=message)]


_MCP_SERVER_INSTRUCTIONS = """\
# Parsimony — financial data discovery tools

These MCP tools search and discover data. They return compact, context-friendly
results — metadata, listings, search matches — not bulk datasets. For bulk
retrieval, write and execute a Python script:

```python
from parsimony import client
result = await client['fred'](series_id='UNRATE')
df = result.data  # pandas DataFrame
```

After discovering data with MCP tools, always execute the fetch — do not just
suggest code.

Workflow: discover (MCP tool) → fetch and execute (client) → analyze.
For SDMX: list_datasets → dsd → codelist → series_keys → fetch.

## Tools

{catalog}
"""


def create_server(connectors: Connectors) -> Server:
    """Build an MCP Server wired to the given connectors.

    The server's ``instructions`` combine MCP-specific framing (authored here)
    with the framework's connector serialization, so the connected agent sees
    both how to operate and what's available.
    """
    instructions = _MCP_SERVER_INSTRUCTIONS.format(catalog=connectors.to_llm())
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
