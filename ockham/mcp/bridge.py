"""Bridge between ockham Connector interface and MCP Tool definitions.

Two pure functions — no side effects, no server state.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from mcp.types import TextContent, Tool

from ockham.connector import Connector
from ockham.result import Result


def connector_to_tool(conn: Connector) -> Tool:
    """Map a Connector to an MCP Tool definition."""
    schema: dict[str, Any] = dict(conn.param_schema)
    # Strip Pydantic $defs — MCP clients may not support JSON Schema $ref
    schema.pop("$defs", None)
    schema.pop("title", None)
    return Tool(
        name=conn.name,
        description=conn.description,
        inputSchema=schema,
    )


def result_to_content(result: Result, max_rows: int = 50) -> list[TextContent]:
    """Serialize a connector Result to MCP text content."""
    if isinstance(result.data, pd.DataFrame):
        df = result.data.head(max_rows)
        text = df.to_markdown(index=False)
        if len(result.data) > max_rows:
            text += f"\n({len(result.data) - max_rows} more rows omitted)"
    elif isinstance(result.data, pd.Series):
        text = result.data.to_markdown()
    else:
        text = str(result.data)
    return [TextContent(type="text", text=text)]
