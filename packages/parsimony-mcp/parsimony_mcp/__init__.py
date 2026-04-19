"""Parsimony MCP server adapter.

Exposes a parsimony :class:`Connectors` collection as MCP tools::

    from parsimony.connectors import build_connectors_from_env
    from parsimony_mcp import create_server

    server = create_server(build_connectors_from_env().filter(tags=["tool"]))
"""

from __future__ import annotations

from parsimony_mcp.bridge import connector_to_tool, result_to_content
from parsimony_mcp.server import create_server

__all__ = ["connector_to_tool", "create_server", "result_to_content"]
