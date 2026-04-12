"""Ockham MCP server — expose search/discovery connectors as MCP tools."""

from __future__ import annotations


def create_server(*args, **kwargs):
    """Build an MCP Server from a parsimony Connectors collection.

    Lazy import to avoid namespace collision between ``parsimony.mcp`` (this package)
    and the top-level ``mcp`` package.
    """
    from parsimony.mcp.server import create_server as _create

    return _create(*args, **kwargs)


__all__ = ["create_server"]
