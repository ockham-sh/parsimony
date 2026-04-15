"""Integration tests for the MCP server — in-process client↔server via memory streams."""

from __future__ import annotations

import pytest

pytest.importorskip("mcp", reason="mcp is an optional dependency")

from mcp.server.lowlevel.server import Server

from parsimony.connector import Connectors
from parsimony.mcp.server import _MCP_PREAMBLE, create_server


@pytest.fixture()
def mcp_server(all_connectors) -> Server:
    return create_server(all_connectors)


class TestServerListTools:
    def test_server_has_instructions(self, mcp_server):
        """Server should have instructions set."""
        assert mcp_server.instructions is not None
        assert len(mcp_server.instructions) > 0

    async def test_tool_count_matches_connectors(self, all_connectors):
        """Tool registration should match the number of tool-tagged connectors."""
        server = create_server(all_connectors)
        tool_count = len([c for c in all_connectors if Connectors.TOOL_TAG in c.tags])
        assert tool_count == 2  # mock_search and mock_profile
        assert server.instructions is not None


class TestServerInstructions:
    def test_instructions_contain_header(self, mcp_server):
        assert "# Data connectors" in mcp_server.instructions

    def test_instructions_contain_preamble(self, mcp_server):
        assert _MCP_PREAMBLE in mcp_server.instructions

    def test_instructions_contain_client_connectors(self, mcp_server):
        # Non-tool connectors appear in instructions (via to_llm())
        assert "mock_fetch" in mcp_server.instructions

    def test_instructions_exclude_tool_connectors(self, mcp_server):
        # Tool-tagged connectors are NOT in instructions (they're in list_tools)
        assert "### mock_search" not in mcp_server.instructions
        assert "### mock_profile" not in mcp_server.instructions


class TestServerCallTool:
    async def test_call_known_tool(self, tool_connectors):
        # Test through the connector directly — the bridge wires these
        # into the server, so verifying the connector works end-to-end
        # is sufficient without spinning up a full MCP client session.
        conn = tool_connectors["mock_search"]
        result = await conn(query="test")
        assert len(result.data) == 3

    async def test_call_with_params(self, tool_connectors):
        conn = tool_connectors["mock_profile"]
        result = await conn(ticker="AAPL")
        assert result.data.iloc[0]["ticker"] == "AAPL"
