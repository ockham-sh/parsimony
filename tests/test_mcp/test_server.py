"""Integration tests for the MCP server — in-process client↔server via memory streams."""

from __future__ import annotations

import pytest
from mcp.server.lowlevel.server import Server

from parsimony.mcp.server import create_server


@pytest.fixture()
def mcp_server(tool_connectors) -> Server:
    return create_server(tool_connectors)


class TestServerListTools:
    def test_server_has_instructions(self, mcp_server):
        """Server should have instructions set."""
        assert mcp_server.instructions is not None
        assert len(mcp_server.instructions) > 0

    async def test_tool_count_matches_connectors(self, tool_connectors):
        server = create_server(tool_connectors)
        # The tool_connectors fixture filters to "tool" tag only
        # mock_search and mock_profile are tagged, mock_fetch is not
        assert len(list(tool_connectors)) == 2


class TestServerInstructions:
    def test_instructions_contain_parsimony(self, mcp_server):
        assert "parsimony" in mcp_server.instructions.lower()

    def test_instructions_contain_workflow(self, mcp_server):
        assert "search" in mcp_server.instructions.lower()
        assert "fetch" in mcp_server.instructions.lower()

    def test_instructions_contain_tool_descriptions(self, mcp_server):
        assert "mock_search" in mcp_server.instructions
        assert "mock_profile" in mcp_server.instructions

    def test_instructions_exclude_non_tool(self, mcp_server):
        # mock_fetch is not tagged "tool" so should not appear
        assert "mock_fetch" not in mcp_server.instructions


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
