"""Tests for MCP server error handling paths and parsimony lazy imports.

Exercises every branch in ``create_server``'s ``call_tool`` handler plus
the ``__getattr__`` lazy-import mechanism in ``parsimony/__init__.py``.
"""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("mcp", reason="mcp is an optional dependency")

from mcp import types as mcp_types
from mcp.server.lowlevel.server import Server
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, connector
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.mcp.server import create_server
from parsimony.result import Column, ColumnRole, OutputConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
    ],
)


class StubParams(BaseModel):
    query: str = Field(..., description="Search query")


def _make_error_connector(name: str, exc: Exception):
    """Create a connector that always raises the given exception."""

    @connector(
        name=name,
        output=SEARCH_OUTPUT,
        tags=["tool"],
    )
    async def _raises(params: StubParams) -> pd.DataFrame:
        """Raises an error for testing."""
        raise exc

    return _raises


@connector(output=SEARCH_OUTPUT, tags=["tool"])
async def ok_tool(params: StubParams) -> pd.DataFrame:
    """A stub tool that returns successfully."""
    return pd.DataFrame({"id": ["X"], "title": ["Result"]})


async def _call_tool(server: Server, name: str, arguments: dict) -> mcp_types.CallToolResult:
    handler = server.request_handlers[mcp_types.CallToolRequest]
    request = mcp_types.CallToolRequest(
        params=mcp_types.CallToolRequestParams(name=name, arguments=arguments),
    )
    result = await handler(request)
    return result.root if hasattr(result, "root") else result


async def _list_tools(server: Server) -> mcp_types.ListToolsResult:
    handler = server.request_handlers[mcp_types.ListToolsRequest]
    request = mcp_types.ListToolsRequest()
    result = await handler(request)
    return result.root if hasattr(result, "root") else result


def _text(result: mcp_types.CallToolResult) -> str:
    assert result.content, "Expected at least one content block"
    block = result.content[0]
    assert isinstance(block, mcp_types.TextContent)
    return block.text


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCallToolSuccess:
    async def test_successful_call(self) -> None:
        server = create_server(Connectors([ok_tool]))
        result = await _call_tool(server, "ok_tool", {"query": "hello"})
        text = _text(result)
        assert "X" in text
        assert "Result" in text


class TestCallToolUnknownTool:
    async def test_unknown_tool(self) -> None:
        server = create_server(Connectors([ok_tool]))
        result = await _call_tool(server, "nonexistent", {"query": "x"})
        text = _text(result)
        assert "Unknown tool" in text


class TestCallToolValidationError:
    async def test_bad_params(self) -> None:
        server = create_server(Connectors([ok_tool]))
        result = await _call_tool(server, "ok_tool", {})
        text = _text(result)
        # MCP may format validation errors itself; check for key indicators
        assert "query" in text.lower()
        assert "required" in text.lower() or "Invalid parameters" in text


class TestCallToolUnauthorizedError:
    async def test_unauthorized(self) -> None:
        c = _make_error_connector("err_unauth", UnauthorizedError(provider="test_prov"))
        server = create_server(Connectors([c]))
        result = await _call_tool(server, "err_unauth", {"query": "x"})
        text = _text(result)
        assert "test_prov" in text
        assert "credentials" in text.lower()


class TestCallToolPaymentRequiredError:
    async def test_payment_required(self) -> None:
        c = _make_error_connector("err_pay", PaymentRequiredError(provider="premium"))
        server = create_server(Connectors([c]))
        result = await _call_tool(server, "err_pay", {"query": "x"})
        text = _text(result)
        assert "premium" in text
        assert "plan" in text.lower()


class TestCallToolRateLimitError:
    async def test_burst(self) -> None:
        c = _make_error_connector("err_rl", RateLimitError(provider="fast", retry_after=30.0))
        server = create_server(Connectors([c]))
        result = await _call_tool(server, "err_rl", {"query": "x"})
        text = _text(result)
        assert "fast" in text
        assert "rate limit" in text.lower()

    async def test_quota_exhausted(self) -> None:
        c = _make_error_connector(
            "err_quota",
            RateLimitError(provider="quota", retry_after=0.0, quota_exhausted=True),
        )
        server = create_server(Connectors([c]))
        result = await _call_tool(server, "err_quota", {"query": "x"})
        text = _text(result)
        assert "do not retry" in text


class TestCallToolEmptyDataError:
    async def test_empty_data(self) -> None:
        c = _make_error_connector("err_empty", EmptyDataError(provider="empty", message="No rows"))
        server = create_server(Connectors([c]))
        result = await _call_tool(server, "err_empty", {"query": "x"})
        text = _text(result)
        assert "No rows" in text


class TestCallToolConnectorError:
    async def test_generic(self) -> None:
        c = _make_error_connector("err_gen", ConnectorError("timeout", provider="slow"))
        server = create_server(Connectors([c]))
        result = await _call_tool(server, "err_gen", {"query": "x"})
        text = _text(result)
        assert "timeout" in text


class TestListTools:
    async def test_list_tools(self) -> None:
        server = create_server(Connectors([ok_tool]))
        result = await _list_tools(server)
        assert len(result.tools) == 1
        assert result.tools[0].name == "ok_tool"


class TestLazyImports:
    def test_lazy_import_connector(self) -> None:
        import parsimony

        cls = parsimony.Connector
        from parsimony.connector import Connector as Direct

        assert cls is Direct

    def test_unknown_attr_raises(self) -> None:
        import parsimony

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = parsimony.totally_nonexistent_symbol

    def test_lazy_import_version(self) -> None:
        import parsimony

        assert isinstance(parsimony.__version__, str)
