"""Unit tests for bridge.py — connector→Tool mapping and result serialization."""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.mcp.bridge import connector_to_tool, result_to_content
from parsimony.result import Provenance, Result


class TestConnectorToTool:
    def test_name_and_description_preserved(self, tool_connectors):
        tools = [connector_to_tool(c) for c in tool_connectors]
        names = {t.name for t in tools}
        assert "mock_search" in names
        assert "mock_profile" in names

    def test_input_schema_has_properties(self, tool_connectors):
        for c in tool_connectors:
            tool = connector_to_tool(c)
            assert "properties" in tool.inputSchema

    def test_defs_stripped_from_schema(self, tool_connectors):
        for c in tool_connectors:
            tool = connector_to_tool(c)
            assert "$defs" not in tool.inputSchema

    def test_title_stripped_from_schema(self, tool_connectors):
        for c in tool_connectors:
            tool = connector_to_tool(c)
            assert "title" not in tool.inputSchema


class TestResultToContent:
    def test_dataframe_to_markdown(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = Result(data=df, provenance=Provenance(source="test"))
        content = result_to_content(result)
        assert len(content) == 1
        text = content[0].text
        assert "|" in text  # markdown table
        assert "a" in text
        assert "x" in text

    def test_truncation(self):
        df = pd.DataFrame({"val": range(100)})
        result = Result(data=df, provenance=Provenance(source="test"))
        content = result_to_content(result, max_rows=10)
        assert "90 more rows omitted" in content[0].text

    def test_string_data(self):
        result = Result(data="hello world", provenance=Provenance(source="test"))
        content = result_to_content(result)
        assert content[0].text == "hello world"

    def test_series_data(self):
        s = pd.Series({"name": "Test", "value": 42})
        result = Result(data=s, provenance=Provenance(source="test"))
        content = result_to_content(result)
        assert "Test" in content[0].text
