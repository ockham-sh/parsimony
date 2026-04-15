"""Tests for Connector.describe(), Connector.to_llm(), Connectors.describe(),
Connectors.to_llm(), and Connectors.filter()."""

from __future__ import annotations

from typing import Annotated

import pandas as pd
import pytest
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, Namespace, connector
from parsimony.result import Column, ColumnRole, OutputConfig

# ---------------------------------------------------------------------------
# Param models
# ---------------------------------------------------------------------------


class SimpleParams(BaseModel):
    query: str = Field(..., description="Search keyword")


class FetchParams(BaseModel):
    series_id: Annotated[str, Namespace("fred_series")] = Field(..., description="FRED series identifier")
    start_date: str | None = Field(None, description="Start date (YYYY-MM-DD)")


class EmptyDescParams(BaseModel):
    value: str


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="fred_series"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="notes", role=ColumnRole.METADATA),
    ]
)

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Test connectors
# ---------------------------------------------------------------------------


@connector(tags=["search", "fred"], properties={"provider": "fred", "tier": "free"})
async def fred_search(params: SimpleParams) -> pd.DataFrame:
    """Search for FRED economic time series by keyword."""
    return pd.DataFrame({"code": ["A"], "title": ["GDP"]})


@connector(
    output=FETCH_OUTPUT,
    tags=["loader", "fred"],
    properties={"provider": "fred", "tier": "premium"},
)
async def fred_fetch(params: FetchParams, *, api_key: str) -> pd.DataFrame:
    """Fetch FRED time series observations by series_id."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})


@connector()
async def bare_connector(params: EmptyDescParams) -> pd.DataFrame:
    """A minimal connector with no tags, output, or properties."""
    return pd.DataFrame()


@connector(
    description="A connector with a very long description that exceeds eighty characters easily "
    "because it contains a detailed explanation of what the connector does and how.",
    result_type="text",
)
async def long_desc_connector(params: SimpleParams) -> str:
    """Ignored docstring."""
    return "text result"


@connector(
    output=SEARCH_OUTPUT,
    tags=["enumerator"],
    properties={"provider": "ecb"},
)
async def ecb_search(params: SimpleParams) -> pd.DataFrame:
    """Search ECB datasets for economic indicators."""
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Connector.describe()
# ---------------------------------------------------------------------------


class TestConnectorDescribe:
    def test_header_and_description(self) -> None:
        text = fred_search.describe()
        assert "Connector: fred_search" in text
        assert "Search for FRED economic time series by keyword." in text

    def test_parameters_section(self) -> None:
        text = fred_search.describe()
        assert "Parameters:" in text
        assert "query: string (required)" in text

    def test_optional_parameter_shown(self) -> None:
        text = fred_fetch.describe()
        assert "start_date" in text
        assert "optional" in text

    def test_namespace_annotation_shown(self) -> None:
        text = fred_fetch.describe()
        assert "namespace=" in text
        assert "fred_series" in text

    def test_parameter_description_shown(self) -> None:
        text = fred_fetch.describe()
        assert "FRED series identifier" in text

    def test_dependencies_shown(self) -> None:
        text = fred_fetch.describe()
        assert "Dependencies" in text
        assert "api_key (required)" in text

    def test_no_dependencies_when_none(self) -> None:
        text = fred_search.describe()
        assert "Dependencies" not in text

    def test_output_schema_shown(self) -> None:
        text = fred_fetch.describe()
        assert "Output Schema:" in text
        assert "date" in text
        assert "KEY" in text
        assert "value" in text
        assert "DATA" in text

    def test_output_schema_with_namespace(self) -> None:
        text = fred_fetch.describe()
        lines = text.split("\n")
        date_line = [ln for ln in lines if "date" in ln and "KEY" in ln][0]
        assert "namespace=" in date_line

    def test_no_output_schema_when_none(self) -> None:
        text = fred_search.describe()
        assert "Output Schema:" not in text

    def test_tags_shown(self) -> None:
        text = fred_search.describe()
        assert "Tags: search, fred" in text

    def test_no_tags_when_empty(self) -> None:
        text = bare_connector.describe()
        assert "Tags:" not in text

    def test_properties_shown(self) -> None:
        text = fred_search.describe()
        assert "Properties:" in text
        assert "'provider': 'fred'" in text

    def test_no_properties_when_empty(self) -> None:
        text = bare_connector.describe()
        assert "Properties:" not in text

    def test_separator_line(self) -> None:
        text = fred_search.describe()
        lines = text.split("\n")
        # Second line should be a separator of dashes matching header length
        header = "Connector: fred_search"
        assert lines[1] == "\u2500" * len(header)

    def test_describe_returns_string(self) -> None:
        assert isinstance(fred_search.describe(), str)

    def test_describe_trailing_whitespace_stripped(self) -> None:
        text = fred_search.describe()
        assert text == text.rstrip()


# ---------------------------------------------------------------------------
# Connector.to_llm()
# ---------------------------------------------------------------------------


class TestConnectorToLlm:
    def test_header_with_name(self) -> None:
        text = fred_search.to_llm()
        assert text.startswith("### fred_search")

    def test_tags_in_header(self) -> None:
        text = fred_search.to_llm()
        first_line = text.split("\n")[0]
        assert "[search, fred]" in first_line

    def test_no_tags_bracket_when_empty(self) -> None:
        text = bare_connector.to_llm()
        first_line = text.split("\n")[0]
        assert "[" not in first_line

    def test_description_in_body(self) -> None:
        text = fred_search.to_llm()
        assert "Search for FRED economic time series by keyword." in text

    def test_output_columns_appended(self) -> None:
        text = fred_fetch.to_llm()
        assert "Returns: date, value." in text

    def test_no_returns_when_no_output(self) -> None:
        text = fred_search.to_llm()
        assert "Returns:" not in text

    def test_result_type_noted_when_not_dataframe(self) -> None:
        text = long_desc_connector.to_llm()
        assert "result.data is text (not a DataFrame)" in text

    def test_result_type_not_noted_for_dataframe(self) -> None:
        text = fred_search.to_llm()
        assert "not a DataFrame" not in text

    def test_parameters_listed(self) -> None:
        text = fred_fetch.to_llm()
        assert "- series_id: string" in text

    def test_optional_param_has_question_mark(self) -> None:
        text = fred_fetch.to_llm()
        assert "- start_date?: string" in text

    def test_required_param_no_question_mark(self) -> None:
        text = fred_fetch.to_llm()
        # series_id should not have ?
        lines = [ln for ln in text.split("\n") if ln.startswith("- series_id")]
        assert len(lines) == 1
        assert "series_id?" not in lines[0]

    def test_namespace_hint_shown(self) -> None:
        text = fred_fetch.to_llm()
        assert "[ns:fred_series]" in text

    def test_param_description_shown(self) -> None:
        text = fred_fetch.to_llm()
        assert "FRED series identifier" in text

    def test_returns_string(self) -> None:
        assert isinstance(fred_search.to_llm(), str)


# ---------------------------------------------------------------------------
# Connector.__repr__ and __str__
# ---------------------------------------------------------------------------


class TestConnectorRepr:
    def test_repr_format(self) -> None:
        r = repr(fred_search)
        assert r.startswith("Connector(")
        assert "fred_search" in r

    def test_str_matches_repr(self) -> None:
        assert str(fred_search) == repr(fred_search)

    def test_long_description_truncated(self) -> None:
        r = repr(long_desc_connector)
        # desc should be truncated to 77 + "..."
        assert "..." in r


# ---------------------------------------------------------------------------
# Connectors.describe()
# ---------------------------------------------------------------------------


class TestConnectorsDescribe:
    def test_empty_collection(self) -> None:
        coll = Connectors([])
        assert coll.describe() == "Connectors (empty)"

    def test_header_with_count(self) -> None:
        coll = Connectors([fred_search, bare_connector])
        text = coll.describe()
        assert text.startswith("Connectors (2):")

    def test_all_names_listed(self) -> None:
        coll = Connectors([fred_search, bare_connector])
        text = coll.describe()
        assert "fred_search" in text
        assert "bare_connector" in text

    def test_numbered_entries(self) -> None:
        coll = Connectors([fred_search, bare_connector])
        lines = coll.describe().split("\n")
        # Lines after header should be numbered
        assert any("1." in ln for ln in lines)
        assert any("2." in ln for ln in lines)

    def test_descriptions_shown(self) -> None:
        coll = Connectors([fred_search])
        text = coll.describe()
        assert "Search for FRED economic time series" in text

    def test_long_description_truncated_at_72(self) -> None:
        coll = Connectors([long_desc_connector])
        text = coll.describe()
        lines = text.split("\n")
        entry_line = [ln for ln in lines if "long_desc_connector" in ln][0]
        # The description portion should end with "..." if truncated
        assert "..." in entry_line

    def test_single_connector(self) -> None:
        coll = Connectors([bare_connector])
        text = coll.describe()
        assert "Connectors (1):" in text


# ---------------------------------------------------------------------------
# Connectors.to_llm()
# ---------------------------------------------------------------------------


class TestConnectorsToLlm:
    def test_code_context_header(self) -> None:
        coll = Connectors([fred_search])
        text = coll.to_llm(context="code")
        assert "Data connectors (code execution)" in text
        assert 'client["name"]' in text

    def test_mcp_context_header(self) -> None:
        coll = Connectors([fred_search])
        text = coll.to_llm(context="mcp")
        assert "financial data discovery tools" in text

    def test_code_context_connectors_label(self) -> None:
        coll = Connectors([fred_search])
        text = coll.to_llm(context="code")
        assert "## Connectors (1)" in text

    def test_mcp_context_tools_label(self) -> None:
        coll = Connectors([fred_search])
        text = coll.to_llm(context="mcp")
        assert "## Tools (1)" in text

    def test_empty_collection_message(self) -> None:
        coll = Connectors([])
        text = coll.to_llm()
        assert "No connectors available." in text

    def test_empty_mcp_collection(self) -> None:
        coll = Connectors([])
        text = coll.to_llm(context="mcp")
        assert "No connectors available." in text

    def test_connector_details_included(self) -> None:
        coll = Connectors([fred_search, bare_connector])
        text = coll.to_llm()
        assert "### fred_search" in text
        assert "### bare_connector" in text

    def test_multiple_connectors_count(self) -> None:
        coll = Connectors([fred_search, bare_connector])
        text = coll.to_llm()
        assert "## Connectors (2)" in text

    def test_returns_string(self) -> None:
        coll = Connectors([fred_search])
        assert isinstance(coll.to_llm(), str)


# ---------------------------------------------------------------------------
# Connectors.__repr__
# ---------------------------------------------------------------------------


class TestConnectorsRepr:
    def test_repr_shows_names(self) -> None:
        coll = Connectors([fred_search, bare_connector])
        r = repr(coll)
        assert "fred_search" in r
        assert "bare_connector" in r

    def test_empty_repr(self) -> None:
        coll = Connectors([])
        assert repr(coll) == "Connectors([])"


# ---------------------------------------------------------------------------
# Connectors.filter()
# ---------------------------------------------------------------------------


class TestConnectorsFilter:
    @pytest.fixture()
    def collection(self) -> Connectors:
        return Connectors([fred_search, fred_fetch, bare_connector, ecb_search])

    def test_filter_by_name_substring(self, collection: Connectors) -> None:
        result = collection.filter(name="fred")
        names = result.names()
        assert "fred_search" in names
        assert "fred_fetch" in names
        assert "bare_connector" not in names

    def test_filter_by_name_case_insensitive(self, collection: Connectors) -> None:
        result = collection.filter(name="FRED")
        assert len(result) >= 2

    def test_filter_by_name_matches_description(self, collection: Connectors) -> None:
        result = collection.filter(name="ECB")
        assert "ecb_search" in result.names()

    def test_filter_by_tags(self, collection: Connectors) -> None:
        result = collection.filter(tags=["fred"])
        names = result.names()
        assert "fred_search" in names
        assert "fred_fetch" in names
        assert "bare_connector" not in names

    def test_filter_by_multiple_tags_intersection(self, collection: Connectors) -> None:
        result = collection.filter(tags=["search", "fred"])
        names = result.names()
        assert "fred_search" in names
        assert "fred_fetch" not in names  # has "loader", not "search"

    def test_filter_by_properties(self, collection: Connectors) -> None:
        result = collection.filter(provider="fred")
        names = result.names()
        assert "fred_search" in names
        assert "fred_fetch" in names
        assert "ecb_search" not in names

    def test_filter_by_property_value(self, collection: Connectors) -> None:
        result = collection.filter(tier="premium")
        names = result.names()
        assert "fred_fetch" in names
        assert "fred_search" not in names

    def test_filter_combined_name_and_tags(self, collection: Connectors) -> None:
        result = collection.filter(name="search", tags=["fred"])
        names = result.names()
        assert names == ["fred_search"]

    def test_filter_combined_name_and_properties(self, collection: Connectors) -> None:
        result = collection.filter(name="fred", tier="free")
        names = result.names()
        assert "fred_search" in names
        assert "fred_fetch" not in names

    def test_filter_no_match(self, collection: Connectors) -> None:
        result = collection.filter(name="nonexistent")
        assert len(result) == 0

    def test_filter_empty_name_ignored(self, collection: Connectors) -> None:
        result = collection.filter(name="  ")
        # Whitespace-only name should be treated as no filter
        assert len(result) == len(collection)

    def test_filter_none_name_no_filter(self, collection: Connectors) -> None:
        result = collection.filter(name=None)
        assert len(result) == len(collection)

    def test_filter_returns_connectors_instance(self, collection: Connectors) -> None:
        result = collection.filter(name="fred")
        assert isinstance(result, Connectors)

    def test_filter_empty_collection(self) -> None:
        coll = Connectors([])
        result = coll.filter(name="anything")
        assert len(result) == 0

    def test_filter_unmatched_property(self, collection: Connectors) -> None:
        result = collection.filter(nonexistent_prop="value")
        assert len(result) == 0

    def test_filter_tags_empty_list(self, collection: Connectors) -> None:
        # Empty tags list means "must have all of [] tags" which is always true
        result = collection.filter(tags=[])
        assert len(result) == len(collection)
