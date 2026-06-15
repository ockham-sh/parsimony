"""Tests for Connector.describe(), to_llm(), and Connectors.filter()."""

from __future__ import annotations

from typing import Annotated

import pandas as pd

from parsimony.connector import Connectors, connector
from parsimony.result import Column, ColumnRole, OutputConfig

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


@connector(tags=["search", "fred"], properties={"provider": "fred", "tier": "free"})
def fred_search(query: str) -> pd.DataFrame:
    """Search for FRED economic time series by keyword."""
    return pd.DataFrame({"code": ["A"], "title": ["GDP"]})


@connector(output=FETCH_OUTPUT, tags=["loader", "fred"], properties={"provider": "fred", "tier": "premium"})
def fred_fetch(
    series_id: Annotated[str, "ns:fred_series"],
    start_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch FRED time series observations by series_id."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})


@connector
def bare_connector(value: str) -> pd.DataFrame:
    """A minimal connector with no tags, output, or properties."""
    return pd.DataFrame()


@connector(
    description=(
        "A connector with a very long description that exceeds eighty characters easily because it explains behavior."
    )
)
def long_desc_connector(query: str) -> str:
    """Ignored docstring."""
    return query


@connector(tags=["enumerator"], properties={"provider": "ecb"})
def ecb_search(query: str) -> pd.DataFrame:
    """Search ECB datasets for economic indicators."""
    return pd.DataFrame()


def _collection() -> Connectors:
    return Connectors([fred_search, fred_fetch, bare_connector, long_desc_connector, ecb_search])


class TestConnectorDescribe:
    def test_header_and_description(self) -> None:
        text = fred_search.describe()
        assert "Connector: fred_search" in text
        assert "Search for FRED economic time series by keyword." in text

    def test_parameters_section(self) -> None:
        text = fred_search.describe()
        assert "Parameters:" in text
        assert "query: str (required)" in text

    def test_optional_parameter_shown(self) -> None:
        text = fred_fetch.bind(api_key="secret").describe()
        assert "start_date" in text
        assert "optional" in text
        assert "api_key" not in text

    def test_namespace_annotation_shown(self) -> None:
        text = fred_fetch.bind(api_key="secret").describe()
        assert "namespace=" in text
        assert "fred_series" in text

    def test_output_schema_section(self) -> None:
        text = fred_fetch.bind(api_key="secret").describe()
        assert "Output Schema:" in text
        assert "date" in text
        assert "KEY" in text
        assert "value" in text
        assert "DATA" in text

    def test_tags_and_properties(self) -> None:
        text = fred_search.describe()
        assert "Tags: search, fred" in text
        assert "Properties:" in text
        assert "provider" in text


class TestConnectorToLlm:
    def test_llm_card_includes_name_description_and_params(self) -> None:
        text = fred_search.to_llm()
        assert "### fred_search" in text
        assert "Search for FRED" in text
        assert "query" in text

    def test_bound_secret_param_not_in_llm_card(self) -> None:
        text = fred_fetch.bind(api_key="secret").to_llm()
        assert "series_id" in text
        assert "api_key" not in text


class TestConnectorsDescribeToLlm:
    def test_collection_describe_lists_connectors(self) -> None:
        text = _collection().describe()
        assert "Connectors (5):" in text
        assert "fred_search" in text
        assert "fred_fetch" in text

    def test_collection_to_llm(self) -> None:
        text = _collection().to_llm(header="Header", heading="Demo")
        assert "Header" in text
        assert "## Demo (5)" in text
        assert "fred_search" in text

    def test_empty_collection(self) -> None:
        assert Connectors([]).describe() == "Connectors (empty)"
        assert Connectors([]).to_llm() == ""


class TestConnectorRepr:
    def test_repr_includes_name_params_and_description(self) -> None:
        r = repr(fred_search)
        assert "Connector(" in r
        assert "fred_search" in r
        assert "query" in r
        assert "Search for FRED" in r


class TestSearch:
    def test_search_by_name_substring(self) -> None:
        assert _collection().search("fred").names() == ["fred_fetch", "fred_search"]

    def test_search_by_tags_subset(self) -> None:
        assert _collection().search("fred", tags=["search", "fred"]).names() == ["fred_search"]

    def test_search_by_property(self) -> None:
        assert _collection().search("fred", tier="premium").names() == ["fred_fetch"]

    def test_search_blank_query_returns_all(self) -> None:
        assert _collection().search("  ").names() == _collection().names()


class TestFilter:
    def test_filter_with_predicate(self) -> None:
        assert _collection().filter(lambda c: c.name.startswith("ecb")).names() == ["ecb_search"]
