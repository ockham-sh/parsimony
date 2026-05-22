"""Tests for Connector, @connector, and Connectors."""

from __future__ import annotations

import asyncio

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict, Field

from parsimony.catalog import CatalogEntry
from parsimony.connector import Connector, Connectors, connector, enumerator, loader
from parsimony.result import Column, ColumnRole, OutputConfig, Result, TabularResult


class _MacroParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str
    indicator: str

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="demo"),
        Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
    ]
)


def _make_search_df(query: str) -> pd.DataFrame:
    return pd.DataFrame({"id": ["A", "B"], "title": [f"Series about {query}", "Another"]})


def _make_fetch_df() -> pd.DataFrame:
    return pd.DataFrame({"date": ["2020-01-01", "2020-02-01"], "value": [1.0, 2.0]})


@connector
async def demo_search(query: str) -> pd.DataFrame:
    """Search for test series by keyword."""
    return _make_search_df(query)


@connector(output=FETCH_OUTPUT)
async def demo_fetch(series_id: str) -> pd.DataFrame:
    """Fetch test time series observations."""
    return _make_fetch_df()


class QueryModel(BaseModel):
    query: str = Field(..., min_length=1)


def _fake_connectors() -> Connectors:
    return Connectors([demo_search, demo_fetch])


class TestConnectorBind:
    def test_bind_returns_connector_with_smaller_exposed_signature(self) -> None:
        @connector
        async def needs_key(query: str, api_key: str) -> pd.DataFrame:
            """Needs key."""
            return _make_search_df(f"{query}:{api_key}")

        bound = needs_key.bind(api_key="secret")

        assert isinstance(bound, Connector)
        assert list(bound.exposed_signature.parameters) == ["query"]
        result = asyncio.run(bound(query="GDP"))
        assert len(result.data) == 2
        assert result.provenance.params == {"query": "GDP"}

    def test_bind_can_be_composed(self) -> None:
        @connector
        async def needs_two(query: str, api_key: str, base_url: str) -> pd.DataFrame:
            """Needs two fixed values."""
            return _make_search_df(f"{query}:{api_key}:{base_url}")

        partially_bound = needs_two.bind(api_key="x")
        assert list(partially_bound.exposed_signature.parameters) == ["query", "base_url"]

        fully_bound = partially_bound.bind(base_url="https://example.test")
        assert list(fully_bound.exposed_signature.parameters) == ["query"]
        result = asyncio.run(fully_bound(query="GDP"))
        assert len(result.data) == 2
        assert result.provenance.params == {"query": "GDP"}

    def test_bind_rejects_unknown_argument(self) -> None:
        with pytest.raises(TypeError, match="unexpected bind arguments"):
            demo_search.bind(api_key="x")

    def test_bind_rejects_duplicate_argument(self) -> None:
        bound = demo_search.bind(query="GDP")
        with pytest.raises(TypeError, match="already-bound"):
            bound.bind(query="CPI")

    def test_connectors_bind_applies_only_to_matching_connectors(self) -> None:
        @connector
        async def keyed(query: str, api_key: str) -> pd.DataFrame:
            """Keyed."""
            return _make_search_df(query)

        wired = Connectors([keyed, demo_fetch]).bind(api_key="k")
        assert list(wired["keyed"].exposed_signature.parameters) == ["query"]
        assert list(wired["demo_fetch"].exposed_signature.parameters) == ["series_id"]


class TestConnectorDecorator:
    def test_explicit_name_and_description(self) -> None:
        @connector(name="public_connector", description="Stable agent-facing description.")
        async def _internal(query: str) -> pd.DataFrame:
            """Implementation docstring; overridden by description= above."""
            return _make_search_df(query)

        assert _internal.name == "public_connector"
        assert _internal.description == "Stable agent-facing description."
        result = asyncio.run(_internal(query="GDP"))
        assert result.provenance.source == "public_connector"

    def test_missing_docstring_and_description_raises(self) -> None:
        with pytest.raises(ValueError, match="docstring"):

            @connector
            async def _no_docs(query: str) -> pd.DataFrame:
                return _make_search_df(query)

    def test_sync_function_rejected(self) -> None:
        with pytest.raises(TypeError, match="must be async"):

            @connector
            def _sync(query: str) -> pd.DataFrame:
                """Sync functions are not connectors."""
                return _make_search_df(query)


class TestExposedSignature:
    def test_exposed_signature_lists_unbound_parameters(self) -> None:
        assert list(demo_search.exposed_signature.parameters) == ["query"]

    def test_bind_removes_bound_parameters_from_exposed_signature(self) -> None:
        @connector
        async def keyed(query: str, api_key: str) -> str:
            """Has a secret-shaped parameter."""
            return query

        bound = keyed.bind(api_key="secret")
        assert list(bound.exposed_signature.parameters) == ["query"]

    def test_pydantic_model_is_ordinary_parameter(self) -> None:
        @connector
        async def model_param(payload: QueryModel) -> str:
            """Accepts a model as one ordinary argument."""
            return payload.query

        result = asyncio.run(model_param(payload=QueryModel(query="GDP")))
        assert result.provenance.params == {"payload": QueryModel(query="GDP")}


class TestEnumerator:
    def test_enumerator_adds_tag(self) -> None:
        from parsimony.catalog import CatalogEntry

        @enumerator(tags=["demo"])
        async def enumerate_demo() -> list[CatalogEntry]:
            """Enumerate demo series."""
            return [
                CatalogEntry(namespace="demo", code="A", title="Series A"),
            ]

        assert enumerate_demo.tags == ("enumerator", "demo")

    @pytest.mark.asyncio
    async def test_enumerator_returning_dataframe_raises_typeerror(self) -> None:
        @enumerator(name="bad_enumerator")
        async def bad_enumerator() -> list[CatalogEntry]:
            """Returns the wrong type at runtime."""
            return pd.DataFrame({"code": ["A"]})  # type: ignore[return-value]

        with pytest.raises(TypeError, match="bad_enumerator"):
            await bad_enumerator()


class TestLoader:
    def test_loader_requires_data_column(self) -> None:
        output = OutputConfig(columns=[Column(name="id", role=ColumnRole.KEY, namespace="demo")])
        with pytest.raises(ValueError, match="DATA"):
            loader(output=output)

    @pytest.mark.parametrize(
        ("columns", "match"),
        [
            (
                [
                    Column(name="id", role=ColumnRole.KEY, namespace="demo"),
                    Column(name="title", role=ColumnRole.TITLE),
                    Column(name="value", role=ColumnRole.DATA),
                ],
                "TITLE",
            ),
            (
                [
                    Column(name="id", role=ColumnRole.KEY, namespace="demo"),
                    Column(name="meta", role=ColumnRole.METADATA),
                    Column(name="value", role=ColumnRole.DATA),
                ],
                "METADATA",
            ),
            (
                [Column(name="value", role=ColumnRole.DATA)],
                "exactly one KEY",
            ),
            (
                [
                    Column(name="id1", role=ColumnRole.KEY, namespace="demo"),
                    Column(name="id2", role=ColumnRole.KEY, namespace="demo"),
                    Column(name="value", role=ColumnRole.DATA),
                ],
                "at most one KEY",
            ),
            (
                [
                    Column(name="id", role=ColumnRole.KEY),
                    Column(name="value", role=ColumnRole.DATA),
                ],
                "namespace",
            ),
        ],
    )
    def test_loader_rejects_invalid_output_schema(self, columns: list[Column], match: str) -> None:
        with pytest.raises(ValueError, match=match):
            loader(output=OutputConfig(columns=columns))

    def test_loader_adds_tag(self) -> None:
        @loader(output=FETCH_OUTPUT, tags=["demo"])
        async def load_demo(series_id: str) -> pd.DataFrame:
            """Load demo observations."""
            return _make_fetch_df()

        assert load_demo.tags == ("loader", "demo")


class TestConnectorExecution:
    def test_repr_includes_name_and_params(self) -> None:
        r = repr(demo_search)
        assert "demo_search" in r
        assert "query" in r

    def test_execute_via_collection(self) -> None:
        result = asyncio.run(_fake_connectors()["demo_search"]("GDP"))
        assert isinstance(result, Result)
        assert len(result.data) == 2
        assert result.provenance.params == {"query": "GDP"}

    def test_execute_fetch_with_output_schema(self) -> None:
        result = asyncio.run(demo_fetch(series_id="GDPC1"))
        assert result.output_schema is not None
        assert list(result.data.columns) == ["date", "value"]
        assert result.provenance.params == {"series_id": "GDPC1"}

    def test_missing_required_argument_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="missing a required argument"):
            asyncio.run(demo_search())

    def test_unexpected_argument_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Invalid parameters"):
            asyncio.run(demo_search(series_id="GDP"))

    def test_bound_secret_not_in_provenance_params(self) -> None:
        @connector(secrets=("api_key",))
        async def keyed(query: str, api_key: str) -> str:
            """Has a declared secret parameter."""
            return query

        bound = keyed.bind(api_key="secret")
        result = asyncio.run(bound(query="GDP"))
        assert result.provenance.params == {"query": "GDP"}

    def test_declared_secret_stripped_from_provenance_at_call_time(self) -> None:
        @connector(secrets=("api_key",))
        async def keyed(query: str, api_key: str) -> str:
            """Has a declared secret parameter."""
            return query

        result = asyncio.run(keyed(query="GDP", api_key="secret"))
        assert result.provenance.params == {"query": "GDP"}

    def test_secrets_unknown_parameter_raises_at_decoration(self) -> None:
        with pytest.raises(ValueError, match="unknown parameters"):

            @connector(secrets=("typo",))
            async def bad(query: str, api_key: str) -> str:
                """Bad secret declaration."""
                return query


class TestConnectorsCollection:
    def test_names_iter_len_and_lookup(self) -> None:
        c = _fake_connectors()
        assert c.names() == ["demo_fetch", "demo_search"]
        assert len(c) == 2
        assert all(isinstance(op, Connector) for op in c)
        assert c.get("demo_search") is c["demo_search"]
        assert "demo_search" in c
        assert "nope" not in c

    def test_missing_lookup_raises(self) -> None:
        with pytest.raises(KeyError, match="No connector 'bogus'"):
            _ = _fake_connectors()["bogus"]

    def test_duplicate_names_raise(self) -> None:
        with pytest.raises(ValueError, match="Duplicate connector names"):
            Connectors([demo_search, demo_search])

    def test_merge_and_add(self) -> None:
        a = Connectors([demo_search])
        b = Connectors([demo_fetch])
        assert Connectors.merge(a, b).names() == ["demo_fetch", "demo_search"]
        assert (a + b).names() == ["demo_fetch", "demo_search"]

    def test_replace(self) -> None:
        replacement = demo_search.bind(query="GDP")
        coll = _fake_connectors().replace("demo_search", replacement)
        assert coll["demo_search"] is replacement


class TestResultWrap:
    def test_wrap_sets_source_description_on_provenance(self) -> None:
        result = asyncio.run(demo_search(query="GDP"))
        assert result.provenance.source == "demo_search"
        assert "Search for test series" in result.provenance.source_description

    def test_framework_overwrites_connector_provenance(self) -> None:
        @connector
        async def manual_result(series_id: str) -> Result:
            """Real connector docstring."""
            return Result(data=_make_fetch_df(), provenance=Result(data=None).provenance)

        result = asyncio.run(manual_result(series_id="GDPC1"))
        assert result.provenance.source == "manual_result"
        assert result.provenance.source_description == "Real connector docstring."
        assert result.provenance.params == {"series_id": "GDPC1"}

    def test_flat_params_recorded_in_provenance(self) -> None:
        @connector()
        async def macro(country: str, indicator: str) -> pd.DataFrame:
            """Fetch macro indicator for a country."""
            _MacroParams(country=country, indicator=indicator)
            return pd.DataFrame({"country": [country], "indicator": [indicator]})

        result = asyncio.run(macro(country="USA", indicator="inflation_consumer_prices_annual"))
        assert result.provenance.params == {
            "country": "USA",
            "indicator": "inflation_consumer_prices_annual",
        }

    def test_output_plus_result_dataframe_raises_typeerror(self) -> None:
        @connector(output=FETCH_OUTPUT)
        async def bad_fetch(series_id: str) -> Result:
            """Tabular fetch that incorrectly wraps a DataFrame in Result."""
            return Result(data=_make_fetch_df())

        with pytest.raises(TypeError, match="declares output="):
            asyncio.run(bad_fetch(series_id="GDPC1"))

    def test_connector_properties_are_preserved(self) -> None:
        @connector
        async def with_props(series_id: str) -> Result:
            """Attaches source-specific metadata."""
            return TabularResult.from_dataframe(_make_fetch_df()).with_properties(series_url="https://example.com/x")

        result = asyncio.run(with_props(series_id="GDPC1"))
        assert result.provenance.properties == {"series_url": "https://example.com/x"}


class TestCallback:
    def test_callback_fires_on_success(self) -> None:
        log: list[str] = []
        c = _fake_connectors().with_callback(lambda r: log.append(r.provenance.source))
        asyncio.run(c["demo_search"](query="GDP"))
        assert log == ["demo_search"]

    def test_callback_preserved_through_bind(self) -> None:
        log: list[str] = []

        @connector
        async def keyed(query: str, api_key: str) -> pd.DataFrame:
            """Keyed."""
            return _make_search_df(query)

        c = Connectors([keyed]).with_callback(lambda r: log.append(r.provenance.source)).bind(api_key="k")
        asyncio.run(c["keyed"](query="GDP"))
        assert log == ["keyed"]

    def test_callback_exceptions_are_logged_not_raised(self) -> None:
        def boom(_result: Result) -> None:
            raise RuntimeError("callback broke")

        result = asyncio.run(_fake_connectors().with_callback(boom)["demo_search"](query="GDP"))
        assert len(result.data) == 2
