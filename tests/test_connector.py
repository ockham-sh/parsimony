"""Tests for Connector, @connector, and Connectors."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict, Field

from parsimony.connector import Connector, Connectors, connector, enumerator, loader
from parsimony.result import Column, ColumnRole, OutputSpec, Result


class _MacroParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str
    indicator: str


SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="demo"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


def _make_search_df(query: str) -> pd.DataFrame:
    return pd.DataFrame({"id": ["A", "B"], "title": [f"Series about {query}", "Another"]})


def _make_fetch_df() -> pd.DataFrame:
    return pd.DataFrame({"date": ["2020-01-01", "2020-02-01"], "value": [1.0, 2.0]})


@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search for test series by keyword."""
    return _make_search_df(query)


@connector(output=FETCH_OUTPUT)
def demo_fetch(series_id: str) -> pd.DataFrame:
    """Fetch test time series observations."""
    return _make_fetch_df()


class QueryModel(BaseModel):
    query: str = Field(..., min_length=1)


def _fake_connectors() -> Connectors:
    return Connectors([demo_search, demo_fetch])


class TestConnectorBind:
    def test_bind_returns_connector_with_smaller_exposed_signature(self) -> None:
        @connector
        def needs_key(query: str, api_key: str) -> pd.DataFrame:
            """Needs key."""
            return _make_search_df(f"{query}:{api_key}")

        bound = needs_key.bind(api_key="secret")

        assert isinstance(bound, Connector)
        assert list(bound.exposed_signature.parameters) == ["query"]
        result = bound(query="GDP")
        assert len(result.frame) == 2
        assert result.provenance.params == {"query": "GDP"}

    def test_bind_can_be_composed(self) -> None:
        @connector
        def needs_two(query: str, api_key: str, base_url: str) -> pd.DataFrame:
            """Needs two fixed values."""
            return _make_search_df(f"{query}:{api_key}:{base_url}")

        partially_bound = needs_two.bind(api_key="x")
        assert list(partially_bound.exposed_signature.parameters) == ["query", "base_url"]

        fully_bound = partially_bound.bind(base_url="https://example.test")
        assert list(fully_bound.exposed_signature.parameters) == ["query"]
        result = fully_bound(query="GDP")
        assert len(result.frame) == 2
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
        def keyed(query: str, api_key: str) -> pd.DataFrame:
            """Keyed."""
            return _make_search_df(query)

        wired = Connectors([keyed, demo_fetch]).bind(api_key="k")
        assert list(wired["keyed"].exposed_signature.parameters) == ["query"]
        assert list(wired["demo_fetch"].exposed_signature.parameters) == ["series_id"]


class TestConnectorDecorator:
    def test_explicit_name_and_description(self) -> None:
        @connector(name="public_connector", description="Stable agent-facing description.")
        def _internal(query: str) -> pd.DataFrame:
            """Implementation docstring; overridden by description= above."""
            return _make_search_df(query)

        assert _internal.name == "public_connector"
        assert _internal.description == "Stable agent-facing description."
        result = _internal(query="GDP")
        assert result.provenance.source == "public_connector"

    def test_missing_docstring_and_description_raises(self) -> None:
        with pytest.raises(ValueError, match="docstring"):

            @connector
            def _no_docs(query: str) -> pd.DataFrame:
                return _make_search_df(query)

    def test_async_function_rejected(self) -> None:
        with pytest.raises(TypeError, match="must be synchronous"):

            @connector
            async def _async_connector(query: str) -> pd.DataFrame:
                """Async connectors are not supported."""
                return _make_search_df(query)


class TestExposedSignature:
    def test_exposed_signature_lists_unbound_parameters(self) -> None:
        assert list(demo_search.exposed_signature.parameters) == ["query"]

    def test_bind_removes_bound_parameters_from_exposed_signature(self) -> None:
        @connector
        def keyed(query: str, api_key: str) -> str:
            """Has a secret-shaped parameter."""
            return query

        bound = keyed.bind(api_key="secret")
        assert list(bound.exposed_signature.parameters) == ["query"]

    def test_pydantic_model_is_ordinary_parameter(self) -> None:
        @connector
        def model_param(payload: QueryModel) -> str:
            """Accepts a model as one ordinary argument."""
            return payload.query

        result = model_param(payload=QueryModel(query="GDP"))
        assert result.provenance.params == {"payload": QueryModel(query="GDP")}


ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


class TestEnumerator:
    def test_enumerator_adds_tag(self) -> None:
        @enumerator(output=ENUMERATE_OUTPUT, tags=["demo"])
        def enumerate_demo() -> pd.DataFrame:
            """Enumerate demo series."""
            return pd.DataFrame({"code": ["A"], "title": ["Series A"]})

        assert enumerate_demo.tags == ("enumerator", "demo")

    def test_enumerator_requires_output(self) -> None:
        with pytest.raises(ValueError, match="exactly one KEY"):

            @enumerator(output=OutputSpec(columns=[Column(name="title", role=ColumnRole.TITLE)]))
            def bad() -> pd.DataFrame:
                """Missing KEY."""
                return pd.DataFrame()

    def test_enumerator_wraps_dataframe_as_tabular_result(self) -> None:
        @enumerator(output=ENUMERATE_OUTPUT, name="good_enumerator")
        def good_enumerator() -> pd.DataFrame:
            """Returns a catalog discovery frame."""
            return pd.DataFrame({"code": ["A"], "title": ["Series A"]})

        result = good_enumerator()
        assert result.is_tabular
        assert list(result.frame.columns) == ["code", "title"]

    def test_enumerator_manual_result_raises_typeerror(self) -> None:
        @enumerator(output=ENUMERATE_OUTPUT, name="bad_enumerator")
        def bad_enumerator() -> pd.DataFrame:
            """Returns Result instead of raw data."""
            return Result(raw=pd.DataFrame({"code": ["A"], "title": ["X"]}))  # type: ignore[return-value]

        with pytest.raises(TypeError, match="must return raw data"):
            bad_enumerator()


class TestLoader:
    def test_loader_requires_data_column(self) -> None:
        output = OutputSpec(columns=[Column(name="id", role=ColumnRole.KEY, namespace="demo")])
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
        # pydantic's ValidationError (raised by OutputSpec's declaration invariants,
        # e.g. the "namespace" case below) is itself a ValueError subclass.
        with pytest.raises(ValueError, match=match):
            loader(output=OutputSpec(columns=columns))

    def test_loader_adds_tag(self) -> None:
        @loader(output=FETCH_OUTPUT, tags=["demo"])
        def load_demo(series_id: str) -> pd.DataFrame:
            """Load demo observations."""
            return _make_fetch_df()

        assert load_demo.tags == ("loader", "demo")


class TestConnectorExecution:
    def test_repr_includes_name_and_params(self) -> None:
        r = repr(demo_search)
        assert "demo_search" in r
        assert "query" in r

    def test_execute_via_collection(self) -> None:
        result = _fake_connectors()["demo_search"]("GDP")
        assert isinstance(result, Result)
        assert len(result.frame) == 2
        assert result.provenance.params == {"query": "GDP"}

    def test_execute_fetch_with_output_spec(self) -> None:
        result = demo_fetch(series_id="GDPC1")
        assert result.output_spec is not None
        assert list(result.frame.columns) == ["date", "value"]
        assert result.provenance.params == {"series_id": "GDPC1"}

    def test_missing_required_argument_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="missing a required argument"):
            demo_search()

    def test_unexpected_argument_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Invalid parameters"):
            demo_search(series_id="GDP")

    def test_bound_secret_not_in_provenance_params(self) -> None:
        @connector(secrets=("api_key",))
        def keyed(query: str, api_key: str) -> str:
            """Has a declared secret parameter."""
            return query

        bound = keyed.bind(api_key="secret")
        result = bound(query="GDP")
        assert result.provenance.params == {"query": "GDP"}

    def test_declared_secret_stripped_from_provenance_at_call_time(self) -> None:
        @connector(secrets=("api_key",))
        def keyed(query: str, api_key: str) -> str:
            """Has a declared secret parameter."""
            return query

        result = keyed(query="GDP", api_key="secret")
        assert result.provenance.params == {"query": "GDP"}

    def test_secrets_unknown_parameter_raises_at_decoration(self) -> None:
        with pytest.raises(ValueError, match="unknown parameters"):

            @connector(secrets=("typo",))
            def bad(query: str, api_key: str) -> str:
                """Bad secret declaration."""
                return query


class TestConnectorRequires:
    def test_requires_defaults_empty(self) -> None:
        assert demo_search.requires == ()

    def test_requires_stored_and_normalized_to_tuple(self) -> None:
        @connector(requires=["FOO_API_KEY"])  # type: ignore[arg-type]
        def keyed(query: str) -> pd.DataFrame:
            """Needs an env var."""
            return _make_search_df(query)

        assert keyed.requires == ("FOO_API_KEY",)

    def test_loader_threads_requires(self) -> None:
        @loader(output=FETCH_OUTPUT, requires=("FOO_API_KEY",))
        def load_keyed(series_id: str) -> pd.DataFrame:
            """Load with a required env var."""
            return _make_fetch_df()

        assert load_keyed.requires == ("FOO_API_KEY",)

    def test_enumerator_threads_requires(self) -> None:
        @enumerator(output=ENUMERATE_OUTPUT, requires=("FOO_API_KEY",))
        def enumerate_keyed() -> pd.DataFrame:
            """Enumerate with a required env var."""
            return pd.DataFrame({"code": ["A"], "title": ["Series A"]})

        assert enumerate_keyed.requires == ("FOO_API_KEY",)


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

    def test_add(self) -> None:
        a = Connectors([demo_search])
        b = Connectors([demo_fetch])
        assert (a + b).names() == ["demo_fetch", "demo_search"]

    def test_env_vars_is_union_of_requires(self) -> None:
        @connector(requires=("FOO_API_KEY", "SHARED_KEY"))
        def a(query: str) -> str:
            """A."""
            return query

        @connector(requires=("SHARED_KEY",))
        def b(query: str) -> str:
            """B."""
            return query

        assert Connectors([a, b, demo_search]).env_vars() == frozenset({"FOO_API_KEY", "SHARED_KEY"})

    def test_env_vars_empty_when_nothing_declared(self) -> None:
        assert _fake_connectors().env_vars() == frozenset()


class TestResultWrap:
    def test_wrap_sets_source_description_on_provenance(self) -> None:
        result = demo_search(query="GDP")
        assert result.provenance.source == "demo_search"
        assert "Search for test series" in result.provenance.source_description

    def test_manual_result_raises_typeerror(self) -> None:
        @connector
        def manual_result(series_id: str) -> Result:
            """Real connector docstring."""
            return Result(raw=_make_fetch_df())

        with pytest.raises(TypeError, match="must return raw data"):
            manual_result(series_id="GDPC1")

    def test_flat_params_recorded_in_provenance(self) -> None:
        @connector()
        def macro(country: str, indicator: str) -> pd.DataFrame:
            """Fetch macro indicator for a country."""
            _MacroParams(country=country, indicator=indicator)
            return pd.DataFrame({"country": [country], "indicator": [indicator]})

        result = macro(country="USA", indicator="inflation_consumer_prices_annual")
        assert result.provenance.params == {
            "country": "USA",
            "indicator": "inflation_consumer_prices_annual",
        }

    def test_output_plus_result_dataframe_raises_typeerror(self) -> None:
        @connector(output=FETCH_OUTPUT)
        def bad_fetch(series_id: str) -> Result:
            """Tabular fetch that incorrectly wraps a DataFrame in Result."""
            return Result(raw=_make_fetch_df())

        with pytest.raises(TypeError, match="must return raw data"):
            bad_fetch(series_id="GDPC1")

    def test_tabular_result_return_raises_typeerror(self) -> None:
        @connector
        def with_props(series_id: str) -> Result:
            """Incorrectly returns Result."""
            return Result(raw=_make_fetch_df())

        with pytest.raises(TypeError, match="must return raw data"):
            with_props(series_id="GDPC1")

    def test_raw_tuple_return_raises_typeerror(self) -> None:
        @connector(output=FETCH_OUTPUT)
        def with_meta(series_id: str) -> tuple[pd.DataFrame, dict[str, object]]:
            """Incorrectly returns a (data, properties) tuple."""
            return _make_fetch_df(), {"source_url": f"https://example.test/{series_id}"}

        with pytest.raises(TypeError, match="must return raw data"):
            with_meta(series_id="GDPC1")
