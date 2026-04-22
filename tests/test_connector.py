"""Tests for Connector, @connector, and Connectors."""

from __future__ import annotations

import asyncio

import pandas as pd
import pytest
from pydantic import BaseModel, Field, ValidationError

from parsimony.connector import Connector, Connectors, connector, enumerator, loader
from parsimony.result import Column, ColumnRole, OutputConfig, Result

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SearchParams(BaseModel):
    query: str = Field(..., min_length=1)


class FetchParams(BaseModel):
    series_id: str = Field(..., min_length=1)


class OtherParams(BaseModel):
    value: str


def _make_search_df(query: str) -> pd.DataFrame:
    return pd.DataFrame({"id": ["A", "B"], "title": [f"Series about {query}", "Another"]})


def _make_fetch_df() -> pd.DataFrame:
    return pd.DataFrame({"date": ["2020-01-01", "2020-02-01"], "value": [1.0, 2.0]})


@connector()
async def demo_search(params: SearchParams) -> pd.DataFrame:
    """Search for test series by keyword."""
    return _make_search_df(params.query)


@connector()
async def demo_fetch(params: FetchParams) -> pd.DataFrame:
    """Fetch test time series observations."""
    return _make_fetch_df()


def _fake_connectors() -> Connectors:
    return Connectors([demo_search, demo_fetch])


# ---------------------------------------------------------------------------
# Connector tests
# ---------------------------------------------------------------------------


class TestConnectorBind:
    def test_bind_returns_connector_with_empty_dep_names(self) -> None:
        @connector()
        async def needs_key(params: SearchParams, *, api_key: str) -> pd.DataFrame:
            """Needs key."""
            return _make_search_df(params.query)

        bound = needs_key.bind(api_key="x")
        assert isinstance(bound, Connector)
        assert bound.dep_names == frozenset()
        result = asyncio.run(bound(query="GDP"))
        assert len(result.data) == 2

    def test_bind_can_be_composed(self) -> None:
        @connector()
        async def needs_two(params: SearchParams, *, api_key: str, base_url: str) -> pd.DataFrame:
            """Needs two deps."""
            return _make_search_df(params.query)

        partially_bound = needs_two.bind(api_key="x")
        assert partially_bound.dep_names == frozenset({"base_url"})

        fully_bound = partially_bound.bind(base_url="https://example.test")
        assert fully_bound.dep_names == frozenset()
        result = asyncio.run(fully_bound(query="GDP"))
        assert len(result.data) == 2

    def test_connectors_bind_registers_without_register_deps(self) -> None:
        @connector()
        async def a(params: SearchParams, *, api_key: str) -> pd.DataFrame:
            """A."""
            return _make_search_df(params.query)

        @connector()
        async def b(params: FetchParams, *, api_key: str) -> pd.DataFrame:
            """B."""
            return _make_fetch_df()

        wired = Connectors([a, b]).bind(api_key="k")
        assert wired.names() == ["a", "b"]

    def test_unbound_connector_call_raises(self) -> None:
        @connector()
        async def needs_key(params: SearchParams, *, api_key: str) -> pd.DataFrame:
            """Needs key."""
            return _make_search_df(params.query)

        with pytest.raises(TypeError, match="unbound dependencies"):
            asyncio.run(needs_key(query="x"))


class TestConnectorDecoratorOverrides:
    """Explicit ``name=`` / ``description=`` when inference is not enough (escape hatches)."""

    def test_explicit_name_and_description(self) -> None:
        @connector(name="public_connector", description="Stable agent-facing description.")
        async def _internal(params: SearchParams) -> pd.DataFrame:
            """Implementation docstring; overridden by description= above."""
            return _make_search_df(params.query)

        assert _internal.name == "public_connector"
        assert _internal.description == "Stable agent-facing description."
        result = asyncio.run(_internal(query="GDP"))
        assert result.provenance.source == "public_connector"

    def test_missing_docstring_and_description_raises(self) -> None:
        with pytest.raises(ValueError, match="docstring"):

            @connector()
            async def _no_docs(params: SearchParams) -> pd.DataFrame:
                return _make_search_df(params.query)


class TestEnumerator:
    """@enumerator is a constrained @connector for catalog population schemas."""

    def test_enumerator_rejects_data_columns(self) -> None:
        with pytest.raises(ValueError, match="DATA"):

            @enumerator(
                output=OutputConfig(
                    columns=[
                        Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                        Column(name="title", role=ColumnRole.TITLE),
                        Column(name="v", role=ColumnRole.DATA),
                    ],
                ),
            )
            async def _bad(_p: SearchParams) -> pd.DataFrame:
                """Bad."""
                return pd.DataFrame()

    def test_enumerator_requires_exactly_one_key(self) -> None:
        with pytest.raises(ValueError, match="exactly one KEY"):

            @enumerator(
                output=OutputConfig(
                    columns=[
                        Column(name="title", role=ColumnRole.TITLE),
                        Column(name="meta", role=ColumnRole.METADATA),
                    ],
                ),
            )
            async def _no_key(_p: SearchParams) -> pd.DataFrame:
                """No key."""
                return pd.DataFrame()

    def test_enumerator_accepts_key_without_namespace(self) -> None:
        """KEY.namespace is optional on enumerators — catalog supplies default at index time."""

        @enumerator(
            output=OutputConfig(
                columns=[
                    Column(name="id", role=ColumnRole.KEY),
                    Column(name="title", role=ColumnRole.TITLE),
                ],
            ),
        )
        async def _no_ns(_p: SearchParams) -> pd.DataFrame:
            """No namespace."""
            return pd.DataFrame({"id": ["a"], "title": ["A"]})

        assert _no_ns.output_config is not None
        assert _no_ns.output_config.columns[0].namespace is None

    def test_enumerator_requires_title_column(self) -> None:
        with pytest.raises(ValueError, match="TITLE"):

            @enumerator(
                output=OutputConfig(
                    columns=[
                        Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                        Column(name="meta", role=ColumnRole.METADATA),
                    ],
                ),
            )
            async def _no_title(_p: SearchParams) -> pd.DataFrame:
                """No title."""
                return pd.DataFrame()

    def test_enumerator_returns_connector_with_output(self) -> None:
        @enumerator(
            output=OutputConfig(
                columns=[
                    Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                    Column(name="title", role=ColumnRole.TITLE),
                ],
            ),
        )
        async def demo_enum(_p: SearchParams) -> pd.DataFrame:
            """List entities."""
            return pd.DataFrame({"id": ["a"], "title": ["A"]})

        assert isinstance(demo_enum, Connector)
        assert demo_enum.output_config is not None
        res = asyncio.run(demo_enum(query="x"))
        assert isinstance(res, Result)
        assert res.output_schema is not None


class TestLoader:
    """@loader is a constrained @connector for data persistence schemas."""

    def test_loader_rejects_title_columns(self) -> None:
        with pytest.raises(ValueError, match="TITLE"):

            @loader(
                output=OutputConfig(
                    columns=[
                        Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                        Column(name="title", role=ColumnRole.TITLE),
                        Column(name="v", role=ColumnRole.DATA),
                    ],
                ),
            )
            async def _bad(_p: SearchParams) -> pd.DataFrame:
                """Bad."""
                return pd.DataFrame()

    def test_loader_rejects_metadata_columns(self) -> None:
        with pytest.raises(ValueError, match="METADATA"):

            @loader(
                output=OutputConfig(
                    columns=[
                        Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                        Column(name="meta", role=ColumnRole.METADATA),
                        Column(name="v", role=ColumnRole.DATA),
                    ],
                ),
            )
            async def _bad(_p: SearchParams) -> pd.DataFrame:
                """Bad."""
                return pd.DataFrame()

    def test_loader_requires_data_columns(self) -> None:
        with pytest.raises(ValueError, match="DATA"):

            @loader(
                output=OutputConfig(
                    columns=[
                        Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                    ],
                ),
            )
            async def _bad(_p: SearchParams) -> pd.DataFrame:
                """Bad."""
                return pd.DataFrame()

    def test_loader_requires_exactly_one_key(self) -> None:
        with pytest.raises(ValueError, match="exactly one KEY"):

            @loader(
                output=OutputConfig(
                    columns=[
                        Column(name="v", role=ColumnRole.DATA),
                    ],
                ),
            )
            async def _bad(_p: SearchParams) -> pd.DataFrame:
                """Bad."""
                return pd.DataFrame()

    def test_loader_requires_key_namespace(self) -> None:
        with pytest.raises(ValueError, match="namespace"):

            @loader(
                output=OutputConfig(
                    columns=[
                        Column(name="id", role=ColumnRole.KEY),
                        Column(name="v", role=ColumnRole.DATA),
                    ],
                ),
            )
            async def _bad(_p: SearchParams) -> pd.DataFrame:
                """Bad."""
                return pd.DataFrame()

    def test_loader_returns_connector_with_output(self) -> None:
        @loader(
            output=OutputConfig(
                columns=[
                    Column(name="id", role=ColumnRole.KEY, namespace="ns"),
                    Column(name="v", role=ColumnRole.DATA),
                ],
            ),
        )
        async def demo_load(_p: SearchParams) -> pd.DataFrame:
            """Load observations."""
            return pd.DataFrame({"id": ["a"], "v": [1.0]})

        assert isinstance(demo_load, Connector)
        assert demo_load.output_config is not None
        res = asyncio.run(demo_load(query="x"))
        assert isinstance(res, Result)
        assert res.output_schema is not None


class TestConnector:
    def test_repr_includes_name_and_params(self) -> None:
        c = demo_search
        r = repr(c)
        assert "demo_search" in r
        assert "query" in r

    def test_repr_includes_description(self) -> None:
        c = demo_search
        r = repr(c)
        assert "Search for test series" in r

    def test_execute_via_bound(self) -> None:
        c = _fake_connectors()
        result = asyncio.run(c["demo_search"](query="GDP"))
        assert isinstance(result, Result)
        assert result.output_schema is None
        assert len(result.data) == 2
        assert result.provenance.params["query"] == "GDP"

    def test_execute_fetch(self) -> None:
        c = Connectors([demo_fetch])
        result = asyncio.run(c["demo_fetch"](series_id="GDPC1"))
        assert result.output_schema is None
        assert list(result.data.columns) == ["date", "value"]
        assert result.output_schema is None
        assert result.provenance.params["series_id"] == "GDPC1"

    def test_execute_wrong_model_type_fails_validation(self) -> None:
        c = Connectors([demo_fetch])
        with pytest.raises(ValidationError):
            asyncio.run(c["demo_fetch"](OtherParams(value="x")))


# ---------------------------------------------------------------------------
# Connectors collection
# ---------------------------------------------------------------------------


class TestConnectorsCollection:
    def _build(self) -> Connectors:
        return _fake_connectors()

    def test_names(self) -> None:
        c = self._build()
        assert c.names() == ["demo_fetch", "demo_search"]

    def test_iter_and_len(self) -> None:
        c = self._build()
        assert len(c) == 2
        assert all(isinstance(op, Connector) for op in c)

    def test_getitem_str(self) -> None:
        c = self._build()
        assert c["demo_search"].name == "demo_search"

    def test_get_returns_connector(self) -> None:
        c = self._build()
        assert c.get("demo_search") is c["demo_search"]

    def test_get_missing_returns_none(self) -> None:
        c = self._build()
        assert c.get("bogus") is None

    def test_getitem_missing_raises(self) -> None:
        c = self._build()
        with pytest.raises(KeyError, match="No connector 'bogus'"):
            _ = c["bogus"]

    def test_contains(self) -> None:
        c = self._build()
        assert "demo_search" in c
        assert "nope" not in c
        assert 0 not in c

    def test_execute_unknown_raises(self) -> None:
        c = self._build()
        with pytest.raises(KeyError, match="No connector 'bogus'"):
            asyncio.run(c["bogus"]())

    def test_init_raises_on_duplicate_connector_names(self) -> None:
        with pytest.raises(ValueError, match="Duplicate connector names"):
            Connectors([demo_search, demo_search])

# ---------------------------------------------------------------------------
# kwargs calling convention
# ---------------------------------------------------------------------------


class TestKwargsCalling:
    def test_call_with_kwargs(self) -> None:
        result = asyncio.run(demo_search(query="GDP"))
        assert len(result.data) == 2
        assert result.provenance.params["query"] == "GDP"

    def test_call_with_kwargs_via_collection(self) -> None:
        c = _fake_connectors()
        result = asyncio.run(c["demo_fetch"](series_id="GDPC1"))
        assert result.provenance.params["series_id"] == "GDPC1"

    def test_kwargs_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            asyncio.run(demo_search(query=""))  # min_length=1

    def test_kwargs_and_params_raises(self) -> None:
        with pytest.raises(TypeError, match="Pass either params"):
            asyncio.run(demo_search(SearchParams(query="GDP"), query="GDP"))

    def test_dict_input_rejected(self) -> None:
        with pytest.raises(TypeError, match="got dict"):
            asyncio.run(demo_search({"query": "GDP"}))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Result wrapping
# ---------------------------------------------------------------------------


class TestResultWrap:
    def test_iter_returns_connector_instances(self) -> None:
        c = _fake_connectors()
        assert all(isinstance(op, Connector) for op in c)

    def test_wrap_sets_source_description_on_provenance(self) -> None:
        result = asyncio.run(demo_search(query="GDP"))
        assert result.provenance.source == "demo_search"
        assert "Search for test series" in result.provenance.source_description


# ---------------------------------------------------------------------------
# Result callbacks (with_callback) — observer semantics
# ---------------------------------------------------------------------------


class TestCallback:
    def test_callback_fires_on_success(self) -> None:
        log: list[str] = []

        def cb(result: Result) -> None:
            log.append(result.provenance.source)

        c = _fake_connectors().with_callback(cb)
        asyncio.run(c["demo_search"](query="GDP"))
        assert log == ["demo_search"]

    def test_callback_does_not_fire_on_validation_error(self) -> None:
        log: list[str] = []

        def cb(result: Result) -> None:
            log.append(result.provenance.source)

        c = _fake_connectors().with_callback(cb)
        with pytest.raises(ValidationError):
            asyncio.run(c["demo_search"](query=""))
        assert log == []

    def test_callback_preserved_through_bind(self) -> None:
        log: list[str] = []

        @connector()
        async def keyed(params: SearchParams, *, api_key: str) -> pd.DataFrame:
            """Keyed."""
            return _make_search_df(params.query)

        c = Connectors([keyed]).with_callback(lambda r: log.append(r.provenance.source)).bind(api_key="k")
        asyncio.run(c["keyed"](query="GDP"))
        assert log == ["keyed"]

    def test_async_callback_awaited(self) -> None:
        log: list[str] = []

        async def cb(result: Result) -> None:
            log.append(result.provenance.source)

        c = _fake_connectors().with_callback(cb)
        asyncio.run(c["demo_search"](query="GDP"))
        assert log == ["demo_search"]

    def test_callback_exceptions_are_logged_not_raised(self) -> None:
        """Observer semantics: a failing callback never breaks the caller's result."""

        def boom(_result: Result) -> None:
            raise RuntimeError("callback broke")

        c = _fake_connectors().with_callback(boom)
        result = asyncio.run(c["demo_search"](query="GDP"))
        assert len(result.data) == 2  # caller still gets their result

    def test_per_connector_callback(self) -> None:
        """Callback on one connector does not fire on others in the collection."""
        log: list[str] = []

        fetcher_with_cb = demo_fetch.with_callback(lambda r: log.append(r.provenance.source))
        c = Connectors([demo_search, fetcher_with_cb])
        asyncio.run(c["demo_fetch"](series_id="X"))
        assert log == ["demo_fetch"]
        log.clear()
        asyncio.run(c["demo_search"](query="GDP"))
        assert log == []

    def test_chained_callbacks_fire_in_order(self) -> None:
        log: list[str] = []
        c = _fake_connectors().with_callback(lambda r: log.append("a")).with_callback(lambda r: log.append("b"))
        asyncio.run(c["demo_search"](query="GDP"))
        assert log == ["a", "b"]


# ---------------------------------------------------------------------------
# Connectors.to_llm()
# ---------------------------------------------------------------------------


class TestConnectorsToLlm:
    def test_caller_header_prepended(self) -> None:
        c = _fake_connectors()
        text = c.to_llm(header="## USAGE GUIDE\n`await client[name](...)`")
        assert "## USAGE GUIDE" in text
        assert "await client" in text

    def test_default_is_bare(self) -> None:
        """No product-specific prose in the kernel — host owns the header."""
        c = _fake_connectors()
        text = c.to_llm()
        assert "# Data connectors" not in text

    def test_includes_all_connector_names(self) -> None:
        c = _fake_connectors()
        text = c.to_llm()
        for conn in c:
            assert conn.name in text

    def test_includes_parameter_info(self) -> None:
        c = _fake_connectors()
        text = c.to_llm()
        assert "query" in text  # SearchParams.query
        assert "series_id" in text  # FetchParams.series_id

    def test_empty_collection(self) -> None:
        c = Connectors([])
        assert c.to_llm() == ""

    def test_single_connector_to_llm_in_output(self) -> None:
        c = _fake_connectors()
        text = c.to_llm()
        # Each connector's to_llm() output should appear
        for conn in c:
            assert conn.to_llm() in text

    def test_no_decorative_separators(self) -> None:
        c = _fake_connectors()
        text = c.to_llm()
        assert "───" not in text
