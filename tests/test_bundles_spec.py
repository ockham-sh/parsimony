"""Unit tests for parsimony.bundles.spec — CatalogSpec + decorator validation."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from parsimony.bundles.errors import BundleSpecError
from parsimony.bundles.spec import (
    DEFAULT_TARGET,
    CatalogPlan,
    CatalogSpec,
    from_decorator_kwargs,
    materialize,
    to_async,
)
from parsimony.connector import connector, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result

# ---------------------------------------------------------------------------
# CatalogPlan
# ---------------------------------------------------------------------------


def test_catalog_plan_freezes_params():
    p = CatalogPlan(namespace="treasury", params={"k": "v"})
    # The dataclass is frozen, so attribute assignment fails.
    with pytest.raises((AttributeError, TypeError)):
        p.namespace = "other"  # type: ignore[misc]


def test_catalog_plan_rejects_empty_namespace():
    with pytest.raises(BundleSpecError):
        CatalogPlan(namespace="")


def test_catalog_plan_default_params_is_empty_mapping():
    p = CatalogPlan(namespace="treasury")
    assert dict(p.params) == {}


# ---------------------------------------------------------------------------
# CatalogSpec.static
# ---------------------------------------------------------------------------


def test_static_spec_defaults():
    s = CatalogSpec.static(namespace="treasury")
    assert s.target == DEFAULT_TARGET
    assert s.embed is True
    assert s.static_namespace == "treasury"
    assert callable(s.plan)


def test_static_spec_rejects_empty_namespace():
    with pytest.raises(BundleSpecError):
        CatalogSpec.static(namespace="")


def test_static_spec_plan_yields_one_item():
    """The synthetic plan must yield exactly one CatalogPlan with the namespace."""
    import asyncio

    spec = CatalogSpec.static(namespace="treasury")

    async def collect():
        return [item async for item in spec.plan()]

    items = asyncio.run(collect())
    assert len(items) == 1
    assert items[0].namespace == "treasury"


# ---------------------------------------------------------------------------
# CatalogSpec (dynamic plan)
# ---------------------------------------------------------------------------


async def _example_plan() -> AsyncIterator[CatalogPlan]:
    yield CatalogPlan(namespace="example_a")
    yield CatalogPlan(namespace="example_b")


def test_dynamic_spec_accepts_callable():
    s = CatalogSpec(plan=_example_plan)
    assert s.plan is _example_plan
    assert s.target == DEFAULT_TARGET
    assert s.static_namespace is None


def test_dynamic_spec_rejects_string_plan():
    with pytest.raises(BundleSpecError, match="callable|string"):
        CatalogSpec(plan="parsimony.example._plan")  # type: ignore[arg-type]


def test_dynamic_spec_rejects_non_callable():
    with pytest.raises(BundleSpecError, match="callable"):
        CatalogSpec(plan=42)  # type: ignore[arg-type]


def test_dynamic_spec_rejects_non_bool_embed():
    with pytest.raises(BundleSpecError):
        CatalogSpec(plan=_example_plan, embed=1)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# from_decorator_kwargs (typed-spec validation + provenance check)
# ---------------------------------------------------------------------------


def test_from_decorator_kwargs_passes_typed_spec_through():
    s = CatalogSpec.static(namespace="treasury")
    out = from_decorator_kwargs(s, connector_module=__name__)
    assert out is s


def test_from_decorator_kwargs_passes_dynamic_spec_through():
    s = CatalogSpec(plan=_example_plan)
    out = from_decorator_kwargs(s, connector_module=_example_plan.__module__)
    assert out is s


def test_from_decorator_kwargs_rejects_cross_plugin_plan():
    """Plan must originate from the same top-level package as the connector."""
    with pytest.raises(BundleSpecError, match="originate from the same plugin"):
        from_decorator_kwargs(
            CatalogSpec(plan=_example_plan),
            connector_module="parsimony_evil.connector",
        )


def test_from_decorator_kwargs_rejects_dict_sugar():
    with pytest.raises(BundleSpecError, match="CatalogSpec"):
        from_decorator_kwargs(
            {"namespace": "treasury"},  # type: ignore[arg-type]
            connector_module="parsimony.connectors.treasury",
        )


def test_from_decorator_kwargs_rejects_unsupported_value_type():
    with pytest.raises(BundleSpecError, match="CatalogSpec"):
        from_decorator_kwargs(42, connector_module="parsimony.connectors.x")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# to_async adapter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_to_async_wraps_sync_iterable():
    plans = [CatalogPlan(namespace="a"), CatalogPlan(namespace="b")]
    plan_callable = to_async(plans)
    out = [item async for item in plan_callable()]
    assert [p.namespace for p in out] == ["a", "b"]


@pytest.mark.asyncio
async def test_to_async_wraps_callable_returning_iterable():
    def _src():
        return [CatalogPlan(namespace="x")]

    plan_callable = to_async(_src)
    out = [item async for item in plan_callable()]
    assert out[0].namespace == "x"


@pytest.mark.asyncio
async def test_to_async_rejects_non_plan_items():
    plan_callable = to_async([{"namespace": "x"}])  # type: ignore[list-item]
    with pytest.raises(BundleSpecError, match="CatalogPlan"):
        async for _ in plan_callable():
            pass


# ---------------------------------------------------------------------------
# materialize
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_materialize_static_spec_yields_one_plan():
    s = CatalogSpec.static(namespace="treasury")
    plans = await materialize(s)
    assert len(plans) == 1
    assert plans[0].namespace == "treasury"
    assert dict(plans[0].params) == {}


@pytest.mark.asyncio
async def test_materialize_dynamic_spec_collects_all_yielded():
    s = CatalogSpec(plan=_example_plan)
    plans = await materialize(s)
    assert [p.namespace for p in plans] == ["example_a", "example_b"]


# ---------------------------------------------------------------------------
# Decorator integration
# ---------------------------------------------------------------------------


_ENUM_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="example"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


class _NoParams(BaseModel):
    pass


def test_enumerator_accepts_typed_static_catalog():
    @enumerator(output=_ENUM_OUTPUT, catalog=CatalogSpec.static(namespace="example"))
    async def my_enum(params: _NoParams) -> Result:
        """List things."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["a"], "title": ["A"]}),
            Provenance(source="example"),
        )

    spec = my_enum.properties["catalog"]
    assert isinstance(spec, CatalogSpec)
    assert spec.static_namespace == "example"


def test_enumerator_rejects_dict_sugar():
    """The decorator boundary requires a typed CatalogSpec instance."""
    with pytest.raises(BundleSpecError, match="CatalogSpec"):
        @enumerator(output=_ENUM_OUTPUT, catalog={"namespace": "example"})  # type: ignore[arg-type]
        async def bad_enum(params: _NoParams) -> Result:
            """Bad."""
            return Result.from_dataframe(__import__("pandas").DataFrame(), Provenance(source="x"))


def test_connector_rejects_catalog_kwarg():
    """@connector(catalog=...) raises — the spec belongs on @enumerator only."""
    with pytest.raises(BundleSpecError, match="@enumerator"):

        @connector(catalog=CatalogSpec.static(namespace="example"))
        async def fetch(params: _NoParams) -> Result:
            """Fetch."""
            return Result.from_dataframe(__import__("pandas").DataFrame(), Provenance(source="x"))


def test_enumerator_without_catalog_kwarg_works():
    """Catalog spec is optional — existing enumerators stay valid."""

    @enumerator(output=_ENUM_OUTPUT)
    async def my_enum(params: _NoParams) -> Result:
        """List things."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["a"], "title": ["A"]}),
            Provenance(source="example"),
        )

    assert "catalog" not in my_enum.properties
