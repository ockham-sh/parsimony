"""Behavioral tests for parsimony.bundles.discovery (plugin spec discovery).

Coverage:

- Connectors **without** a catalog declaration are skipped (not yielded as
  errors).
- Connectors **with** a catalog declaration are yielded once each.
- Yield order is deterministic (provider order × connector order).

Strategy: stub :func:`parsimony.discovery.discovered_providers` so
the test depends only on the spec-shape contract, not on which plugins
happen to be installed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from parsimony.bundles.discovery import iter_specs
from parsimony.bundles.spec import (
    CatalogPlan,
    CatalogSpec,
)
from parsimony.connector import Connectors, connector
from parsimony.discovery import DiscoveredProvider
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result

_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="example"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


class _NoParams(BaseModel):
    pass


def _make_provider(*, name: str, connectors: list) -> DiscoveredProvider:
    return DiscoveredProvider(
        name=name,
        module_path=f"parsimony_fake_{name}",
        connectors=Connectors(connectors),
    )


@pytest.fixture
def stub_providers(monkeypatch):
    def _set(providers):
        monkeypatch.setattr(
            "parsimony.bundles.discovery.discovered_providers",
            lambda: providers,
        )

    return _set


# ---------------------------------------------------------------------------
# Filter behavior
# ---------------------------------------------------------------------------


def test_iter_specs_skips_connector_without_catalog(stub_providers):
    """Connectors without ``properties['catalog']`` must not appear."""

    @connector(output=_OUTPUT)
    async def fetch_no_catalog(params: _NoParams) -> Result:
        """No catalog declaration here."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["x"], "title": ["X"]}),
            Provenance(source="x"),
        )

    from parsimony.connector import enumerator

    @enumerator(output=_OUTPUT, catalog=CatalogSpec.static(namespace="alpha"))
    async def list_alpha(params: _NoParams) -> Result:
        """List things."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["a"], "title": ["A"]}),
            Provenance(source="alpha"),
        )

    provider = _make_provider(
        name="alpha_provider",
        connectors=[fetch_no_catalog, list_alpha],
    )
    stub_providers([provider])

    discovered = list(iter_specs())
    assert len(discovered) == 1
    assert discovered[0].connector.name == "list_alpha"
    assert discovered[0].spec.static_namespace == "alpha"


# ---------------------------------------------------------------------------
# Order
# ---------------------------------------------------------------------------


def test_iter_specs_order_is_deterministic(stub_providers):
    """Output order tracks provider × connector iteration order."""
    from parsimony.connector import enumerator

    @enumerator(output=_OUTPUT, catalog=CatalogSpec.static(namespace="alpha"))
    async def list_alpha(params: _NoParams) -> Result:
        """List."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["a"], "title": ["A"]}),
            Provenance(source="alpha"),
        )

    @enumerator(output=_OUTPUT, catalog=CatalogSpec.static(namespace="beta"))
    async def list_beta(params: _NoParams) -> Result:
        """List."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["b"], "title": ["B"]}),
            Provenance(source="beta"),
        )

    @enumerator(output=_OUTPUT, catalog=CatalogSpec.static(namespace="gamma"))
    async def list_gamma(params: _NoParams) -> Result:
        """List."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["c"], "title": ["C"]}),
            Provenance(source="gamma"),
        )

    provider1 = _make_provider(name="p1", connectors=[list_alpha, list_beta])
    provider2 = _make_provider(name="p2", connectors=[list_gamma])
    stub_providers([provider1, provider2])

    seen = [d.connector.name for d in iter_specs()]
    assert seen == ["list_alpha", "list_beta", "list_gamma"]


# ---------------------------------------------------------------------------
# Dynamic spec yields ok
# ---------------------------------------------------------------------------


def test_iter_specs_yields_dynamic_specs(stub_providers):
    from parsimony.connector import enumerator

    async def _plan() -> AsyncIterator[CatalogPlan]:
        yield CatalogPlan(namespace="dyn_a")
        yield CatalogPlan(namespace="dyn_b")

    @enumerator(output=_OUTPUT, catalog=CatalogSpec(plan=_plan))
    async def list_dyn(params: _NoParams) -> Result:
        """List."""
        import pandas as pd

        return Result.from_dataframe(
            pd.DataFrame({"code": ["x"], "title": ["X"]}),
            Provenance(source="dyn"),
        )

    provider = _make_provider(name="p_dyn", connectors=[list_dyn])
    stub_providers([provider])

    discovered = list(iter_specs())
    assert len(discovered) == 1
    assert isinstance(discovered[0].spec, CatalogSpec)
    assert discovered[0].spec.static_namespace is None
