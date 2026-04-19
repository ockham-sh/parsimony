"""Self-tests for ProviderTestSuite.

Constructs a deliberately well-formed provider and a deliberately broken one,
then runs the suite against each to verify the checks fire as expected.
"""

from __future__ import annotations

from typing import Any

import pytest
from parsimony.connector import Connectors, connector
from parsimony.connectors import ProviderSpec
from parsimony_plugin_tests import ProviderTestSuite
from pydantic import BaseModel


class _Params(BaseModel):
    q: str = "x"


@connector(name="demo_fetch", description="Fetch demo data.")
async def _demo_fetch(params: _Params) -> dict[str, Any]:
    return {"q": params.q}


_DEMO_PROVIDER = ProviderSpec(name="demo", connectors=Connectors([_demo_fetch]))


class TestDemoProviderConformance(ProviderTestSuite):
    """Reference conformance run: the demo provider must pass everything."""

    provider = _DEMO_PROVIDER


# --------------------------------------------------------------------------
# Negative tests: each fixture below intentionally violates one rule and we
# assert the corresponding check fails.
# --------------------------------------------------------------------------


def _suite_for(provider: ProviderSpec) -> ProviderTestSuite:
    suite = ProviderTestSuite()
    suite.provider = provider  # type: ignore[misc]
    return suite


def test_uppercase_provider_name_fails() -> None:
    bad = ProviderSpec(name="DEMO", connectors=Connectors([_demo_fetch]))
    with pytest.raises(AssertionError, match="lowercase"):
        _suite_for(bad).test_provider_name_is_nonempty_slug()


def test_wrong_prefix_fails() -> None:
    @connector(name="other_fetch", description="x")
    async def _wrong_prefix(params: _Params) -> dict[str, Any]:
        return {}

    bad = ProviderSpec(name="demo", connectors=Connectors([_wrong_prefix]))
    with pytest.raises(AssertionError, match="start with"):
        _suite_for(bad).test_connector_names_share_provider_prefix()


def test_unknown_env_var_dep_fails() -> None:
    bad = ProviderSpec(
        name="demo",
        connectors=Connectors([_demo_fetch]),
        env_vars={"api_key": "DEMO_API_KEY"},
    )
    with pytest.raises(AssertionError, match="unknown deps"):
        _suite_for(bad).test_env_var_dep_names_are_known()


def test_lowercase_env_var_value_fails() -> None:
    @connector(name="demo_fetch_keyed", description="x")
    async def _keyed(params: _Params, *, api_key: str) -> dict[str, Any]:
        return {"key": api_key}

    bad = ProviderSpec(
        name="demo",
        connectors=Connectors([_keyed]),
        env_vars={"api_key": "demo_api_key"},  # lowercase value
    )
    with pytest.raises(AssertionError, match="UPPER_SNAKE_CASE"):
        _suite_for(bad).test_env_var_keys_are_uppercase()
