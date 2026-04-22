"""Tests for the pytest-native ProviderTestSuite base class."""

from __future__ import annotations

import sys
import types
from typing import Any

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector
from parsimony.result import Provenance, Result
from parsimony.testing import ConformanceError, ProviderTestSuite


class _Params(BaseModel):
    pass


async def _demo_fn(params: _Params) -> Result:
    return Result.from_dataframe(
        pd.DataFrame({"x": [1]}),
        Provenance(source="demo"),
    )


def _make_connector(
    name: str,
    *,
    description: str = "A connector with a sufficiently long description.",
    tags: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> Any:
    return connector(
        name=name,
        description=description,
        params=_Params,
        tags=tags or [],
        env=env,
    )(_demo_fn)


def _make_module(
    name: str,
    *,
    connectors: list[Any],
) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.CONNECTORS = Connectors(connectors)
    sys.modules[name] = mod
    return mod


# ---------------------------------------------------------------------------
# Happy path — all three checks pass
# ---------------------------------------------------------------------------


def test_happy_path_suite_passes_all_checks() -> None:
    c = _make_connector("demo_fetch")
    _make_module("test_happy_module", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_happy_module"

    s = Suite()
    s.test_connectors_exported()
    s.test_descriptions_non_empty()
    s.test_env_map_matches_deps()


# ---------------------------------------------------------------------------
# Configuration errors
# ---------------------------------------------------------------------------


def test_missing_module_raises_typeerror() -> None:
    class Suite(ProviderTestSuite):
        pass

    with pytest.raises(TypeError, match="module"):
        Suite().test_connectors_exported()


def test_module_attribute_overrides_module_path() -> None:
    c = _make_connector("demo_fetch")
    mod = _make_module("test_module_attr", connectors=[c])

    class Suite(ProviderTestSuite):
        module = mod

    Suite().test_connectors_exported()


# ---------------------------------------------------------------------------
# Contract failures surface as ConformanceError
# ---------------------------------------------------------------------------


def test_missing_connectors_export_raises_conformance_error() -> None:
    mod = types.ModuleType("test_no_connectors")
    sys.modules["test_no_connectors"] = mod

    class Suite(ProviderTestSuite):
        module = mod

    with pytest.raises(ConformanceError, match="CONNECTORS"):
        Suite().test_connectors_exported()


def test_env_map_key_not_matching_dep_fails() -> None:
    # A connector with no dep but an env_map keyed on "nonexistent_dep".
    async def _public(params: _Params) -> Result:
        return Result.from_dataframe(
            pd.DataFrame({"x": [1]}),
            Provenance(source="public"),
        )

    bad = connector(
        name="bad_fetch",
        description="Has env_map without dep.",
        params=_Params,
        env={"nonexistent_dep": "DEMO_API_KEY"},
    )(_public)
    _make_module("test_env_bad_key", connectors=[bad])

    class Suite(ProviderTestSuite):
        module_path = "test_env_bad_key"

    with pytest.raises(ConformanceError, match="env_map key"):
        Suite().test_env_map_matches_deps()


# ---------------------------------------------------------------------------
# Entry-point resolution method
# ---------------------------------------------------------------------------


def test_entry_point_skips_when_name_not_set() -> None:
    c = _make_connector("demo_fetch")
    _make_module("test_ep_skip", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_ep_skip"

    with pytest.raises(pytest.skip.Exception):
        Suite().test_entry_point_resolves()
