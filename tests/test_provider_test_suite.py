"""Tests for the pytest-native ProviderTestSuite base class."""

from __future__ import annotations

import sys
import types
from typing import Any

import pandas as pd
import pytest

from parsimony.connector import Connectors, connector
from parsimony.result import Result, TabularResult
from parsimony.testing import ConformanceError, ProviderTestSuite


async def _demo_fn() -> Result:
    return TabularResult.from_dataframe(pd.DataFrame({"x": [1]}))


def _make_connector(
    name: str,
    *,
    description: str = "A connector with a sufficiently long description.",
    tags: list[str] | None = None,
) -> Any:
    return connector(name=name, description=description, tags=tags or [])(_demo_fn)


def _make_module(
    name: str,
    *,
    connectors: list[Any],
) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.CONNECTORS = Connectors(connectors)
    sys.modules[name] = mod
    return mod


def test_happy_path_suite_passes_all_checks() -> None:
    c = _make_connector("demo_fetch")
    _make_module("test_happy_module", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_happy_module"

    Suite().test_plugin_conforms()


def test_missing_module_raises_typeerror() -> None:
    class Suite(ProviderTestSuite):
        pass

    with pytest.raises(TypeError, match="module"):
        Suite().test_plugin_conforms()


def test_module_attribute_overrides_module_path() -> None:
    c = _make_connector("demo_fetch")
    mod = _make_module("test_module_attr", connectors=[c])

    class Suite(ProviderTestSuite):
        module = mod

    Suite().test_plugin_conforms()


def test_missing_connectors_export_raises_conformance_error() -> None:
    mod = types.ModuleType("test_no_connectors")
    sys.modules["test_no_connectors"] = mod

    class Suite(ProviderTestSuite):
        module = mod

    with pytest.raises(ConformanceError, match="CONNECTORS"):
        Suite().test_plugin_conforms()


def test_entry_point_skips_when_name_not_set() -> None:
    c = _make_connector("demo_fetch")
    _make_module("test_ep_skip", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_ep_skip"

    with pytest.raises(pytest.skip.Exception):
        Suite().test_entry_point_resolves()
