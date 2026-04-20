"""Tests for the pytest-native ProviderTestSuite base class.

We exercise the base class by subclassing it against stub modules, then
collecting+running the inherited test_* methods via pytest's programmatic
API.
"""

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

# ---------------------------------------------------------------------------
# Fixture plugin modules
# ---------------------------------------------------------------------------


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
    description: str = "A connector with a sufficiently long description for MCP.",
    tags: list[str] | None = None,
) -> Any:
    return connector(
        name=name,
        description=description,
        params=_Params,
        tags=tags or [],
    )(_demo_fn)


def _make_module(
    name: str,
    *,
    connectors: list[Any],
    env_vars: dict[str, str] | None = None,
    provider_metadata: dict[str, Any] | None = None,
) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.CONNECTORS = Connectors(connectors)
    if env_vars is not None:
        mod.ENV_VARS = env_vars
    if provider_metadata is not None:
        mod.PROVIDER_METADATA = provider_metadata
    sys.modules[name] = mod
    return mod


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_suite_passes_all_checks() -> None:
    c = _make_connector("demo_fetch", tags=["tool"])
    _make_module("test_happy_module", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_happy_module"

    s = Suite()
    s.test_connectors_exported()
    s.test_descriptions_non_empty()
    s.test_tool_tag_description_length()
    s.test_env_vars_shape()
    s.test_env_vars_map_to_deps()
    s.test_name_env_var_collisions()
    s.test_provider_metadata_shape()


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

    Suite().test_connectors_exported()  # does not import by path


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


def test_tool_short_description_fails() -> None:
    c = _make_connector("short_tool", description="too short", tags=["tool"])
    _make_module("test_short_tool", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_short_tool"

    with pytest.raises(ConformanceError, match="tool-tagged"):
        Suite().test_tool_tag_description_length()


def test_env_var_not_mapping_to_dep_fails() -> None:
    c = _make_connector("demo_fetch")
    _make_module(
        "test_env_bad_key",
        connectors=[c],
        env_vars={"nonexistent_dep": "DEMO_API_KEY"},
    )

    class Suite(ProviderTestSuite):
        module_path = "test_env_bad_key"

    with pytest.raises(ConformanceError, match="ENV_VARS key"):
        Suite().test_env_vars_map_to_deps()


def test_provider_metadata_wrong_type_fails() -> None:
    c = _make_connector("demo_fetch")
    _make_module(
        "test_bad_meta",
        connectors=[c],
        provider_metadata="not a dict",  # type: ignore[arg-type]
    )

    class Suite(ProviderTestSuite):
        module_path = "test_bad_meta"

    with pytest.raises(ConformanceError, match="PROVIDER_METADATA"):
        Suite().test_provider_metadata_shape()


# ---------------------------------------------------------------------------
# Entry-point path
# ---------------------------------------------------------------------------


def test_entry_point_skips_when_name_not_set() -> None:
    c = _make_connector("demo_fetch")
    _make_module("test_ep_skip", connectors=[c])

    class Suite(ProviderTestSuite):
        module_path = "test_ep_skip"
        # entry_point_name = None  (default)

    # Method calls pytest.skip — pytest raises Skipped internally.
    with pytest.raises(pytest.skip.Exception):
        Suite().test_entry_point_resolves()
