"""Tests for :mod:`parsimony.testing` — the plugin conformance suite."""

from types import ModuleType
from typing import Any

import pandas as pd
import pytest

from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputSpec


def _mk_connector(
    name: str,
    *,
    doc: str = "Fetch a toy observation.",
    tags: list[str] | None = None,
) -> Any:
    def _fn(x: str = "y") -> dict[str, Any]:
        return {"ok": True, "x": x}

    _fn.__doc__ = doc
    _fn.__name__ = name
    return connector(tags=tags)(_fn)


def _make_module(
    name: str,
    *,
    connectors: Connectors | None = None,
) -> ModuleType:
    mod = ModuleType(name)
    if connectors is not None:
        mod.CONNECTORS = connectors  # type: ignore[attr-defined]
    return mod


def test_valid_plugin_passes() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module("pkg_good", connectors=Connectors([_mk_connector("good_fetch")]))
    assert_plugin_valid(mod)


def test_missing_connectors_attribute_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    mod = _make_module("pkg_nothing")
    with pytest.raises(ConformanceError, match="CONNECTORS"):
        assert_plugin_valid(mod)


def test_non_connectors_type_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    mod = _make_module("pkg_list")
    mod.CONNECTORS = [_mk_connector("foo")]  # type: ignore[attr-defined]
    with pytest.raises(ConformanceError, match="Connectors"):
        assert_plugin_valid(mod)


def test_empty_connectors_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    mod = _make_module("pkg_empty", connectors=Connectors([]))
    with pytest.raises(ConformanceError, match="at least one connector"):
        assert_plugin_valid(mod)


def test_connector_with_empty_description_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    toy = _mk_connector("fine")
    object.__setattr__(toy, "description", "   ")
    mod = _make_module("pkg_blank", connectors=Connectors([toy]))
    with pytest.raises(ConformanceError, match="empty description"):
        assert_plugin_valid(mod)


def test_description_too_short_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    toy = _mk_connector("fine", doc="Too short desc.")
    mod = _make_module("pkg_short_desc", connectors=Connectors([toy]))
    with pytest.raises(ConformanceError, match="too short"):
        assert_plugin_valid(mod)


def test_description_too_long_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    toy = _mk_connector("fine", doc="x" * 801)
    mod = _make_module("pkg_long_desc", connectors=Connectors([toy]))
    with pytest.raises(ConformanceError, match="too long"):
        assert_plugin_valid(mod)


ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


def test_enumerator_missing_return_annotation_fails() -> None:
    with pytest.raises(ValueError, match="must annotate return type"):

        @enumerator(output=ENUMERATE_OUTPUT, name="bad_enum")
        def bad_enum():
            """An enumerator without a return type annotation."""
            return pd.DataFrame()


def test_enumerator_list_entity_return_annotation_fails() -> None:
    with pytest.raises(ValueError, match="pd.DataFrame"):

        @enumerator(output=ENUMERATE_OUTPUT, name="entity_enum")
        def entity_enum() -> list:
            """An enumerator that declares list return type."""
            return []


def test_iter_check_names_contains_minimal_checks() -> None:
    from parsimony.testing import iter_check_names

    assert set(iter_check_names()) == {
        "check_connectors_exported",
        "check_descriptions_non_empty",
        "check_enumerator_decorator",
        "check_enumerator_return_type",
        "check_flat_public_params",
        "check_secrets_declared",
    }
