"""Tests for :mod:`parsimony.testing` — the plugin conformance suite."""

from types import ModuleType
from typing import Any

import pytest

from parsimony.connector import Connectors, connector


def _mk_connector(
    name: str,
    *,
    doc: str = "Fetch a toy observation.",
    tags: list[str] | None = None,
) -> Any:
    async def _fn(x: str = "y") -> dict[str, Any]:
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


def test_skip_unknown_check_raises() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module("pkg_bad_skip", connectors=Connectors([_mk_connector("good")]))
    with pytest.raises(ValueError, match="unknown"):
        assert_plugin_valid(mod, skip=["not_a_real_check"])


def test_connectors_exported_not_skippable() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module("pkg_skip_first", connectors=Connectors([_mk_connector("x")]))
    with pytest.raises(ValueError, match="not skippable"):
        assert_plugin_valid(mod, skip=["check_connectors_exported"])


def test_iter_check_names_contains_minimal_checks() -> None:
    from parsimony.testing import iter_check_names

    assert set(iter_check_names()) == {"check_connectors_exported", "check_descriptions_non_empty"}
