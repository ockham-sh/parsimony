"""Tests for :mod:`parsimony.testing` — the plugin conformance suite.

Three checks: ``check_connectors_exported``, ``check_descriptions_non_empty``,
``check_env_vars_map_to_deps``.
"""

from types import ModuleType
from typing import Any

import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector


class _ToyParams(BaseModel):
    x: str = "y"


def _mk_connector(
    name: str,
    *,
    doc: str = "Fetch a toy observation.",
    tags: "list[str] | None" = None,
    has_dep: bool = True,
) -> Any:
    if has_dep:

        async def _fn(params: _ToyParams, *, api_key: str) -> "dict[str, Any]":
            return {"ok": True}

    else:

        async def _fn(params: _ToyParams) -> "dict[str, Any]":  # type: ignore[no-redef]
            return {"ok": True}

    _fn.__doc__ = doc
    _fn.__name__ = name
    return connector(tags=tags)(_fn)


def _make_module(
    name: str,
    *,
    connectors: Connectors | None = None,
    env_vars: dict[str, str] | None = None,
) -> ModuleType:
    mod = ModuleType(name)
    if connectors is not None:
        mod.CONNECTORS = connectors  # type: ignore[attr-defined]
    if env_vars is not None:
        mod.ENV_VARS = env_vars  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_valid_plugin_passes() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_good",
        connectors=Connectors([_mk_connector("good_fetch")]),
        env_vars={"api_key": "GOOD_API_KEY"},
    )
    assert_plugin_valid(mod)


def test_plugin_without_env_vars_passes() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_public",
        connectors=Connectors([_mk_connector("public_fetch", has_dep=False)]),
    )
    assert_plugin_valid(mod)


# ---------------------------------------------------------------------------
# Check 1: connectors_exported
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Check 2: descriptions_non_empty
# ---------------------------------------------------------------------------


def test_connector_with_empty_description_fails() -> None:
    """A Connector with only whitespace in its description should fail."""
    from parsimony.testing import ConformanceError, assert_plugin_valid

    toy = _mk_connector("fine")
    # Rewrite the frozen dataclass's description field through dict access —
    # Connector is frozen so we go through object.__setattr__.
    object.__setattr__(toy, "description", "   ")
    mod = _make_module("pkg_blank", connectors=Connectors([toy]))
    with pytest.raises(ConformanceError, match="empty description"):
        assert_plugin_valid(mod)


# ---------------------------------------------------------------------------
# Check 3: env_vars_map_to_deps
# ---------------------------------------------------------------------------


def test_env_var_not_mapping_to_dep_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    mod = _make_module(
        "pkg_bad_env",
        connectors=Connectors([_mk_connector("no_dep", has_dep=False)]),
        env_vars={"api_key": "WHATEVER"},
    )
    with pytest.raises(ConformanceError, match="api_key"):
        assert_plugin_valid(mod)


def test_env_vars_wrong_type_fails() -> None:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    mod = _make_module(
        "pkg_env_list",
        connectors=Connectors([_mk_connector("x", has_dep=False)]),
    )
    mod.ENV_VARS = ["API_KEY"]  # type: ignore[attr-defined]
    with pytest.raises(ConformanceError, match="dict"):
        assert_plugin_valid(mod)


# ---------------------------------------------------------------------------
# skip=
# ---------------------------------------------------------------------------


def test_skip_env_vars_map_to_deps_allows_bypass() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_skip_envmap",
        connectors=Connectors([_mk_connector("no_dep_fetch", has_dep=False)]),
        env_vars={"api_key": "WHATEVER"},
    )
    assert_plugin_valid(mod, skip=["check_env_vars_map_to_deps"])


def test_skip_unknown_check_raises() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_bad_skip",
        connectors=Connectors([_mk_connector("good")]),
    )
    with pytest.raises(ValueError, match="unknown"):
        assert_plugin_valid(mod, skip=["not_a_real_check"])


def test_connectors_exported_not_skippable() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module("pkg_skip_first", connectors=Connectors([_mk_connector("x")]))
    with pytest.raises(ValueError, match="not skippable"):
        assert_plugin_valid(mod, skip=["check_connectors_exported"])
