"""Tests for :mod:`parsimony.testing.conformance` — the plugin conformance suite.

The conformance suite is itself under test so that plugin authors can trust
:func:`parsimony.testing.assert_plugin_valid` to actually catch contract
violations.
"""

from types import ModuleType
from typing import Any

import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector


class _ToyParams(BaseModel):
    x: str = "y"


# ---------------------------------------------------------------------------
# Fixtures — build throw-away plugin modules
# ---------------------------------------------------------------------------


def _mk_connector(
    name: str,
    *,
    doc: str = "Fetch a toy observation with a long enough description to please the tool-tag check.",
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
    provider_metadata: dict[str, Any] | None = None,
) -> ModuleType:
    mod = ModuleType(name)
    if connectors is not None:
        mod.CONNECTORS = connectors  # type: ignore[attr-defined]
    if env_vars is not None:
        mod.ENV_VARS = env_vars  # type: ignore[attr-defined]
    if provider_metadata is not None:
        mod.PROVIDER_METADATA = provider_metadata  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_valid_plugin_passes() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_good",
        connectors=Connectors([_mk_connector("good_fetch", tags=["tool"])]),
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
# Contract violations
# ---------------------------------------------------------------------------


def test_missing_connectors_attribute_fails() -> None:
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module("pkg_nothing")
    with pytest.raises(ConformanceError, match="CONNECTORS"):
        assert_plugin_valid(mod)


def test_non_connectors_type_fails() -> None:
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module("pkg_list")
    mod.CONNECTORS = [_mk_connector("foo")]  # type: ignore[attr-defined]
    with pytest.raises(ConformanceError, match="Connectors"):
        assert_plugin_valid(mod)


def test_empty_connectors_fails() -> None:
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module("pkg_empty", connectors=Connectors([]))
    with pytest.raises(ConformanceError, match="at least one connector"):
        assert_plugin_valid(mod)


def test_tool_tagged_short_description_fails() -> None:
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module(
        "pkg_terse",
        connectors=Connectors([_mk_connector("terse", doc="Too short.", tags=["tool"])]),
    )
    with pytest.raises(ConformanceError, match="40 characters"):
        assert_plugin_valid(mod)


def test_non_tool_short_description_passes() -> None:
    """Only tool-tagged connectors enforce the ≥40 char rule."""
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_fetch_terse",
        connectors=Connectors([_mk_connector("terse", doc="Short.")]),
    )
    assert_plugin_valid(mod)


def test_env_var_not_mapping_to_dep_fails() -> None:
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module(
        "pkg_bad_env",
        connectors=Connectors([_mk_connector("no_dep", has_dep=False)]),
        env_vars={"api_key": "WHATEVER"},
    )
    with pytest.raises(ConformanceError, match="api_key"):
        assert_plugin_valid(mod)


def test_env_vars_wrong_type_fails() -> None:
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module(
        "pkg_env_list",
        connectors=Connectors([_mk_connector("x", has_dep=False)]),
    )
    mod.ENV_VARS = ["API_KEY"]  # type: ignore[attr-defined]
    with pytest.raises(ConformanceError, match="dict"):
        assert_plugin_valid(mod)


def test_name_env_var_collision_fails() -> None:
    """Connector name shadowing an ENV_VARS key is usually a bug."""
    from parsimony.testing import assert_plugin_valid
    from parsimony.testing.conformance import ConformanceError

    mod = _make_module(
        "pkg_collision",
        connectors=Connectors([_mk_connector("api_key")]),
        env_vars={"api_key": "API_KEY"},
    )
    with pytest.raises(ConformanceError, match="collision"):
        assert_plugin_valid(mod)


# ---------------------------------------------------------------------------
# skip= escape hatch
# ---------------------------------------------------------------------------


def test_skip_allows_bypassing_specific_checks() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_skip",
        connectors=Connectors([_mk_connector("skip_me", doc="Short.", tags=["tool"])]),
    )
    # Without skip this fails on tool-tag description length
    assert_plugin_valid(mod, skip=["check_tool_tag_description_length"])


def test_skip_unknown_check_raises() -> None:
    from parsimony.testing import assert_plugin_valid

    mod = _make_module(
        "pkg_bad_skip",
        connectors=Connectors([_mk_connector("good", tags=["tool"])]),
    )
    with pytest.raises(ValueError, match="unknown"):
        assert_plugin_valid(mod, skip=["not_a_real_check"])


# ---------------------------------------------------------------------------
# Bundled connectors sanity check
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "module_path",
    [
        "parsimony.connectors.treasury",
        "parsimony.connectors.polymarket",
    ],
)
def test_bundled_connectors_conform(module_path: str) -> None:
    """Every currently-bundled connector module should pass assert_plugin_valid.

    This is the dogfood gate: if bundled modules don't conform, the contract
    is wrong — either the spec needs to loosen or the modules need fixing.
    """
    import importlib

    from parsimony.testing import assert_plugin_valid

    module = importlib.import_module(module_path)
    assert_plugin_valid(module)
