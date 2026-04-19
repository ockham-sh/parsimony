"""Conformance checks for ``parsimony`` plugins.

Every official plugin is expected to pass :func:`assert_plugin_valid`
against its own module, and CI is expected to treat failure as release-
blocking. The checks implemented here encode the contract documented in
``docs/contract.md``.

Each check is an independent callable registered in :data:`_CHECKS`. An
author can opt out of a specific check by name via ``skip=[...]`` — useful
for pragmatic edge cases, but every skip should be justified inline.

Two entry points cover every caller:

* :func:`assert_plugin_valid` — procedural. Used by the ``parsimony
  conformance verify`` CLI and by scripts that want a single raising call.
  Does not import :mod:`pytest`.
* :class:`ProviderTestSuite` — pytest-native base class. Plugin test files
  inherit it, set :attr:`~ProviderTestSuite.module` or
  :attr:`~ProviderTestSuite.module_path`, and pytest discovers the inherited
  ``test_*`` methods. :mod:`pytest` is imported lazily inside the methods
  that need it so ``from parsimony.testing import ProviderTestSuite`` is
  free from outside a test context.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from types import ModuleType
from typing import Any, ClassVar

from parsimony.connector import Connector, Connectors

__all__ = [
    "ConformanceError",
    "ProviderTestSuite",
    "assert_plugin_valid",
    "iter_check_names",
]


# Tool-tagged connectors are exposed directly to LLMs via MCP; a short,
# uninformative description hurts tool selection quality. 40 chars is a
# deliberately low bar — anything less is almost certainly a missing docstring.
_TOOL_DESCRIPTION_MIN_CHARS = 40


class ConformanceError(AssertionError):
    """Raised when a plugin module fails a conformance check.

    Inherits from ``AssertionError`` so it surfaces cleanly in pytest runs.
    Exposes the same ``to_report_dict()`` shape as :class:`PluginError`
    subclasses so the CLI can render one consistent report structure across
    import, contract, and conformance failures.
    """

    def __init__(
        self,
        check: str,
        reason: str,
        *,
        module_path: str | None = None,
        next_action: str | None = None,
    ) -> None:
        self.check = check
        self.reason = reason
        self.module_path = module_path
        self.next_action = next_action
        super().__init__(f"[{check}] {reason}")

    def to_report_dict(self) -> dict[str, Any]:
        """Structured fields for JSON-report consumers (CLI, CI)."""
        return {
            "check": self.check,
            "module_path": self.module_path,
            "reason": self.reason,
            "next_action": self.next_action,
        }


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def _check_connectors_exported(module: ModuleType) -> Connectors:
    if not hasattr(module, "CONNECTORS"):
        raise ConformanceError(
            "check_connectors_exported",
            f"module {module.__name__!r} must export CONNECTORS",
        )
    connectors = module.CONNECTORS
    if not isinstance(connectors, Connectors):
        raise ConformanceError(
            "check_connectors_exported",
            f"CONNECTORS must be a parsimony.Connectors instance; got {type(connectors).__name__}",
        )
    if len(connectors) == 0:
        raise ConformanceError(
            "check_connectors_exported",
            "CONNECTORS must contain at least one connector",
        )
    return connectors


def _check_descriptions_non_empty(module: ModuleType) -> None:
    connectors: Connectors = module.CONNECTORS
    for c in connectors:
        if not c.description or not c.description.strip():
            raise ConformanceError(
                "check_descriptions_non_empty",
                f"connector {c.name!r} has an empty description",
            )


def _check_tool_tag_description_length(module: ModuleType) -> None:
    connectors: Connectors = module.CONNECTORS
    for c in connectors:
        if "tool" not in c.tags:
            continue
        first_line = c.description.splitlines()[0] if c.description else ""
        if len(first_line) < _TOOL_DESCRIPTION_MIN_CHARS:
            raise ConformanceError(
                "check_tool_tag_description_length",
                (
                    f"tool-tagged connector {c.name!r} first description line is "
                    f"{len(first_line)} chars; must be >= {_TOOL_DESCRIPTION_MIN_CHARS} characters "
                    "for MCP tool descriptions"
                ),
            )


def _check_env_vars_shape(module: ModuleType) -> dict[str, str]:
    env_vars = getattr(module, "ENV_VARS", {})
    if not isinstance(env_vars, dict):
        raise ConformanceError(
            "check_env_vars_shape",
            f"ENV_VARS must be a dict[str, str]; got {type(env_vars).__name__}",
        )
    for k, v in env_vars.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ConformanceError(
                "check_env_vars_shape",
                f"ENV_VARS entries must be str -> str; got {k!r} -> {v!r}",
            )
    return env_vars


def _check_env_vars_map_to_deps(module: ModuleType) -> None:
    connectors: Connectors = module.CONNECTORS
    env_vars = getattr(module, "ENV_VARS", {}) or {}
    if not env_vars:
        return
    all_deps: set[str] = set()
    for c in connectors:
        all_deps |= set(c.dep_names) | set(c.optional_dep_names)
    for key in env_vars:
        if key not in all_deps:
            raise ConformanceError(
                "check_env_vars_map_to_deps",
                (
                    f"ENV_VARS key {key!r} does not match any connector dependency "
                    f"(known deps across all connectors: {sorted(all_deps)})"
                ),
            )


def _check_name_env_var_collisions(module: ModuleType) -> None:
    connectors: Connectors = module.CONNECTORS
    env_vars = getattr(module, "ENV_VARS", {}) or {}
    names = {c.name for c in connectors}
    collisions = names & set(env_vars.keys())
    if collisions:
        raise ConformanceError(
            "check_name_env_var_collisions",
            (
                f"connector name(s) collision with ENV_VARS keys: {sorted(collisions)}. "
                "This usually indicates accidental shadowing — rename the connector or env var."
            ),
        )


def _check_provider_metadata_shape(module: ModuleType) -> None:
    raw_meta = getattr(module, "PROVIDER_METADATA", {})
    if not isinstance(raw_meta, dict):
        raise ConformanceError(
            "check_provider_metadata_shape",
            f"PROVIDER_METADATA must be a dict; got {type(raw_meta).__name__}",
        )


# Registry of named checks. Tests may opt out of any check via skip=[name].
# Ordering matters — earlier checks set up invariants relied on by later ones.
_CHECKS: dict[str, Callable[[ModuleType], object]] = {
    "check_connectors_exported": _check_connectors_exported,
    "check_descriptions_non_empty": _check_descriptions_non_empty,
    "check_tool_tag_description_length": _check_tool_tag_description_length,
    "check_env_vars_shape": _check_env_vars_shape,
    "check_env_vars_map_to_deps": _check_env_vars_map_to_deps,
    "check_name_env_var_collisions": _check_name_env_var_collisions,
    "check_provider_metadata_shape": _check_provider_metadata_shape,
}


def _validate_skip_list(skip: Iterable[str]) -> set[str]:
    skip_set = set(skip)
    unknown = skip_set - set(_CHECKS)
    if unknown:
        raise ValueError(
            f"unknown conformance check(s) in skip=: {sorted(unknown)}. "
            f"Known checks: {sorted(_CHECKS)}"
        )
    return skip_set


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def assert_plugin_valid(
    module: ModuleType,
    *,
    skip: Iterable[str] = (),
) -> None:
    """Assert that *module* conforms to the ``parsimony`` plugin contract.

    Raises :class:`ConformanceError` on the first failure. Downstream CI
    should treat this as release-blocking.

    Parameters
    ----------
    module:
        The plugin's entry-point target module (the one exporting
        ``CONNECTORS``).
    skip:
        Optional iterable of check names to skip. Useful for pragmatic
        edge cases, but every skip should be justified inline. See
        ``_CHECKS`` for the list of available check names.
    """
    skip_set = _validate_skip_list(skip)
    # check_connectors_exported must run unconditionally — every other check
    # assumes the module has a valid CONNECTORS attribute.
    if "check_connectors_exported" in skip_set:
        raise ValueError("check_connectors_exported is not skippable — it sets up every other check")
    for name, fn in _CHECKS.items():
        if name in skip_set:
            continue
        fn(module)


# ---------------------------------------------------------------------------
# Introspection helpers (used by CLI list-plugins --strict and by tests)
# ---------------------------------------------------------------------------


def iter_check_names() -> Iterable[str]:
    """Yield the registered check names. Useful for tools that want to present
    a selectable list."""
    return iter(_CHECKS)


def connector_count(module: ModuleType) -> int:
    """Return the number of connectors exported by *module*, or 0 if none."""
    connectors = getattr(module, "CONNECTORS", None)
    if not isinstance(connectors, Connectors):
        return 0
    return len(connectors)


def iter_connectors(module: ModuleType) -> Iterable[Connector]:
    """Yield the connectors exported by *module*, or an empty iterator."""
    connectors = getattr(module, "CONNECTORS", None)
    if not isinstance(connectors, Connectors):
        return iter(())
    return iter(connectors)


# ---------------------------------------------------------------------------
# ProviderTestSuite — pytest-native entry point
# ---------------------------------------------------------------------------


class ProviderTestSuite:
    """Pytest base class for plugin conformance.

    Subclass in a plugin's test file and set one of:

    * :attr:`module` — the already-imported plugin module.
    * :attr:`module_path` — the dotted import path of the plugin's
      CONNECTORS-exporting module (resolved at test-collection time).

    Pytest discovers the inherited ``test_*`` methods. Each method runs one
    named check from :data:`_CHECKS`, so a failure pinpoints the specific
    contract clause violated.

    Optionally set :attr:`entry_point_name` to additionally verify the
    plugin is installed under the ``parsimony.providers`` entry-point
    group and resolves to the same module.

    Example::

        from parsimony.testing import ProviderTestSuite
        import parsimony_fred

        class TestFredConformance(ProviderTestSuite):
            module = parsimony_fred
            entry_point_name = "fred"

    :mod:`pytest` is imported lazily inside methods that need it, so
    ``from parsimony.testing import ProviderTestSuite`` never requires
    pytest in production code.
    """

    #: The plugin module exporting ``CONNECTORS``. Set this OR :attr:`module_path`.
    module: ClassVar[ModuleType | None] = None

    #: Dotted import path to the plugin module. Set this OR :attr:`module`.
    module_path: ClassVar[str | None] = None

    #: Optional entry-point name. When set, :meth:`test_entry_point_resolves`
    #: additionally verifies installation.
    entry_point_name: ClassVar[str | None] = None

    @classmethod
    def _resolve_module(cls) -> ModuleType:
        if cls.module is not None:
            return cls.module
        if cls.module_path is not None:
            import importlib

            return importlib.import_module(cls.module_path)
        raise TypeError(
            f"{cls.__name__} must set either `module = <module>` "
            "or `module_path = 'package.submodule'`"
        )

    def test_connectors_exported(self) -> None:
        _check_connectors_exported(self._resolve_module())

    def test_descriptions_non_empty(self) -> None:
        _check_descriptions_non_empty(self._resolve_module())

    def test_tool_tag_description_length(self) -> None:
        _check_tool_tag_description_length(self._resolve_module())

    def test_env_vars_shape(self) -> None:
        _check_env_vars_shape(self._resolve_module())

    def test_env_vars_map_to_deps(self) -> None:
        _check_env_vars_map_to_deps(self._resolve_module())

    def test_name_env_var_collisions(self) -> None:
        _check_name_env_var_collisions(self._resolve_module())

    def test_provider_metadata_shape(self) -> None:
        _check_provider_metadata_shape(self._resolve_module())

    def test_entry_point_resolves(self) -> None:
        """Verify the plugin is installed under ``parsimony.providers`` and
        its entry point resolves to the module under test.

        Skipped when :attr:`entry_point_name` is not set.
        """
        if self.entry_point_name is None:
            import pytest

            pytest.skip("entry_point_name not set; skipping installation check")
        from parsimony.discovery import discovered_providers

        expected = self._resolve_module()
        providers = {p.name: p for p in discovered_providers()}
        if self.entry_point_name not in providers:
            raise ConformanceError(
                "check_entry_point_registered",
                (
                    f"entry point {self.entry_point_name!r} not installed under "
                    "the 'parsimony.providers' group; check your plugin's pyproject.toml"
                ),
            )
        provider = providers[self.entry_point_name]
        if provider.module is not expected:
            raise ConformanceError(
                "check_entry_point_matches",
                (
                    f"entry point {self.entry_point_name!r} resolves to "
                    f"{provider.module_path!r}, but the suite is configured against "
                    f"{expected.__name__!r}"
                ),
            )
