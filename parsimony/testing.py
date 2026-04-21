"""Conformance checks for ``parsimony`` plugins.

Three checks — the minimal integrity set every official plugin must pass:

1. :func:`_check_connectors_exported` — module exports ``CONNECTORS``
   (a :class:`Connectors` with at least one entry).
2. :func:`_check_descriptions_non_empty` — every connector has a description
   (no silently empty tool schemas).
3. :func:`_check_env_vars_map_to_deps` — every ``ENV_VARS`` key names a real
   connector dependency (catches typos / renames).

Two entry points:

* :func:`assert_plugin_valid` — procedural, raises :class:`ConformanceError`.
* :class:`ProviderTestSuite` — pytest-native base class with 4 ``test_*``
  methods; :mod:`pytest` is imported lazily inside them.
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


class ConformanceError(AssertionError):
    """Raised when a plugin module fails a conformance check."""

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
        return {
            "check": self.check,
            "module_path": self.module_path,
            "reason": self.reason,
            "next_action": self.next_action,
        }


# ---------------------------------------------------------------------------
# Checks
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


def _check_env_vars_map_to_deps(module: ModuleType) -> None:
    connectors: Connectors = module.CONNECTORS
    env_vars = getattr(module, "ENV_VARS", {}) or {}
    if not env_vars:
        return
    if not isinstance(env_vars, dict):
        raise ConformanceError(
            "check_env_vars_map_to_deps",
            f"ENV_VARS must be a dict[str, str]; got {type(env_vars).__name__}",
        )
    all_deps: set[str] = set()
    for c in connectors:
        all_deps |= set(c.dep_names) | set(c.optional_dep_names)
    for key, value in env_vars.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ConformanceError(
                "check_env_vars_map_to_deps",
                f"ENV_VARS entries must be str -> str; got {key!r} -> {value!r}",
            )
        if key not in all_deps:
            raise ConformanceError(
                "check_env_vars_map_to_deps",
                (
                    f"ENV_VARS key {key!r} does not match any connector dependency "
                    f"(known deps across all connectors: {sorted(all_deps)})"
                ),
            )


_CHECKS: dict[str, Callable[[ModuleType], object]] = {
    "check_connectors_exported": _check_connectors_exported,
    "check_descriptions_non_empty": _check_descriptions_non_empty,
    "check_env_vars_map_to_deps": _check_env_vars_map_to_deps,
}


def _validate_skip_list(skip: Iterable[str]) -> set[str]:
    skip_set = set(skip)
    unknown = skip_set - set(_CHECKS)
    if unknown:
        raise ValueError(f"unknown conformance check(s) in skip=: {sorted(unknown)}. Known checks: {sorted(_CHECKS)}")
    return skip_set


def assert_plugin_valid(
    module: ModuleType,
    *,
    skip: Iterable[str] = (),
) -> None:
    """Assert that *module* conforms to the ``parsimony`` plugin contract.

    Raises :class:`ConformanceError` on the first failure.
    """
    skip_set = _validate_skip_list(skip)
    if "check_connectors_exported" in skip_set:
        raise ValueError("check_connectors_exported is not skippable — it sets up every other check")
    for name, fn in _CHECKS.items():
        if name in skip_set:
            continue
        fn(module)


def iter_check_names() -> Iterable[str]:
    """Yield the registered check names."""
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
    * :attr:`module_path` — the dotted import path of the CONNECTORS-exporting module.

    Pytest discovers the four inherited ``test_*`` methods — one per
    conformance check plus :meth:`test_entry_point_resolves` when
    :attr:`entry_point_name` is set.
    """

    module: ClassVar[ModuleType | None] = None
    module_path: ClassVar[str | None] = None
    entry_point_name: ClassVar[str | None] = None

    @classmethod
    def _resolve_module(cls) -> ModuleType:
        if cls.module is not None:
            return cls.module
        if cls.module_path is not None:
            import importlib

            return importlib.import_module(cls.module_path)
        raise TypeError(f"{cls.__name__} must set either `module = <module>` or `module_path = 'package.submodule'`")

    def test_connectors_exported(self) -> None:
        _check_connectors_exported(self._resolve_module())

    def test_descriptions_non_empty(self) -> None:
        _check_descriptions_non_empty(self._resolve_module())

    def test_env_vars_map_to_deps(self) -> None:
        _check_env_vars_map_to_deps(self._resolve_module())

    def test_entry_point_resolves(self) -> None:
        """Verify the plugin is installed under ``parsimony.providers``.

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
