"""Conformance checks for ``parsimony`` plugins.

Six checks — the minimal integrity set every official plugin must pass:

1. :func:`_check_connectors_exported` — module exports ``CONNECTORS``
   (a :class:`Connectors` with at least one entry).
2. :func:`_check_descriptions_non_empty` — every connector has a non-empty
   description within length bounds (20–800 chars).
3. :func:`_check_enumerator_decorator` — enumerators use :func:`enumerator`,
   not :func:`connector` with an ``enumerator`` tag alone.
4. :func:`_check_enumerator_return_type` — enumerators declare ``output=``
   and annotate ``pd.DataFrame`` return types.
5. :func:`_check_flat_public_params` — public connector parameters are flat
   (no bundled ``params: BaseModel`` surface).
6. :func:`_check_secrets_declared` — credential-like parameters are listed
   in ``secrets=``.

Two entry points:

* :func:`assert_plugin_valid` — procedural, raises :class:`ConformanceError`.
* :class:`ProviderTestSuite` — pytest-native base class; :mod:`pytest` is
  imported lazily inside optional entry-point tests.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterable
from types import ModuleType
from typing import Any, ClassVar, get_type_hints

from parsimony.connector import Connector, Connectors

__all__ = [
    "ConformanceError",
    "ProviderTestSuite",
    "assert_plugin_valid",
    "iter_check_names",
]

_SECRET_PARAM_RE = re.compile(r"^(api_key|token|secret|.*_key)$", re.IGNORECASE)


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
        desc = (c.description or "").strip()
        if not desc:
            raise ConformanceError(
                "check_descriptions_non_empty",
                f"connector {c.name!r} has an empty description",
            )
        if len(desc) < 20:
            raise ConformanceError(
                "check_descriptions_non_empty",
                f"connector {c.name!r} description is too short ({len(desc)} chars; minimum 20)",
            )
        if len(desc) > 800:
            raise ConformanceError(
                "check_descriptions_non_empty",
                f"connector {c.name!r} description is too long ({len(desc)} chars; maximum 800)",
            )


def _check_flat_public_params(module: ModuleType) -> None:
    """Public connector parameters must be flat — no ``params: BaseModel`` surface."""
    from pydantic import BaseModel

    from parsimony.connector import _strip_annotated

    for c in module.CONNECTORS:
        if "params" not in c.exposed_signature.parameters:
            continue
        try:
            hints = get_type_hints(c.fn, include_extras=True)
        except Exception:  # noqa: BLE001
            hints = {}
        ann = hints.get("params", c.exposed_signature.parameters["params"].annotation)
        if ann is inspect.Parameter.empty:
            continue
        ann = _strip_annotated(ann)
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            raise ConformanceError(
                "check_flat_public_params",
                (
                    f"connector {c.name!r} exposes bundled params: {ann.__name__}. "
                    "Public connector parameters must be flat function parameters; "
                    "use Pydantic models only for internal validation."
                ),
                next_action="Flatten the connector signature to top-level parameters.",
            )


def _check_enumerator_decorator(module: ModuleType) -> None:
    for c in module.CONNECTORS:
        if "enumerator" not in c.tags:
            continue
        if c.role != "enumerator":
            raise ConformanceError(
                "check_enumerator_decorator",
                (
                    f"connector {c.name!r}: use @enumerator(output=...) instead of "
                    "@connector(..., tags=[..., 'enumerator'])"
                ),
                next_action="Replace @connector(..., tags=['enumerator']) with @enumerator(output=...).",
            )


def _check_enumerator_return_type(module: ModuleType) -> None:
    for c in module.CONNECTORS:
        if "enumerator" not in c.tags:
            continue
        if c.output_config is None:
            raise ConformanceError(
                "check_enumerator_return_type",
                f"connector {c.name!r}: enumerator must declare output=",
            )
        ann = c.fn.__annotations__.get("return")
        if ann is None:
            raise ConformanceError(
                "check_enumerator_return_type",
                f"connector {c.name!r}: enumerator must annotate return type pd.DataFrame",
            )
        return_str = str(ann)
        if "DataFrame" not in return_str and "Series" not in return_str:
            raise ConformanceError(
                "check_enumerator_return_type",
                f"connector {c.name!r}: enumerator return must be pd.DataFrame",
            )
        if "Entity" in return_str or "list[" in return_str:
            raise ConformanceError(
                "check_enumerator_return_type",
                f"connector {c.name!r}: enumerator must not return list[Entity]",
            )


def _check_secrets_declared(module: ModuleType) -> None:
    """Credential-like exposed parameters must be listed in secrets=."""
    for c in module.CONNECTORS:
        secret_set = set(c.secrets)
        for name in c.exposed_signature.parameters:
            if _SECRET_PARAM_RE.fullmatch(name) and name not in secret_set:
                raise ConformanceError(
                    "check_secrets_declared",
                    (
                        f"connector {c.name!r}: parameter {name!r} looks like a credential "
                        f"but is not listed in secrets={c.secrets!r}"
                    ),
                    next_action=f"Add secrets=({name!r}, ...) to the @connector decorator.",
                )


_CHECKS: dict[str, Callable[[ModuleType], object]] = {
    "check_connectors_exported": _check_connectors_exported,
    "check_descriptions_non_empty": _check_descriptions_non_empty,
    "check_enumerator_decorator": _check_enumerator_decorator,
    "check_enumerator_return_type": _check_enumerator_return_type,
    "check_flat_public_params": _check_flat_public_params,
    "check_secrets_declared": _check_secrets_declared,
}


def assert_plugin_valid(module: ModuleType) -> None:
    """Assert that *module* conforms to the ``parsimony`` plugin contract.

    Raises :class:`ConformanceError` on the first failure.
    """
    for fn in _CHECKS.values():
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

    Pytest discovers :meth:`test_plugin_conforms` (all registered checks via
    :func:`assert_plugin_valid`) plus optional :meth:`test_entry_point_resolves`
    when :attr:`entry_point_name` is set.
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

    def test_plugin_conforms(self) -> None:
        """Run every registered conformance check against the plugin module."""
        assert_plugin_valid(self._resolve_module())

    def test_entry_point_resolves(self) -> None:
        """Verify the plugin is installed under ``parsimony.providers``.

        Skipped when :attr:`entry_point_name` is not set.
        """
        if self.entry_point_name is None:
            import pytest

            pytest.skip("entry_point_name not set; skipping installation check")
        from parsimony.discover import iter_providers

        expected = self._resolve_module()
        providers = {p.name: p for p in iter_providers()}
        if self.entry_point_name not in providers:
            raise ConformanceError(
                "check_entry_point_registered",
                (
                    f"entry point {self.entry_point_name!r} not installed under "
                    "the 'parsimony.providers' group; check your plugin's pyproject.toml"
                ),
            )
        provider = providers[self.entry_point_name]
        import importlib

        resolved = importlib.import_module(provider.module_path)
        if resolved is not expected:
            raise ConformanceError(
                "check_entry_point_matches",
                (
                    f"entry point {self.entry_point_name!r} resolves to "
                    f"{provider.module_path!r}, but the suite is configured against "
                    f"{expected.__name__!r}"
                ),
            )
