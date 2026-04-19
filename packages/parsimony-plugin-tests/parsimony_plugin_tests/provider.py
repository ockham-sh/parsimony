"""Conformance test suite for :class:`~parsimony.connectors.ProviderSpec` plugins.

Inherit :class:`ProviderTestSuite`, set :attr:`provider` to the plugin's
``PROVIDER`` instance and (optionally) :attr:`entry_point_name` to the name
declared in the plugin's ``[project.entry-points."parsimony.providers"]``
section. Pytest discovers the inherited methods as ``test_*`` cases.

Every check is structural — no network calls, no credentials, no I/O. This
is the minimum every plugin must satisfy; behavioural tests (cassettes,
result-shape verification) live in the plugin itself or in a future
behavioural suite.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import ClassVar

import pytest
from parsimony.connector import Connector
from parsimony.connectors import ProviderSpec
from parsimony.plugins import PROVIDERS_GROUP, discover_providers
from pydantic import BaseModel


class ProviderTestSuite:
    """Inherit this class in your plugin's tests.

    Required class attributes:

    * ``provider`` — the :class:`ProviderSpec` exported by the plugin
      (typically ``PROVIDER``).

    Optional class attributes:

    * ``entry_point_name`` — the name declared under
      ``[project.entry-points."parsimony.providers"]``. When set, the suite
      additionally verifies the entry point is installed and resolves to the
      same :class:`ProviderSpec`.
    """

    provider: ClassVar[ProviderSpec]
    entry_point_name: ClassVar[str | None] = None

    # ------------------------------------------------------------------ #
    # Spec shape
    # ------------------------------------------------------------------ #

    def test_provider_is_provider_spec(self) -> None:
        assert isinstance(self.provider, ProviderSpec), (
            f"provider must be a ProviderSpec, got {type(self.provider).__name__}"
        )

    def test_provider_name_is_nonempty_slug(self) -> None:
        name = self.provider.name
        assert isinstance(name, str) and name, "provider.name must be a non-empty string"
        assert name == name.lower(), f"provider.name must be lowercase: {name!r}"
        assert " " not in name, f"provider.name must not contain whitespace: {name!r}"

    def test_provider_resolves_to_connectors(self) -> None:
        connectors, _env_vars = self.provider.resolve()
        assert connectors is not None
        assert len(list(connectors)) > 0, "provider must expose at least one connector"

    # ------------------------------------------------------------------ #
    # Connector hygiene
    # ------------------------------------------------------------------ #

    def test_connector_names_are_unique(self) -> None:
        connectors, _ = self.provider.resolve()
        names = [c.name for c in connectors]
        duplicates = {n for n in names if names.count(n) > 1}
        assert not duplicates, f"duplicate connector names: {sorted(duplicates)}"

    def test_connector_names_share_provider_prefix(self) -> None:
        connectors, _ = self.provider.resolve()
        name = self.provider.name
        prefix = f"{name}_"
        offenders = [
            c.name for c in connectors if c.name != name and not c.name.startswith(prefix)
        ]
        assert not offenders, (
            f"connector names must equal {name!r} or start with {prefix!r} "
            f"for discoverability; offenders: {offenders}"
        )

    def test_connectors_are_connector_instances(self) -> None:
        connectors, _ = self.provider.resolve()
        for c in connectors:
            assert isinstance(c, Connector), f"{c!r} is not a Connector"

    def test_connector_param_types_are_pydantic_models(self) -> None:
        connectors, _ = self.provider.resolve()
        for c in connectors:
            assert issubclass(c.param_type, BaseModel), (
                f"{c.name}: param_type {c.param_type!r} is not a pydantic BaseModel"
            )

    def test_connector_fns_are_async(self) -> None:
        connectors, _ = self.provider.resolve()
        for c in connectors:
            unwrapped = inspect.unwrap(c.fn)
            assert asyncio.iscoroutinefunction(unwrapped), f"{c.name}: underlying function must be `async def`"

    def test_connector_descriptions_are_nonempty(self) -> None:
        connectors, _ = self.provider.resolve()
        for c in connectors:
            assert c.description and c.description.strip(), (
                f"{c.name}: missing description (set via decorator or function docstring)"
            )

    # ------------------------------------------------------------------ #
    # Env-var declarations
    # ------------------------------------------------------------------ #

    def test_env_var_keys_are_uppercase(self) -> None:
        _, env_vars = self.provider.resolve()
        offenders = {k: v for k, v in env_vars.items() if v != v.upper()}
        assert not offenders, f"env-var values must be UPPER_SNAKE_CASE: {offenders}"

    def test_env_var_dep_names_are_known(self) -> None:
        connectors, env_vars = self.provider.resolve()
        if not env_vars:
            return
        all_dep_names: set[str] = set()
        for c in connectors:
            all_dep_names |= set(c.dep_names) | set(c.optional_dep_names)
        unknown = set(env_vars.keys()) - all_dep_names
        assert not unknown, (
            f"env-var keys must match a connector dep_name; "
            f"unknown deps: {sorted(unknown)}; declared on connectors: {sorted(all_dep_names)}"
        )

    # ------------------------------------------------------------------ #
    # Entry-point registration (only when entry_point_name is set)
    # ------------------------------------------------------------------ #

    def test_entry_point_resolves_to_same_provider(self) -> None:
        if self.entry_point_name is None:
            pytest.skip("entry_point_name not set; skipping entry-point installation check")
        discovered = {spec.name: spec for spec in discover_providers()}
        assert self.entry_point_name in discovered, (
            f"plugin not installed via entry point group {PROVIDERS_GROUP!r}; "
            f"check [project.entry-points.\"{PROVIDERS_GROUP}\"] in pyproject.toml"
        )
        assert discovered[self.entry_point_name] is self.provider, (
            "entry point resolved to a different ProviderSpec instance than the one passed to the suite"
        )
