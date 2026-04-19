"""Compose discovered providers into a :class:`~parsimony.connector.Connectors` collection.

:func:`build_connectors_from_env` walks :func:`parsimony.discovery.discovered_providers`,
binds declared env vars via each connector's ``dep_names``, and merges the
resulting collections into a single flat surface. Providers whose required
deps are absent are silently skipped (the design default for installed
plugins).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from parsimony.connector import Connectors
from parsimony.discovery._scan import discovered_providers

__all__ = ["build_connectors_from_env"]


# ---------------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------------


def _resolve_env_deps(
    connectors: Connectors,
    env_vars: dict[str, str],
    env: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Resolve env vars to bind_deps kwargs using the Connector's own dep info.

    Returns a dict of resolved deps, or ``None`` if the provider should be
    skipped (a required dep's env var is absent).
    """
    if not env_vars:
        return {}

    sample = next(iter(connectors))
    required_deps = sample.dep_names
    deps: dict[str, Any] = {}

    for dep_name, env_var in env_vars.items():
        value = env.get(env_var, "")
        if not value:
            if dep_name in required_deps:
                return None  # can't construct — caller decides raise vs skip
            continue  # optional dep: skip binding, function default applies
        deps[dep_name] = value

    return deps


def _bind_optional_deps(
    result: Connectors,
    connectors: Connectors,
    env_vars: dict[str, str],
    env: Mapping[str, Any],
) -> Connectors:
    """Bind env-var deps and add a provider. Skips silently if a required dep is missing."""
    deps = _resolve_env_deps(connectors, env_vars, env)
    if deps is None:
        return result  # skip — required dep absent
    if deps:
        connectors = connectors.bind_deps(**deps)
    return result + connectors


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
) -> Connectors:
    """Build the full connector surface from environment variables.

    Composes providers discovered via :mod:`parsimony.discovery` — the
    :func:`parsimony.discovery.discovered_providers` walk iterates every
    ``parsimony.providers`` entry point and validates the contract. Providers
    whose declared env vars are absent are silently skipped.

    Pass *env* to override ``os.environ`` (useful for testing).
    """
    _env = env if env is not None else dict(os.environ)
    result = Connectors([])

    for provider in discovered_providers():
        result = _bind_optional_deps(result, provider.connectors, provider.env_vars, _env)

    return result
