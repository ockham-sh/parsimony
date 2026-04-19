"""Provider registry: bundled connector modules + entry-point-discovered plugins.

A :class:`ProviderSpec` carries the connector collection and the env-var deps
needed to construct it. Built-in providers ship inside the parsimony
distribution; plugin providers register themselves via the
``parsimony.providers`` entry-point group.

Consumers that want a subset (e.g. only ``"tool"``-tagged connectors for MCP,
or excluding enumerators) filter the returned :class:`~parsimony.connector.Connectors`
collection themselves::

    all_connectors = build_connectors_from_env()
    tool_connectors = all_connectors.filter(tags=["tool"])
"""

from __future__ import annotations

__all__ = [
    "PROVIDERS",
    "ProviderSpec",
    "build_connectors_from_env",
    "iter_providers",
]

import importlib
import logging
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from parsimony.connector import Connectors
from parsimony.plugins import discover_providers

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ProviderSpec
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProviderSpec:
    """Declarative description of a data-source provider.

    Plugin providers (loaded via the ``parsimony.providers`` entry-point
    group) populate :attr:`connectors` directly. The :attr:`module` field is
    a *transitional* mechanism for connectors still bundled in core: it lazy-
    loads ``CONNECTORS`` from the named module so we can extract them one
    package at a time. When the last bundled connector is extracted, both
    :attr:`module` and the bundled :data:`PROVIDERS` tuple disappear.
    """

    name: str
    """Short slug identifying the provider (e.g. ``"sdmx"``, ``"fred"``)."""

    connectors: Connectors | None = None
    """Connectors collection. ``None`` for bundled providers, where the
    collection is loaded lazily from :attr:`module` at first access."""

    module: str | None = None
    """Importable module path for bundled providers (transitional; see class
    docstring). Ignored when :attr:`connectors` is supplied directly."""

    env_vars: Mapping[str, str] = field(default_factory=dict)
    """Mapping of dep name → env var (empty for public/no-auth providers)."""

    required: bool = False
    """If ``True``, :func:`build_connectors_from_env` raises when env vars are
    missing (unless ``lenient=True``). If ``False``, the provider is silently
    skipped."""

    def resolve(self) -> tuple[Connectors, Mapping[str, str]]:
        """Return ``(connectors, env_vars)`` for this provider."""
        if self.connectors is not None:
            return self.connectors, self.env_vars
        if not self.module:
            raise ValueError(f"ProviderSpec {self.name!r} has neither connectors nor module")
        module = importlib.import_module(self.module)
        connectors: Connectors = module.CONNECTORS
        env_vars: Mapping[str, str] = getattr(module, "ENV_VARS", self.env_vars)
        return connectors, env_vars


# ---------------------------------------------------------------------------
# Bundled providers (transitional: extracted plugins should be removed here)
# ---------------------------------------------------------------------------

PROVIDERS: tuple[ProviderSpec, ...] = (
    # --- Required providers (raise if env vars absent, unless lenient=True) ---
    ProviderSpec(name="fred", module="parsimony.connectors.fred", required=True),
    # --- Optional providers with credentials ---
    # NOTE: ``sdmx`` is now distributed as the ``parsimony-sdmx`` plugin and
    # registered via the ``parsimony.providers`` entry-point group.
    ProviderSpec(name="fmp", module="parsimony.connectors.fmp"),
    ProviderSpec(name="fmp_screener", module="parsimony.connectors.fmp_screener"),
    ProviderSpec(name="eodhd", module="parsimony.connectors.eodhd"),
    ProviderSpec(name="coingecko", module="parsimony.connectors.coingecko"),
    ProviderSpec(name="finnhub", module="parsimony.connectors.finnhub"),
    ProviderSpec(name="tiingo", module="parsimony.connectors.tiingo"),
    # NOTE: ``financial_reports`` is now distributed as ``parsimony-financial-reports``.
    ProviderSpec(name="eia", module="parsimony.connectors.eia"),
    ProviderSpec(name="bdf", module="parsimony.connectors.bdf"),
    ProviderSpec(name="alpha_vantage", module="parsimony.connectors.alpha_vantage"),
    ProviderSpec(name="riksbank", module="parsimony.connectors.riksbank"),
    ProviderSpec(name="destatis", module="parsimony.connectors.destatis"),
    ProviderSpec(name="bls", module="parsimony.connectors.bls"),
    # --- Public providers (no credentials) ---
    ProviderSpec(name="polymarket", module="parsimony.connectors.polymarket"),
    # NOTE: ``sec_edgar`` is now distributed as ``parsimony-edgar``.
    ProviderSpec(name="treasury", module="parsimony.connectors.treasury"),
    ProviderSpec(name="snb", module="parsimony.connectors.snb"),
    ProviderSpec(name="rba", module="parsimony.connectors.rba"),
    ProviderSpec(name="bde", module="parsimony.connectors.bde"),
    ProviderSpec(name="boc", module="parsimony.connectors.boc"),
    ProviderSpec(name="boj", module="parsimony.connectors.boj"),
    ProviderSpec(name="bdp", module="parsimony.connectors.bdp"),
)


def iter_providers() -> Iterator[ProviderSpec]:
    """Yield bundled providers and every entry-point-discovered plugin provider."""
    yield from PROVIDERS
    yield from discover_providers()


# ---------------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------------


def _resolve_env_deps(
    connectors: Connectors,
    env_vars: Mapping[str, str],
    env: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not env_vars:
        return {}

    sample = next(iter(connectors))
    required_deps = sample.dep_names
    deps: dict[str, Any] = {}

    for dep_name, env_var in env_vars.items():
        value = env.get(env_var, "")
        if not value:
            if dep_name in required_deps:
                return None
            continue
        deps[dep_name] = value

    return deps


def _bind_required_deps(
    result: Connectors,
    connectors: Connectors,
    env_vars: Mapping[str, str],
    env: Mapping[str, Any],
) -> Connectors:
    deps = _resolve_env_deps(connectors, env_vars, env)
    if deps is None:
        for _dep_name, env_var in env_vars.items():
            if not env.get(env_var, ""):
                raise ValueError(f"{env_var} is not configured")
        raise ValueError("Required provider dependency missing")
    if deps:
        connectors = connectors.bind_deps(**deps)
    return result + connectors


def _bind_optional_deps(
    result: Connectors,
    connectors: Connectors,
    env_vars: Mapping[str, str],
    env: Mapping[str, Any],
) -> Connectors:
    deps = _resolve_env_deps(connectors, env_vars, env)
    if deps is None:
        return result
    if deps:
        connectors = connectors.bind_deps(**deps)
    return result + connectors


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_connectors_from_env(
    *,
    env: dict[str, Any] | None = None,
    lenient: bool = False,
) -> Connectors:
    """Build the full connector surface from environment variables.

    Iterates :func:`iter_providers` (bundled providers + entry-point plugins).
    Each provider's connector collection is bound with deps resolved from
    *env* (or ``os.environ`` when ``None``).

    Set *lenient* to ``True`` to skip providers whose env vars are missing
    even when they are marked ``required``. Useful for partial environments
    such as catalog builds that may lack all API keys.
    """
    _env = env if env is not None else dict(os.environ)
    result = Connectors([])

    for spec in iter_providers():
        try:
            connectors, env_vars = spec.resolve()
        except ImportError as exc:
            logger.debug("Provider %r failed to resolve: %s", spec.name, exc)
            continue

        if not env_vars:
            result = result + connectors
        elif spec.required and not lenient:
            result = _bind_required_deps(result, connectors, env_vars, _env)
        else:
            result = _bind_optional_deps(result, connectors, env_vars, _env)

    return result
