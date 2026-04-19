"""Discover bundle specs from installed Parsimony provider plugins.

Walks every entry point under ``parsimony.providers``, finds connectors that
declare ``properties["catalog"]`` (i.e. were decorated with
``@enumerator(catalog=...)``), and yields a :class:`DiscoveredSpec` per
connector.

**No cache.** Every :func:`iter_specs` call re-walks the underlying
:func:`parsimony.discovery.discovered_providers` — which is itself cached
at the right layer (memoized per process by the discovery module). Caching
here would only cache a *projection* of that data and create a stale-spec
failure mode the moment a plugin is reloaded during a test or hot-reload
session.

Sync function (the upstream :func:`discovered_providers` is sync), so
this is callable from CLI top-level code without an event loop.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from parsimony.bundles.spec import CatalogSpec
from parsimony.connector import Connector
from parsimony.discovery import DiscoveredProvider, discovered_providers


@dataclass(frozen=True)
class DiscoveredSpec:
    """One discovered bundle declaration: which plugin owns it, the connector,
    the spec. Immutable; safe to share across coroutines."""

    provider: DiscoveredProvider
    """The plugin entry point this connector came from. Carries env-var
    bindings (``provider.env_vars``) the CLI needs to construct the runner."""

    connector: Connector
    """The decorated enumerator. Use :meth:`Connector.bind_deps` to bind env
    values before invoking the plan."""

    spec: CatalogSpec
    """The validated :class:`CatalogSpec` extracted from
    ``connector.properties['catalog']``."""


def iter_specs() -> Iterator[DiscoveredSpec]:
    """Yield every connector with a bundle declaration, across all providers.

    Order is deterministic (provider order from
    :func:`discovered_providers`, then connector order from each provider's
    ``CONNECTORS`` collection). Connectors without ``properties["catalog"]``
    are skipped silently — bundle discovery is a strict subset of plugin
    discovery, not an error.
    """
    for provider in discovered_providers():
        for connector in provider.connectors:
            spec = connector.properties.get("catalog")
            if spec is None:
                continue
            yield DiscoveredSpec(
                provider=provider,
                connector=connector,
                spec=spec,
            )


__all__ = [
    "DiscoveredSpec",
    "iter_specs",
]
