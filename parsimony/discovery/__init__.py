"""Plugin discovery and connector composition for parsimony.

This module is the only mechanism the kernel uses to learn about installed
connectors. External packages expose connectors via the ``parsimony.providers``
``importlib.metadata`` entry-point group; see the contract specification in
``docs/contract.md`` for the author-facing spec.

Public API
----------

* :class:`DiscoveredProvider` — immutable record of one loaded entry point.
* :func:`discovered_providers` — enumerate every valid, loaded plugin.
* :func:`iter_entry_points` — low-level iteration over the entry-point group.
* :func:`load_provider` — load and validate a single entry point.
* :func:`build_connectors_from_env` — compose every discovered provider into a
  single :class:`~parsimony.connector.Connectors` collection, binding deps from
  environment variables.
* :class:`PluginError` / :class:`PluginImportError` / :class:`PluginContractError`
  — exceptions raised at discovery time.
"""

from __future__ import annotations

from parsimony.discovery._compose import build_connectors_from_env
from parsimony.discovery._scan import (
    DiscoveredProvider,
    discovered_providers,
    iter_entry_points,
    load_provider,
)
from parsimony.discovery.errors import (
    PluginContractError,
    PluginError,
    PluginImportError,
)

__all__ = [
    "DiscoveredProvider",
    "PluginContractError",
    "PluginError",
    "PluginImportError",
    "build_connectors_from_env",
    "discovered_providers",
    "iter_entry_points",
    "load_provider",
]
