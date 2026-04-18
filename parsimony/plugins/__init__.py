"""Plugin discovery and contract enforcement for parsimony.

External packages expose connectors via the ``parsimony.providers``
``importlib.metadata`` entry-point group. See
:mod:`parsimony.plugins.discovery` for the machinery and
``docs/plugin-contract.md`` for the author-facing spec.
"""

from __future__ import annotations

from parsimony.plugins.discovery import (
    DiscoveredProvider,
    discovered_providers,
    iter_entry_points,
    load_provider,
)
from parsimony.plugins.errors import PluginContractError, PluginError, PluginImportError

__all__ = [
    "DiscoveredProvider",
    "PluginContractError",
    "PluginError",
    "PluginImportError",
    "discovered_providers",
    "iter_entry_points",
    "load_provider",
]
