"""Discovery of plugin packages registered via entry points.

A single group is recognised:

* ``parsimony.providers`` — :class:`~parsimony.connectors.ProviderSpec` objects
  exposing connector collections.

Failures to load surface as :class:`RegistryWarning` rather than silent
``ImportError``\\s — this avoids the install-but-broken bug class familiar from
heavily plugin-based ecosystems.
"""

from __future__ import annotations

import importlib.metadata
import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from parsimony.connectors import ProviderSpec

PROVIDERS_GROUP = "parsimony.providers"


class RegistryWarning(UserWarning):
    """Emitted when an entry-point fails to load."""


def _entry_points(group: str) -> list[importlib.metadata.EntryPoint]:
    eps = importlib.metadata.entry_points()
    return list(eps.select(group=group)) if hasattr(eps, "select") else list(eps.get(group, []))


def _load(group: str, name: str | None = None) -> Iterator[Any]:
    """Yield every loaded object in *group*, filtered by *name* when set."""
    for ep in _entry_points(group):
        if name is not None and ep.name != name:
            continue
        try:
            yield ep.load()
        except Exception as exc:
            warnings.warn(
                f"Failed to load entry point {ep.name!r} in group {group!r}: {exc}",
                RegistryWarning,
                stacklevel=2,
            )


def discover_providers() -> Iterator[ProviderSpec]:
    """Yield every :class:`ProviderSpec` registered under ``parsimony.providers``."""
    yield from _load(PROVIDERS_GROUP)
