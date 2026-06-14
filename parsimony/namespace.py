"""Typed symbology markers for connector parameters."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Namespace:
    """Mark a connector parameter as belonging to a symbology namespace.

    Use with ``typing.Annotated`` on fetch parameters whose legal values come
    from a catalog search in the same namespace, e.g.::

        series_id: Annotated[str, Namespace("fred")]

    The framework surfaces this on :class:`~parsimony.connector.Connector`
    cards as a hint for agents and humans.
    """

    name: str

    def __str__(self) -> str:
        return self.name
