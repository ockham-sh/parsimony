"""Catalog-adjacent helpers that are not entity projection.

Entity extraction lives on :class:`~parsimony.result.Result` itself
(``result.to_entities()`` / ``result.entities``) — there is no separate
catalog-side conversion function. Build a :class:`~parsimony.result.Result`
around any DataFrame plus its :class:`~parsimony.result.OutputSpec` to reuse
that same projection outside of a connector call, e.g. when concatenating or
deduplicating multiple enumerator pages before extracting entities.
"""

from __future__ import annotations


def lazy_catalog_dir(provider: str, namespace: str) -> str:
    """Return the on-disk lazy-cache directory for a provider catalog namespace."""
    from parsimony import cache

    return str(cache.connectors_dir(provider) / "catalogs" / namespace)


__all__ = ["lazy_catalog_dir"]
