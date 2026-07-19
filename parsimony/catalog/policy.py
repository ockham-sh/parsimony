"""Discovery indexing policy for catalog snapshots."""

from __future__ import annotations

from collections.abc import Sequence

from parsimony.catalog import BM25Index, CatalogIndex, Entity, HybridIndex, VectorIndex
from parsimony.embedder import EmbeddingProvider, SentenceTransformerEmbedder

_DEFAULT_EMBEDDER: EmbeddingProvider | None = None


def _shared_embedder() -> EmbeddingProvider:
    global _DEFAULT_EMBEDDER
    if _DEFAULT_EMBEDDER is None:
        _DEFAULT_EMBEDDER = SentenceTransformerEmbedder()
    return _DEFAULT_EMBEDDER


def discovery_indexes(
    entries: Sequence[Entity],
    *,
    include_description: bool = True,
    embedder: EmbeddingProvider | None = None,
) -> dict[str, CatalogIndex]:
    """Typical discovery catalog: BM25 code + hybrid title (+ optional description).

    Index kind follows the field's *role*, not its cardinality: identifiers
    are BM25 only (token semantics of IDs are noise), while title and
    description are bounded discovery vocabularies (one value per catalog
    entry) and always carry a vector component — search semantics never
    depend on how many entries a provider happens to publish. *entries* is
    accepted for call-site compatibility; the role policy does not inspect it.
    """
    del entries

    def _hybrid() -> CatalogIndex:
        return HybridIndex(components=[BM25Index(), VectorIndex(embedder=embedder or _shared_embedder())])

    indexes: dict[str, CatalogIndex] = {"code": BM25Index(), "title": _hybrid()}
    if include_description:
        indexes["description"] = _hybrid()
    return indexes


__all__ = [
    "discovery_indexes",
]
