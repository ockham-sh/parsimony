"""Adaptive discovery indexing policy for catalog snapshots."""

from __future__ import annotations

from collections.abc import Sequence

from parsimony.catalog import BM25Index, CatalogIndex, Entity, HybridIndex, VectorIndex, field_values
from parsimony.embedder import EmbeddingProvider, SentenceTransformerEmbedder
from parsimony.ranking import ZScoreFusion

HYBRID_UNIQUE_VALUE_LIMIT = 1000
HYBRID_BM25_WEIGHT = 0.5
HYBRID_VECTOR_WEIGHT = 1.0

_DEFAULT_EMBEDDER: EmbeddingProvider | None = None


def _shared_embedder() -> EmbeddingProvider:
    global _DEFAULT_EMBEDDER
    if _DEFAULT_EMBEDDER is None:
        _DEFAULT_EMBEDDER = SentenceTransformerEmbedder()
    return _DEFAULT_EMBEDDER


def adaptive_field_index(
    field: str,
    entries: Sequence[Entity],
    *,
    bm25_weight: float = HYBRID_BM25_WEIGHT,
    vector_weight: float = HYBRID_VECTOR_WEIGHT,
    embedder: EmbeddingProvider | None = None,
) -> CatalogIndex:
    """Hybrid BM25+vector index when unique values are below the limit, else BM25-only."""
    seen: set[str] = set()
    for entry in entries:
        for val in field_values(entry, field):
            seen.add(val)
    if len(seen) < HYBRID_UNIQUE_VALUE_LIMIT:
        return HybridIndex(
            components=[
                BM25Index(),
                VectorIndex(embedder=embedder or _shared_embedder()),
            ],
            fusion=ZScoreFusion(weights={"bm25": bm25_weight, "vector": vector_weight}),
        )
    return BM25Index()


def discovery_indexes(
    entries: Sequence[Entity],
    *,
    include_description: bool = True,
) -> dict[str, CatalogIndex]:
    """Typical discovery catalog: code BM25 + adaptive title (+ optional description)."""
    indexes: dict[str, CatalogIndex] = {
        "code": BM25Index(),
        "title": adaptive_field_index("title", entries),
    }
    if include_description:
        indexes["description"] = adaptive_field_index("description", entries)
    return indexes


# Backward-compatible aliases used by connector tooling during migration.
hybrid_field_index = adaptive_field_index
macro_discovery_indexes = discovery_indexes

__all__ = [
    "HYBRID_BM25_WEIGHT",
    "HYBRID_UNIQUE_VALUE_LIMIT",
    "HYBRID_VECTOR_WEIGHT",
    "adaptive_field_index",
    "discovery_indexes",
    "hybrid_field_index",
    "macro_discovery_indexes",
]
