"""FAISS + BM25 helpers for the standard catalog.

Pure functions over numpy arrays and token lists, separated from the catalog
class to keep the latter focused on orchestration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import faiss

HNSW_THRESHOLD = 4096
HNSW_M = 16
HNSW_EF_CONSTRUCTION = 200
HNSW_EF_SEARCH = 64

RRF_K = 60


def tokenize(text: str) -> list[str]:
    """Whitespace + lowercase tokenization for BM25."""
    return text.lower().split()


def build_faiss(matrix: np.ndarray, *, dim: int, normalize: bool) -> faiss.Index:
    """Build a FAISS index from *matrix* (shape ``(n, dim)``)."""
    import faiss

    if normalize:
        faiss.normalize_L2(matrix)
    if matrix.shape[0] < HNSW_THRESHOLD:
        index: faiss.Index = faiss.IndexFlatIP(dim)
    else:
        hnsw = faiss.IndexHNSWFlat(dim, HNSW_M, faiss.METRIC_INNER_PRODUCT)
        hnsw.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        hnsw.hnsw.efSearch = HNSW_EF_SEARCH
        index = hnsw
    index.add(matrix)
    return index


def read_faiss(path: str, *, expected_rows: int) -> faiss.Index:
    """Read a FAISS index from *path* and validate row count."""
    import faiss

    index = faiss.read_index(path)
    if index.ntotal != expected_rows:
        raise ValueError(
            f"FAISS index at {path} has {index.ntotal} rows but parquet has {expected_rows}; snapshot is inconsistent"
        )
    if isinstance(index, faiss.IndexHNSW):
        index.hnsw.efSearch = HNSW_EF_SEARCH
    return index


def write_faiss(index: faiss.Index | None, path: str, *, dim: int) -> None:
    """Write *index* to *path*; write an empty Flat index when ``None``."""
    import faiss

    if index is None:
        faiss.write_index(faiss.IndexFlatIP(dim), path)
        return
    faiss.write_index(index, path)


def faiss_query(
    index: faiss.Index,
    query_vector: list[float],
    *,
    k: int,
    normalize: bool,
) -> list[tuple[int, int]]:
    """Return ``[(row_idx, rank), ...]`` for the top-k FAISS hits."""
    import faiss

    q = np.asarray([query_vector], dtype=np.float32)
    if normalize:
        faiss.normalize_L2(q)
    _scores, ids = index.search(q, min(k, index.ntotal))
    return [(int(idx), rank) for rank, idx in enumerate(ids[0]) if idx != -1]


def bm25_query(bm25: object, query: str, *, k: int) -> list[tuple[int, int]]:
    """Return ``[(row_idx, rank), ...]`` for the top-k BM25 hits."""
    scores = bm25.get_scores(tokenize(query))  # type: ignore[attr-defined]
    order = np.argsort(scores)[::-1]
    ranked: list[tuple[int, int]] = []
    for rank, idx in enumerate(order[:k]):
        if scores[idx] <= 0:
            break
        ranked.append((int(idx), rank))
    return ranked


def rrf_fuse(*rankings: list[tuple[int, int]]) -> list[tuple[int, float]]:
    """Reciprocal Rank Fusion (Cormack et al. 2009) with k=:data:`RRF_K`."""
    scored: dict[int, float] = {}
    for ranking in rankings:
        for idx, rank in ranking:
            scored[idx] = scored.get(idx, 0.0) + 1.0 / (RRF_K + rank + 1)
    return sorted(scored.items(), key=lambda kv: kv[1], reverse=True)


__all__ = [
    "HNSW_THRESHOLD",
    "RRF_K",
    "bm25_query",
    "build_faiss",
    "faiss_query",
    "read_faiss",
    "rrf_fuse",
    "tokenize",
    "write_faiss",
]
