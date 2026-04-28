"""FAISS + BM25 helpers for the standard catalog.

Pure functions over numpy arrays and token lists, separated from the catalog
class to keep the latter focused on orchestration.

Index choice in :func:`build_faiss` is adaptive on row count:

* ``N < HNSW_THRESHOLD`` (4 096): ``IndexFlatIP`` — exact, no build cost.
* ``HNSW_THRESHOLD ≤ N < IVF_THRESHOLD`` (500 000): ``IndexHNSWFlat`` —
  highest recall, fits in RAM for medium catalogs.
* ``N ≥ IVF_THRESHOLD``: ``IndexIVFFlat`` — ~3× lower build peak. HNSW's
  build memory is ~3-5× the raw embeddings; on a 27 GB host it OOM-kills
  around 1.27 M rows. IVFFlat trades a few percent recall for the
  headroom we need at scale.

Override the threshold via ``PARSIMONY_FAISS_IVF_THRESHOLD``.
"""

from __future__ import annotations

import logging
import math
import os
import re
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import faiss

logger = logging.getLogger(__name__)

HNSW_THRESHOLD = 4096
HNSW_M = 16
HNSW_EF_CONSTRUCTION = 200
HNSW_EF_SEARCH = 64

# IVFFlat path. Defaults derived from FAISS conventions (nlist ≈ 4·√N,
# nprobe ≈ 5-15 % of nlist) plus a training-sample cap that keeps k-means
# time bounded on very large catalogs while staying ≫ 50× nlist.
IVF_THRESHOLD = int(os.environ.get("PARSIMONY_FAISS_IVF_THRESHOLD", "500000"))
IVF_NLIST_FACTOR = 4
IVF_NLIST_MIN = 64
IVF_NLIST_MAX = 65_536
IVF_NPROBE_FRACTION = 0.10
IVF_TRAIN_SAMPLE_CAP = 256_000

RRF_K = 60


_TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    """Lowercase + split on any non-alphanumeric char for BM25.

    Whitespace splitting alone leaves identifier-style tokens like
    ``debt_to_penny`` or ``v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt``
    as single opaque tokens — a query of ``debt_to_penny`` then never matches
    the stored row and BM25 silently returns zero hits. Splitting on any
    non-``[a-z0-9]`` run makes code fragments, snake_case columns, and
    hash-separated compound keys all tokenize into their constituent words.
    """
    return _TOKEN_RE.findall(text.lower())


def _ivf_nlist(n: int) -> int:
    """Pick ``nlist`` for an IVF index over *n* vectors.

    ``4·√N`` clamped to ``[IVF_NLIST_MIN, IVF_NLIST_MAX]``: gives ~280
    vectors per cell at N=1.27 M, well above the FAISS rule-of-thumb
    minimum of 30-100.
    """
    return max(IVF_NLIST_MIN, min(IVF_NLIST_MAX, IVF_NLIST_FACTOR * int(math.sqrt(n))))


def _build_ivfflat(matrix: np.ndarray, *, dim: int) -> faiss.Index:
    """Build a trained, populated ``IndexIVFFlat`` over *matrix*."""
    import faiss

    n = matrix.shape[0]
    nlist = _ivf_nlist(n)
    quantizer = faiss.IndexFlatIP(dim)
    index = faiss.IndexIVFFlat(quantizer, dim, nlist, faiss.METRIC_INNER_PRODUCT)

    if n > IVF_TRAIN_SAMPLE_CAP:
        rng = np.random.default_rng(seed=0)
        sample_ids = rng.choice(n, size=IVF_TRAIN_SAMPLE_CAP, replace=False)
        train = matrix[sample_ids]
    else:
        train = matrix
    index.train(train)
    index.nprobe = max(1, int(nlist * IVF_NPROBE_FRACTION))
    index.add(matrix)
    logger.info(
        "build_faiss: IndexIVFFlat n=%d nlist=%d nprobe=%d trained_on=%d",
        n,
        nlist,
        index.nprobe,
        train.shape[0],
    )
    return index


def build_faiss(matrix: np.ndarray, *, dim: int, normalize: bool) -> faiss.Index:
    """Build a FAISS index from *matrix* (shape ``(n, dim)``).

    Adaptive on ``n``: :class:`faiss.IndexFlatIP` for tiny catalogs,
    :class:`faiss.IndexHNSWFlat` for medium, :class:`faiss.IndexIVFFlat`
    once the HNSW build peak would exceed comfortable host RAM.
    """
    import faiss

    if normalize:
        faiss.normalize_L2(matrix)
    n = matrix.shape[0]
    if n < HNSW_THRESHOLD:
        index: faiss.Index = faiss.IndexFlatIP(dim)
        index.add(matrix)
        return index
    if n < IVF_THRESHOLD:
        hnsw = faiss.IndexHNSWFlat(dim, HNSW_M, faiss.METRIC_INNER_PRODUCT)
        hnsw.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        hnsw.hnsw.efSearch = HNSW_EF_SEARCH
        hnsw.add(matrix)
        return hnsw
    return _build_ivfflat(matrix, dim=dim)


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
    elif isinstance(index, faiss.IndexIVF):
        # Re-derive nprobe on load so a tuning change to IVF_NPROBE_FRACTION
        # propagates without re-publishing every snapshot.
        index.nprobe = max(1, int(index.nlist * IVF_NPROBE_FRACTION))
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
    "IVF_THRESHOLD",
    "RRF_K",
    "bm25_query",
    "build_faiss",
    "faiss_query",
    "read_faiss",
    "rrf_fuse",
    "tokenize",
    "write_faiss",
]
