"""Adaptive index selection in :func:`parsimony.indexes.build_faiss`.

Three groups:

1. **Selection** — assert the right ``faiss.Index`` subclass at each band.
2. **Persistence** — IVFFlat round-trips through ``write_faiss``/``read_faiss``
   with ``nprobe`` re-applied on load.
3. **Recall regression (slow, opt-in)** — IVFFlat recall@10 ≥ 0.90 on a real
   ESTAT fixture. Skipped automatically if the fixture is absent so the
   test stays portable.
"""

from __future__ import annotations

import os
from pathlib import Path

import faiss
import numpy as np
import pytest

from parsimony import indexes
from parsimony.indexes import (
    HNSW_THRESHOLD,
    IVF_THRESHOLD,
    build_faiss,
    read_faiss,
    write_faiss,
)


def _random_matrix(n: int, dim: int = 32, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((n, dim), dtype=np.float32)


# --- Selection -------------------------------------------------------------


def test_small_n_picks_flat() -> None:
    n = HNSW_THRESHOLD - 1
    idx = build_faiss(_random_matrix(n), dim=32, normalize=False)
    assert isinstance(idx, faiss.IndexFlat)
    assert idx.ntotal == n


def test_medium_n_picks_hnsw() -> None:
    n = HNSW_THRESHOLD + 1
    idx = build_faiss(_random_matrix(n), dim=32, normalize=False)
    assert isinstance(idx, faiss.IndexHNSW)
    assert idx.ntotal == n


def test_large_n_picks_ivfflat(monkeypatch: pytest.MonkeyPatch) -> None:
    # Lower the threshold so the test runs in milliseconds; the real
    # threshold is 500k and would take seconds to materialize even on
    # synthetic data.
    monkeypatch.setattr(indexes, "IVF_THRESHOLD", HNSW_THRESHOLD + 100)
    n = HNSW_THRESHOLD + 200
    idx = build_faiss(_random_matrix(n), dim=32, normalize=False)
    assert isinstance(idx, faiss.IndexIVF)
    assert idx.ntotal == n
    assert idx.nprobe >= 1


def test_ivf_threshold_env_resolution() -> None:
    # The module-level constant is captured at import time from the env.
    # Asserting the default matches the documented value.
    assert IVF_THRESHOLD == int(os.environ.get("PARSIMONY_FAISS_IVF_THRESHOLD", "500000"))


# --- Persistence round-trip ------------------------------------------------


def test_ivfflat_persists_through_read_faiss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(indexes, "IVF_THRESHOLD", HNSW_THRESHOLD + 100)
    n = HNSW_THRESHOLD + 500
    matrix = _random_matrix(n, dim=64, seed=1)
    idx = build_faiss(matrix.copy(), dim=64, normalize=False)
    assert isinstance(idx, faiss.IndexIVF)
    nprobe_before = idx.nprobe

    path = str(tmp_path / "ivf.faiss")
    write_faiss(idx, path, dim=64)
    loaded = read_faiss(path, expected_rows=n)

    assert isinstance(loaded, faiss.IndexIVF)
    assert loaded.ntotal == n
    assert loaded.nprobe == nprobe_before  # re-applied on load

    q = matrix[:5]
    _, ids_a = idx.search(q, 10)
    _, ids_b = loaded.search(q, 10)
    np.testing.assert_array_equal(ids_a, ids_b)


# --- Recall regression on a real ESTAT fixture (slow, opt-in) --------------


_FIXTURE_DIR = Path(
    os.environ.get(
        "PARSIMONY_RECALL_FIXTURE",
        "/home/espinet/ockham/catalogs/sdmx/repo/sdmx_series_estat_hlth_ehis_mh2c",
    )
)


@pytest.mark.slow
def test_ivfflat_recall_on_estat_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    """IVFFlat recall@10 ≥ 0.90 on a real ESTAT flow.

    Loads the existing on-disk HNSW index, extracts its raw embeddings,
    and rebuilds both an HNSW (control) and an IVFFlat (under test) over
    the same matrix. For 100 random rows we query each index by that
    row's own embedding and check whether the source row appears in
    top-10. HNSW should always find itself; IVFFlat may miss occasional
    rows whose cell is just outside ``nprobe``.

    Set ``PARSIMONY_RECALL_FIXTURE`` to point at a different snapshot.
    """
    src_path = _FIXTURE_DIR / "embeddings.faiss"
    if not src_path.is_file():
        pytest.skip(f"recall fixture not present at {src_path}")

    src = faiss.read_index(str(src_path))
    n = src.ntotal
    dim = src.d
    matrix = src.reconstruct_n(0, n).astype(np.float32)
    assert matrix.shape == (n, dim)

    # The fixture is published with normalized embeddings; pass
    # ``normalize=False`` so build_faiss doesn't re-normalize (idempotent
    # but wasted work) and keep the source matrix pristine for both
    # builds.
    hnsw = build_faiss(matrix.copy(), dim=dim, normalize=False)
    assert isinstance(hnsw, faiss.IndexHNSW)

    monkeypatch.setattr(indexes, "IVF_THRESHOLD", 1)
    ivf = build_faiss(matrix.copy(), dim=dim, normalize=False)
    assert isinstance(ivf, faiss.IndexIVF)

    rng = np.random.default_rng(seed=0)
    sample_ids = rng.choice(n, size=100, replace=False)

    hnsw_hits = 0
    ivf_hits = 0
    for i in sample_ids:
        q = matrix[i : i + 1]
        _, ids_h = hnsw.search(q, 10)
        _, ids_i = ivf.search(q, 10)
        if i in ids_h[0]:
            hnsw_hits += 1
        if i in ids_i[0]:
            ivf_hits += 1

    hnsw_recall = hnsw_hits / 100
    ivf_recall = ivf_hits / 100
    print(
        f"\nrecall@10 over {n} entries (dim={dim}): "
        f"HNSW={hnsw_recall:.2%}  IVFFlat (nlist={ivf.nlist}, nprobe={ivf.nprobe})={ivf_recall:.2%}"
    )

    # HNSW should always find a vector that's literally in the index.
    assert hnsw_recall >= 0.95, f"HNSW recall regressed: {hnsw_recall:.2%}"
    # IVFFlat target: 0.90. The agent-driven retrieval pattern in
    # parsimony tolerates a few percent of misses on rare items; this
    # bar catches a real recall collapse from a misconfigured nlist or
    # nprobe.
    assert ivf_recall >= 0.90, f"IVFFlat recall below floor: {ivf_recall:.2%}"
