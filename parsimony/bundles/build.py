"""Orchestrate the bundle build lifecycle for one namespace.

End-to-end shape::

    plans (>=1, all sharing namespace)
        ↓ (call enumerator per plan, concat results)
    entries: list[SeriesEntry]
        ↓ (embed)
    vectors: np.ndarray (N, dim)         + entries  (parquet write input)
        ↓ (build FAISS, drop ndarray)
    index + entries
        ↓ (write parquet + faiss + manifest)
    BundleManifest

**Coalescing rule** (per :mod:`parsimony.bundles.spec`). All plans passed
to :func:`build_bundle_dir` MUST share the same ``namespace``. The CLI
groups plans by namespace before dispatching builds. Mismatched
namespaces raise :class:`BundleSpecError` here as a defensive check.
"""

from __future__ import annotations

import logging
import subprocess
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from parsimony.bundles.errors import BundleSpecError
from parsimony.bundles.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    ENTRIES_PARQUET_SCHEMA,
    FAISS_HNSW_EF_CONSTRUCTION,
    FAISS_HNSW_EF_SEARCH_DEFAULT,
    FAISS_HNSW_M,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    MAX_INDEX_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_PARQUET_BYTES,
    BundleManifest,
    sha256_file,
)
from parsimony.bundles.spec import CatalogPlan
from parsimony.catalog.arrow_adapters import entries_to_arrow_table
from parsimony.catalog.catalog import embed_entries_in_batches
from parsimony.catalog.models import EmbeddingProvider, SeriesEntry

logger = logging.getLogger(__name__)


PlanRunner = Callable[[CatalogPlan], Awaitable[list[SeriesEntry]]]
"""Per-plan runner: takes a :class:`CatalogPlan`, returns its enumerator's rows.

The CLI is responsible for adapting a discovered :class:`Connector` (with
deps already bound from env vars) into this shape — keeps :func:`build_bundle_dir`
agnostic of connector internals. The runner is called once per plan and
must be idempotent.
"""


def _build_faiss_index(
    vectors: np.ndarray,
    *,
    dim: int,
    m: int = FAISS_HNSW_M,
    ef_construction: int = FAISS_HNSW_EF_CONSTRUCTION,
    ef_search: int = FAISS_HNSW_EF_SEARCH_DEFAULT,
) -> Any:
    """Build an in-memory FAISS HNSWFlat index. Vectors must be L2-normalized."""
    import faiss

    if vectors.ndim != 2 or vectors.shape[1] != dim:
        raise ValueError(f"Expected (N, {dim}) float32 array, got shape {vectors.shape}")
    if vectors.shape[0] == 0:
        raise ValueError("Cannot build FAISS index over zero vectors")
    if vectors.dtype != np.float32:
        vectors = vectors.astype(np.float32, copy=False)

    index = faiss.IndexHNSWFlat(dim, m, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = ef_construction
    index.hnsw.efSearch = ef_search
    index.add(vectors)
    return index


def _write_faiss_index(index: Any, path: Path) -> None:
    """Serialize a FAISS index to disk."""
    import faiss

    faiss.write_index(index, str(path))


def _enforce_size_cap(
    path: Path,
    cap: int,
    *,
    namespace: str,
    label: str,
) -> None:
    """Post-write ``stat`` check; raise :class:`~parsimony.bundles.errors.BundleError` if over cap."""
    from parsimony.bundles.errors import BundleError

    size = path.stat().st_size
    if size > cap:
        raise BundleError(
            f"{label} size {size} bytes exceeds cap {cap} bytes",
            namespace=namespace,
            resource=str(path),
        )


def _current_git_sha() -> str | None:
    """Best-effort 40-char commit SHA of the builder's working tree."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = result.stdout.strip()
    return sha if len(sha) == 40 else None


async def _enumerate_all(
    plans: list[CatalogPlan],
    runner: PlanRunner,
    *,
    namespace: str,
) -> list[SeriesEntry]:
    """Run the enumerator per plan; concatenate the entries.

    Plans with the same params would produce duplicate rows — the build
    side does NOT dedupe (the plan generator is the source of truth on
    intended shape; deduping silently would mask bugs).
    """
    out: list[SeriesEntry] = []
    for plan in plans:
        if plan.namespace != namespace:
            raise BundleSpecError(
                f"Plan namespace mismatch: plan={plan.namespace!r} but bundle is {namespace!r}",
                namespace=namespace,
            )
        entries = await runner(plan)
        out.extend(entries)
    return out


async def build_bundle_dir(
    *,
    namespace: str,
    plans: list[CatalogPlan],
    runner: PlanRunner,
    out_dir: Path,
    provider: EmbeddingProvider,
    embed_batch_size: int = 64,
    ef_search: int = FAISS_HNSW_EF_SEARCH_DEFAULT,
    git_sha: str | None = None,
) -> BundleManifest:
    """Build one bundle for one namespace from N plan items.

    Caller-provided ``runner`` adapts a :class:`CatalogPlan` to the
    enumerator's actual call (binds deps, instantiates the param model
    from ``plan.params``, awaits the enumerator, hydrates entries from
    the result). Keeps this function decoupled from
    :class:`~parsimony.connector.Connector` internals.

    Returns the written :class:`BundleManifest`. Raises:

    - :class:`~parsimony.bundles.errors.BundleSpecError` if any plan's
      namespace differs from ``namespace`` (defensive check; the CLI
      should already group plans by namespace).
    - :class:`ValueError` if entries or vectors are empty / mismatched
      (downstream of :func:`embed_entries_in_batches` and FAISS).
    - :class:`RuntimeError` if the embedding provider misbehaves.
    """
    if not plans:
        raise BundleSpecError(
            "build_bundle_dir requires at least one plan",
            namespace=namespace,
        )

    t_total = time.monotonic()

    t_phase = time.monotonic()
    entries = await _enumerate_all(plans, runner, namespace=namespace)
    if not entries:
        raise RuntimeError(
            f"Enumerator(s) for namespace={namespace!r} returned zero entries across {len(plans)} plan(s)"
        )
    t_enumerate = time.monotonic() - t_phase
    logger.info(
        "bundle.build.enumerated namespace=%s plans=%d entries=%d elapsed_s=%.2f",
        namespace,
        len(plans),
        len(entries),
        t_enumerate,
    )

    t_phase = time.monotonic()
    vectors = await embed_entries_in_batches(entries, provider=provider, batch_size=embed_batch_size)
    t_embed = time.monotonic() - t_phase
    logger.info(
        "bundle.build.embedded namespace=%s vectors=%d dim=%d elapsed_s=%.2f",
        namespace,
        vectors.shape[0],
        vectors.shape[1],
        t_embed,
    )

    t_phase = time.monotonic()
    if vectors.ndim != 2 or vectors.shape[0] != len(entries):
        raise ValueError(
            f"vectors shape {vectors.shape} must be (entries={len(entries)}, dim)"
        )

    model_id = getattr(provider, "model_id", None)
    revision = getattr(provider, "revision", None)
    if model_id is None or revision is None:
        raise RuntimeError(
            "Provider must expose model_id and revision attributes for the bundle manifest; "
            "use a SentenceTransformersEmbeddingProvider"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    entries_path = out_dir / ENTRIES_FILENAME
    index_path = out_dir / INDEX_FILENAME
    manifest_path = out_dir / MANIFEST_FILENAME

    table = entries_to_arrow_table(entries)
    if table.schema != ENTRIES_PARQUET_SCHEMA:
        raise RuntimeError(
            "entries_to_arrow_table produced a schema that doesn't match the bundle contract"
        )
    pq.write_table(table, entries_path)
    _enforce_size_cap(entries_path, MAX_PARQUET_BYTES, namespace=namespace, label="entries.parquet")

    dim = provider.dimension
    index = _build_faiss_index(vectors, dim=dim, ef_search=ef_search)
    _write_faiss_index(index, index_path)
    _enforce_size_cap(index_path, MAX_INDEX_BYTES, namespace=namespace, label="index.faiss")

    manifest = BundleManifest(
        namespace=namespace,
        built_at=datetime.now(UTC),
        entry_count=len(entries),
        embedding_model=model_id,
        embedding_model_revision=revision,
        embedding_dim=dim,
        faiss_hnsw_ef_search=ef_search,
        entries_sha256=sha256_file(entries_path),
        index_sha256=sha256_file(index_path),
        builder_git_sha=git_sha or _current_git_sha(),
    )
    manifest_path.write_text(
        manifest.model_dump_json(indent=2, round_trip=True) + "\n",
        encoding="utf-8",
    )
    _enforce_size_cap(manifest_path, MAX_MANIFEST_BYTES, namespace=namespace, label="manifest.json")

    actual = {p.name for p in out_dir.iterdir() if p.is_file()}
    extras = actual - BUNDLE_FILENAMES
    if extras:
        logger.warning("Unexpected extra files in bundle dir: %s", sorted(extras))

    t_write = time.monotonic() - t_phase

    total_s = time.monotonic() - t_total
    logger.info(
        "bundle.build.done namespace=%s entries=%d total_s=%.2f "
        "(enumerate=%.2f embed=%.2f write=%.2f)",
        namespace,
        manifest.entry_count,
        total_s,
        t_enumerate,
        t_embed,
        t_write,
    )

    return manifest


__all__ = [
    "PlanRunner",
    "build_bundle_dir",
]
