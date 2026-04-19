"""On-disk parse + integrity verification + FAISS search for bundles.

Hosts the :class:`LoadedNamespace` snapshot dataclass plus the pure
helpers that load a bundle directory, verify its sha256 + identity +
shape invariants against the manifest, and execute FAISS search against
the resulting index.

These functions are CPU- and IO-heavy; the store dispatches them via
:func:`asyncio.to_thread`.
"""

from __future__ import annotations

import builtins
import contextlib
import dataclasses
from pathlib import Path
from typing import Any

import pyarrow as pa

from parsimony.bundles.errors import BundleError
from parsimony.bundles.format import (
    ENTRIES_FILENAME,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    MAX_INDEX_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_PARQUET_BYTES,
    BundleManifest,
)
from parsimony.catalog.arrow_adapters import arrow_rows_to_entries
from parsimony.catalog.models import (
    EmbeddingProvider,
    SeriesMatch,
    series_match_from_entry,
)


@dataclasses.dataclass(frozen=True)
class LoadedNamespace:
    """Immutable snapshot of a loaded bundle.

    :meth:`HFBundleCatalogStore.refresh` swaps the store's pointer to a new
    instance atomically — in-flight searches hold a reference to the old
    instance and complete against it, avoiding a use-after-free in FAISS C.

    ``code_index`` maps each entry's ``code`` to its dense ``row_id``
    (= Parquet row position = FAISS vector position), built once at load
    time so point-lookups are O(1) rather than O(N) full-scans.
    """

    namespace: str
    manifest: BundleManifest
    table: pa.Table
    index: Any  # faiss.IndexHNSWFlat
    revision: str
    bundle_dir: Path
    code_index: dict[str, int]


def _enforce_size_cap(path: Path, cap: int, namespace: str, label: str) -> None:
    size = path.stat().st_size
    if size > cap:
        raise BundleError(
            f"{label} size {size} bytes exceeds cap {cap}",
            namespace=namespace,
            resource=str(path),
            next_action=(
                f"increase the size cap (currently {cap} bytes) if the bundle is legitimately large, "
                "or investigate the source for tampering"
            ),
        )


def _load_faiss_index(path: Path, *, namespace: str) -> Any:
    try:
        import faiss
    except ImportError as exc:
        raise BundleError(
            "faiss is not installed but a bundle requires it",
            namespace=namespace,
            next_action="install parsimony's base dependencies: pip install 'parsimony'",
        ) from exc
    try:
        return faiss.read_index(str(path))
    except Exception as exc:
        raise BundleError(
            f"faiss.read_index failed on {path}: {exc}",
            namespace=namespace,
            resource=str(path),
        ) from exc


def _load_from_dir(
    *,
    bundle_dir: Path,
    namespace: str,
    revision: str,
    provider: EmbeddingProvider,
) -> LoadedNamespace:
    """Parse + verify a bundle from an on-disk snapshot."""
    import pyarrow.parquet as pq

    manifest_path = bundle_dir / MANIFEST_FILENAME
    entries_path = bundle_dir / ENTRIES_FILENAME
    index_path = bundle_dir / INDEX_FILENAME

    _enforce_size_cap(manifest_path, MAX_MANIFEST_BYTES, namespace, "manifest.json")
    _enforce_size_cap(entries_path, MAX_PARQUET_BYTES, namespace, "entries.parquet")
    _enforce_size_cap(index_path, MAX_INDEX_BYTES, namespace, "index.faiss")

    try:
        manifest = BundleManifest.model_validate_json(manifest_path.read_bytes())
    except Exception as exc:
        raise BundleError(
            f"manifest.json failed validation: {exc}",
            namespace=namespace,
            resource=str(manifest_path),
        ) from exc

    if manifest.namespace != namespace:
        raise BundleError(
            f"manifest.namespace={manifest.namespace!r} doesn't match expected {namespace!r}",
            namespace=namespace,
            resource=str(manifest_path),
        )

    # The bundle directory is named by the 40-char HF commit SHA and
    # `huggingface_hub` verifies file hashes at download time, so the
    # directory name itself is the integrity proof. Skipping a redundant
    # in-process sha256 save ~O(bytes) per namespace load.

    # Identity check is fail-closed. The manifest validator guarantees
    # both ``embedding_model`` and ``embedding_model_revision`` are present,
    # so the provider MUST declare them too — silently encoding queries with
    # a different model would corrupt similarity scores beyond detection.
    provider_model = getattr(provider, "model_id", None)
    provider_rev = getattr(provider, "revision", None)
    if not provider_model or not provider_rev:
        raise BundleError(
            "EmbeddingProvider must expose non-empty model_id and revision attributes; "
            f"got model_id={provider_model!r} revision={provider_rev!r}",
            namespace=namespace,
            resource=str(manifest_path),
            next_action="construct the Catalog with a SentenceTransformersEmbeddingProvider",
        )
    if provider_model != manifest.embedding_model:
        raise BundleError(
            f"Provider model_id={provider_model!r} does not match bundle {manifest.embedding_model!r}",
            namespace=namespace,
            resource=str(manifest_path),
            next_action="construct the Catalog with an EmbeddingProvider matching the bundle",
        )
    if provider_rev != manifest.embedding_model_revision:
        raise BundleError(
            f"Provider revision={provider_rev!r} does not match bundle {manifest.embedding_model_revision!r}",
            namespace=namespace,
            resource=str(manifest_path),
        )
    if provider.dimension != manifest.embedding_dim:
        raise BundleError(
            f"Provider dimension={provider.dimension} does not match manifest embedding_dim={manifest.embedding_dim}",
            namespace=namespace,
        )

    table = pq.read_table(entries_path, memory_map=True)
    if table.num_rows != manifest.entry_count:
        raise BundleError(
            f"Parquet row count {table.num_rows} does not match manifest entry_count={manifest.entry_count}",
            namespace=namespace,
        )

    index = _load_faiss_index(index_path, namespace=namespace)
    if index.ntotal != manifest.entry_count:
        raise BundleError(
            f"FAISS ntotal={index.ntotal} does not match manifest entry_count={manifest.entry_count}",
            namespace=namespace,
        )
    if int(index.d) != manifest.embedding_dim:
        raise BundleError(
            f"FAISS dim={index.d} does not match manifest embedding_dim={manifest.embedding_dim}",
            namespace=namespace,
        )
    with contextlib.suppress(AttributeError):
        index.hnsw.efSearch = int(manifest.faiss_hnsw_ef_search)

    # Build the code → row_id index once. Codes are unique within a namespace
    # by the catalog invariant; a duplicate means the bundle is inconsistent.
    codes_col = table.column("code").to_pylist()
    row_ids_col = table.column("row_id").to_pylist()
    code_index: dict[str, int] = {}
    for code, rid in zip(codes_col, row_ids_col, strict=True):
        if code in code_index:
            raise BundleError(
                f"Duplicate code {code!r} at row_ids {code_index[code]} and {rid}",
                namespace=namespace,
            )
        code_index[str(code)] = int(rid)

    return LoadedNamespace(
        namespace=namespace,
        manifest=manifest,
        table=table,
        index=index,
        revision=revision,
        bundle_dir=bundle_dir,
        code_index=code_index,
    )


def _validate_query_embedding(vec: builtins.list[float], *, expected_dim: int) -> Any:
    """Assert shape/dtype/finite/normalized before handing to FAISS."""
    import numpy as np

    if not isinstance(vec, (list, tuple)) or len(vec) != expected_dim:
        raise BundleError(
            f"query embedding has length {len(vec) if hasattr(vec, '__len__') else '?'}, expected {expected_dim}",
        )

    arr = np.asarray(vec, dtype=np.float32).reshape(1, expected_dim)
    if not np.isfinite(arr).all():
        raise BundleError(
            "query embedding contains NaN or inf values",
            next_action="investigate the embedding provider output",
        )
    norm = float(np.linalg.norm(arr[0]))
    if abs(norm - 1.0) > 1e-3:
        raise BundleError(
            f"query embedding is not L2-normalized (norm={norm:.6f}); "
            "HNSWFlat bundles are built on L2-normalized vectors for cosine similarity",
        )
    return arr


def _search_one_bundle(
    bundle: LoadedNamespace,
    query_vec: Any,
    limit: int,
) -> list[SeriesMatch]:
    """FAISS search + Parquet row hydration."""
    k = min(limit, bundle.index.ntotal)
    if k <= 0:
        return []
    distances, indices = bundle.index.search(query_vec, k)

    row_ids: list[int] = []
    scores: list[float] = []
    for idx, score in zip(indices[0].tolist(), distances[0].tolist(), strict=False):
        if idx == -1:
            continue
        row_ids.append(int(idx))
        scores.append(float(score))
    if not row_ids:
        return []

    subset = bundle.table.take(pa.array(row_ids, type=pa.int64()))
    entries = arrow_rows_to_entries(subset, namespace=bundle.namespace)

    # Inner product on L2-normalized vectors == cosine similarity, bounded to [0, 1].
    results: list[SeriesMatch] = []
    for entry, score in zip(entries, scores, strict=False):
        sim = max(0.0, min(1.0, score))
        results.append(series_match_from_entry(entry, similarity=sim))
    return results
