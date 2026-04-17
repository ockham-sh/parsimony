"""HFBundleCatalogStore — reads Parquet + FAISS bundles from HuggingFace Hub.

Runtime option A: FAISS is the search index, Parquet is the row store. No
SQLite rehydration. Every bundle is a single atomic snapshot at a specific
HF commit SHA; revision pinning is the integrity root.

Implements :class:`~parsimony.stores.catalog_store.CatalogStore`. Write
operations (``upsert``, ``delete``) are not supported — bundles are
read-only on the client side.

Cache layout::

    <cache_base>/<namespace>/<commit_sha>/
        manifest.json
        entries.parquet
        index.faiss

One cached revision per namespace is kept; a fresh download replaces any
older sibling. The commit SHA is the directory name — no sidecar file.

Heavy imports (``faiss``, ``huggingface_hub``, ``pyarrow.parquet``) live
inside method bodies so importing this module is cheap.
"""

from __future__ import annotations

import asyncio
import builtins
import collections
import contextlib
import dataclasses
import hashlib
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any

import platformdirs
import pyarrow as pa

from parsimony.catalog.arrow_adapters import arrow_rows_to_entries
from parsimony.catalog.models import (
    EmbeddingProvider,
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    normalize_code,
    series_match_from_entry,
)
from parsimony.stores.catalog_store import CatalogStore
from parsimony.stores.hf_bundle.errors import (
    BundleIntegrityError,
    BundleNotFoundError,
)
from parsimony.stores.hf_bundle.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    MAX_INDEX_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_PARQUET_BYTES,
    BundleManifest,
    hf_repo_id,
)

logger = logging.getLogger(__name__)

_SHA40_RE = re.compile(r"^[0-9a-f]{40}$")

# Per-operation timeouts.
_MODEL_INFO_TIMEOUT_S = 10.0
_SNAPSHOT_TIMEOUT_S = 300.0

# Default LRU capacity for the per-store _loaded cache. Each entry holds a
# FAISS HNSW graph + mmap'd Parquet + manifest — not free. Callers who want
# unbounded behaviour can set PARSIMONY_MAX_LOADED_BUNDLES=0.
_DEFAULT_MAX_LOADED_BUNDLES = 16


def _resolve_max_loaded_bundles() -> int:
    raw = os.environ.get("PARSIMONY_MAX_LOADED_BUNDLES")
    if not raw:
        return _DEFAULT_MAX_LOADED_BUNDLES
    try:
        return int(raw)
    except ValueError:
        return _DEFAULT_MAX_LOADED_BUNDLES


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


def _default_cache_dir() -> Path:
    override = os.environ.get("PARSIMONY_CACHE_DIR")
    if override:
        return Path(override)
    return Path(platformdirs.user_cache_dir("parsimony")) / "bundles"


def _resolve_pin() -> str | None:
    pin = os.environ.get("PARSIMONY_CATALOG_PIN", "").strip()
    if not pin:
        return None
    if not _SHA40_RE.match(pin):
        raise ValueError(
            f"PARSIMONY_CATALOG_PIN={pin!r} is not a valid 40-char commit SHA; "
            "reject pins that aren't full SHAs to prevent tag-pointer drift"
        )
    return pin


class HFBundleCatalogStore(CatalogStore):
    """Read-only catalog store backed by HuggingFace Hub bundles.

    Parameters
    ----------
    embeddings:
        :class:`EmbeddingProvider` used to embed queries. The provider's
        ``model_id`` / ``revision`` are validated against every loaded
        manifest.
    cache_dir:
        Local cache directory for downloaded snapshots. Defaults to
        ``platformdirs.user_cache_dir("parsimony")/bundles`` or
        ``PARSIMONY_CACHE_DIR`` env var.
    pin:
        Pinned commit SHA, overriding ``PARSIMONY_CATALOG_PIN``. Full 40-char
        hex only. When set, freshness HEAD check is skipped.
    repo_id_for_namespace:
        Optional override for the HF repo id (default: :func:`hf_repo_id`).
    """

    def __init__(
        self,
        *,
        embeddings: EmbeddingProvider,
        cache_dir: Path | str | None = None,
        pin: str | None = None,
        repo_id_for_namespace: Any | None = None,
    ) -> None:
        self._embeddings = embeddings
        self._cache_dir = Path(cache_dir) if cache_dir is not None else _default_cache_dir()
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_base = self._cache_dir.resolve()

        self._pin = pin if pin is not None else _resolve_pin()
        self._repo_id = repo_id_for_namespace or hf_repo_id

        # OrderedDict-backed LRU. MRU lives at the tail; evict from head on
        # overflow. Cap <= 0 means unbounded.
        self._loaded: collections.OrderedDict[str, LoadedNamespace] = collections.OrderedDict()
        self._max_loaded = _resolve_max_loaded_bundles()
        # Single-flight: concurrent try_load_remote/refresh callers share one
        # Future per namespace. Resolves to LoadedNamespace on success or
        # None when the namespace has no bundle (BundleNotFoundError).
        self._load_futures: dict[str, asyncio.Future[LoadedNamespace | None]] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public observability surface
    # ------------------------------------------------------------------

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    @property
    def pin(self) -> str | None:
        return self._pin

    def loaded_namespaces(self) -> dict[str, LoadedNamespace]:
        """Snapshot of currently-loaded bundles (not a live view)."""
        return dict(self._loaded)

    def has(self, namespace: str) -> bool:
        return normalize_code(namespace) in self._loaded

    def status(self) -> dict[str, Any]:
        """Operator-facing snapshot of store state.

        Returns a dict (not a dataclass) with ``cache_dir``, ``pin``, and
        ``namespaces`` mapping namespace names to per-bundle info. Safe to
        log, JSON-serialize, assert on in tests.
        """
        namespaces: dict[str, dict[str, Any]] = {}
        for ns, loaded in self._loaded.items():
            namespaces[ns] = {
                "namespace": ns,
                "revision": loaded.revision,
                "embedding_model": loaded.manifest.embedding_model,
                "embedding_dim": loaded.manifest.embedding_dim,
                "entry_count": loaded.manifest.entry_count,
                "bundle_dir": str(loaded.bundle_dir),
            }
        return {
            "cache_dir": str(self._cache_dir),
            "pin": self._pin,
            "namespaces": namespaces,
        }

    def _insert_loaded(self, ns: str, loaded: LoadedNamespace) -> None:
        """Insert/refresh a loaded bundle in the LRU, evicting head on overflow.

        Caller must hold ``self._lock`` when mutating from a coroutine that
        races with other loaders.
        """
        self._loaded.pop(ns, None)
        self._loaded[ns] = loaded
        if self._max_loaded > 0:
            while len(self._loaded) > self._max_loaded:
                evicted_ns, evicted = self._loaded.popitem(last=False)
                with contextlib.suppress(Exception):
                    evicted.index.reset()
                logger.info(
                    "catalog.bundle_evicted namespace=%s revision=%s",
                    evicted_ns,
                    evicted.revision,
                )

    # ------------------------------------------------------------------
    # CatalogStore ABC — write operations not supported on bundle store
    # ------------------------------------------------------------------

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:  # pragma: no cover
        raise NotImplementedError(
            "HFBundleCatalogStore is read-only; write by rebuilding a bundle "
            "and re-publishing. For local mutations use SQLiteCatalogStore."
        )

    async def delete(self, namespace: str, code: str) -> None:  # pragma: no cover
        raise NotImplementedError(
            "HFBundleCatalogStore is read-only; delete by rebuilding a bundle. "
            "For local mutations use SQLiteCatalogStore."
        )

    # ------------------------------------------------------------------
    # CatalogStore ABC — read operations
    # ------------------------------------------------------------------

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        ns, c = catalog_key(namespace, code)
        loaded = self._loaded.get(ns)
        if loaded is None:
            return None
        row_id = loaded.code_index.get(c)
        if row_id is None:
            return None
        subset = loaded.table.take(pa.array([row_id], type=pa.int64()))
        entries = arrow_rows_to_entries(subset, namespace=ns)
        return entries[0] if entries else None

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        if not keys:
            return set()
        out: set[tuple[str, str]] = set()
        for ns, c in keys:
            nns, nc = catalog_key(ns, c)
            loaded = self._loaded.get(nns)
            if loaded is None:
                continue
            if nc in loaded.code_index:
                out.add((nns, nc))
        return out

    async def list_namespaces(self) -> builtins.list[str]:
        return sorted(self._loaded.keys())

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        """Paginate entries across one or all loaded namespaces.

        Filtering is pushed to pyarrow.compute; only the ``[offset, offset+limit]``
        window is hydrated to Pydantic ``SeriesEntry``.
        """
        import pyarrow.compute as pc

        namespaces = [normalize_code(namespace)] if namespace is not None else sorted(self._loaded.keys())
        needle = q.strip().lower() if q and q.strip() else None

        keyed: builtins.list[tuple[str, str, int]] = []
        for ns in namespaces:
            loaded = self._loaded.get(ns)
            if loaded is None:
                continue
            tbl = loaded.table
            if needle is not None:
                title_mask = pc.match_substring(pc.utf8_lower(tbl.column("title")), needle)
                code_mask = pc.match_substring(pc.utf8_lower(tbl.column("code")), needle)
                mask = pc.or_(title_mask, code_mask)
                filtered = tbl.filter(mask)
            else:
                filtered = tbl
            codes = filtered.column("code").to_pylist()
            row_ids = filtered.column("row_id").to_pylist()
            for c, rid in zip(codes, row_ids, strict=True):
                keyed.append((ns, str(c), int(rid)))

        keyed.sort(key=lambda t: (t[0], t[1]))
        total = len(keyed)
        window = keyed[offset : offset + limit]
        if not window:
            return [], total

        out: builtins.list[SeriesEntry] = []
        by_ns: dict[str, builtins.list[tuple[int, int]]] = {}
        for i, (ns, _c, rid) in enumerate(window):
            by_ns.setdefault(ns, []).append((i, rid))
        hydrated: dict[int, SeriesEntry] = {}
        for ns, pairs in by_ns.items():
            loaded = self._loaded[ns]
            positions = [rid for _i, rid in pairs]
            subset = loaded.table.take(pa.array(positions, type=pa.int64()))
            entries = arrow_rows_to_entries(subset, namespace=ns)
            for (i, _rid), entry in zip(pairs, entries, strict=True):
                hydrated[i] = entry
        for i in range(len(window)):
            out.append(hydrated[i])
        return out, total

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
        query_embedding: builtins.list[float] | None = None,
    ) -> builtins.list[SeriesMatch]:
        """Vector search over loaded bundles.

        ``namespaces`` must be a non-empty list of already-loaded namespaces.
        :class:`~parsimony.catalog.catalog.Catalog` enforces this at the
        higher-level API.

        Emits one structured log line per call with model identity, embed
        latency, fan-out, and top similarity — no raw query text.
        """
        if not query or not query.strip():
            return []
        if namespaces is None or not namespaces:
            return []

        ns_list = [normalize_code(n) for n in namespaces]
        loaded = [self._loaded[n] for n in ns_list if n in self._loaded]
        if not loaded:
            return []

        t_embed0 = time.monotonic()
        if query_embedding is None:
            query_embedding = await self._embeddings.embed_query(query)
        embed_ms = (time.monotonic() - t_embed0) * 1000.0

        dims = {b.manifest.embedding_dim for b in loaded}
        if len(dims) > 1:
            raise BundleIntegrityError(
                f"loaded bundles declare mixed embedding_dim values {dims}; "
                "a single query embedding cannot search all of them",
                next_action="split search across compatible namespaces, or rebuild to a shared model",
            )

        expected_dim = loaded[0].manifest.embedding_dim
        vec = _validate_query_embedding(query_embedding, expected_dim=expected_dim)

        t_search0 = time.monotonic()
        results = await asyncio.gather(*(asyncio.to_thread(_search_one_bundle, b, vec, limit) for b in loaded))
        search_ms = (time.monotonic() - t_search0) * 1000.0

        merged: builtins.list[SeriesMatch] = []
        for result in results:
            merged.extend(result)
        merged.sort(key=lambda m: -m.similarity)
        final = merged[:limit]

        logger.info(
            "catalog.search model=%s revision=%s dim=%d namespaces=%s "
            "embed_ms=%.1f search_ms=%.1f top_similarity=%.4f results=%d",
            loaded[0].manifest.embedding_model,
            loaded[0].manifest.embedding_model_revision,
            expected_dim,
            ",".join(ns_list),
            embed_ms,
            search_ms,
            final[0].similarity if final else 0.0,
            len(final),
        )
        return final

    # ------------------------------------------------------------------
    # Remote loading: single-flight for both try_load_remote and refresh
    # ------------------------------------------------------------------

    async def try_load_remote(self, namespace: str) -> bool:
        """Download and load a namespace bundle; return True on success.

        Concurrent callers share one future per namespace. Returns False when
        the HF repo does not exist (legitimate "no bundle published"); other
        failures raise.
        """
        ns = normalize_code(namespace)
        if ns in self._loaded:
            return True
        loaded = await self._load_via_single_flight(ns, force=False)
        return loaded is not None

    async def refresh(self, namespace: str) -> dict[str, Any]:
        """Force a freshness re-check and reload if the remote revision changed.

        Routes through the same single-flight map so concurrent refresh +
        try_load_remote share one download. Returns a dict with
        ``namespace``, ``updated``, ``old_revision``, ``new_revision``.
        """
        ns = normalize_code(namespace)
        old = self._loaded.get(ns)
        old_revision = old.revision if old is not None else None

        new = await self._load_via_single_flight(ns, force=True)
        if new is None:
            raise BundleNotFoundError(
                f"refresh failed: no bundle published for {ns!r}",
                namespace=ns,
            )

        updated = old_revision != new.revision
        logger.info(
            "catalog.refresh namespace=%s old_revision=%s new_revision=%s updated=%s",
            ns,
            old_revision,
            new.revision,
            updated,
        )
        return {
            "namespace": ns,
            "updated": updated,
            "old_revision": old_revision,
            "new_revision": new.revision,
        }

    async def _load_via_single_flight(self, ns: str, *, force: bool) -> LoadedNamespace | None:
        """Core single-flight: N concurrent callers → one physical download.

        ``force=True`` (used by refresh) always hits the network even if a
        cached revision matches the pin.
        """
        async with self._lock:
            fut = self._load_futures.get(ns)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._load_futures[ns] = fut
                launch = True
            else:
                launch = False

        if launch:
            try:
                loaded = await self._load_one(ns, force=force)
                async with self._lock:
                    self._insert_loaded(ns, loaded)
                fut.set_result(loaded)
                logger.info(
                    "catalog.bundle_loaded namespace=%s revision=%s entries=%d",
                    ns,
                    loaded.revision,
                    loaded.manifest.entry_count,
                )
                return loaded
            except BundleNotFoundError:
                fut.set_result(None)
                return None
            except Exception as exc:
                fut.set_exception(exc)
                fut.exception()  # mark retrieved for launcher
                raise
            finally:
                async with self._lock:
                    self._load_futures.pop(ns, None)
        else:
            return await fut

    async def _load_one(self, namespace: str, *, force: bool) -> LoadedNamespace:
        """Resolve target revision, ensure it's on disk, load it.

        Inline freshness policy (previously ``_FreshnessChecker``):

        - pin set + force=False + cached matches → use cache, no network.
        - pin set + no match → target=pin, download (hard fail on
          unavailability).
        - no pin → HEAD; on success use cache if it matches else download;
          on failure fall back to cache with WARN, else raise.
        """
        repo_id = self._repo_id(namespace)
        cached_rev = _scan_cached_revision(self._cache_base, namespace)

        if self._pin is not None:
            if not force and cached_rev == self._pin:
                bundle_dir = _bundle_dir(self._cache_base, namespace, self._pin)
                return _load_from_dir(
                    bundle_dir=bundle_dir,
                    namespace=namespace,
                    revision=self._pin,
                    provider=self._embeddings,
                )
            target_revision = self._pin
        else:
            remote_rev = await _head_check(repo_id=repo_id, timeout_s=_MODEL_INFO_TIMEOUT_S)
            if remote_rev is not None:
                if not force and cached_rev == remote_rev:
                    bundle_dir = _bundle_dir(self._cache_base, namespace, remote_rev)
                    return _load_from_dir(
                        bundle_dir=bundle_dir,
                        namespace=namespace,
                        revision=remote_rev,
                        provider=self._embeddings,
                    )
                target_revision = remote_rev
            elif cached_rev is not None:
                logger.warning(
                    "catalog.freshness_check_failed namespace=%s cached_revision=%s",
                    namespace,
                    cached_rev,
                )
                bundle_dir = _bundle_dir(self._cache_base, namespace, cached_rev)
                return _load_from_dir(
                    bundle_dir=bundle_dir,
                    namespace=namespace,
                    revision=cached_rev,
                    provider=self._embeddings,
                )
            else:
                raise BundleIntegrityError(
                    "HuggingFace Hub is unreachable and no local cache is available",
                    namespace=namespace,
                    resource=repo_id,
                    next_action="check network, or set PARSIMONY_CATALOG_PIN to a cached revision",
                )

        try:
            bundle_dir = await _download_snapshot(
                repo_id=repo_id,
                revision=target_revision,
                cache_base=self._cache_base,
                namespace=namespace,
            )
        except BundleNotFoundError:
            raise
        except Exception as exc:
            if self._pin is not None and cached_rev != self._pin:
                raise BundleIntegrityError(
                    f"Pinned revision {self._pin!r} not available and download failed: {exc}",
                    namespace=namespace,
                    resource=repo_id,
                    next_action="unset PARSIMONY_CATALOG_PIN or restore network connectivity",
                ) from exc
            raise

        loaded = _load_from_dir(
            bundle_dir=bundle_dir,
            namespace=namespace,
            revision=target_revision,
            provider=self._embeddings,
        )
        # One cached revision per namespace: clean up siblings.
        _cleanup_old_revisions(self._cache_base, namespace, keep=target_revision)
        return loaded


# ---------------------------------------------------------------------------
# Freshness HEAD check
# ---------------------------------------------------------------------------


async def _head_check(*, repo_id: str, timeout_s: float) -> str | None:
    """Return the remote commit SHA for ``repo_id``, or ``None`` on any failure."""
    from huggingface_hub import HfApi
    from huggingface_hub.utils import HfHubHTTPError

    def _call() -> str | None:
        try:
            api = HfApi()
            info = api.repo_info(repo_id=repo_id, repo_type="dataset", files_metadata=False)
        except HfHubHTTPError as exc:
            logger.debug("head_check http_error repo=%s exc=%s", repo_id, exc)
            return None
        except Exception as exc:
            logger.debug("head_check error repo=%s exc=%s", repo_id, exc)
            return None
        sha = getattr(info, "sha", None)
        if not isinstance(sha, str) or not _SHA40_RE.match(sha):
            logger.debug("head_check invalid_sha repo=%s sha=%r", repo_id, sha)
            return None
        return sha

    try:
        async with asyncio.timeout(timeout_s):
            return await asyncio.to_thread(_call)
    except TimeoutError:
        logger.debug("head_check timeout repo=%s", repo_id)
        return None


# ---------------------------------------------------------------------------
# Cache layout: cache_base/<ns>/<sha>/{manifest,entries.parquet,index.faiss}
# ---------------------------------------------------------------------------


def _bundle_dir(cache_base: Path, namespace: str, revision: str) -> Path:
    return cache_base / namespace / revision


def _bundle_files_present(bundle_dir: Path) -> bool:
    return all((bundle_dir / name).exists() for name in BUNDLE_FILENAMES)


def _scan_cached_revision(cache_base: Path, namespace: str) -> str | None:
    """Return the cached revision for a namespace, or None.

    Scans ``cache_base/<namespace>/`` for a subdirectory whose name matches
    a 40-char SHA and whose three bundle files are present. If multiple
    match, picks the most recently modified — siblings get cleaned up on the
    next successful load.
    """
    ns_dir = cache_base / namespace
    if not ns_dir.is_dir():
        return None
    candidates: list[tuple[float, str]] = []
    for child in ns_dir.iterdir():
        if not child.is_dir() or not _SHA40_RE.match(child.name):
            continue
        if not _bundle_files_present(child):
            continue
        candidates.append((child.stat().st_mtime, child.name))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _cleanup_old_revisions(cache_base: Path, namespace: str, *, keep: str) -> None:
    """Remove sibling revision directories, keeping only ``keep``."""
    ns_dir = cache_base / namespace
    if not ns_dir.is_dir():
        return
    for child in ns_dir.iterdir():
        if not child.is_dir() or child.name == keep:
            continue
        if not _SHA40_RE.match(child.name):
            continue
        with contextlib.suppress(OSError):
            shutil.rmtree(child)


# ---------------------------------------------------------------------------
# Download + integrity
# ---------------------------------------------------------------------------


async def _download_snapshot(
    *,
    repo_id: str,
    revision: str,
    cache_base: Path,
    namespace: str,
) -> Path:
    """Download the three bundle files into ``cache_base/<namespace>/<revision>/``."""
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import (
        EntryNotFoundError,
        RepositoryNotFoundError,
        RevisionNotFoundError,
    )

    target_dir = _bundle_dir(cache_base, namespace, revision)
    target_dir.mkdir(parents=True, exist_ok=True)

    def _call() -> str:
        # Anonymous access (token=False) and strict filename allowlist — the
        # only three files ever land on disk.
        return snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            revision=revision,
            local_dir=str(target_dir),
            allow_patterns=list(BUNDLE_FILENAMES),
            token=False,
        )

    t0 = time.monotonic()
    try:
        async with asyncio.timeout(_SNAPSHOT_TIMEOUT_S):
            downloaded = await asyncio.to_thread(_call)
    except TimeoutError as exc:
        raise BundleIntegrityError(
            f"Snapshot download exceeded {_SNAPSHOT_TIMEOUT_S}s",
            namespace=namespace,
            resource=repo_id,
        ) from exc
    except RepositoryNotFoundError as exc:
        raise BundleNotFoundError(
            f"No HF repository {repo_id!r}",
            namespace=namespace,
            resource=repo_id,
        ) from exc
    except RevisionNotFoundError as exc:
        raise BundleNotFoundError(
            f"HF revision {revision!r} not found in {repo_id!r}",
            namespace=namespace,
            resource=f"{repo_id}@{revision}",
        ) from exc
    except EntryNotFoundError as exc:
        raise BundleNotFoundError(
            f"Expected file missing from HF repo {repo_id!r}: {exc}",
            namespace=namespace,
            resource=repo_id,
        ) from exc
    except Exception as exc:
        raise BundleIntegrityError(
            f"snapshot_download failed for {repo_id!r}@{revision}: {exc}",
            namespace=namespace,
            resource=repo_id,
        ) from exc

    elapsed = time.monotonic() - t0
    logger.info(
        "catalog.snapshot_downloaded namespace=%s revision=%s duration_s=%.2f path=%s",
        namespace,
        revision,
        elapsed,
        downloaded,
    )

    # Path confinement: every file must resolve under cache_base.
    cache_base_resolved = cache_base.resolve()
    for name in BUNDLE_FILENAMES:
        path = (target_dir / name).resolve()
        try:
            path.relative_to(cache_base_resolved)
        except ValueError as exc:
            raise BundleIntegrityError(
                f"Downloaded file {name!r} resolved outside cache dir: {path}",
                namespace=namespace,
                resource=str(path),
            ) from exc
        if not path.is_file():
            raise BundleIntegrityError(
                f"Required file {name!r} missing from snapshot",
                namespace=namespace,
                resource=repo_id,
            )

    # Reject any extra files.
    extras = [p.name for p in target_dir.iterdir() if p.is_file() and p.name not in BUNDLE_FILENAMES]
    if extras:
        raise BundleIntegrityError(
            f"Unexpected files in snapshot: {extras}",
            namespace=namespace,
            resource=repo_id,
        )

    return target_dir


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
        raise BundleIntegrityError(
            f"manifest.json failed validation: {exc}",
            namespace=namespace,
            resource=str(manifest_path),
        ) from exc

    if manifest.namespace != namespace:
        raise BundleIntegrityError(
            f"manifest.namespace={manifest.namespace!r} doesn't match expected {namespace!r}",
            namespace=namespace,
            resource=str(manifest_path),
        )

    actual_entries_sha = _sha256_file(entries_path)
    if actual_entries_sha != manifest.entries_sha256:
        raise BundleIntegrityError(
            f"entries.parquet sha256={actual_entries_sha} does not match manifest={manifest.entries_sha256}",
            namespace=namespace,
            resource=str(entries_path),
        )
    actual_index_sha = _sha256_file(index_path)
    if actual_index_sha != manifest.index_sha256:
        raise BundleIntegrityError(
            f"index.faiss sha256={actual_index_sha} does not match manifest={manifest.index_sha256}",
            namespace=namespace,
            resource=str(index_path),
        )

    provider_model = getattr(provider, "model_id", None)
    provider_rev = getattr(provider, "revision", None)
    if provider_model is not None and provider_model != manifest.embedding_model:
        raise BundleIntegrityError(
            f"Provider model_id={provider_model!r} does not match bundle {manifest.embedding_model!r}",
            namespace=namespace,
            resource=str(manifest_path),
            next_action="construct the Catalog with an EmbeddingProvider matching the bundle",
        )
    if provider_rev is not None and provider_rev != manifest.embedding_model_revision:
        raise BundleIntegrityError(
            f"Provider revision={provider_rev!r} does not match bundle {manifest.embedding_model_revision!r}",
            namespace=namespace,
            resource=str(manifest_path),
        )
    if provider.dimension != manifest.embedding_dim:
        raise BundleIntegrityError(
            f"Provider dimension={provider.dimension} does not match manifest embedding_dim={manifest.embedding_dim}",
            namespace=namespace,
        )

    table = pq.read_table(entries_path, memory_map=True)
    if table.num_rows != manifest.entry_count:
        raise BundleIntegrityError(
            f"Parquet row count {table.num_rows} does not match manifest entry_count={manifest.entry_count}",
            namespace=namespace,
        )

    index = _load_faiss_index(index_path, namespace=namespace)
    if index.ntotal != manifest.entry_count:
        raise BundleIntegrityError(
            f"FAISS ntotal={index.ntotal} does not match manifest entry_count={manifest.entry_count}",
            namespace=namespace,
        )
    if int(index.d) != manifest.embedding_dim:
        raise BundleIntegrityError(
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
            raise BundleIntegrityError(
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


def _load_faiss_index(path: Path, *, namespace: str) -> Any:
    try:
        import faiss
    except ImportError as exc:
        raise BundleIntegrityError(
            "faiss is not installed but a bundle requires it",
            namespace=namespace,
            next_action="install parsimony's base dependencies: pip install 'parsimony'",
        ) from exc
    try:
        return faiss.read_index(str(path))
    except Exception as exc:
        raise BundleIntegrityError(
            f"faiss.read_index failed on {path}: {exc}",
            namespace=namespace,
            resource=str(path),
        ) from exc


def _enforce_size_cap(path: Path, cap: int, namespace: str, label: str) -> None:
    size = path.stat().st_size
    if size > cap:
        raise BundleIntegrityError(
            f"{label} size {size} bytes exceeds cap {cap}",
            namespace=namespace,
            resource=str(path),
            next_action=(
                f"increase the size cap (currently {cap} bytes) if the bundle is legitimately large, "
                "or investigate the source for tampering"
            ),
        )


def _sha256_file(path: Path, *, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            buf = fh.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Query-time validation + FAISS search
# ---------------------------------------------------------------------------


def _validate_query_embedding(vec: builtins.list[float], *, expected_dim: int) -> Any:
    """Assert shape/dtype/finite/normalized before handing to FAISS."""
    import numpy as np

    if not isinstance(vec, (list, tuple)) or len(vec) != expected_dim:
        raise BundleIntegrityError(
            f"query embedding has length {len(vec) if hasattr(vec, '__len__') else '?'}, expected {expected_dim}",
        )

    arr = np.asarray(vec, dtype=np.float32).reshape(1, expected_dim)
    if not np.isfinite(arr).all():
        raise BundleIntegrityError(
            "query embedding contains NaN or inf values",
            next_action="investigate the embedding provider output",
        )
    norm = float(np.linalg.norm(arr[0]))
    if abs(norm - 1.0) > 1e-3:
        raise BundleIntegrityError(
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


__all__ = [
    "HFBundleCatalogStore",
    "LoadedNamespace",
]
