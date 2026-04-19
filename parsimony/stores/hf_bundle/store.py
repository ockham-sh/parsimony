"""HFBundleCatalogStore: read-only catalog store backed by HF Hub bundles.

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

Internal helpers live in sibling modules and are imported with leading
underscores because they are package-internal, not part of the public
API: :mod:`.cache_layout`, :mod:`.hf_download`, :mod:`.integrity`.
"""

from __future__ import annotations

import asyncio
import builtins
import logging
import os
import time
from pathlib import Path
from typing import Any

import platformdirs
import pyarrow as pa

from parsimony.bundles.errors import BundleError, BundleNotFoundError
from parsimony.bundles.format import hf_repo_id
from parsimony.catalog.arrow_adapters import arrow_rows_to_entries
from parsimony.catalog.models import (
    EmbeddingProvider,
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    normalize_code,
)
from parsimony.stores.catalog_store import CatalogStore
from parsimony.stores.hf_bundle.cache_layout import (
    _SHA40_RE,
    _bundle_dir,
    _cleanup_old_revisions,
    _scan_cached_revision,
)
from parsimony.stores.hf_bundle.hf_download import (
    _MODEL_INFO_TIMEOUT_S,
    _download_snapshot,
    _head_check,
)
from parsimony.stores.hf_bundle.integrity import (
    LoadedNamespace,
    _load_from_dir,
    _search_one_bundle,
    _validate_query_embedding,
)

logger = logging.getLogger(__name__)


def _default_cache_dir() -> Path:
    override = os.environ.get("PARSIMONY_CACHE_DIR")
    if override:
        return Path(override)
    return Path(platformdirs.user_cache_dir("parsimony")) / "bundles"


def _validate_pin(pin: str) -> str:
    """Reject pins that aren't full 40-char commit SHAs.

    Branches and tags are mutable pointers; resolving ``pin="main"`` would
    silently drift to whatever HEAD is today. The store rejects everything
    but a content-addressable SHA so the bundle's integrity guarantee holds
    regardless of where the pin came from (env, CLI flag, programmatic
    constructor).
    """
    if not _SHA40_RE.match(pin):
        raise ValueError(
            f"pin={pin!r} is not a valid 40-char commit SHA; "
            "reject pins that aren't full SHAs to prevent tag-pointer drift"
        )
    return pin


def _resolve_pin() -> str | None:
    pin = os.environ.get("PARSIMONY_CATALOG_PIN", "").strip()
    if not pin:
        return None
    return _validate_pin(pin)


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

        if pin is not None:
            self._pin: str | None = _validate_pin(pin)
        else:
            self._pin = _resolve_pin()
        self._repo_id = repo_id_for_namespace or hf_repo_id

        self._loaded: dict[str, LoadedNamespace] = {}

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
        """Operator-facing snapshot of store state."""
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
        """Paginate entries across one or all loaded namespaces."""
        import pyarrow.compute as pc

        namespaces = [normalize_code(namespace)] if namespace is not None else sorted(self._loaded.keys())
        needle = q.strip().lower() if q and q.strip() else None

        per_ns: builtins.list[tuple[str, pa.Table]] = []
        total = 0
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
            sort_indices = pc.sort_indices(filtered.column("code"))
            sorted_tbl = filtered.take(sort_indices)
            per_ns.append((ns, sorted_tbl))
            total += sorted_tbl.num_rows

        if not per_ns or limit <= 0 or offset >= total:
            return [], total

        out: builtins.list[SeriesEntry] = []
        remaining = limit
        skip = offset
        for ns, sorted_tbl in per_ns:
            n = sorted_tbl.num_rows
            if skip >= n:
                skip -= n
                continue
            take_n = min(remaining, n - skip)
            window = sorted_tbl.slice(skip, take_n)
            skip = 0
            entries = arrow_rows_to_entries(window, namespace=ns)
            out.extend(entries)
            remaining -= take_n
            if remaining <= 0:
                break
        return out, total

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
        query_embedding: builtins.list[float] | None = None,
    ) -> builtins.list[SeriesMatch]:
        """Vector search over loaded bundles."""
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
            raise BundleError(
                f"loaded bundles declare mixed embedding_dim values {dims}; "
                "a single query embedding cannot search all of them",
                next_action="split search across compatible namespaces, or rebuild to a shared model",
            )

        expected_dim = loaded[0].manifest.embedding_dim
        vec = _validate_query_embedding(query_embedding, expected_dim=expected_dim)

        t_search0 = time.monotonic()
        results = await asyncio.gather(
            *(asyncio.to_thread(_search_one_bundle, b, vec, limit) for b in loaded)
        )
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
    # Remote loading
    # ------------------------------------------------------------------

    async def try_load_remote(self, namespace: str) -> bool:
        """Download and load a namespace bundle; return True on success.

        Returns False when the HF repo does not exist (legitimate "no bundle
        published"); other failures raise.
        """
        ns = normalize_code(namespace)
        if ns in self._loaded:
            return True
        try:
            loaded = await self._load_one(ns, force=False)
        except BundleNotFoundError:
            return False
        self._loaded[ns] = loaded
        logger.info(
            "catalog.bundle_loaded namespace=%s revision=%s entries=%d",
            ns,
            loaded.revision,
            loaded.manifest.entry_count,
        )
        return True

    def get_embedding_identity(self, namespace: str) -> tuple[str, str, int] | None:
        """Return ``(model, revision, dim)`` from the loaded bundle's manifest."""
        loaded = self._loaded.get(normalize_code(namespace))
        if loaded is None:
            return None
        return (
            loaded.manifest.embedding_model,
            loaded.manifest.embedding_model_revision,
            loaded.manifest.embedding_dim,
        )

    async def refresh(self, namespace: str) -> dict[str, Any]:
        """Force a freshness re-check and reload if the remote revision changed."""
        ns = normalize_code(namespace)
        old = self._loaded.get(ns)
        old_revision = old.revision if old is not None else None

        new = await self._load_one(ns, force=True)
        self._loaded[ns] = new

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

    async def _load_one(self, namespace: str, *, force: bool) -> LoadedNamespace:
        """Resolve target revision, ensure it's on disk, load it.

        Inline freshness policy:

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
                return await asyncio.to_thread(
                    _load_from_dir,
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
                    return await asyncio.to_thread(
                        _load_from_dir,
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
                return await asyncio.to_thread(
                    _load_from_dir,
                    bundle_dir=bundle_dir,
                    namespace=namespace,
                    revision=cached_rev,
                    provider=self._embeddings,
                )
            else:
                raise BundleError(
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
                raise BundleError(
                    f"Pinned revision {self._pin!r} not available and download failed: {exc}",
                    namespace=namespace,
                    resource=repo_id,
                    next_action="unset PARSIMONY_CATALOG_PIN or restore network connectivity",
                ) from exc
            raise

        loaded = await asyncio.to_thread(
            _load_from_dir,
            bundle_dir=bundle_dir,
            namespace=namespace,
            revision=target_revision,
            provider=self._embeddings,
        )
        # One cached revision per namespace: clean up siblings.
        _cleanup_old_revisions(self._cache_base, namespace, keep=target_revision)
        return loaded


__all__ = [
    "HFBundleCatalogStore",
    "LoadedNamespace",
]
