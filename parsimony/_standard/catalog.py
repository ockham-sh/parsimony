"""The standard parsimony catalog (Parquet rows + FAISS vectors + BM25 text).

In-memory representation:

* ``self._entries``: list of :class:`SeriesEntry`, ordered. The list index is
  the FAISS row id and the BM25 doc id.
* ``self._faiss``: a FAISS index (Flat IP for small catalogs, HNSW above
  :data:`HNSW_THRESHOLD` rows).
* ``self._bm25``: a BM25Okapi over tokenized embedding text.

All three structures are kept consistent on every :meth:`upsert` /
:meth:`delete`. They are saved to disk by :meth:`save` (atomic via a temp
directory rename) and reconstructed by :meth:`load`.
"""

from __future__ import annotations

import asyncio
import builtins
import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony._standard.embedder import EmbeddingProvider, SentenceTransformerEmbedder
from parsimony._standard.indexes import (
    bm25_query,
    build_faiss,
    faiss_query,
    read_faiss,
    rrf_fuse,
    tokenize,
    write_faiss,
)
from parsimony._standard.meta import (
    ENTRIES_FILENAME,
    INDEX_FILENAME,
    META_FILENAME,
    BuildInfo,
    CatalogMeta,
)
from parsimony.catalog.catalog import BaseCatalog, _url_scheme
from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.catalog.models import (
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    normalize_code,
    series_match_from_entry,
)

if TYPE_CHECKING:
    import faiss
    from rank_bm25 import BM25Okapi

EMBED_BATCH = 256


class Catalog(BaseCatalog):
    """Standard catalog: Parquet rows, FAISS vectors, BM25 text, RRF fusion.

    Construct an empty catalog and :meth:`ingest` rows, then :meth:`save`
    locally or :meth:`push` to a URL. Construct a populated catalog with
    :meth:`from_url`.

    The embedder is owned by the catalog; index-time and query-time embeddings
    must come from the same model. The default
    :class:`SentenceTransformerEmbedder` is loaded lazily — instantiation is
    cheap, the model is fetched on first use.
    """

    def __init__(self, name: str, *, embedder: EmbeddingProvider | None = None) -> None:
        self.name = normalize_code(name)
        self._embedder: EmbeddingProvider = embedder if embedder is not None else SentenceTransformerEmbedder()
        self._embedder_info: EmbedderInfo | None = None  # populated lazily
        self._entries: builtins.list[SeriesEntry] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._faiss: faiss.Index | None = None
        self._bm25: BM25Okapi | None = None
        self._tokens: builtins.list[builtins.list[str]] = []
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def embedder_info(self) -> EmbedderInfo:
        """Identity of the embedder this catalog uses (built lazily)."""
        if self._embedder_info is None:
            self._embedder_info = self._embedder.info()
        return self._embedder_info

    @property
    def entries(self) -> builtins.list[SeriesEntry]:
        return list(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    # ------------------------------------------------------------------
    # BaseCatalog: persistence
    # ------------------------------------------------------------------

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        if not entries:
            return
        async with self._lock:
            entries = await self._embed_missing(entries)
            for entry in entries:
                key = (entry.namespace, entry.code)
                if key in self._key_to_idx:
                    idx = self._key_to_idx[key]
                    self._entries[idx] = entry
                    self._tokens[idx] = tokenize(entry.embedding_text())
                else:
                    self._key_to_idx[key] = len(self._entries)
                    self._entries.append(entry)
                    self._tokens.append(tokenize(entry.embedding_text()))
            self._rebuild_indices()

    async def delete(self, namespace: str, code: str) -> None:
        key = catalog_key(namespace, code)
        async with self._lock:
            idx = self._key_to_idx.pop(key, None)
            if idx is None:
                return
            del self._entries[idx]
            del self._tokens[idx]
            self._key_to_idx = {(e.namespace, e.code): i for i, e in enumerate(self._entries)}
            self._rebuild_indices()

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        idx = self._key_to_idx.get(catalog_key(namespace, code))
        return self._entries[idx] if idx is not None else None

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        return {catalog_key(ns, code) for ns, code in keys if catalog_key(ns, code) in self._key_to_idx}

    # ------------------------------------------------------------------
    # BaseCatalog: retrieval
    # ------------------------------------------------------------------

    async def list_namespaces(self) -> builtins.list[str]:
        return sorted({e.namespace for e in self._entries})

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        ns = catalog_key(namespace, "_")[0] if namespace else None
        ql = q.lower() if q else None
        filtered = [
            e
            for e in self._entries
            if (ns is None or e.namespace == ns) and (ql is None or ql in e.code.lower() or ql in e.title.lower())
        ]
        return filtered[offset : offset + limit], len(filtered)

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
    ) -> builtins.list[SeriesMatch]:
        if not self._entries:
            return []
        ns_filter = {catalog_key(ns, "_")[0] for ns in namespaces} if namespaces else None

        # Pull a generous candidate set from each retriever before fusion;
        # filter by namespace afterwards so each retriever sees the full
        # corpus and the RRF rankings stay meaningful.
        candidate_k = max(limit * 5, 50)
        bm25_ranks = self._bm25_ranks(query, candidate_k)
        vec_ranks = await self._faiss_ranks(query, candidate_k)
        fused = rrf_fuse(bm25_ranks, vec_ranks)

        out: builtins.list[SeriesMatch] = []
        for idx, score in fused:
            entry = self._entries[idx]
            if ns_filter is not None and entry.namespace not in ns_filter:
                continue
            out.append(series_match_from_entry(entry, similarity=score))
            if len(out) >= limit:
                break
        return out

    # ------------------------------------------------------------------
    # On-disk snapshot
    # ------------------------------------------------------------------

    async def save(self, path: str | Path, *, builder: str | None = None) -> None:
        """Atomically write the three-file snapshot to *path*.

        Writes to a sibling temp directory and renames into place, so a
        partially-written snapshot is never visible at *path*.
        """
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)

        info = self.embedder_info
        await asyncio.to_thread(self._write_parquet, tmp / ENTRIES_FILENAME, info)
        await asyncio.to_thread(write_faiss, self._faiss, str(tmp / INDEX_FILENAME), dim=info.dim)
        await asyncio.to_thread(self._write_meta, tmp / META_FILENAME, info, builder)

        if target.exists():
            shutil.rmtree(target)
        tmp.rename(target)

    @classmethod
    async def load(
        cls,
        path: str | Path,
        *,
        embedder: EmbeddingProvider | None = None,
    ) -> Catalog:
        """Load a snapshot from *path*.

        The snapshot's :attr:`name` is read from ``meta.json``.

        When *embedder* is omitted, an embedder matching the snapshot's
        recorded :class:`EmbedderInfo` is constructed (currently always a
        :class:`SentenceTransformerEmbedder`). When provided, its
        ``dimension`` and ``info().normalize`` must match the snapshot or
        :class:`ValueError` is raised.
        """
        src = Path(path)
        meta = read_meta(src)
        chosen = (
            embedder
            if embedder is not None
            else SentenceTransformerEmbedder(
                model=meta.embedder.model,
                normalize=meta.embedder.normalize,
            )
        )
        if chosen.dimension != meta.embedder.dim:
            raise ValueError(
                f"Embedder dimension {chosen.dimension} does not match meta.embedder.dim "
                f"{meta.embedder.dim} for catalog at {src}"
            )

        catalog = cls(meta.name, embedder=chosen)
        catalog._embedder_info = meta.embedder
        entries, embeddings = await asyncio.to_thread(_read_parquet, src / ENTRIES_FILENAME)
        catalog._entries = entries
        catalog._key_to_idx = {(e.namespace, e.code): i for i, e in enumerate(entries)}
        catalog._tokens = [tokenize(e.embedding_text()) for e in entries]
        catalog._faiss = await asyncio.to_thread(read_faiss, str(src / INDEX_FILENAME), expected_rows=len(entries))
        if catalog._tokens:
            from rank_bm25 import BM25Okapi

            catalog._bm25 = BM25Okapi(catalog._tokens)
        return catalog

    # ------------------------------------------------------------------
    # URL-based distribution
    # ------------------------------------------------------------------

    @classmethod
    async def from_url(cls, url: str) -> Catalog:
        """Load from *url*. Schemes: ``file://``, ``hf://`` (``s3://`` planned)."""
        from parsimony._standard.sources import load_from_url

        return await load_from_url(url)

    async def push(self, url: str) -> None:
        """Publish to *url*. Schemes: ``file://``, ``hf://`` (``s3://`` planned)."""
        from parsimony._standard.sources import push_to_url

        await push_to_url(self, url)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _embed_missing(self, entries: builtins.list[SeriesEntry]) -> builtins.list[SeriesEntry]:
        missing = [(i, e) for i, e in enumerate(entries) if e.embedding is None]
        if not missing:
            return entries
        out = list(entries)
        for start in range(0, len(missing), EMBED_BATCH):
            chunk = missing[start : start + EMBED_BATCH]
            texts = [e.embedding_text() for _, e in chunk]
            vectors = await self._embedder.embed_texts(texts)
            for (i, entry), vec in zip(chunk, vectors, strict=True):
                out[i] = entry.model_copy(update={"embedding": list(vec)})
        return out

    def _rebuild_indices(self) -> None:
        if not self._entries:
            self._faiss = None
            self._bm25 = None
            return
        info = self.embedder_info
        matrix = np.asarray([e.embedding for e in self._entries], dtype=np.float32)
        self._faiss = build_faiss(matrix, dim=info.dim, normalize=info.normalize)
        from rank_bm25 import BM25Okapi

        self._bm25 = BM25Okapi(self._tokens)

    def _bm25_ranks(self, query: str, k: int) -> builtins.list[tuple[int, int]]:
        if self._bm25 is None:
            return []
        return bm25_query(self._bm25, query, k=k)

    async def _faiss_ranks(self, query: str, k: int) -> builtins.list[tuple[int, int]]:
        if self._faiss is None or self._faiss.ntotal == 0:
            return []
        info = self.embedder_info
        query_vec = await self._embedder.embed_query(query)
        return faiss_query(self._faiss, query_vec, k=k, normalize=info.normalize)

    def _write_parquet(self, target: Path, info: EmbedderInfo) -> None:
        if not self._entries:
            schema = pa.schema(
                [
                    ("namespace", pa.string()),
                    ("code", pa.string()),
                    ("title", pa.string()),
                    ("description", pa.string()),
                    ("tags_json", pa.string()),
                    ("metadata_json", pa.string()),
                    ("embedding", pa.list_(pa.float32(), info.dim)),
                ]
            )
            pq.write_table(pa.Table.from_pylist([], schema=schema), target)
            return
        rows = [
            {
                "namespace": e.namespace,
                "code": e.code,
                "title": e.title,
                "description": e.description,
                "tags_json": json.dumps(e.tags),
                "metadata_json": json.dumps(e.metadata),
                "embedding": e.embedding,
            }
            for e in self._entries
        ]
        pq.write_table(pa.Table.from_pylist(rows), target, compression="zstd")

    def _write_meta(self, target: Path, info: EmbedderInfo, builder: str | None) -> None:
        meta = CatalogMeta(
            name=self.name,
            namespaces=sorted({e.namespace for e in self._entries}),
            entry_count=len(self._entries),
            embedder=info,
            build=BuildInfo(builder=builder),
        )
        target.write_text(meta.model_dump_json(indent=2))


# ----------------------------------------------------------------------
# Standalone helpers
# ----------------------------------------------------------------------


def read_meta(path: str | Path) -> CatalogMeta:
    """Read ``meta.json`` from *path* (the catalog directory)."""
    return CatalogMeta.model_validate_json((Path(path) / META_FILENAME).read_text())


def _read_parquet(target: Path) -> tuple[list[SeriesEntry], np.ndarray]:
    table = pq.read_table(target)
    rows = table.to_pylist()
    entries = [
        SeriesEntry(
            namespace=row["namespace"],
            code=row["code"],
            title=row["title"],
            description=row.get("description"),
            tags=json.loads(row["tags_json"]) if row.get("tags_json") else [],
            metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
            embedding=list(row["embedding"]) if row.get("embedding") is not None else None,
        )
        for row in rows
    ]
    embeddings = (
        np.asarray([e.embedding for e in entries], dtype=np.float32) if entries else np.zeros((0, 0), dtype=np.float32)
    )
    return entries, embeddings


__all__ = ["Catalog", "read_meta", "_url_scheme"]
