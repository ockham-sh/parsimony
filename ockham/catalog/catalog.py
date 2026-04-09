"""Catalog: search, bulk indexing, optional embeddings, and lazy namespace population."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from ockham.catalog.models import (
    EmbeddingProvider,
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
    normalize_entity_code,
)
from ockham.catalog.store import CatalogStore
from ockham.result import ColumnRole, SemanticTableResult

logger = logging.getLogger(__name__)

_DEFAULT_DB_DIR = Path.home() / ".ockham"
_HF_ORG = "Ockham-sh"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_embedding_text(entry: SeriesEntry) -> str:
    """Compose text embedded for semantic search."""
    parts = [entry.title]
    if entry.metadata:
        meta_parts = [f"{k}: {v}" for k, v in entry.metadata.items() if v is not None]
        if meta_parts:
            parts.append(", ".join(meta_parts))
    if entry.tags:
        parts.append(f"tags: {', '.join(entry.tags)}")
    return " | ".join(parts)


async def _embed_batch(
    embeddings: EmbeddingProvider,
    entries: list[SeriesEntry],
) -> list[SeriesEntry]:
    """Embed catalog entries; raises on batch size or dimension mismatch."""
    texts = [build_embedding_text(e) for e in entries]
    vectors = await embeddings.embed_texts(texts)
    if len(vectors) != len(entries):
        raise ValueError("Embedding batch size mismatch")
    out: list[SeriesEntry] = []
    for entry, vec in zip(entries, vectors):
        if len(vec) != embeddings.dimension:
            raise ValueError(
                f"Embedding dimension {len(vec)} != expected {embeddings.dimension}"
            )
        out.append(entry.model_copy(update={"embedding": vec}))
    return out


def _entries_from_table_result(
    table: SemanticTableResult,
    *,
    extra_tags: list[str] | None = None,
) -> list[SeriesEntry]:
    """Build :class:`SeriesEntry` rows from a :class:`SemanticTableResult`.

    Namespace is read from the KEY column's :attr:`~ockham.result.Column.namespace`.
    """
    if not isinstance(table.data, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"indexing expected tabular data, got {type(table.data).__name__}"
        )
    df = table.df
    if df.empty:
        return []

    cols = table.output_schema.columns
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(
            "SemanticTableResult must have exactly one KEY column in output_schema, "
            f"found {len(key_cols)}"
        )
    key_col = key_cols[0]
    if not key_col.namespace:
        raise ValueError(
            "KEY column must declare namespace=... on the schema for catalog indexing"
        )
    key_name = key_col.name
    if key_name not in df.columns:
        raise ValueError(
            f"SemanticTableResult missing KEY column {key_name!r}. Available: {list(df.columns)}"
        )

    title_cols = [c for c in cols if c.role == ColumnRole.TITLE]
    title_name = title_cols[0].name if len(title_cols) == 1 else None
    if title_name is not None and title_name not in df.columns:
        raise ValueError(
            f"SemanticTableResult missing TITLE column {title_name!r}. "
            f"Available: {list(df.columns)}"
        )

    meta_names = [c.name for c in cols if c.role == ColumnRole.METADATA]
    for mn in meta_names:
        if mn not in df.columns:
            raise ValueError(
                f"SemanticTableResult missing METADATA column {mn!r}. Available: {list(df.columns)}"
            )

    raw_codes = df[key_name].dropna().unique()
    ns = normalize_code(key_col.namespace)
    tag_list = [table.provenance.source]
    if extra_tags:
        tag_list.extend(extra_tags)

    entries: list[SeriesEntry] = []
    for raw_code in raw_codes:
        code = normalize_entity_code(str(raw_code))
        mask = df[key_name] == raw_code
        sub = df.loc[mask]

        if title_name and title_name in sub.columns:
            titles = sub[title_name].dropna()
            title = str(titles.iloc[0]) if len(titles) > 0 else code
        else:
            title = code

        meta: dict[str, Any] = {}
        for mn in meta_names:
            vals = sub[mn].dropna()
            if len(vals) > 0:
                v = vals.iloc[0]
                meta[mn] = v.item() if hasattr(v, "item") else v

        entries.append(
            SeriesEntry(
                namespace=ns,
                code=code,
                title=title,
                tags=tag_list,
                metadata=meta,
            )
        )
    return entries


# ---------------------------------------------------------------------------
# Lazy namespace helpers
# ---------------------------------------------------------------------------


def _find_enumerator(connectors: object, namespace: str) -> object | None:
    """Find an ``@enumerator`` whose KEY column declares *namespace*."""
    for conn in connectors:  # type: ignore[union-attr]
        oc = conn.output_config
        if oc is None:
            continue
        roles = {c.role for c in oc.columns}
        if ColumnRole.DATA in roles:
            continue
        if ColumnRole.TITLE not in roles:
            continue
        for col in oc.columns:
            if col.role == ColumnRole.KEY and col.namespace == namespace:
                return conn
    return None


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


class Catalog:
    """Catalog service: search, bulk indexing, optional embeddings, and lazy loading.

    Composes a :class:`CatalogStore` and optional :class:`EmbeddingProvider`.

    When *connectors* is provided, :meth:`search` auto-populates requested
    namespaces from HuggingFace pre-built catalogs or live enumerators before
    querying.
    """

    def __init__(
        self,
        store: CatalogStore,
        *,
        embeddings: EmbeddingProvider | None = None,
        connectors: object | None = None,
        hf_org: str = _HF_ORG,
    ) -> None:
        self._store = store
        self._embeddings = embeddings
        self._connectors = connectors
        self._hf_org = hf_org
        self._populated: set[str] = set()

    @property
    def store(self) -> CatalogStore:
        """The underlying catalog store."""
        return self._store

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(
        self,
        query: str,
        limit: int = 10,
        *,
        namespaces: list[str] | None = None,
    ) -> list[SeriesMatch]:
        """Search the catalog.

        When connectors are configured, auto-populates requested namespaces.
        When an embedding provider is configured, embeds the query for hybrid search.
        """
        if namespaces and self._connectors is not None:
            for ns in namespaces:
                await self._ensure_namespace(ns)

        query_embedding: list[float] | None = None
        if self._embeddings is not None:
            query_embedding = await self._embeddings.embed_query(query)

        return await self._store.search(
            query, limit, namespaces=namespaces, query_embedding=query_embedding
        )

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------

    async def index_result(
        self,
        table: SemanticTableResult,
        *,
        embed: bool = True,
        batch_size: int = 100,
        extra_tags: list[str] | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> IndexResult:
        """Extract catalog rows from *table* and :meth:`ingest` them."""
        entries = _entries_from_table_result(table, extra_tags=extra_tags)
        if dry_run:
            return await self._preview_ingest(
                entries, batch_size=batch_size, force=force
            )
        return await self.ingest(
            entries, embed=embed, batch_size=batch_size, force=force
        )

    async def ingest(
        self,
        entries: list[SeriesEntry],
        *,
        embed: bool = True,
        batch_size: int = 100,
        force: bool = False,
    ) -> IndexResult:
        """Dedupe, optionally embed, and upsert entries in batches."""
        if embed and self._embeddings is None:
            raise RuntimeError(
                "ingest(embed=True) requires an EmbeddingProvider; "
                "pass embeddings=... to Catalog, or use embed=False."
            )
        result = IndexResult()
        result.total = len(entries)
        if not entries:
            return result

        for start in range(0, len(entries), batch_size):
            batch = entries[start : start + batch_size]
            if force:
                to_insert = batch
            else:
                keys = [(e.namespace, e.code) for e in batch]
                existing = await self._store.exists(keys)
                to_insert = [e for e in batch if (e.namespace, e.code) not in existing]
                result.skipped += len(batch) - len(to_insert)
            if not to_insert:
                continue
            try:
                if embed:
                    assert self._embeddings is not None
                    out = await _embed_batch(self._embeddings, to_insert)
                else:
                    out = [r.model_copy() for r in to_insert]
                await self._store.upsert(out)
                result.indexed += len(out)
            except Exception:
                logger.exception("ingest batch failed")
                result.errors += len(to_insert)
        return result

    async def _preview_ingest(
        self,
        entries: list[SeriesEntry],
        *,
        batch_size: int,
        force: bool = False,
    ) -> IndexResult:
        """Count rows that would be inserted vs skipped (no writes, no embedding)."""
        result = IndexResult()
        result.total = len(entries)
        if not entries:
            return result
        for start in range(0, len(entries), batch_size):
            batch = entries[start : start + batch_size]
            if force:
                result.indexed += len(batch)
                continue
            keys = [(e.namespace, e.code) for e in batch]
            existing = await self._store.exists(keys)
            to_insert = [e for e in batch if (e.namespace, e.code) not in existing]
            result.skipped += len(batch) - len(to_insert)
            result.indexed += len(to_insert)
        return result

    # ------------------------------------------------------------------
    # Embeddings
    # ------------------------------------------------------------------

    async def embed_pending(
        self,
        limit: int | None = None,
        *,
        namespace: str | None = None,
    ) -> int:
        """Backfill embeddings for entries that don't have them."""
        if self._embeddings is None:
            raise RuntimeError(
                "embed_pending requires an EmbeddingProvider; "
                "pass embeddings=... to Catalog."
            )
        lim = limit if limit is not None else 10_000
        entries, _ = await self._store.list(namespace=namespace, limit=lim)
        missing = [e for e in entries if e.embedding is None]
        if not missing:
            return 0
        embedded = await _embed_batch(self._embeddings, missing)
        await self._store.upsert(embedded)
        return len(embedded)

    # ------------------------------------------------------------------
    # CRUD pass-throughs
    # ------------------------------------------------------------------

    async def list_namespaces(self) -> list[str]:
        return await self._store.list_namespaces()

    async def list_entries(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[SeriesEntry], int]:
        return await self._store.list(
            namespace=namespace, q=q, limit=limit, offset=offset
        )

    async def get_entry(self, namespace: str, code: str) -> SeriesEntry | None:
        return await self._store.get(namespace, code)

    async def delete_entry(self, namespace: str, code: str) -> None:
        await self._store.delete(namespace, code)

    async def upsert_entries(self, entries: list[SeriesEntry]) -> None:
        await self._store.upsert(entries)

    # ------------------------------------------------------------------
    # Lazy namespace population
    # ------------------------------------------------------------------

    async def _ensure_namespace(self, namespace: str) -> None:
        """Populate *namespace* if not already in the local catalog."""
        if namespace in self._populated:
            return

        existing = await self._store.list_namespaces()
        if namespace in existing:
            self._populated.add(namespace)
            return

        if await self._try_hf_download(namespace):
            self._populated.add(namespace)
            return

        if await self._try_enumerate(namespace):
            self._populated.add(namespace)
            return

        self._populated.add(namespace)  # Don't retry
        logger.debug("Namespace %r not available from HF or enumerator", namespace)

    async def _try_hf_download(self, namespace: str) -> bool:
        """Download pre-built catalog.db from HuggingFace and merge."""
        try:
            from huggingface_hub import hf_hub_download
        except ImportError:
            logger.debug("huggingface_hub not installed; skipping HF download")
            return False

        repo_id = f"{self._hf_org}/{namespace}"
        try:
            import asyncio

            path = await asyncio.to_thread(
                hf_hub_download,
                repo_id=repo_id,
                filename="catalog.db",
                repo_type="dataset",
            )
            self._merge_remote_db(path)
            logger.info("Downloaded %s catalog from HF", namespace)
            return True
        except Exception as exc:
            logger.debug("HF download failed for %s: %s", repo_id, exc)
            return False

    def _merge_remote_db(self, remote_path: str) -> None:
        """ATTACH a remote SQLite catalog and INSERT its rows.

        Uses SQLiteCatalogStore internals for efficiency. Falls back to
        read + upsert for other store types.
        """
        from ockham.stores.sqlite_catalog import SQLiteCatalogStore

        if isinstance(self._store, SQLiteCatalogStore):
            conn = self._store._get_conn()
            conn.execute(f'ATTACH DATABASE "{remote_path}" AS remote')
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO series_catalog SELECT * FROM remote.series_catalog"
                )
                conn.commit()
            finally:
                conn.execute("DETACH DATABASE remote")
        else:
            logger.warning(
                "HF catalog merge not supported for %s stores",
                type(self._store).__name__,
            )

    async def _try_enumerate(self, namespace: str) -> bool:
        """Find and run the enumerator for *namespace* from the connectors."""
        if self._connectors is None:
            return False
        enumerator = _find_enumerator(self._connectors, namespace)
        if enumerator is None:
            return False

        try:
            params = enumerator.param_type() if enumerator.param_type else None  # type: ignore[union-attr]
            result = await enumerator(params)  # type: ignore[misc]
            entries = _entries_from_table_result(result)
            if not entries:
                return False
            idx = await self.ingest(entries, embed=False, force=True)
            logger.info("Enumerated %s: %d entries", namespace, idx.indexed)
            return idx.indexed > 0
        except Exception as exc:
            logger.warning("Enumerator failed for %s: %s", namespace, exc)
            return False

    async def close(self) -> None:
        """Close the underlying store if it supports closing."""
        if hasattr(self._store, "close"):
            await self._store.close()
