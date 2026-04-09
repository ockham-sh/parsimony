"""SeriesCatalog: search and bulk indexing over a catalog store."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from ockham.catalog.embeddings import EmbeddingProvider
from ockham.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
    normalize_entity_code,
)
from ockham.catalog.series_pipeline import build_embedding_text
from ockham.catalog.store import CatalogStore
from ockham.result import ColumnRole, SemanticTableResult

logger = logging.getLogger(__name__)


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
    """Build :class:`SeriesEntry` rows from a :class:`SemanticTableResult` (catalog-local extraction).

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


class SeriesCatalog:
    """Catalog service: search and bulk indexing.

    Composes a :class:`CatalogStore` and optional :class:`EmbeddingProvider` for
    vector embeddings on ingest. :meth:`search` delegates to the store (implementation-defined).
    Bulk loading uses :meth:`ingest` with a flat list of :class:`SeriesEntry` rows.
    """

    def __init__(
        self,
        store: CatalogStore,
        *,
        embeddings: EmbeddingProvider | None = None,
    ) -> None:
        self._store = store
        self._embeddings = embeddings

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
        """Extract catalog rows from *table* and :meth:`ingest` them.

        The catalog namespace is taken from the KEY column's ``namespace=...`` in
        :attr:`SemanticTableResult.output_schema`. Use ``conn.with_callback(catalog.index_result)``
        for auto-indexing after fetch.

        With ``dry_run=True``, counts mirror live :meth:`ingest` dedupe (``indexed`` /
        ``skipped``) using :meth:`~ockham.catalog.store.CatalogStore.exists`;
        no rows are written and embeddings are not computed.

        With ``force=True``, dedupe is skipped: every extracted row is counted as
        ``indexed`` and passed to the store upsert (updates existing keys).
        """
        entries = _entries_from_table_result(table, extra_tags=extra_tags)
        if dry_run:
            return await self._preview_ingest(
                entries, batch_size=batch_size, force=force
            )
        return await self.ingest(
            entries, embed=embed, batch_size=batch_size, force=force
        )

    async def _preview_ingest(
        self,
        entries: list[SeriesEntry],
        *,
        batch_size: int,
        force: bool = False,
    ) -> IndexResult:
        """Count rows that would be inserted vs skipped, mirroring :meth:`ingest` dedupe logic.

        No store writes and no embedding calls.
        """
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

    async def search(
        self,
        query: str,
        limit: int = 10,
        *,
        namespaces: list[str] | None = None,
    ) -> list[SeriesMatch]:
        """Search the catalog; behavior depends on :class:`CatalogStore` implementation."""
        return await self._store.search(query, limit, namespaces=namespaces)

    async def list_namespaces(self) -> list[str]:
        return await self._store.list_namespaces()

    async def ingest(
        self,
        entries: list[SeriesEntry],
        *,
        embed: bool = True,
        batch_size: int = 100,
        force: bool = False,
    ) -> IndexResult:
        """Dedupe, optionally embed, and upsert entries in batches of ``batch_size``.

        When ``force`` is true, skip existence checks and upsert every entry (overwrites).
        """
        if embed and self._embeddings is None:
            raise RuntimeError(
                "ingest(embed=True) requires an EmbeddingProvider; "
                "pass embeddings=... to SeriesCatalog, or use embed=False."
            )
        result = IndexResult()
        result.total = len(entries)
        if not entries:
            return result

        # Process in chunks for embedding batching and store throughput
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

    async def codes_missing_embedding(
        self,
        limit: int | None,
        *,
        only_keys: list[tuple[str, str]] | None = None,
    ) -> list[tuple[str, str]]:
        return await self._store.list_codes_missing_embedding(
            limit, only_keys=only_keys
        )

    async def embed_pending(
        self,
        limit: int | None = None,
        *,
        only_keys: list[tuple[str, str]] | None = None,
    ) -> int:
        if self._embeddings is None:
            raise RuntimeError(
                "embed_pending requires an EmbeddingProvider; "
                "pass embeddings=... to SeriesCatalog."
            )
        keys = await self._store.list_codes_missing_embedding(
            limit, only_keys=only_keys
        )
        if not keys:
            return 0
        rows: list[SeriesEntry] = []
        for ns, c in keys:
            row = await self._store.get(ns, c)
            if row is None or row.embedding is not None:
                continue
            rows.append(row)
        if not rows:
            return 0
        embedded = await _embed_batch(self._embeddings, rows)
        updates: list[tuple[tuple[str, str], list[float]]] = []
        for e in embedded:
            vec = e.embedding
            if vec is None:
                raise ValueError("embed_pending: _embed_batch must set embedding on each entry")
            updates.append(((e.namespace, e.code), vec))
        await self._store.update_embeddings(updates)
        return len(updates)
