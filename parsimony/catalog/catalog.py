"""Catalog: search and lazy namespace population from HF bundles or enumerators.

Search flow:

1. Caller names one or more namespaces (``namespaces=...`` is required;
   implicit "search all namespaces" is rejected by design).
2. :meth:`Catalog._ensure_namespace` populates each requested namespace:
   if the store already has it → return; else ask the store to
   :meth:`~parsimony.stores.catalog_store.CatalogStore.try_load_remote`
   (HF bundle stores know how); else fall back to a live
   ``@enumerator``-decorated connector.

Catalog distribution is owned by
:class:`~parsimony.stores.hf_bundle.HFBundleCatalogStore` (HF Hub bundles).
This module no longer performs any HTTP downloads itself.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx
import numpy as np
import pandas as pd

from parsimony.catalog.models import (
    EmbeddingProvider,
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
    normalize_entity_code,
)
from parsimony.errors import ConnectorError
from parsimony.result import (
    ColumnRole,
    SemanticTableResult,
    resolve_namespace_template,
)
from parsimony.stores.catalog_store import CatalogStore

logger = logging.getLogger(__name__)


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


async def embed_entries_in_batches(
    entries: list[SeriesEntry],
    *,
    provider: EmbeddingProvider,
    batch_size: int = 64,
) -> np.ndarray:
    """Embed entries in fixed-size batches; returns ``(N, dim)`` float32 ndarray.

    Embedding text is the entry's title plus metadata/tags via
    :func:`build_embedding_text` — same function used at query time so build
    and query see the same text shape. Each batch is converted to ndarray
    immediately so the Python list-of-lists never co-exists with the entries
    list and the FAISS index.
    """
    if not entries:
        return np.empty((0, provider.dimension), dtype=np.float32)

    texts = [build_embedding_text(e) for e in entries]
    chunks: list[np.ndarray] = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        vectors = await provider.embed_texts(batch)
        if len(vectors) != len(batch):
            raise ValueError(
                f"embed_texts returned {len(vectors)} vectors for {len(batch)} texts; "
                "provider contract violation"
            )
        chunks.append(np.asarray(vectors, dtype=np.float32))

    arr = np.concatenate(chunks, axis=0)
    if arr.shape != (len(entries), provider.dimension):
        raise ValueError(
            f"embedded ndarray shape {arr.shape} does not match "
            f"(entries={len(entries)}, dim={provider.dimension})"
        )
    return arr


async def _embed_batch(
    embeddings: EmbeddingProvider,
    entries: list[SeriesEntry],
) -> list[SeriesEntry]:
    """Embed catalog entries and attach the vectors to each entry."""
    arr = await embed_entries_in_batches(entries, provider=embeddings)
    return [
        entry.model_copy(update={"embedding": arr[i].tolist()})
        for i, entry in enumerate(entries)
    ]


def entries_from_table_result(
    table: SemanticTableResult,
    *,
    extra_tags: list[str] | None = None,
) -> list[SeriesEntry]:
    """Build :class:`SeriesEntry` rows from a :class:`SemanticTableResult`.

    Namespace is read from the KEY column's :attr:`~parsimony.result.Column.namespace`.
    """
    if not isinstance(table.data, (pd.DataFrame, pd.Series)):
        raise TypeError(f"indexing expected tabular data, got {type(table.data).__name__}")
    df = table.df
    if df.empty:
        return []

    cols = table.output_schema.columns
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(
            f"SemanticTableResult must have exactly one KEY column in output_schema, found {len(key_cols)}"
        )
    key_col = key_cols[0]
    if not key_col.namespace:
        raise ValueError("KEY column must declare namespace=... on the schema for catalog indexing")
    key_name = key_col.name
    if key_name not in df.columns:
        raise ValueError(f"SemanticTableResult missing KEY column {key_name!r}. Available: {list(df.columns)}")

    title_cols = [c for c in cols if c.role == ColumnRole.TITLE]
    title_name = title_cols[0].name if len(title_cols) == 1 else None
    if title_name is not None and title_name not in df.columns:
        raise ValueError(f"SemanticTableResult missing TITLE column {title_name!r}. Available: {list(df.columns)}")

    meta_names = [c.name for c in cols if c.role == ColumnRole.METADATA]
    for mn in meta_names:
        if mn not in df.columns:
            raise ValueError(f"SemanticTableResult missing METADATA column {mn!r}. Available: {list(df.columns)}")

    raw_codes = df[key_name].dropna().unique()
    # Templated namespaces resolve per row from other declared columns; static
    # namespaces normalize once. All placeholders are guaranteed to be declared
    # columns (validated at OutputConfig construction time).
    template_placeholders = key_col.namespace_placeholders
    static_ns = None if template_placeholders else normalize_code(key_col.namespace)
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

        if static_ns is not None:
            ns = static_ns
        else:
            # Per-row resolve: pull placeholder values from the row's metadata
            # columns (OutputConfig validator guarantees they're declared).
            # Values are stringified and lowercased so the resolved namespace
            # passes normalize_code's lowercase-snake_case contract regardless
            # of the source column's casing — the same lowercasing applies on
            # the reverse-resolution path (_find_enumerator via Catalog.search).
            first_row = sub.iloc[0]
            values: dict[str, Any] = {}
            for placeholder in template_placeholders:
                cell = first_row[placeholder]
                if pd.isna(cell):
                    raise ValueError(
                        f"namespace template {key_col.namespace!r} placeholder {placeholder!r} "
                        f"is null for row with key {raw_code!r}; populate the column or drop the row"
                    )
                raw = cell.item() if hasattr(cell, "item") else cell
                values[placeholder] = str(raw).lower()
            ns = normalize_code(resolve_namespace_template(key_col.namespace, values))

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


def _template_to_regex(template: str) -> re.Pattern[str]:
    """Compile a namespace template into a reverse-resolution regex.

    ``"sdmx-series-{agency}-{dataset_id}"`` → pattern matching
    ``"sdmx-series-<agency>-<dataset_id>"`` with named groups. Literal
    template segments are regex-escaped; placeholders become ``(?P<name>.+?)``
    (non-greedy so adjacent placeholders don't merge).
    """
    # Split on {placeholder} while keeping the placeholder names.
    parts = re.split(r"(\{[A-Za-z_][A-Za-z0-9_]*\})", template)
    regex = ""
    for part in parts:
        if part.startswith("{") and part.endswith("}"):
            name = part[1:-1]
            regex += rf"(?P<{name}>.+?)"
        else:
            regex += re.escape(part)
    return re.compile(f"^{regex}$")


def _find_enumerator(connectors: Any, namespace: str) -> tuple[Any, dict[str, Any]] | None:
    """Find an ``@enumerator`` whose KEY column declares *namespace*.

    Returns ``(enumerator, extracted_params)`` or ``None``. For static namespaces
    ``extracted_params`` is ``{}``. For template namespaces (e.g. declared as
    ``"sdmx-series-{agency}-{dataset_id}"``), the resolved *namespace* is
    reverse-matched against the template and the captured groups are returned
    as params suitable for ``enumerator.param_type(**extracted)``.
    """
    for conn in connectors:
        oc = conn.output_config
        if oc is None:
            continue
        roles = {c.role for c in oc.columns}
        if ColumnRole.DATA in roles:
            continue
        if ColumnRole.TITLE not in roles:
            continue
        for col in oc.columns:
            if col.role != ColumnRole.KEY or col.namespace is None:
                continue
            if not col.namespace_is_template:
                if col.namespace == namespace:
                    return conn, {}
                continue
            match = _template_to_regex(col.namespace).match(namespace)
            if match is not None:
                return conn, dict(match.groupdict())
    return None


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


_NAMESPACE_REQUIRED_MSG = (
    "Catalog.search requires an explicit non-empty namespaces=[...] list. "
    "Implicit cross-namespace search was removed — each catalog namespace "
    "lives in its own HF bundle with its own embedding model. "
    "Migration: change Catalog.search(query) -> "
    "Catalog.search(query, namespaces=['fred', 'snb', ...])."
)


class Catalog:
    """Catalog service: search, bulk indexing, and lazy namespace loading.

    Composes a :class:`CatalogStore` and optional :class:`EmbeddingProvider`.

    When *connectors* is provided, :meth:`search` auto-populates requested
    namespaces via the store's
    :meth:`~parsimony.stores.catalog_store.CatalogStore.try_load_remote`
    (HF bundle stores fetch from HuggingFace Hub), falling back to a live
    enumerator when no bundle is published.
    """

    def __init__(
        self,
        store: CatalogStore,
        *,
        embeddings: EmbeddingProvider | None = None,
        connectors: Any | None = None,  # duck-typed iterable of Connector-like objects
    ) -> None:
        self._store = store
        self._embeddings = embeddings
        self._connectors = connectors
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
        """Search the catalog for a query.

        ``namespaces`` is required: must be a non-empty list. The old
        implicit "search every namespace" behavior was removed — each
        namespace ships in its own HF bundle and may declare its own
        embedding model; merging without explicit scoping is unsound.

        Embedding ownership: the store owns ``embed_query`` on vector
        searches. :class:`Catalog` does not pre-embed — doing so either
        duplicates work (when the store has its own provider) or leaves
        the store without a provider to re-embed with. Stores that don't
        need an embedding ignore ``query_embedding=None``.
        """
        if namespaces is None or len(namespaces) == 0:
            raise ValueError(_NAMESPACE_REQUIRED_MSG)

        for ns in namespaces:
            await self._ensure_namespace(ns)

        return await self._store.search(
            query,
            limit,
            namespaces=namespaces,
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
        entries = entries_from_table_result(table, extra_tags=extra_tags)
        if dry_run:
            return await self._preview_ingest(entries, batch_size=batch_size, force=force)
        return await self.ingest(entries, embed=embed, batch_size=batch_size, force=force)

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
                "ingest(embed=True) requires an EmbeddingProvider; pass embeddings=... to Catalog, or use embed=False."
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
            if embed:
                if self._embeddings is None:
                    raise RuntimeError("embed=True requires an EmbeddingProvider, but none was configured")
                try:
                    out = await _embed_batch(self._embeddings, to_insert)
                except httpx.TransportError as exc:
                    # Retryable network blip — keep going, count as errors.
                    # Programmer bugs (dim mismatch, misconfigured provider)
                    # raise RuntimeError / ValueError and must propagate.
                    logger.warning("ingest embed network error: %s", exc)
                    result.errors += len(to_insert)
                    continue
            else:
                out = [r.model_copy() for r in to_insert]
            try:
                await self._store.upsert(out)
                result.indexed += len(out)
            except httpx.TransportError as exc:
                logger.warning("ingest upsert network error: %s", exc)
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
            raise RuntimeError("embed_pending requires an EmbeddingProvider; pass embeddings=... to Catalog.")
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
        return await self._store.list(namespace=namespace, q=q, limit=limit, offset=offset)

    async def get_entry(self, namespace: str, code: str) -> SeriesEntry | None:
        return await self._store.get(namespace, code)

    async def delete_entry(self, namespace: str, code: str) -> None:
        await self._store.delete(namespace, code)

    async def upsert_entries(self, entries: list[SeriesEntry]) -> None:
        await self._store.upsert(entries)

    async def refresh(self, namespace: str) -> Any:
        """Force a store-side refresh of *namespace* (stores that support it).

        Returns whatever the store's ``refresh`` returned (e.g.,
        :class:`~parsimony.stores.hf_bundle.RefreshResult`). Raises
        ``NotImplementedError`` for stores that do not support refresh.
        """
        refresh = getattr(self._store, "refresh", None)
        if refresh is None:
            raise NotImplementedError(f"{type(self._store).__name__} does not support refresh")
        result = await refresh(normalize_code(namespace))
        self._populated.discard(normalize_code(namespace))
        return result

    # ------------------------------------------------------------------
    # Lazy namespace population (Task 10 — thin dispatcher)
    # ------------------------------------------------------------------

    async def _ensure_namespace(self, namespace: str) -> None:
        """Populate *namespace* if not already known to the store.

        Three-step dispatcher: already-populated guard, store remote load,
        live-enumerator fallback. All remote-fetch logic lives inside the
        store — this method only sequences the fallbacks.
        """
        ns = normalize_code(namespace)
        if ns in self._populated:
            return

        existing = await self._store.list_namespaces()
        if ns in existing:
            self._populated.add(ns)
            return

        if await self._store.try_load_remote(ns):
            self._populated.add(ns)
            return

        if await self._try_enumerate(ns):
            self._populated.add(ns)
            return

        # Mark as attempted even on miss so we don't retry every call.
        self._populated.add(ns)
        logger.debug("Namespace %r not available from remote or enumerator", ns)

    async def _try_enumerate(self, namespace: str) -> bool:
        """Find and run the enumerator for *namespace* from the connectors.

        For template-namespace enumerators, placeholder values extracted from
        the resolved *namespace* are passed as constructor kwargs to the
        enumerator's ``param_type``, so a lookup on ``sdmx-series-ECB-YC``
        invokes the enumerator with ``agency='ECB', dataset_id='YC'``.
        """
        if self._connectors is None:
            return False
        match = _find_enumerator(self._connectors, namespace)
        if match is None:
            return False
        enumerator, extracted = match

        try:
            if enumerator.param_type:
                params = enumerator.param_type(**extracted) if extracted else enumerator.param_type()
            else:
                params = None
            result = await enumerator(params)
            entries = entries_from_table_result(result)
            if not entries:
                return False
            idx = await self.ingest(entries, embed=False, force=True)
            logger.info("Enumerated %s: %d entries", namespace, idx.indexed)
            return idx.indexed > 0
        except ConnectorError as exc:
            logger.warning("Enumerator failed for %s: %s", namespace, exc)
            return False

    async def close(self) -> None:
        """Close the underlying store if it supports closing."""
        if hasattr(self._store, "close"):
            await self._store.close()
