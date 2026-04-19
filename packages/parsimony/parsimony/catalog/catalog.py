"""BaseCatalog: ABC for namespace-keyed catalogs of named entities.

A catalog persists :class:`SeriesEntry` rows keyed by ``(namespace, code)`` and
exposes hybrid retrieval over them. Persistence and search are a single
contract — the model that produced an entry's embedding *must* be the same
model used at query time, so a catalog implementation owns its embedder.

The framework ships one canonical implementation under
:class:`parsimony.Catalog` (Parquet rows + FAISS vectors + BM25 keywords).
Custom backends — Postgres+pgvector, Redis, OpenSearch, in-memory mocks —
satisfy this ABC directly.

This module deliberately knows nothing about file formats, vector libraries,
embedders, or the Hugging Face Hub. URL-based load/push (:meth:`from_url`,
:meth:`push`) dispatches to the implementation class via the
:meth:`load_from_url` / :meth:`save_to_url` abstract methods.
"""

from __future__ import annotations

import builtins
import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx
import pandas as pd

from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
    normalize_entity_code,
)
from parsimony.result import ColumnRole, SemanticTableResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SemanticTableResult → SeriesEntry conversion
# ---------------------------------------------------------------------------


def _entries_from_table_result(
    table: SemanticTableResult,
    *,
    extra_tags: builtins.list[str] | None = None,
) -> builtins.list[SeriesEntry]:
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
    ns = normalize_code(key_col.namespace)
    tag_list = [table.provenance.source]
    if extra_tags:
        tag_list.extend(extra_tags)

    entries: builtins.list[SeriesEntry] = []
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
# BaseCatalog
# ---------------------------------------------------------------------------


def _url_scheme(url: str) -> str:
    if "://" not in url:
        raise ValueError(f"URL must include a scheme (e.g. 'hf://...'); got {url!r}")
    return url.split("://", 1)[0].lower()


class BaseCatalog(ABC):
    """Persistence and hybrid search for ``(namespace, code)`` entries.

    A catalog is to :class:`SeriesEntry` what :class:`~parsimony.Connectors`
    is to :class:`~parsimony.Connector` — a named collection. Entries are
    self-describing (they carry their own ``namespace``); the catalog's
    :attr:`name` is the collection's label (the HF repo suffix, the ``meta.json``
    identifier). A catalog typically holds one source's entries
    (``Catalog(name="fred")`` with all entries having ``namespace="fred"``),
    but the framework does not enforce that — composition by ``upsert`` of one
    catalog's entries into another is supported.

    Implementations own the storage layout and the embedder. The orchestration
    layered on top — extracting rows from a :class:`SemanticTableResult`,
    deduping, batching :meth:`upsert`, and dispatching URL-based load/push by
    scheme — is concrete here so every implementation gets it for free.

    Implementations:

    * Set :attr:`name` in ``__init__`` and expose entries via :attr:`entries`.
    * Override the abstract methods (:meth:`upsert`, :meth:`get`, :meth:`exists`,
      :meth:`delete`, :meth:`search`, :meth:`list`, :meth:`list_namespaces`).
    * Override :meth:`embedder_info` if the catalog uses an embedder whose
      identity needs to be persisted (e.g. for redistribution).
    * Override :meth:`load_from_url` and :meth:`save_to_url` to participate in
      :meth:`from_url` / :meth:`push`. The default implementations raise
      :class:`NotImplementedError`.
    """

    #: Catalog identifier — lowercase snake_case. Subclasses MUST set this in ``__init__``.
    name: str

    # ------------------------------------------------------------------
    # Abstract: persistence
    # ------------------------------------------------------------------

    @abstractmethod
    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        """Insert or update entries. The catalog computes embeddings as needed."""
        ...

    @abstractmethod
    async def get(self, namespace: str, code: str) -> SeriesEntry | None: ...

    @abstractmethod
    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        """Return the subset of ``(namespace, code)`` pairs that already exist."""
        ...

    @abstractmethod
    async def delete(self, namespace: str, code: str) -> None: ...

    # ------------------------------------------------------------------
    # Abstract: retrieval
    # ------------------------------------------------------------------

    @abstractmethod
    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
    ) -> builtins.list[SeriesMatch]:
        """Rank entries against *query*. Hybrid retrieval is left to the implementation."""
        ...

    @abstractmethod
    async def list_namespaces(self) -> builtins.list[str]:
        """Distinct catalog namespaces, sorted lexicographically."""
        ...

    @abstractmethod
    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        """Paginated browse. Returns ``(entries, total_count)``."""
        ...

    # ------------------------------------------------------------------
    # Composition
    # ------------------------------------------------------------------

    @property
    def entries(self) -> builtins.list[SeriesEntry]:
        """All entries in this catalog as a fresh list.

        The default implementation pages through :meth:`list`. Subclasses with
        cheaper in-memory access should override.

        Two catalogs are combined by :meth:`upsert`-ing one's entries into the
        other::

            await big.upsert(small.entries)
            # or, equivalently:
            await big.extend(small)
        """
        raise NotImplementedError(
            f"{type(self).__name__}.entries is not implemented. Override on the concrete subclass."
        )

    async def extend(self, other: BaseCatalog) -> None:
        """Upsert every entry from *other* into this catalog.

        Convenience for ``await self.upsert(other.entries)``. Embeddings on
        *other*'s entries are reused when present; otherwise this catalog's
        embedder fills them in.
        """
        await self.upsert(other.entries)

    # ------------------------------------------------------------------
    # Optional metadata
    # ------------------------------------------------------------------

    @property
    def embedder_info(self) -> EmbedderInfo | None:
        """Identity of the embedder this catalog uses, if any."""
        return None

    async def close(self) -> None:
        """Release any held resources. Default: no-op."""
        return None

    # ------------------------------------------------------------------
    # Concrete orchestration
    # ------------------------------------------------------------------

    async def index_result(
        self,
        table: SemanticTableResult,
        *,
        batch_size: int = 100,
        extra_tags: builtins.list[str] | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> IndexResult:
        """Extract :class:`SeriesEntry` rows from *table* and :meth:`ingest` them."""
        entries = _entries_from_table_result(table, extra_tags=extra_tags)
        return await self.ingest(entries, batch_size=batch_size, dry_run=dry_run, force=force)

    async def ingest(
        self,
        entries: builtins.list[SeriesEntry],
        *,
        batch_size: int = 100,
        dry_run: bool = False,
        force: bool = False,
    ) -> IndexResult:
        """Dedupe and upsert entries in batches.

        ``dry_run=True`` performs the dedupe pass without writing anything;
        ``force=True`` skips the dedupe pass entirely and counts every entry
        as inserted.
        """
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
                existing = await self.exists(keys)
                to_insert = [e for e in batch if (e.namespace, e.code) not in existing]
                result.skipped += len(batch) - len(to_insert)
            if not to_insert:
                continue
            if dry_run:
                result.indexed += len(to_insert)
                continue
            try:
                await self.upsert(to_insert)
                result.indexed += len(to_insert)
            except (OSError, RuntimeError, httpx.HTTPError) as exc:
                logger.warning("ingest batch failed: %s", exc)
                result.errors += len(to_insert)
        return result

    # ------------------------------------------------------------------
    # URL-based distribution
    # ------------------------------------------------------------------

    @classmethod
    async def from_url(cls, url: str) -> BaseCatalog:
        """Load a catalog from *url*.

        The default raises :class:`NotImplementedError`; the canonical
        :class:`parsimony.Catalog` overrides this to dispatch on the URL
        scheme (``file://``, ``hf://``, ``s3://``).
        """
        raise NotImplementedError(
            f"{cls.__name__}.from_url is not implemented. Override on a concrete subclass "
            "or use parsimony.Catalog.from_url(url) for the standard Parquet+FAISS layout."
        )

    async def push(self, url: str) -> None:
        """Publish this catalog to *url*. Default: :class:`NotImplementedError`."""
        raise NotImplementedError(
            f"{type(self).__name__}.push is not implemented. Override on a concrete subclass "
            "or use parsimony.Catalog for the standard Parquet+FAISS layout."
        )
