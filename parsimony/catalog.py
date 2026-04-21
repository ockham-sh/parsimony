"""Catalog: namespace-keyed hybrid-search over ``(namespace, code)`` entries.

This module contains three tightly related concerns:

* **Protocol** — :class:`CatalogBackend` describes the structural contract
  every catalog satisfies: ``add`` and ``search``. Custom backends
  (Postgres+pgvector, Redis, OpenSearch, in-memory mocks) are any class
  matching this shape.
* **Canonical implementation** — :class:`Catalog` ships the parsimony
  format: Parquet rows + FAISS vectors + BM25 keywords + Reciprocal Rank
  Fusion. Owns distribution (``save`` / ``load`` / ``push`` / ``from_url``).
* **URL helpers** — :func:`parse_catalog_url` lifts ``scheme://path`` into
  ``(scheme, root, sub)``; :class:`Catalog.from_url` / :meth:`Catalog.push`
  dispatch ``file://`` and ``hf://`` in-process.

Value types (:class:`SeriesEntry`, :class:`SeriesMatch`, :class:`IndexResult`)
live here rather than in their own module to keep the catalog reader's full
mental model in a single scroll.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import httpx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field, field_validator

from parsimony.embedder import EmbedderInfo, EmbeddingProvider, SentenceTransformerEmbedder
from parsimony.indexes import bm25_query, build_faiss, faiss_query, read_faiss, rrf_fuse, tokenize, write_faiss
from parsimony.result import ColumnRole, Result

if TYPE_CHECKING:
    import faiss
    from rank_bm25 import BM25Okapi

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Namespace / code normalization
# ---------------------------------------------------------------------------

CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def code_token(value: str) -> str:
    """Normalize a string for use in series codes (provider-side derivation)."""
    token = value.strip().lower()
    token = token.replace("-", "_").replace(" ", "_").replace(".", "_")
    token = re.sub(r"[^a-z0-9_]", "_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    if not token:
        return "unknown"
    if token[0].isdigit():
        return f"v_{token}"
    return token


def normalize_code(value: str) -> str:
    """Normalize catalog namespace strings: lowercase snake_case."""
    normalized = value.strip()
    if not normalized:
        raise ValueError("Value must be non-empty")
    if not CODE_PATTERN.fullmatch(normalized):
        raise ValueError("Value must be lowercase snake_case (letters, numbers, underscores)")
    return normalized


def normalize_entity_code(value: str) -> str:
    """Normalize entity `code` within a namespace: non-empty trimmed string."""
    normalized = value.strip()
    if not normalized:
        raise ValueError("code must be non-empty")
    return normalized


def catalog_key(namespace: str, code: str) -> tuple[str, str]:
    """Canonical in-memory key for (namespace, code)."""
    return (normalize_code(namespace), normalize_entity_code(code))


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------


class SeriesEntry(BaseModel):
    """Canonical catalog row: indexing input and persisted store shape.

    Identity is ``(namespace, code)``. ``code`` is the connector-native
    identifier string for that namespace (e.g. FRED ``GDPC1``, FMP ``AAPL``).
    """

    namespace: str
    code: str
    title: str
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None

    @field_validator("namespace")
    @classmethod
    def _normalize_namespace(cls, value: str) -> str:
        return normalize_code(value)

    @field_validator("code")
    @classmethod
    def _normalize_code_field(cls, value: str) -> str:
        return normalize_entity_code(value)

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must be non-empty")
        return normalized

    def embedding_text(self) -> str:
        """Compose the text an embedder should index for this entry."""
        parts = [self.title]
        if self.metadata:
            meta_parts = [f"{k}: {v}" for k, v in self.metadata.items() if v is not None]
            if meta_parts:
                parts.append(", ".join(meta_parts))
        if self.tags:
            parts.append(f"tags: {', '.join(self.tags)}")
        return " | ".join(parts)


class SeriesMatch(BaseModel):
    """Search projection: catalog row fields needed for display + fetch, plus similarity."""

    namespace: str
    code: str
    title: str
    similarity: float
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("namespace")
    @classmethod
    def _normalize_namespace(cls, value: str) -> str:
        return normalize_code(value)

    @field_validator("code")
    @classmethod
    def _normalize_code_field(cls, value: str) -> str:
        return normalize_entity_code(value)

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must be non-empty")
        return normalized


def series_match_from_entry(entry: SeriesEntry, *, similarity: float) -> SeriesMatch:
    """Build a :class:`SeriesMatch` from a stored catalog row."""
    return SeriesMatch(
        namespace=entry.namespace,
        code=entry.code,
        title=entry.title,
        similarity=similarity,
        tags=list(entry.tags),
        description=entry.description,
        metadata=dict(entry.metadata),
    )


class IndexResult(BaseModel):
    """Statistics from an indexing run."""

    total: int = 0
    indexed: int = 0
    skipped: int = 0
    errors: int = 0


# ---------------------------------------------------------------------------
# Protocol (pluggable backend contract)
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogBackend(Protocol):
    """Structural contract every catalog backend satisfies.

    Two methods — ``add`` (persist + index entries) and ``search`` (return
    ranked matches). Everything else (URL-based load/push, snapshot format,
    embedder identity) is the concrete implementation's business.
    """

    async def add(self, entries: list[SeriesEntry]) -> None:
        """Insert or update entries. Backends compute embeddings as needed."""
        ...

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
    ) -> list[SeriesMatch]:
        """Rank entries against *query*. Hybrid retrieval left to the backend."""
        ...


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedCatalogURL:
    """Decomposition of a catalog URL.

    ``scheme``: lowercased scheme (``file``, ``hf``, …).
    ``root``: the top-level location — local directory or HF repo id.
    ``sub``: optional sub-path inside ``root`` (``""`` when absent).
    """

    scheme: str
    root: str
    sub: str


def parse_catalog_url(url: str) -> ParsedCatalogURL:
    """Parse ``scheme://path[/sub]`` into :class:`ParsedCatalogURL`.

    The root is everything after the scheme up to (and excluding) the first
    slash that introduces a sub-path; the sub is the remainder. Paths
    without a leading scheme raise :class:`ValueError`.
    """
    if "://" not in url:
        raise ValueError(f"URL must include a scheme (e.g. 'file://...'); got {url!r}")
    scheme, _, rest = url.partition("://")
    scheme = scheme.lower()
    if not scheme:
        raise ValueError(f"URL has empty scheme: {url!r}")
    if not rest:
        raise ValueError(f"URL has empty path: {url!r}")
    return ParsedCatalogURL(scheme=scheme, root=rest.rstrip("/"), sub="")


# ---------------------------------------------------------------------------
# Snapshot metadata
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1
META_FILENAME = "meta.json"
ENTRIES_FILENAME = "entries.parquet"
INDEX_FILENAME = "embeddings.faiss"


class BuildInfo(BaseModel):
    """Provenance for a published snapshot."""

    built_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    parsimony_version: str | None = None
    builder: str | None = Field(
        default=None,
        description="Free-form identifier of the script or job that built this catalog.",
    )


class CatalogMeta(BaseModel):
    """Catalog snapshot manifest (``meta.json``)."""

    schema_version: int = Field(default=SCHEMA_VERSION)
    name: str = Field(
        description=(
            "Catalog name (lowercase snake_case). Identifies the snapshot; conventionally matches the HF repo suffix."
        ),
    )
    namespaces: list[str] = Field(
        description="Distinct entry namespaces in entries.parquet (lowercase snake_case).",
    )
    entry_count: int = Field(ge=0)
    embedder: EmbedderInfo
    build: BuildInfo = Field(default_factory=BuildInfo)


def read_meta(path: str | Path) -> CatalogMeta:
    """Read ``meta.json`` from *path* (the catalog directory)."""
    return CatalogMeta.model_validate_json((Path(path) / META_FILENAME).read_text())


# ---------------------------------------------------------------------------
# Result → entries adapter
# ---------------------------------------------------------------------------


def entries_from_result(
    table: Result,
    *,
    extra_tags: list[str] | None = None,
    namespace: str | None = None,
) -> list[SeriesEntry]:
    """Build :class:`SeriesEntry` rows from a :class:`Result` with an output schema.

    If the KEY column does not declare a ``namespace=``, the caller must
    supply *namespace* — :class:`Catalog.add_from_result` passes its own
    ``name`` as the default.
    """
    if table.output_schema is None:
        raise ValueError("Result must carry an output_schema for catalog indexing")
    if not isinstance(table.data, (pd.DataFrame, pd.Series)):
        raise TypeError(f"indexing expected tabular data, got {type(table.data).__name__}")
    df = table.df
    if df.empty:
        return []

    cols = table.output_schema.columns
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    if len(key_cols) != 1:
        raise ValueError(f"Result must have exactly one KEY column in output_schema, found {len(key_cols)}")
    key_col = key_cols[0]
    resolved_ns = key_col.namespace or namespace
    if not resolved_ns:
        raise ValueError("KEY column must declare namespace=... on the schema, or the caller must supply one")
    key_name = key_col.name
    if key_name not in df.columns:
        raise ValueError(f"Result missing KEY column {key_name!r}. Available: {list(df.columns)}")

    title_cols = [c for c in cols if c.role == ColumnRole.TITLE]
    title_name = title_cols[0].name if len(title_cols) == 1 else None
    if title_name is not None and title_name not in df.columns:
        raise ValueError(f"Result missing TITLE column {title_name!r}. Available: {list(df.columns)}")

    meta_names = [c.name for c in cols if c.role == ColumnRole.METADATA]
    for mn in meta_names:
        if mn not in df.columns:
            raise ValueError(f"Result missing METADATA column {mn!r}. Available: {list(df.columns)}")

    raw_codes = df[key_name].dropna().unique()
    static_ns = normalize_code(resolved_ns)
    tag_list = [table.provenance.source] if table.provenance.source else []
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
                namespace=static_ns,
                code=code,
                title=title,
                tags=tag_list,
                metadata=meta,
            )
        )
    return entries


# ---------------------------------------------------------------------------
# Canonical Catalog
# ---------------------------------------------------------------------------

EMBED_BATCH = 256
REPO_TYPE = "dataset"


class Catalog:
    """Canonical catalog: Parquet rows + FAISS vectors + BM25 text + RRF.

    Construct an empty catalog and :meth:`add` rows, then :meth:`save`
    locally or :meth:`push` to a URL. Construct a populated catalog with
    :meth:`from_url`.

    The embedder is owned by the catalog; index-time and query-time embeddings
    must come from the same model. The default
    :class:`SentenceTransformerEmbedder` is loaded lazily — instantiation is
    cheap, the model is fetched on first use.

    Conforms structurally to :class:`CatalogBackend` (``add`` + ``search``).
    """

    def __init__(self, name: str, *, embedder: EmbeddingProvider | None = None) -> None:
        self.name = normalize_code(name)
        self._embedder: EmbeddingProvider = embedder if embedder is not None else SentenceTransformerEmbedder()
        self._embedder_info: EmbedderInfo | None = None
        self._entries: list[SeriesEntry] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._faiss: faiss.Index | None = None
        self._bm25: BM25Okapi | None = None
        self._tokens: list[list[str]] = []
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
    def entries(self) -> list[SeriesEntry]:
        return list(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    # ------------------------------------------------------------------
    # Protocol surface
    # ------------------------------------------------------------------

    async def add(self, entries: list[SeriesEntry]) -> None:
        """Insert or update *entries*. Computes missing embeddings and rebuilds indices."""
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

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
    ) -> list[SeriesMatch]:
        if not self._entries:
            return []
        ns_filter = {catalog_key(ns, "_")[0] for ns in namespaces} if namespaces else None

        candidate_k = max(limit * 5, 50)
        bm25_ranks = self._bm25_ranks(query, candidate_k)
        vec_ranks = await self._faiss_ranks(query, candidate_k)
        fused = rrf_fuse(bm25_ranks, vec_ranks)

        out: list[SeriesMatch] = []
        for idx, score in fused:
            entry = self._entries[idx]
            if ns_filter is not None and entry.namespace not in ns_filter:
                continue
            out.append(series_match_from_entry(entry, similarity=score))
            if len(out) >= limit:
                break
        return out

    # ------------------------------------------------------------------
    # Direct access helpers
    # ------------------------------------------------------------------

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        idx = self._key_to_idx.get(catalog_key(namespace, code))
        return self._entries[idx] if idx is not None else None

    async def exists(self, keys: list[tuple[str, str]]) -> set[tuple[str, str]]:
        return {catalog_key(ns, code) for ns, code in keys if catalog_key(ns, code) in self._key_to_idx}

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

    async def list_namespaces(self) -> list[str]:
        return sorted({e.namespace for e in self._entries})

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[SeriesEntry], int]:
        ns = catalog_key(namespace, "_")[0] if namespace else None
        ql = q.lower() if q else None
        filtered = [
            e
            for e in self._entries
            if (ns is None or e.namespace == ns) and (ql is None or ql in e.code.lower() or ql in e.title.lower())
        ]
        return filtered[offset : offset + limit], len(filtered)

    # ------------------------------------------------------------------
    # High-level ingestion
    # ------------------------------------------------------------------

    async def add_from_result(
        self,
        table: Result,
        *,
        extra_tags: list[str] | None = None,
        batch_size: int = 100,
        dry_run: bool = False,
    ) -> IndexResult:
        """Extract entries from *table* and ingest them.

        The KEY column's ``namespace=`` wins when set; otherwise this
        catalog's ``name`` is used as the default namespace.
        """
        entries = entries_from_result(table, extra_tags=extra_tags, namespace=self.name)
        return await self._ingest(entries, batch_size=batch_size, dry_run=dry_run)

    async def _ingest(
        self,
        entries: list[SeriesEntry],
        *,
        batch_size: int = 100,
        dry_run: bool = False,
    ) -> IndexResult:
        result = IndexResult()
        result.total = len(entries)
        if not entries:
            return result

        for start in range(0, len(entries), batch_size):
            batch = entries[start : start + batch_size]
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
                await self.add(to_insert)
                result.indexed += len(to_insert)
            except (OSError, RuntimeError, httpx.HTTPError) as exc:
                logger.warning("ingest batch failed: %s", exc)
                result.errors += len(to_insert)
        return result

    # ------------------------------------------------------------------
    # On-disk snapshot
    # ------------------------------------------------------------------

    async def save(self, path: str | Path, *, builder: str | None = None) -> None:
        """Atomically write the three-file snapshot to *path*."""
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
        """Load a snapshot from *path*."""
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
        entries, _ = await asyncio.to_thread(_read_parquet, src / ENTRIES_FILENAME)
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
    async def from_url(cls, url: str, *, embedder: EmbeddingProvider | None = None) -> Catalog:
        """Load from *url*. Schemes: ``file://``, ``hf://``."""
        parsed = parse_catalog_url(url)
        handler = _url_handlers().get(parsed.scheme)
        if handler is None:
            raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: {sorted(_url_handlers())}")
        return await handler[0](parsed.root, embedder=embedder)

    async def push(self, url: str) -> None:
        """Publish to *url*. Schemes: ``file://``, ``hf://``."""
        parsed = parse_catalog_url(url)
        handler = _url_handlers().get(parsed.scheme)
        if handler is None:
            raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: {sorted(_url_handlers())}")
        await handler[1](self, parsed.root)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _embed_missing(self, entries: list[SeriesEntry]) -> list[SeriesEntry]:
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

    def _bm25_ranks(self, query: str, k: int) -> list[tuple[int, int]]:
        if self._bm25 is None:
            return []
        return bm25_query(self._bm25, query, k=k)

    async def _faiss_ranks(self, query: str, k: int) -> list[tuple[int, int]]:
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


# ---------------------------------------------------------------------------
# URL scheme handlers (file://, hf://)
# ---------------------------------------------------------------------------


async def _load_file(root: str, *, embedder: EmbeddingProvider | None = None) -> Catalog:
    path = Path(root)
    if not path.exists():
        raise FileNotFoundError(f"Catalog directory does not exist: {path}")
    return await Catalog.load(path, embedder=embedder)


async def _push_file(catalog: Catalog, root: str) -> None:
    await catalog.save(Path(root))


async def _load_hf(root: str, *, embedder: EmbeddingProvider | None = None) -> Catalog:
    from huggingface_hub import snapshot_download

    local = await asyncio.to_thread(lambda: Path(snapshot_download(repo_id=root, repo_type=REPO_TYPE)))
    return await Catalog.load(local, embedder=embedder)


async def _push_hf(catalog: Catalog, root: str) -> None:
    from huggingface_hub import HfApi

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "snapshot"
        await catalog.save(staging)

        def _upload() -> None:
            api = HfApi()
            api.create_repo(repo_id=root, repo_type=REPO_TYPE, exist_ok=True)
            api.upload_folder(folder_path=str(staging), repo_id=root, repo_type=REPO_TYPE)

        await asyncio.to_thread(_upload)


def _url_handlers() -> dict[str, tuple[Callable[..., Any], Callable[..., Any]]]:
    """(load, push) pair per supported scheme. Kept as a function so tests can monkeypatch."""
    return {
        "file": (_load_file, _push_file),
        "hf": (_load_hf, _push_hf),
    }


__all__ = [
    "BuildInfo",
    "Catalog",
    "CatalogBackend",
    "CatalogMeta",
    "EmbedderInfo",
    "ENTRIES_FILENAME",
    "INDEX_FILENAME",
    "IndexResult",
    "META_FILENAME",
    "ParsedCatalogURL",
    "SCHEMA_VERSION",
    "SeriesEntry",
    "SeriesMatch",
    "catalog_key",
    "code_token",
    "entries_from_result",
    "normalize_code",
    "normalize_entity_code",
    "parse_catalog_url",
    "read_meta",
    "series_match_from_entry",
]
