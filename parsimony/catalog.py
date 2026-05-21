"""Catalog entries, indexes, ranking, and portable snapshots."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import tempfile
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self, overload, runtime_checkable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, field_validator

from parsimony.embedder import EmbedderInfo, EmbeddingProvider
from parsimony.indexes import build_faiss, read_faiss, tokenize, write_faiss
from parsimony.ranking import (
    RANKING_COLUMNS,
    Ranker,
    RankerSpec,
    Ranking,
    ZScoreFusion,
    concat,
    ranker_from_spec,
    ranker_to_spec,
    ranking_from_scores,
)
from parsimony.result import ColumnRole, Result

if TYPE_CHECKING:
    import faiss
    from rank_bm25 import BM25Okapi

logger = logging.getLogger(__name__)

CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def code_token(value: str) -> str:
    """Normalize a string for use in provider-derived codes."""

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
    """Normalize entity codes: non-empty trimmed strings."""

    normalized = value.strip()
    if not normalized:
        raise ValueError("code must be non-empty")
    return normalized


def catalog_key(namespace: str, code: str) -> tuple[str, str]:
    """Canonical in-memory key for ``(namespace, code)``."""

    return (normalize_code(namespace), normalize_entity_code(code))


class CatalogEntry(BaseModel):
    """Canonical catalog row."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
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


class CatalogMatch(BaseModel):
    """Resolved search result: catalog entry fields plus final score."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    code: str
    title: str
    score: float
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


@dataclass(frozen=True)
class CatalogMatches(Sequence[CatalogMatch]):
    """Immutable sequence of catalog matches."""

    items: tuple[CatalogMatch, ...]

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Iterator[CatalogMatch]:
        return iter(self.items)

    @overload
    def __getitem__(self, index: int) -> CatalogMatch: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[CatalogMatch]: ...

    def __getitem__(self, index: int | slice) -> CatalogMatch | Sequence[CatalogMatch]:
        return self.items[index]


def catalog_match_from_entry(entry: CatalogEntry, *, score: float) -> CatalogMatch:
    """Build a :class:`CatalogMatch` from a stored catalog row."""

    return CatalogMatch(
        namespace=entry.namespace,
        code=entry.code,
        title=entry.title,
        score=score,
        metadata=dict(entry.metadata),
    )


@runtime_checkable
class CatalogIndex(Protocol):
    """Runtime contract for a component that produces one ranking."""

    name: str
    field: str

    async def build(self, entries: list[CatalogEntry]) -> None:
        """Build the index from entries."""
        ...

    async def ranking(self, query: str, *, limit: int) -> Ranking:
        """Return one ranked list of catalog identities."""
        ...


def _field_value(entry: CatalogEntry, field: str) -> Any:
    if field == "namespace":
        return entry.namespace
    if field == "code":
        return entry.code
    if field == "title":
        return entry.title
    return entry.metadata.get(field)


def _field_text(entry: CatalogEntry, field: str) -> str:
    value = _field_value(entry, field)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return " ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return " ".join(f"{key}: {item}" for key, item in value.items() if item is not None)
    return str(value)


class HybridIndex:
    """Hybrid index over one catalog field, fusing multiple indexes."""

    kind: Literal["hybrid"] = "hybrid"

    def __init__(
        self,
        name: str,
        field: str,
        *,
        indexes: list[CatalogIndex],
        fusion: Ranker | None = None,
    ) -> None:
        self.name = normalize_code(name)
        self.field = field
        if not indexes:
            raise ValueError("HybridIndex requires at least one child index")
        for idx in indexes:
            if idx.field != field:
                raise ValueError(f"HybridIndex child index field ({idx.field}) must match hybrid field ({field})")

        child_names = [idx.name for idx in indexes]
        if len(child_names) != len(set(child_names)):
            raise ValueError("HybridIndex child index names must be unique")

        self._indexes = indexes
        self._fusion = fusion if fusion is not None else ZScoreFusion()

    async def build(self, entries: list[CatalogEntry]) -> None:
        await asyncio.gather(*(c.build(entries) for c in self._indexes))

    async def ranking(self, query: str, *, limit: int) -> Ranking:
        child_rankings = {}
        for c in self._indexes:
            child_rankings[c.name] = await c.ranking(query, limit=max(limit * 5, 50))
        rs = concat(child_rankings)
        return self._fusion(rs, limit=limit)

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        for c in self._indexes:
            child_path = path / c.name
            if isinstance(c, (VectorIndex, BM25Index, HybridIndex)):
                c.save(child_path)
            else:
                raise TypeError(f"Catalog index {c.name!r} under hybrid index is not serializable")

        (path / META_FILENAME).write_text(
            json.dumps(
                {
                    "kind": self.kind,
                    "name": self.name,
                    "field": self.field,
                    "fusion": ranker_to_spec(self._fusion).model_dump(mode="json"),
                    "children": [c.name for c in self._indexes],
                },
                indent=2,
            )
        )

    @classmethod
    def load(cls, path: Path) -> Self:
        raw = json.loads((path / META_FILENAME).read_text())
        from pydantic import TypeAdapter

        fusion = ranker_from_spec(TypeAdapter(RankerSpec).validate_python(raw["fusion"]))

        children_indexes: list[CatalogIndex] = []
        for child_name in raw["children"]:
            child_path = path / child_name
            child_raw = json.loads((child_path / META_FILENAME).read_text())
            child_kind = child_raw["kind"]
            child_idx: CatalogIndex
            if child_kind == "vector":
                child_idx = VectorIndex.load(child_path)
            elif child_kind == "bm25":
                child_idx = BM25Index.load(child_path)
            elif child_kind == "hybrid":
                child_idx = HybridIndex.load(child_path)
            else:
                raise ValueError(f"Unsupported child index kind {child_kind!r} under hybrid index {raw['name']!r}")
            children_indexes.append(child_idx)

        return cls(raw["name"], raw["field"], indexes=children_indexes, fusion=fusion)


class VectorIndex:
    """Vector index over one catalog field."""

    kind: Literal["vector"] = "vector"

    def __init__(self, name: str, field: str = "title", *, embedder: EmbeddingProvider | None = None) -> None:
        self.name = normalize_code(name)
        self.field = field
        self._embedder = embedder
        self._embedder_info: EmbedderInfo | None = None
        self._keys: list[tuple[str, str]] = []
        self._faiss: faiss.Index | None = None

    @property
    def embedder_info(self) -> EmbedderInfo:
        embedder = self._require_embedder()
        if self._embedder_info is None:
            self._embedder_info = embedder.info()
        return self._embedder_info

    async def build(self, entries: list[CatalogEntry]) -> None:
        docs = [
            ((entry.namespace, entry.code), text)
            for entry in entries
            if (text := _field_text(entry, self.field).strip())
        ]
        self._keys = [key for key, _ in docs]
        if not docs:
            self._faiss = None
            return
        embedder = self._require_embedder()
        info = self.embedder_info
        texts = [text for _, text in docs]
        unique_texts = list(dict.fromkeys(texts))
        vectors_by_text: dict[str, list[float]] = {}
        batch = 256
        for start in range(0, len(unique_texts), batch):
            batch_texts = unique_texts[start : start + batch]
            batch_vectors = await embedder.embed_texts(batch_texts)
            vectors_by_text.update(zip(batch_texts, batch_vectors, strict=True))
        vectors = [vectors_by_text[text] for text in texts]
        matrix = np.asarray(vectors, dtype=np.float32)
        self._faiss = build_faiss(matrix, dim=info.dim, normalize=info.normalize)

    async def ranking(self, query: str, *, limit: int) -> Ranking:
        if self._faiss is None or self._faiss.ntotal == 0:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        info = self.embedder_info
        query_vector = await self._require_embedder().embed_query(query)
        rows = _faiss_query_scores(self._faiss, query_vector, limit=limit, normalize=info.normalize)
        return _ranking_from_index_scores(self._keys, rows, limit=limit)

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        info = self.embedder_info
        (path / META_FILENAME).write_text(
            json.dumps(
                {
                    "kind": self.kind,
                    "name": self.name,
                    "field": self.field,
                    "keys": [{"namespace": ns, "code": code} for ns, code in self._keys],
                    "embedder": info.model_dump(mode="json"),
                },
                indent=2,
            )
        )
        write_faiss(self._faiss, str(path / "index.faiss"), dim=info.dim)

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        embedder: EmbeddingProvider | None = None,
    ) -> Self:
        raw = json.loads((path / META_FILENAME).read_text())
        stored_info = EmbedderInfo.model_validate(raw["embedder"])
        if embedder is not None:
            chosen_info = embedder.info()
            expected = (stored_info.model, stored_info.dim, stored_info.normalize)
            actual = (chosen_info.model, chosen_info.dim, chosen_info.normalize)
            if expected != actual:
                raise ValueError(
                    f"Embedder identity mismatch for index at {path}:\n"
                    f"  expected (model, dim, normalize): {expected}\n"
                    f"  actual:                           {actual}"
                )
        index = cls(raw["name"], raw["field"], embedder=embedder)
        index._embedder_info = stored_info
        raw_keys = raw.get("keys")
        if raw_keys is None:
            raise ValueError(f"VectorIndex at {path} is missing serialized keys")
        index._keys = [(str(item["namespace"]), str(item["code"])) for item in raw_keys]
        index._faiss = read_faiss(str(path / "index.faiss"), expected_rows=len(index._keys))
        return index

    def _require_embedder(self) -> EmbeddingProvider:
        if self._embedder is None:
            from parsimony.embedder import SentenceTransformerEmbedder

            model = self._embedder_info.model if self._embedder_info else "all-MiniLM-L6-v2"
            normalize = self._embedder_info.normalize if self._embedder_info else True
            self._embedder = SentenceTransformerEmbedder(model=model, normalize=normalize)
        return self._embedder


class BM25Index:
    """BM25 index over one catalog field."""

    kind: Literal["bm25"] = "bm25"

    def __init__(self, name: str, field: str = "title") -> None:
        self.name = normalize_code(name)
        self.field = field
        self._keys: list[tuple[str, str]] = []
        self._tokens: list[list[str]] = []
        self._bm25: BM25Okapi | None = None

    async def build(self, entries: list[CatalogEntry]) -> None:
        docs = [
            ((entry.namespace, entry.code), tokens)
            for entry in entries
            if (tokens := tokenize(_field_text(entry, self.field)))
        ]
        self._keys = [key for key, _ in docs]
        self._tokens = [tokens for _, tokens in docs]
        if not self._tokens:
            self._bm25 = None
            return
        from rank_bm25 import BM25Okapi

        self._bm25 = BM25Okapi(self._tokens)

    async def ranking(self, query: str, *, limit: int) -> Ranking:
        if self._bm25 is None:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))
        rows = _bm25_query_scores(self._bm25, query, doc_tokens=self._tokens)
        return _ranking_from_index_scores(self._keys, rows, limit=limit)

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / META_FILENAME).write_text(
            json.dumps(
                {
                    "kind": self.kind,
                    "name": self.name,
                    "field": self.field,
                },
                indent=2,
            )
        )
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema(
            [
                ("namespace", pa.string()),
                ("code", pa.string()),
                ("tokens", pa.list_(pa.string())),
            ]
        )
        if not self._keys:
            pq.write_table(pa.Table.from_pylist([], schema=schema), path / "tokens.parquet", compression="zstd")
        else:
            rows = [
                {
                    "namespace": ns,
                    "code": code,
                    "tokens": tokens,
                }
                for (ns, code), tokens in zip(self._keys, self._tokens, strict=True)
            ]
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), path / "tokens.parquet", compression="zstd")

    @classmethod
    def load(cls, path: Path) -> Self:
        raw = json.loads((path / META_FILENAME).read_text())
        index = cls(raw["name"], raw["field"])
        import pyarrow.parquet as pq

        table = pq.read_table(path / "tokens.parquet")
        rows = table.to_pylist()
        index._keys = [(str(row["namespace"]), str(row["code"])) for row in rows]
        index._tokens = [list(row["tokens"]) for row in rows]
        if not index._tokens:
            index._bm25 = None
        else:
            from rank_bm25 import BM25Okapi

            index._bm25 = BM25Okapi(index._tokens)
        return index


def _faiss_query_scores(
    index: faiss.Index,
    query_vector: list[float],
    *,
    limit: int,
    normalize: bool,
) -> list[tuple[int, float]]:
    import faiss

    q = np.asarray([query_vector], dtype=np.float32)
    if normalize:
        faiss.normalize_L2(q)
    query_size = min(index.ntotal, max(limit * 10, 100))
    scores, ids = index.search(q, query_size)
    return [(int(idx), float(scores[0][pos])) for pos, idx in enumerate(ids[0]) if idx != -1]


def _bm25_query_scores(
    bm25: object,
    query: str,
    *,
    doc_tokens: list[list[str]] | None = None,
) -> list[tuple[int, float]]:
    query_tokens = tokenize(query)
    if not query_tokens:
        return []
    scores = bm25.get_scores(query_tokens)  # type: ignore[attr-defined]
    rows = [(int(idx), float(score)) for idx, score in enumerate(scores) if float(score) > 0]
    if rows or doc_tokens is None:
        return rows
    query_set = set(query_tokens)
    overlap_rows = [
        (idx, float(sum(1 for token in tokens if token in query_set)))
        for idx, tokens in enumerate(doc_tokens)
        if any(token in query_set for token in tokens)
    ]
    return overlap_rows


def _ranking_from_index_scores(
    keys: list[tuple[str, str]],
    rows: Sequence[tuple[int, float]],
    *,
    limit: int,
) -> Ranking:
    return ranking_from_scores([(keys[idx][0], keys[idx][1], score) for idx, score in rows], limit=limit)


@dataclass(frozen=True)
class ParsedCatalogURL:
    """Decomposition of a catalog URL."""

    scheme: str
    root: str
    sub: str


def parse_catalog_url(url: str | Path) -> ParsedCatalogURL:
    """Parse ``scheme://...`` into :class:`ParsedCatalogURL`."""
    import os

    url = str(url)
    if "://" not in url:
        path_str = str(Path(url).absolute()) if os.path.isabs(url) else url
        return ParsedCatalogURL(scheme="file", root=path_str, sub="")
    scheme, _, rest = url.partition("://")
    scheme = scheme.lower()
    if not scheme:
        raise ValueError(f"URL has empty scheme: {url!r}")
    if not rest:
        raise ValueError(f"URL has empty path: {url!r}")
    rest = rest.rstrip("/")
    if scheme == "hf":
        parts = rest.split("/")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise ValueError(f"hf:// URL needs '<org>/<repo>'; got {url!r}")
        return ParsedCatalogURL(scheme=scheme, root="/".join(parts[:2]), sub="/".join(parts[2:]))
    return ParsedCatalogURL(scheme=scheme, root=rest, sub="")


SCHEMA_VERSION = 3
META_FILENAME = "meta.json"
ENTRIES_FILENAME = "entries.parquet"
INDEXES_DIRNAME = "indexes"


class BuildInfo(BaseModel):
    """Provenance for a published snapshot."""

    built_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    parsimony_version: str | None = None
    builder: str | None = Field(
        default=None,
        description="Free-form identifier of the script or job that built this catalog.",
    )
    content_sha256: str = Field(
        default="",
        description="Integrity digest of all files in the catalog except meta.json",
    )


class CatalogMeta(BaseModel):
    """Catalog snapshot manifest."""

    schema_version: Literal[3] = 3
    name: str
    namespaces: list[str]
    entry_count: int = Field(ge=0)
    indexes: list[dict[str, Any]] = Field(default_factory=list)
    default_field: str = "title"
    build: BuildInfo = Field(default_factory=BuildInfo)


def read_meta(path: str | Path) -> CatalogMeta:
    """Read ``meta.json`` from *path*."""

    return CatalogMeta.model_validate_json((Path(path) / META_FILENAME).read_text())


def _compute_content_sha256(directory: Path) -> str:
    import hashlib

    lines: list[str] = []
    for p in sorted(directory.rglob("*")):
        if p.is_file() and p.name != "meta.json":
            relpath = p.relative_to(directory).as_posix()
            file_hash = hashlib.sha256()
            with open(p, "rb") as f:
                while chunk := f.read(65536):
                    file_hash.update(chunk)
            lines.append(f"{relpath}:{file_hash.hexdigest()}\n")

    lines.sort()
    concatenated = "".join(lines).encode("utf-8")
    return hashlib.sha256(concatenated).hexdigest()


def entries_from_result(table: Result) -> list[CatalogEntry]:
    """Build :class:`CatalogEntry` rows from a tabular :class:`Result`."""

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
    resolved_ns = key_col.namespace
    if not resolved_ns:
        raise ValueError("KEY column must declare namespace=... on the schema")
    key_name = key_col.name
    if key_name not in df.columns:
        raise ValueError(f"Result missing KEY column {key_name!r}. Available: {list(df.columns)}")

    title_cols = [c for c in cols if c.role == ColumnRole.TITLE]
    title_name = title_cols[0].name if len(title_cols) == 1 else None
    if title_name is not None and title_name not in df.columns:
        raise ValueError(f"Result missing TITLE column {title_name!r}. Available: {list(df.columns)}")

    meta_names = [c.name for c in cols if c.role == ColumnRole.METADATA]
    for meta_name in meta_names:
        if meta_name not in df.columns:
            raise ValueError(f"Result missing METADATA column {meta_name!r}. Available: {list(df.columns)}")

    static_ns = normalize_code(resolved_ns)
    needed_cols = {key_name, *meta_names}
    if title_name:
        needed_cols.add(title_name)
    sub_df = df[list(needed_cols)]
    grouped = sub_df.groupby(key_name, sort=False, dropna=True)

    entries: list[CatalogEntry] = []
    for raw_code, sub in grouped:
        code = normalize_entity_code(str(raw_code))
        if title_name and title_name in sub.columns:
            titles = sub[title_name].dropna()
            title = str(titles.iloc[0]) if len(titles) > 0 else code
        else:
            title = code
        metadata: dict[str, Any] = {}
        for meta_name in meta_names:
            vals = sub[meta_name].dropna()
            if len(vals) > 0:
                value = vals.iloc[0]
                metadata[meta_name] = _metadata_value(value)
        entries.append(CatalogEntry(namespace=static_ns, code=code, title=title, metadata=metadata))
    return entries


def _metadata_value(value: Any) -> Any:
    if hasattr(value, "tolist"):
        value = value.tolist()
    elif hasattr(value, "item"):
        value = value.item()
    if isinstance(value, list):
        return [item.item() if hasattr(item, "item") else item for item in value]
    return value


@dataclass(frozen=True)
class StructuredQuery:
    clauses: list[tuple[str, list[str]]]  # [(field, [value, ...]), ...]


def _parse_query(q: str, known_fields: set[str]) -> StructuredQuery | None:
    if not re.search(r"^\s*\w+\s*:", q):
        return None

    clauses: list[tuple[str, list[str]]] = []
    parts = q.split("&&")
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Malformed clause in structured query: {part!r}")
        field, _, value_str = part.partition(":")
        field = field.strip()
        if not field:
            raise ValueError(f"Empty field in clause: {part!r}")
        values = [val.strip() for val in value_str.split(",") if val.strip()]
        if not values:
            raise ValueError(f"No values provided for field {field!r} in structured query")
        clauses.append((field, values))

    if not clauses:
        return None

    if clauses[0][0] not in known_fields:
        return None

    return StructuredQuery(clauses)


def _filter_namespaces(ranking: Ranking, namespaces: list[str]) -> Ranking:
    allowed = {normalize_code(ns) for ns in namespaces}
    table = ranking.to_table()
    filtered_df = table[table["namespace"].isin(allowed)].reset_index(drop=True)
    rows = [(row.namespace, row.code, float(row.score)) for row in filtered_df.itertuples()]
    return ranking_from_scores(rows, limit=len(rows))


class Catalog:
    """Portable in-memory catalog over entries and configured indexes."""

    def __init__(
        self,
        name: str,
        *,
        indexes: list[CatalogIndex] | None = None,
        default_field: str = "title",
    ) -> None:
        self.name = normalize_code(name)
        self.default_field = default_field
        self._indexes = list(indexes) if indexes is not None else _default_indexes()
        self._entries: list[CatalogEntry] = []
        self._key_to_idx: dict[tuple[str, str], int] = {}
        self._dirty = True
        self._lock = asyncio.Lock()
        self._validate_indexes()

    @property
    def entries(self) -> list[CatalogEntry]:
        return list(self._entries)

    def _validate_indexes(self) -> None:
        fields = [idx.field for idx in self._indexes]
        if len(fields) != len(set(fields)):
            raise ValueError("Exactly one index is allowed per field")

    def index_for(self, field: str) -> CatalogIndex:
        """Return the index configured for the given field, or raise KeyError."""
        for idx in self._indexes:
            if idx.field == field:
                return idx
        raise KeyError(f"No index found for field {field!r}")

    def __len__(self) -> int:
        return len(self._entries)

    async def build(self) -> None:
        """Build configured indexes over the catalog's current entries."""

        try:
            self.index_for(self.default_field)
        except KeyError as err:
            raise ValueError(f"No index configured for default_field {self.default_field!r}") from err

        async with self._lock:
            await self._rebuild_indices()
            self._dirty = False

    def set_entries(self, entries: list[CatalogEntry]) -> None:
        """Replace entries without rebuilding indexes."""

        self._entries = []
        self._key_to_idx = {}
        self._upsert_entries(entries)
        self._invalidate()

    def set_indexes(self, indexes: list[CatalogIndex]) -> None:
        """Replace index channels without rebuilding indexes."""

        self._indexes = list(indexes)
        self._validate_indexes()
        self._invalidate()

    async def _execute_structured(self, query: StructuredQuery, *, limit: int) -> Ranking:
        # Resolve all fields first to fail-fast
        for field, _ in query.clauses:
            try:
                self.index_for(field)
            except KeyError as err:
                raise ValueError(f"No index configured for field {field!r}") from err

        clause_results: list[dict[tuple[str, str], float]] = []
        for field, values in query.clauses:
            idx = self.index_for(field)
            merged_clause_scores: dict[tuple[str, str], float] = {}
            for val in values:
                ranking = await idx.ranking(val, limit=max(limit * 5, 50))
                for row in ranking.to_table().itertuples(index=False):
                    score = float(row.score)
                    if score <= 0.0:
                        continue
                    key = (row.namespace, row.code)
                    if key not in merged_clause_scores:
                        merged_clause_scores[key] = score
                    else:
                        merged_clause_scores[key] = max(merged_clause_scores[key], score)
            clause_results.append(merged_clause_scores)

        if not clause_results:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))

        final_scores: dict[tuple[str, str], float] = {}
        first_clause = clause_results[0]
        for key, score in first_clause.items():
            final_scores[key] = score

        for clause in clause_results[1:]:
            intersection_keys = set(final_scores.keys()).intersection(clause.keys())
            new_scores: dict[tuple[str, str], float] = {}
            for key in intersection_keys:
                new_scores[key] = final_scores[key] + clause[key]
            final_scores = new_scores

        if not final_scores:
            return Ranking(pd.DataFrame(columns=RANKING_COLUMNS))

        rows = [(ns, code, score) for (ns, code), score in final_scores.items()]
        return ranking_from_scores(rows, limit=limit)

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
    ) -> CatalogMatches:
        """Search entries."""

        self._ensure_built("searched")
        parsed = _parse_query(query, known_fields={idx.field for idx in self._indexes})
        if parsed is None:
            ranking = await self.index_for(self.default_field).ranking(query, limit=limit)
        else:
            ranking = await self._execute_structured(parsed, limit=limit)

        if namespaces is not None:
            ranking = _filter_namespaces(ranking, namespaces)

        return self._matches_from_ranking(ranking)

    async def get(self, namespace: str, code: str) -> CatalogEntry | None:
        idx = self._key_to_idx.get(catalog_key(namespace, code))
        return self._entries[idx] if idx is not None else None

    async def delete_many(self, keys: Iterable[tuple[str, str]]) -> int:
        async with self._lock:
            targets: set[int] = set()
            for ns, code in keys:
                idx = self._key_to_idx.get(catalog_key(ns, code))
                if idx is not None:
                    targets.add(idx)
            if not targets:
                return 0
            self._entries = [entry for i, entry in enumerate(self._entries) if i not in targets]
            self._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(self._entries)}
            self._invalidate()
            return len(targets)

    async def save(self, url: str | Path, *, builder: str | None = None) -> None:
        """Save a catalog snapshot to a URL or bare path."""
        self._ensure_built("saved")
        url = str(url)
        parsed = parse_catalog_url(url)
        handler = _url_handlers().get(parsed.scheme)
        if handler is None:
            raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: {sorted(_url_handlers())}")
        await handler[1](self, parsed.root, parsed.sub, builder=builder)

    async def _save_to_path(self, path: Path, *, builder: str | None = None) -> None:
        """Atomically write a portable catalog snapshot to a local directory."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        await asyncio.to_thread(self._write_parquet, tmp / ENTRIES_FILENAME)
        await asyncio.to_thread(self._write_indexes, tmp / INDEXES_DIRNAME)
        await asyncio.to_thread(self._write_meta, tmp, builder)
        if target.exists():
            shutil.rmtree(target)
        tmp.rename(target)

    @classmethod
    async def load(cls, url: str | Path) -> Catalog:
        """Load a built, searchable catalog snapshot from a URL or bare path.

        Supports local file paths (bare or file://) and remote Hugging Face
        dataset URLs (hf://). Caching loaded catalogs is the caller's
        responsibility.
        """
        return await _dispatch_load(str(url))

    @classmethod
    async def _load_from_path(cls, path: Path) -> Catalog:
        """Load a clean-slate catalog snapshot from a local directory."""
        src = Path(path)
        meta = read_meta(src)
        if meta.schema_version != 3:
            raise ValueError(f"Unsupported catalog schema_version {meta.schema_version}; expected 3")

        if meta.build.content_sha256:
            actual_sha = _compute_content_sha256(src)
            if meta.build.content_sha256 != actual_sha:
                raise ValueError(
                    f"Catalog snapshot integrity check failed for {src}:\n"
                    f"  expected sha256: {meta.build.content_sha256}\n"
                    f"  actual sha256:   {actual_sha}"
                )

        entries = await asyncio.to_thread(_read_parquet, src / ENTRIES_FILENAME)
        indexes = _load_indexes(src / INDEXES_DIRNAME, manifests=meta.indexes)

        catalog = cls(meta.name, indexes=indexes, default_field=meta.default_field)
        catalog._entries = entries
        catalog._key_to_idx = {(entry.namespace, entry.code): i for i, entry in enumerate(entries)}
        catalog._dirty = False
        return catalog

    def _invalidate(self) -> None:
        self._dirty = True

    def _ensure_built(self, action: str) -> None:
        if self._dirty:
            raise ValueError(f"Catalog must be built before it can be {action}")

    def _upsert_entries(self, entries: list[CatalogEntry]) -> None:
        for entry in entries:
            key = (entry.namespace, entry.code)
            if key in self._key_to_idx:
                self._entries[self._key_to_idx[key]] = entry
            else:
                self._key_to_idx[key] = len(self._entries)
                self._entries.append(entry)

    async def _rebuild_indices(self) -> None:
        for index in self._indexes:
            await index.build(self._entries)

    def _matches_from_ranking(self, ranking: Ranking) -> CatalogMatches:
        matches: list[CatalogMatch] = []
        for row in ranking.to_table().itertuples(index=False):
            idx = self._key_to_idx.get((row.namespace, row.code))
            if idx is not None:
                matches.append(catalog_match_from_entry(self._entries[idx], score=float(row.score)))
        return CatalogMatches(tuple(matches))

    def _write_parquet(self, target: Path) -> None:
        if not self._entries:
            schema = pa.schema(
                [
                    ("namespace", pa.string()),
                    ("code", pa.string()),
                    ("title", pa.string()),
                    ("metadata_json", pa.string()),
                ]
            )
            pq.write_table(pa.Table.from_pylist([], schema=schema), target)
            return
        rows = [
            {
                "namespace": entry.namespace,
                "code": entry.code,
                "title": entry.title,
                "metadata_json": json.dumps(entry.metadata),
            }
            for entry in self._entries
        ]
        pq.write_table(pa.Table.from_pylist(rows), target, compression="zstd")

    def _write_indexes(self, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        for index in self._indexes:
            if isinstance(index, (VectorIndex, BM25Index, HybridIndex)):
                index.save(target / index.name)
            else:
                raise TypeError(f"Catalog index {index.name!r} is runtime-only and cannot be serialized")

    def _write_meta(self, target: Path, builder: str | None) -> None:
        content_sha = _compute_content_sha256(target)
        meta = CatalogMeta(
            name=self.name,
            namespaces=sorted({entry.namespace for entry in self._entries}),
            entry_count=len(self._entries),
            indexes=[_index_manifest(index) for index in self._indexes],
            default_field=self.default_field,
            build=BuildInfo(builder=builder, content_sha256=content_sha),
        )
        (target / META_FILENAME).write_text(meta.model_dump_json(indent=2))


def _default_indexes() -> list[CatalogIndex]:
    return [
        BM25Index("title", field="title"),
    ]


def _index_manifest(index: CatalogIndex) -> dict[str, Any]:
    if isinstance(index, VectorIndex):
        return {"name": index.name, "kind": index.kind, "field": index.field}
    if isinstance(index, BM25Index):
        return {"name": index.name, "kind": index.kind, "field": index.field}
    if isinstance(index, HybridIndex):
        return {"name": index.name, "kind": index.kind, "field": index.field}
    return {"name": index.name, "kind": "runtime"}


def _load_indexes(
    path: Path,
    *,
    manifests: list[dict[str, Any]],
) -> list[CatalogIndex]:
    indexes: list[CatalogIndex] = []
    if not path.exists():
        raise FileNotFoundError(f"Catalog snapshot missing indexes directory: {path}")
    children = [path / manifest["name"] for manifest in manifests]
    for child in children:
        raw = json.loads((child / META_FILENAME).read_text())
        kind = raw.get("kind")
        if kind == "vector":
            indexes.append(VectorIndex.load(child))
        elif kind == "bm25":
            indexes.append(BM25Index.load(child))
        elif kind == "hybrid":
            indexes.append(HybridIndex.load(child))
        else:
            raise ValueError(f"Unsupported catalog index kind {kind!r} in {child}")
    return indexes


def _read_parquet(target: Path) -> list[CatalogEntry]:
    table = pq.read_table(target)
    rows = table.to_pylist()
    return [
        CatalogEntry(
            namespace=row["namespace"],
            code=row["code"],
            title=row["title"],
            metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
        )
        for row in rows
    ]


REPO_TYPE = "dataset"


async def _load_file(root: str, sub: str) -> Catalog:
    path = Path(root) / sub if sub else Path(root)
    if not path.exists():
        raise FileNotFoundError(f"Catalog directory does not exist: {path}")
    return await Catalog._load_from_path(path)


async def _save_file(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    target = Path(root) / sub if sub else Path(root)
    await catalog._save_to_path(target, builder=builder)


async def _load_hf(root: str, sub: str) -> Catalog:
    from huggingface_hub import snapshot_download

    from parsimony import cache

    cache_dir = cache.catalogs_dir()
    if sub:
        local = await asyncio.to_thread(
            lambda: Path(
                snapshot_download(
                    repo_id=root,
                    repo_type=REPO_TYPE,
                    cache_dir=cache_dir,
                    allow_patterns=[f"{sub}/*"],
                )
            )
        )
        return await Catalog._load_from_path(local / sub)
    local = await asyncio.to_thread(
        lambda: Path(snapshot_download(repo_id=root, repo_type=REPO_TYPE, cache_dir=cache_dir))
    )
    return await Catalog._load_from_path(local)


async def _save_hf(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    from huggingface_hub import HfApi

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "snapshot"
        await catalog._save_to_path(staging, builder=builder)

        def _upload() -> None:
            api = HfApi()
            api.create_repo(repo_id=root, repo_type=REPO_TYPE, exist_ok=True)
            api.upload_folder(
                folder_path=str(staging),
                repo_id=root,
                repo_type=REPO_TYPE,
                path_in_repo=sub or None,
            )

        await asyncio.to_thread(_upload)


_LoadFn = Callable[[str, str], Awaitable[Catalog]]
_SaveFn = Callable[..., Awaitable[None]]


def _url_handlers() -> dict[str, tuple[_LoadFn, _SaveFn]]:
    return {
        "file": (_load_file, _save_file),
        "hf": (_load_hf, _save_hf),
    }


async def _dispatch_load(url: str) -> Catalog:
    parsed = parse_catalog_url(url)
    handler = _url_handlers().get(parsed.scheme)
    if handler is None:
        raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: {sorted(_url_handlers())}")
    return await handler[0](parsed.root, parsed.sub)


__all__ = [
    "BM25Index",
    "BuildInfo",
    "Catalog",
    "CatalogEntry",
    "CatalogIndex",
    "CatalogMatch",
    "CatalogMatches",
    "CatalogMeta",
    "ENTRIES_FILENAME",
    "INDEXES_DIRNAME",
    "META_FILENAME",
    "ParsedCatalogURL",
    "SCHEMA_VERSION",
    "VectorIndex",
    "catalog_key",
    "catalog_match_from_entry",
    "code_token",
    "entries_from_result",
    "normalize_code",
    "normalize_entity_code",
    "parse_catalog_url",
    "read_meta",
]
