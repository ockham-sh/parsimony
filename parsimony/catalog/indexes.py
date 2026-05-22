"""Catalog search indexes and ranking helpers."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, Self, runtime_checkable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import TypeAdapter

from parsimony.catalog.models import CatalogEntry, field_text, normalize_code
from parsimony.catalog.storage import META_FILENAME
from parsimony.embedder import EmbedderInfo, EmbeddingProvider
from parsimony.indexes import build_faiss, read_faiss, tokenize, write_faiss
from parsimony.ranking import (
    Ranker,
    RankerSpec,
    Ranking,
    ZScoreFusion,
    concat,
    ranker_from_spec,
    ranker_to_spec,
    ranking_from_scores,
)

if TYPE_CHECKING:
    import faiss
    from rank_bm25 import BM25Okapi


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
            if (text := field_text(entry, self.field).strip())
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
            return Ranking.empty()
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
            if (tokens := tokenize(field_text(entry, self.field)))
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
            return Ranking.empty()
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
    rows: list[tuple[int, float]],
    *,
    limit: int,
) -> Ranking:
    return ranking_from_scores([(keys[idx][0], keys[idx][1], score) for idx, score in rows], limit=limit)
