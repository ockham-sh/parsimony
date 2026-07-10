"""Catalog search indexes: value-deduplicated BM25, vector, and hybrid fusion."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import TypeAdapter

from parsimony.catalog.storage import META_FILENAME, POSTINGS_FILENAME, VALUES_FILENAME, VECTORS_FILENAME
from parsimony.embedder import EmbedderInfo, EmbeddingProvider
from parsimony.entity import Entity, field_values
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

COMPONENTS_DIRNAME = "components"
#: Lowest score a fuzzy hybrid candidate is shifted to, so a below-mean fusion
#: score (z-score fusion produces negatives) stays > 0 and is not dropped by
#: ``_ValuePostings.expand`` (which discards ``score <= 0``).
_FUZZY_SCORE_FLOOR = 1e-3


def _component_kind(index: CatalogIndex) -> str:
    if isinstance(index, BM25Index):
        return "bm25"
    if isinstance(index, VectorIndex):
        return "vector"
    raise TypeError(f"Unsupported hybrid component type: {type(index)!r}")


def _embedder_key(info: EmbedderInfo) -> tuple[str, int, bool]:
    return (info.model, info.dim, info.normalize)


@dataclass
class IndexBuildContext:
    """Transient build-time state shared across indexes in one catalog build."""

    field: str
    vector_cache: dict[tuple[str, int, bool], dict[str, np.ndarray]]

    def embed_texts(self, embedder: EmbeddingProvider, texts: list[str]) -> list[np.ndarray]:
        if not texts:
            return []
        info = embedder.info()
        key = _embedder_key(info)
        bucket = self.vector_cache.setdefault(key, {})
        missing = [text for text in texts if text not in bucket]
        if missing:
            batch = 256
            for start in range(0, len(missing), batch):
                chunk = missing[start : start + batch]
                vectors = embedder.embed_texts(chunk)
                for text, vector in zip(chunk, vectors, strict=True):
                    bucket[text] = np.asarray(vector, dtype=np.float32)
        return [bucket[text] for text in texts]


@dataclass
class _ValuePostings:
    values: list[str]
    value_to_id: dict[str, int]
    offsets: np.ndarray
    row_ids: np.ndarray

    @classmethod
    def build(cls, entries: list[Entity], field_name: str) -> _ValuePostings:
        value_to_id: dict[str, int] = {}
        values: list[str] = []
        postings: list[tuple[int, int]] = []
        for row_id, entry in enumerate(entries):
            for text in field_values(entry, field_name):
                vid = value_to_id.get(text)
                if vid is None:
                    vid = len(values)
                    value_to_id[text] = vid
                    values.append(text)
                postings.append((vid, row_id))
        if not postings:
            return cls(
                values=[],
                value_to_id={},
                offsets=np.zeros(1, dtype=np.int64),
                row_ids=np.zeros(0, dtype=np.int32),
            )
        postings.sort(key=lambda item: (item[0], item[1]))
        offsets = np.zeros(len(values) + 1, dtype=np.int64)
        row_ids = np.fromiter((row for _, row in postings), dtype=np.int32, count=len(postings))
        pos = 0
        for vid in range(len(values)):
            offsets[vid] = pos
            while pos < len(postings) and postings[pos][0] == vid:
                pos += 1
        offsets[len(values)] = pos
        return cls(values=values, value_to_id=value_to_id, offsets=offsets, row_ids=row_ids)

    def expand(self, value_scores: dict[int, float]) -> dict[int, float]:
        out: dict[int, float] = {}
        for vid, score in value_scores.items():
            if score <= 0.0:
                continue
            start = int(self.offsets[vid])
            end = int(self.offsets[vid + 1])
            for row_id in self.row_ids[start:end]:
                rid = int(row_id)
                prev = out.get(rid)
                if prev is None or score > prev:
                    out[rid] = score
        return out

    def save_postings(self, path: Path) -> None:
        rows: list[dict[str, int]] = []
        for vid in range(len(self.values)):
            start = int(self.offsets[vid])
            end = int(self.offsets[vid + 1])
            for row_id in self.row_ids[start:end]:
                rows.append({"value_id": vid, "row_id": int(row_id)})
        schema = pa.schema([("value_id", pa.int32()), ("row_id", pa.int32())])
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")

    @classmethod
    def load_postings(cls, path: Path, values: list[str]) -> _ValuePostings:
        table = pq.read_table(path)
        rows = table.to_pylist()
        value_to_id = {text: idx for idx, text in enumerate(values)}
        if not rows:
            return cls(
                values=values,
                value_to_id=value_to_id,
                offsets=np.zeros(len(values) + 1, dtype=np.int64),
                row_ids=np.zeros(0, dtype=np.int32),
            )
        postings = [(int(row["value_id"]), int(row["row_id"])) for row in rows]
        postings.sort(key=lambda item: (item[0], item[1]))
        offsets = np.zeros(len(values) + 1, dtype=np.int64)
        row_ids = np.fromiter((row for _, row in postings), dtype=np.int32, count=len(postings))
        pos = 0
        for vid in range(len(values)):
            offsets[vid] = pos
            while pos < len(postings) and postings[pos][0] == vid:
                pos += 1
        offsets[len(values)] = pos
        return cls(values=values, value_to_id=value_to_id, offsets=offsets, row_ids=row_ids)


def value_query_coverage(text: str, query: str) -> float:
    """Coverage-gate one indexed value against a query.

    A value is *consumed* when its full token set is a subset of the query's
    (it asserts nothing the query didn't ask for); its coverage is the fraction
    of the query's tokens it consumes. Any value with leftover tokens gates to
    0.0 and competes on fuzzy score alone. An exact match is the limit point:
    coverage 1.0 — case-insensitive string equality covers even values that
    tokenize to nothing. No parameters, no sentinels.
    """
    if text.strip().casefold() == query.strip().casefold():
        return 1.0
    query_tokens = frozenset(tokenize(query))
    if not query_tokens:
        return 0.0
    vtokens = set(tokenize(text))
    if not vtokens or not vtokens <= query_tokens:
        return 0.0
    return len(vtokens) / len(query_tokens)


def consumed_value_tokens(index: CatalogIndex, query: str) -> dict[str, frozenset[str]]:
    """Indexed values fully consumed by *query*: ``{text: value token set}``.

    The catalog layer unions these token sets across a row's fields to grade
    the row's query coverage (see :meth:`Catalog.search`). A value that is
    string-equal to the query (the tokenless limit of containment) carries the
    query's full token set so the union grades it 1.0.
    """
    normalized = query.strip().casefold()
    query_tokens = frozenset(tokenize(query))
    try:
        texts = _value_texts_from_index(index)
    except TypeError:
        return {}
    out: dict[str, frozenset[str]] = {}
    for text in texts:
        if text.strip().casefold() == normalized:
            out[text] = query_tokens
            continue
        if not query_tokens:
            continue
        vtokens = frozenset(tokenize(text))
        if vtokens and vtokens <= query_tokens:
            out[text] = vtokens
    return out


@runtime_checkable
class CatalogIndex(Protocol):
    """Runtime contract for a field-scoped catalog index."""

    kind: str

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None:
        """Build the index from entries for ``ctx.field``."""
        ...

    def score_candidates(
        self,
        query: str,
        *,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> dict[int, float]:
        """Return candidate row scores keyed by entry row id."""
        ...

    def save(self, path: Path) -> None: ...

    @classmethod
    def load(cls, path: Path) -> Self: ...


class BM25Index:
    """BM25 index over unique field values with row-id postings."""

    kind: str = "bm25"

    def __init__(self) -> None:
        self._postings = _ValuePostings.build([], "")
        self._tokens: list[list[str]] = []
        self._bm25: BM25Okapi | None = None

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None:
        self._postings = _ValuePostings.build(entries, ctx.field)
        self._tokens = [tokenize(text) for text in self._postings.values]
        if not self._tokens:
            self._bm25 = None
            return
        if not any(self._tokens):
            self._bm25 = None
            return
        from rank_bm25 import BM25Okapi

        self._bm25 = BM25Okapi(self._tokens)

    def score_candidates(
        self,
        query: str,
        *,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> dict[int, float]:
        del query_vectors
        value_scores = self._score_values(query)
        return self._postings.expand(value_scores)

    def ranking(self, query: str, *, limit: int, entries: list[Entity]) -> Ranking:
        row_scores = self.score_candidates(query)
        return _ranking_from_row_scores(entries, row_scores, limit=limit)

    def _score_values(self, query: str) -> dict[int, float]:
        if not self._postings.values or self._bm25 is None:
            return {}
        rows = _bm25_value_scores(self._bm25, query, doc_tokens=self._tokens)
        return {vid: score for vid, score in rows}

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / META_FILENAME).write_text(json.dumps({"kind": self.kind}, indent=2))
        value_rows = [
            {"value_id": vid, "text": text, "tokens": tokens}
            for vid, (text, tokens) in enumerate(zip(self._postings.values, self._tokens, strict=True))
        ]
        schema = pa.schema(
            [
                ("value_id", pa.int32()),
                ("text", pa.string()),
                ("tokens", pa.list_(pa.string())),
            ]
        )
        pq.write_table(pa.Table.from_pylist(value_rows, schema=schema), path / VALUES_FILENAME, compression="zstd")
        self._postings.save_postings(path / POSTINGS_FILENAME)

    @classmethod
    def load(cls, path: Path) -> Self:
        index = cls()
        table = pq.read_table(path / VALUES_FILENAME)
        rows = table.to_pylist()
        values = [str(row["text"]) for row in rows]
        index._tokens = [list(row["tokens"]) for row in rows]
        index._postings = _ValuePostings.load_postings(path / POSTINGS_FILENAME, values)
        if index._tokens and any(index._tokens):
            from rank_bm25 import BM25Okapi

            index._bm25 = BM25Okapi(index._tokens)
        return index


class VectorIndex:
    """Vector index over unique field values with row-id postings."""

    kind: str = "vector"

    def __init__(self, *, embedder: EmbeddingProvider | None = None) -> None:
        self._embedder = embedder
        self._embedder_info: EmbedderInfo | None = None
        self._postings = _ValuePostings.build([], "")
        self._faiss: faiss.Index | None = None

    @property
    def embedder_info(self) -> EmbedderInfo:
        embedder = self._require_embedder()
        if self._embedder_info is None:
            self._embedder_info = embedder.info()
        return self._embedder_info

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None:
        self._postings = _ValuePostings.build(entries, ctx.field)
        if not self._postings.values:
            self._faiss = None
            return
        embedder = self._require_embedder()
        info = self.embedder_info
        vectors = ctx.embed_texts(embedder, self._postings.values)
        matrix = np.vstack(vectors).astype(np.float32, copy=False)
        self._faiss = build_faiss(matrix, dim=info.dim, normalize=info.normalize)

    def score_candidates(
        self,
        query: str,
        *,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> dict[int, float]:
        value_scores = self._score_values(query, query_vectors=query_vectors)
        return self._postings.expand(value_scores)

    def ranking(
        self,
        query: str,
        *,
        limit: int,
        entries: list[Entity],
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> Ranking:
        row_scores = self.score_candidates(query, query_vectors=query_vectors)
        return _ranking_from_row_scores(entries, row_scores, limit=limit)

    def _score_values(
        self,
        query: str,
        *,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None,
    ) -> dict[int, float]:
        if not self._postings.values:
            return {}
        if self._faiss is None or self._faiss.ntotal == 0:
            return {}
        info = self.embedder_info
        key = _embedder_key(info)
        if query_vectors is not None and key in query_vectors:
            query_vector = query_vectors[key]
        else:
            raise ValueError("VectorIndex search requires a precomputed query vector for its embedder")
        rows = _faiss_query_scores(
            self._faiss,
            query_vector,
            limit=len(self._postings.values),
            normalize=info.normalize,
        )
        return {vid: score for vid, score in rows}

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        info = self.embedder_info
        (path / META_FILENAME).write_text(
            json.dumps({"kind": self.kind, "embedder": info.model_dump(mode="json")}, indent=2)
        )
        value_rows = [{"value_id": vid, "text": text} for vid, text in enumerate(self._postings.values)]
        schema = pa.schema([("value_id", pa.int32()), ("text", pa.string())])
        pq.write_table(pa.Table.from_pylist(value_rows, schema=schema), path / VALUES_FILENAME, compression="zstd")
        self._postings.save_postings(path / POSTINGS_FILENAME)
        write_faiss(self._faiss, str(path / VECTORS_FILENAME), dim=info.dim)

    @classmethod
    def load(cls, path: Path, *, embedder: EmbeddingProvider | None = None) -> Self:
        raw = json.loads((path / META_FILENAME).read_text())
        stored_info = EmbedderInfo.model_validate(raw["embedder"])
        if embedder is not None:
            chosen_info = embedder.info()
            expected = _embedder_key(stored_info)
            actual = _embedder_key(chosen_info)
            if expected != actual:
                raise ValueError(
                    f"Embedder identity mismatch for index at {path}:\n"
                    f"  expected (model, dim, normalize): {expected}\n"
                    f"  actual:                           {actual}"
                )
        index = cls(embedder=embedder)
        index._embedder_info = stored_info
        table = pq.read_table(path / VALUES_FILENAME)
        values = [str(row["text"]) for row in table.to_pylist()]
        index._postings = _ValuePostings.load_postings(path / POSTINGS_FILENAME, values)
        index._faiss = read_faiss(str(path / VECTORS_FILENAME), expected_rows=len(values))
        return index

    def _require_embedder(self) -> EmbeddingProvider:
        if self._embedder is None:
            from parsimony.embedder import SentenceTransformerEmbedder

            model = self._embedder_info.model if self._embedder_info else "all-MiniLM-L6-v2"
            normalize = self._embedder_info.normalize if self._embedder_info else True
            self._embedder = SentenceTransformerEmbedder(model=model, normalize=normalize)
        return self._embedder


class HybridIndex:
    """Hybrid index fusing BM25 and vector components over one field."""

    kind: str = "hybrid"

    def __init__(
        self,
        *,
        components: list[CatalogIndex],
        fusion: Ranker | None = None,
    ) -> None:
        if not components:
            raise ValueError("HybridIndex requires at least one component")
        kinds: dict[str, CatalogIndex] = {}
        for component in components:
            kind = _component_kind(component)
            if kind in kinds:
                raise ValueError(f"HybridIndex duplicate component kind {kind!r}")
            kinds[kind] = component
        self._components = kinds
        self._fusion = fusion if fusion is not None else ZScoreFusion()

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None:
        for component in self._components.values():
            component.build(entries, ctx=ctx)

    def score_candidates(
        self,
        query: str,
        *,
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> dict[int, float]:
        value_rankings: dict[str, Ranking] = {}
        per_kind_scores: dict[str, dict[int, float]] = {}
        for kind, component in self._components.items():
            if isinstance(component, BM25Index):
                value_scores = component._score_values(query)
            elif isinstance(component, VectorIndex):
                value_scores = component._score_values(query, query_vectors=query_vectors)
            else:
                raise TypeError(f"Unsupported hybrid component: {type(component)!r}")
            per_kind_scores[kind] = value_scores
            rows = [("", str(vid), float(score)) for vid, score in value_scores.items() if score > 0.0]
            value_rankings[kind] = ranking_from_scores(rows, limit=len(rows))
        if not value_rankings:
            return {}
        positive_vids = {vid for scores in per_kind_scores.values() for vid, score in scores.items() if score > 0.0}
        fused_values = self._fusion(concat(value_rankings), limit=max(len(positive_vids), 1))
        fused_by_vid = {int(item.code): item.score for item in fused_values.items}
        fused_scores = self._compose_fused_scores(positive_vids, fused_by_vid)
        reference = next(iter(self._components.values()))
        if not isinstance(reference, (BM25Index, VectorIndex)):
            raise TypeError(f"Unsupported hybrid component for postings: {type(reference)!r}")
        return reference._postings.expand(fused_scores)

    @staticmethod
    def _compose_fused_scores(
        positive_vids: set[int],
        fused_by_vid: dict[int, float],
    ) -> dict[int, float]:
        """Compose the per-vid row score from the configured fusion's output.

        The configured fusion (ZScore/MinMax/RRF + weights) decides the order of
        every candidate — that is the whole point of choosing a fusion. One
        correction keeps that honest: fusion scores are centred (z-score produces
        negatives) and ``_ValuePostings.expand`` drops ``score <= 0`` — which would
        silently lose every below-mean candidate. The band is translated into the
        positive range (lowest → ``_FUZZY_SCORE_FLOOR``), preserving the fusion's
        order and relative gaps without dropping anyone (a vid a degenerate fusion
        omits — e.g. MinMax skipping a uniform-score group — lands at the floor).
        """
        if not positive_vids:
            return {}
        present = [fused_by_vid[vid] for vid in positive_vids if vid in fused_by_vid]
        lo = min(present) if present else 0.0
        return {vid: (fused_by_vid.get(vid, lo) - lo) + _FUZZY_SCORE_FLOOR for vid in positive_vids}

    def ranking(
        self,
        query: str,
        *,
        limit: int,
        entries: list[Entity],
        query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
    ) -> Ranking:
        row_scores = self.score_candidates(query, query_vectors=query_vectors)
        return _ranking_from_row_scores(entries, row_scores, limit=limit)

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / META_FILENAME).write_text(
            json.dumps(
                {
                    "kind": self.kind,
                    "fusion": ranker_to_spec(self._fusion).model_dump(mode="json"),
                    "components": sorted(self._components),
                },
                indent=2,
            )
        )
        components_dir = path / COMPONENTS_DIRNAME
        components_dir.mkdir(parents=True, exist_ok=True)
        for kind, component in self._components.items():
            if isinstance(component, (BM25Index, VectorIndex)):
                component.save(components_dir / kind)
            else:
                raise TypeError(f"Hybrid component {kind!r} is not serializable")

    @classmethod
    def load(cls, path: Path) -> Self:
        raw = json.loads((path / META_FILENAME).read_text())
        fusion = ranker_from_spec(TypeAdapter(RankerSpec).validate_python(raw["fusion"]))
        components: list[CatalogIndex] = []
        for kind in raw["components"]:
            component_path = path / COMPONENTS_DIRNAME / kind
            component_raw = json.loads((component_path / META_FILENAME).read_text())
            if component_raw["kind"] == "bm25":
                components.append(BM25Index.load(component_path))
            elif component_raw["kind"] == "vector":
                components.append(VectorIndex.load(component_path))
            else:
                raise ValueError(f"Unsupported hybrid component kind {component_raw['kind']!r}")
        return cls(components=components, fusion=fusion)


def collect_vector_indexes(index: CatalogIndex) -> list[VectorIndex]:
    if isinstance(index, VectorIndex):
        return [index]
    if isinstance(index, HybridIndex):
        return [idx for idx in index._components.values() if isinstance(idx, VectorIndex)]
    return []


def embed_query_vectors(
    query: str,
    indexes: Iterable[CatalogIndex],
) -> dict[tuple[str, int, bool], list[float]]:
    """Embed *query* once per distinct embedder identity used by *indexes*."""
    vectors: dict[tuple[str, int, bool], list[float]] = {}
    pending: dict[tuple[str, int, bool], EmbeddingProvider] = {}
    for index in indexes:
        for vector_index in collect_vector_indexes(index):
            info = vector_index.embedder_info
            key = _embedder_key(info)
            if key not in vectors and key not in pending:
                pending[key] = vector_index._require_embedder()
    for key, embedder in pending.items():
        batch = embedder.embed_texts([query])
        vectors[key] = list(batch[0])
    return vectors


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
    query_size = min(index.ntotal, max(limit, 1))
    scores, ids = index.search(q, query_size)
    return [(int(idx), float(scores[0][pos])) for pos, idx in enumerate(ids[0]) if idx != -1]


def _bm25_value_scores(
    bm25: object,
    query: str,
    *,
    doc_tokens: list[list[str]],
) -> list[tuple[int, float]]:
    query_tokens = tokenize(query)
    if not query_tokens:
        return []
    scores = bm25.get_scores(query_tokens)  # type: ignore[attr-defined]
    rows = [(int(idx), float(score)) for idx, score in enumerate(scores) if float(score) > 0]
    if rows:
        return rows
    query_set = set(query_tokens)
    return [
        (idx, float(sum(1 for token in tokens if token in query_set)))
        for idx, tokens in enumerate(doc_tokens)
        if any(token in query_set for token in tokens)
    ]


def _ranking_from_row_scores(
    entries: list[Entity],
    row_scores: dict[int, float],
    *,
    limit: int,
) -> Ranking:
    rows = [
        (entries[row_id].namespace, entries[row_id].code, score)
        for row_id, score in row_scores.items()
        if 0 <= row_id < len(entries)
    ]
    return ranking_from_scores(rows, limit=limit)


def _value_texts_from_index(index: CatalogIndex) -> list[str]:
    if isinstance(index, BM25Index):
        return list(index._postings.values)
    if isinstance(index, VectorIndex):
        return list(index._postings.values)
    if isinstance(index, HybridIndex):
        reference = next(iter(index._components.values()))
        if isinstance(reference, (BM25Index, VectorIndex)):
            return list(reference._postings.values)
    raise TypeError(f"search_values is not supported for index type {type(index)!r}")


def _fused_value_scores(
    index: CatalogIndex,
    query: str,
    *,
    query_vectors: dict[tuple[str, int, bool], list[float]] | None,
) -> dict[int, float]:
    if isinstance(index, BM25Index):
        return index._score_values(query)
    if isinstance(index, VectorIndex):
        return index._score_values(query, query_vectors=query_vectors)
    if isinstance(index, HybridIndex):
        value_rankings: dict[str, Ranking] = {}
        per_kind_scores: dict[str, dict[int, float]] = {}
        for kind, component in index._components.items():
            if isinstance(component, BM25Index):
                value_scores = component._score_values(query)
            elif isinstance(component, VectorIndex):
                value_scores = component._score_values(query, query_vectors=query_vectors)
            else:
                raise TypeError(f"Unsupported hybrid component: {type(component)!r}")
            per_kind_scores[kind] = value_scores
            rows = [("", str(vid), float(score)) for vid, score in value_scores.items() if score > 0.0]
            value_rankings[kind] = ranking_from_scores(rows, limit=len(rows))
        if not value_rankings:
            return {}
        positive_vids = {vid for scores in per_kind_scores.values() for vid, score in scores.items() if score > 0.0}
        fused_values = index._fusion(concat(value_rankings), limit=max(len(positive_vids), 1))
        return {
            int(item.code): max(scores.get(int(item.code), 0.0) for scores in per_kind_scores.values())
            for item in fused_values.items
        }
    raise TypeError(f"search_values is not supported for index type {type(index)!r}")


def search_index_values(
    index: CatalogIndex,
    query: str,
    *,
    limit: int,
    query_vectors: dict[tuple[str, int, bool], list[float]] | None = None,
) -> list[tuple[str, float, float]]:
    """Rank distinct indexed values for *query*: top-*limit* (text, score, coverage).

    Values order by (coverage desc, fuzzy score desc): a value fully consumed by
    the query (an exact hit is coverage 1.0) ranks above every fuzzy-only
    candidate, while near-misses stay visible below it in the fuzzy band.
    """
    value_scores = _fused_value_scores(index, query, query_vectors=query_vectors)
    texts = _value_texts_from_index(index)
    # An equal-but-tokenless value produces no fuzzy score; keep it findable.
    normalized = query.strip().casefold()
    for vid, text in enumerate(texts):
        if vid not in value_scores and text.strip().casefold() == normalized:
            value_scores[vid] = 0.0
    if not value_scores:
        return []
    scored = [
        (texts[vid], float(score), value_query_coverage(texts[vid], query))
        for vid, score in value_scores.items()
        if 0 <= vid < len(texts)
    ]
    scored.sort(key=lambda item: (-item[2], -item[1], item[0]))
    return scored[:limit]
