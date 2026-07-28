"""Catalog search indexes: value-deduplicated BM25, vector, and hybrid fusion.

Index classes own their native scoring. :func:`search_index_values` is the one
standardization boundary shared by every index type: exact-value recovery,
reciprocal-rank relevance in ``(0, 1]``, deterministic order, and truncation.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple, Protocol, Self, runtime_checkable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog.storage import META_FILENAME, POSTINGS_FILENAME, VALUES_FILENAME, VECTORS_FILENAME
from parsimony.embedder import EmbedderInfo, EmbeddingProvider
from parsimony.entity import Entity, field_values
from parsimony.indexes import RRF_K, build_faiss, competition_ranks, read_faiss, rrf_traced, tokenize, write_faiss

if TYPE_CHECKING:
    import faiss
    from rank_bm25 import BM25Okapi

COMPONENTS_DIRNAME = "components"


def _embedder_key(info: EmbedderInfo) -> tuple[str, int, bool]:
    return (info.model, info.dim, info.normalize)


@dataclass(frozen=True)
class ComponentEvidence:
    """Index-native evidence for one component hit on a distinct value."""

    kind: Literal["bm25", "vector"]
    raw_score: float
    rank: int


class IndexHit(NamedTuple):
    """One index hit: fused or native score plus component evidence.

    ``components`` is empty for exact-only reinjection (no fuzzy retrieval hit).
    """

    score: float
    components: tuple[ComponentEvidence, ...]


@dataclass(frozen=True)
class ScoredValue:
    """Standardized distinct-value hit from :func:`search_index_values`."""

    text: str
    relevance: float
    exact: bool
    components: tuple[ComponentEvidence, ...]


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
class QueryContext:
    """Query-scoped embedding memo shared across indexes in one catalog search.

    Internal orchestration detail: one context per ``search`` /
    ``multi_field_search`` / ``search_values`` call guarantees one forward pass
    per distinct embedder identity. Not a connector-facing feature.
    """

    query: str
    _vectors: dict[tuple[str, int, bool], list[float]] = field(default_factory=dict, repr=False)

    def query_vector(self, embedder: EmbeddingProvider) -> list[float]:
        """Return the query embedding for *embedder*, computing it at most once."""
        key = _embedder_key(embedder.info())
        cached = self._vectors.get(key)
        if cached is not None:
            return cached
        batch = embedder.embed_texts([self.query])
        vector = list(batch[0])
        self._vectors[key] = vector
        return vector


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


def is_exact_value(text: str, query: str) -> bool:
    """Whether *text* is the value the query literally names.

    Case-folded, whitespace-trimmed string equality — nothing else. Exactness is
    the one fact a value can assert about a query, so it stays cheap to state and
    impossible to argue with. Anything short of equality competes on fuzzy score,
    where it belongs: a value that adds tokens the query never asked for names a
    different concept, and grading how *nearly* it matched only dresses a guess up
    as a fact.
    """
    return text.strip().casefold() == query.strip().casefold()


@runtime_checkable
class CatalogIndex(Protocol):
    """Runtime contract for a field-scoped catalog index."""

    kind: str

    @property
    def values(self) -> Sequence[str]:
        """Distinct indexed value texts, ordered by value id."""
        ...

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None:
        """Build the index from entries for ``ctx.field``."""
        ...

    def score_values(self, ctx: QueryContext, *, limit: int) -> dict[int, IndexHit]:
        """Return index-native scores for distinct values, capped at *limit*."""
        ...

    def save(self, path: Path) -> None: ...

    @classmethod
    def load(cls, path: Path) -> Self: ...


def _validate_hits(hits: Mapping[int, IndexHit], *, n_values: int, index_kind: str) -> None:
    """Reject non-finite scores and value ids outside the index corpus."""
    for vid, hit in hits.items():
        if not isinstance(vid, int) or vid < 0 or vid >= n_values:
            raise ValueError(f"{index_kind} score_values returned invalid value id {vid!r} (n={n_values})")
        if not math.isfinite(hit.score):
            raise ValueError(f"{index_kind} score_values scored value id {vid} as {hit.score!r}")


class BM25Index:
    """BM25 index over unique field values with row-id postings."""

    kind: str = "bm25"

    def __init__(self) -> None:
        self._postings = _ValuePostings.build([], "")
        self._tokens: list[list[str]] = []
        self._bm25: BM25Okapi | None = None

    @property
    def values(self) -> Sequence[str]:
        return self._postings.values

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

    def score_values(self, ctx: QueryContext, *, limit: int) -> dict[int, IndexHit]:
        if not self._postings.values or self._bm25 is None or limit < 1:
            return {}
        rows = _bm25_value_scores(self._bm25, ctx.query, doc_tokens=self._tokens)
        ranked = sorted(rows, key=lambda item: (-item[1], item[0]))[:limit]
        scores = {vid: score for vid, score in ranked if score > 0.0}
        ranks = competition_ranks(scores)
        hits = {
            vid: IndexHit(
                score=score,
                components=(ComponentEvidence(kind="bm25", raw_score=score, rank=ranks[vid]),),
            )
            for vid, score in scores.items()
        }
        _validate_hits(hits, n_values=len(self._postings.values), index_kind=self.kind)
        return hits

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
    def values(self) -> Sequence[str]:
        return self._postings.values

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

    def score_values(self, ctx: QueryContext, *, limit: int) -> dict[int, IndexHit]:
        if not self._postings.values or self._faiss is None or self._faiss.ntotal == 0 or limit < 1:
            return {}
        info = self.embedder_info
        query_vector = ctx.query_vector(self._require_embedder())
        rows = _faiss_query_scores(
            self._faiss,
            query_vector,
            limit=limit,
            normalize=info.normalize,
        )
        scores = {vid: score for vid, score in rows}
        ranks = competition_ranks(scores)
        hits = {
            vid: IndexHit(
                score=score,
                components=(ComponentEvidence(kind="vector", raw_score=score, rank=ranks[vid]),),
            )
            for vid, score in scores.items()
        }
        _validate_hits(hits, n_values=len(self._postings.values), index_kind=self.kind)
        return hits

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

    def __init__(self, *, components: list[CatalogIndex]) -> None:
        if not components:
            raise ValueError("HybridIndex requires at least one component")
        kinds: dict[str, CatalogIndex] = {}
        for component in components:
            kind = getattr(component, "kind", None)
            if kind not in ("bm25", "vector"):
                raise TypeError(f"Unsupported hybrid component type: {type(component)!r}")
            if kind in kinds:
                raise ValueError(f"HybridIndex duplicate component kind {kind!r}")
            kinds[kind] = component
        self._components = kinds

    @property
    def values(self) -> Sequence[str]:
        reference = next(iter(self._components.values()))
        return reference.values

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None:
        for component in self._components.values():
            component.build(entries, ctx=ctx)
        self._assert_aligned_corpora()

    def score_values(self, ctx: QueryContext, *, limit: int) -> dict[int, IndexHit]:
        if limit < 1:
            return {}
        self._assert_aligned_corpora()
        rankings: dict[str, dict[int, float]] = {}

        bm25 = self._components.get("bm25")
        if isinstance(bm25, BM25Index):
            lexical = {vid: hit.score for vid, hit in bm25.score_values(ctx, limit=limit).items() if hit.score > 0.0}
            if lexical:
                rankings["bm25"] = lexical

        vector = self._components.get("vector")
        if isinstance(vector, VectorIndex):
            semantic = {vid: hit.score for vid, hit in vector.score_values(ctx, limit=limit).items() if hit.score > 0.0}
            if semantic:
                rankings["vector"] = semantic

        if not rankings:
            return {}

        fused, source_ranks = rrf_traced(rankings)
        hits: dict[int, IndexHit] = {}
        component_kinds: tuple[Literal["bm25"], Literal["vector"]] = ("bm25", "vector")
        for vid, score in fused.items():
            components: list[ComponentEvidence] = []
            for kind in component_kinds:
                raw = rankings.get(kind, {}).get(vid)
                if raw is None:
                    continue
                components.append(ComponentEvidence(kind=kind, raw_score=raw, rank=source_ranks[kind][vid]))
            hits[vid] = IndexHit(score=score, components=tuple(components))
        _validate_hits(hits, n_values=len(self.values), index_kind=self.kind)
        return hits

    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / META_FILENAME).write_text(
            json.dumps(
                {
                    "kind": self.kind,
                    # Frozen legacy key: pre-0.0.2 loaders validate a fusion
                    # spec out of hybrid meta. Fusion is computed natively at
                    # query time now; load() ignores this key.
                    "fusion": {"kind": "rrf", "weights": {}, "k": RRF_K},
                    "components": sorted(self._components),
                },
                indent=2,
            )
        )
        components_dir = path / COMPONENTS_DIRNAME
        components_dir.mkdir(parents=True, exist_ok=True)
        for kind, component in self._components.items():
            _save_index(component, components_dir / kind)

    @classmethod
    def load(cls, path: Path) -> Self:
        # The legacy "fusion" key in meta is ignored: fusion semantics are
        # computed natively at query time, not configured per snapshot.
        raw = json.loads((path / META_FILENAME).read_text())
        components: list[CatalogIndex] = []
        for kind in raw["components"]:
            components.append(_load_index(path / COMPONENTS_DIRNAME / kind))
        return cls(components=components)

    def _assert_aligned_corpora(self) -> None:
        sequences = [list(component.values) for component in self._components.values()]
        if not sequences:
            return
        reference = sequences[0]
        for values in sequences[1:]:
            if values != reference:
                raise ValueError(
                    "HybridIndex components must share an identical value corpus; "
                    f"got lengths {[len(item) for item in sequences]}"
                )


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
    """Positive BM25 scores per value id, with a tiny-corpus fallback.

    On a corpus of one-to-few values, Okapi IDF goes non-positive for tokens
    that appear in most documents, zeroing every score; the fallback ranks by
    matched-token count so a 1-entity catalog still matches at all.
    """
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


def search_index_values(
    index: CatalogIndex,
    ctx: QueryContext,
    *,
    limit: int,
) -> list[ScoredValue]:
    """Rank distinct indexed values: top-*limit* (text, relevance, exact, components).

    Index-native magnitudes are turned into reciprocal-rank relevance via
    :func:`parsimony.indexes.rrf` (best value ``1.0``, remainder in ``(0, 1]``).
    An exact value is force-ranked first via a finite sentinel above every fuzzy
    score so a tokenless literal match still reports ``1.0``. Values order by
    ``(exact desc, relevance desc, text)``. An exact value is reinjected even
    when fuzzy retrieval missed it (``components`` empty in that case).

    The *limit* truncation is a deliberate noise floor, not only a cost cap:
    values beyond the top-*limit* table contribute no fuzzy evidence.
    """
    if limit < 1:
        return []
    texts = list(index.values)
    hits = index.score_values(ctx, limit=limit)
    _validate_hits(hits, n_values=len(texts), index_kind=getattr(index, "kind", type(index).__name__))

    # Exact recovery: a literal match with no fuzzy score still participates.
    for vid, text in enumerate(texts):
        if vid not in hits and is_exact_value(text, ctx.query):
            hits[vid] = IndexHit(score=0.0, components=())
    if not hits:
        return []

    # Exact values pin at rank 1 via a finite sentinel above every fuzzy score
    # (rrf rejects non-finite inputs; inf is an implementation idea, not a wire
    # value). Tokenless exacts with score 0.0 still outrank every fuzzy hit.
    fuzzy_ceiling = max(
        (hit.score for vid, hit in hits.items() if not is_exact_value(texts[vid], ctx.query)),
        default=0.0,
    )
    ranking = {
        vid: (fuzzy_ceiling + 1.0 if is_exact_value(texts[vid], ctx.query) else hit.score) for vid, hit in hits.items()
    }
    contributions, _ = rrf_traced({"field": ranking})
    if not contributions:
        return []

    scored = [
        ScoredValue(
            text=texts[vid],
            relevance=contributions[vid],
            exact=is_exact_value(texts[vid], ctx.query),
            components=hits[vid].components,
        )
        for vid in contributions
        if 0 <= vid < len(texts)
    ]
    scored.sort(key=lambda item: (not item.exact, -item.relevance, item.text))
    return scored[:limit]


# ---------------------------------------------------------------------------
# Built-in persistence dispatch (top-level catalogs and nested hybrid components)
# ---------------------------------------------------------------------------


_INDEX_LOADERS: dict[str, Callable[[Path], CatalogIndex]] = {
    "bm25": BM25Index.load,
    "vector": VectorIndex.load,
    "hybrid": HybridIndex.load,
}


def _load_index(path: Path) -> CatalogIndex:
    """Load a built-in index from *path* by reading ``meta.json`` ``kind``."""
    raw = json.loads((path / META_FILENAME).read_text())
    kind = raw.get("kind")
    loader = _INDEX_LOADERS.get(kind) if isinstance(kind, str) else None
    if loader is None:
        raise TypeError(f"Unsupported catalog index kind {kind!r} at {path}")
    return loader(path)


def _save_index(index: CatalogIndex, path: Path) -> None:
    """Persist a built-in index; reject unknown runtime-only kinds clearly."""
    kind = getattr(index, "kind", None)
    if kind not in _INDEX_LOADERS or not isinstance(index, (BM25Index, VectorIndex, HybridIndex)):
        raise TypeError(f"Catalog index kind {kind!r} ({type(index)!r}) is runtime-only and cannot be serialized")
    index.save(path)
