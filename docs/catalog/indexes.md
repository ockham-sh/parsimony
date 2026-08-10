# Indexes

A [catalog](index.md) holds [`Entity`](entities.md) records, but the actual matching is
done by per-field **indexes**. An index is scoped to one searchable field, knows how to
build itself from the entities, and (through module-level scoring functions) knows how
to score a query against them. Parsimony ships three index types — lexical
(`BM25Index`), dense-vector (`VectorIndex`), and their fusion (`HybridIndex`) — plus a
role-based selection policy (`discovery_indexes`) for a typical discovery catalog.
Spanning several fields under one query is a call-time concern, not an index type:
`Catalog.multi_field_search` fuses whichever indexes you name with weighted RRF
— see [Search](search.md) and [Ranking and fusion](ranking-and-fusion.md).

This page covers the `CatalogIndex` protocol every index satisfies, the three concrete
index types, how values are deduplicated before scoring, the build-time embedding cache,
the discovery index policy in `parsimony.catalog.policy`, and the low-level
FAISS/tokenizer helpers in `parsimony.indexes`.

!!! note "Optional `catalog` extra"
    `BM25Index` lazily imports `rank_bm25`, and `VectorIndex` lazily imports `faiss` and a
    sentence-transformers embedder. Both are pulled in by the `catalog` extra:
    `pip install "parsimony[catalog]"`. The imports happen inside `build`/`load`/`search`,
    so a missing dependency surfaces only when you actually run one of those — not at import.
    Index *construction* and the policy *selection* logic below run with only `parsimony`.

## The `CatalogIndex` protocol

Every index implements a small, runtime-checkable `Protocol`. A `Catalog` only ever talks
to an index through these members:

```python
from pathlib import Path
from typing import Protocol, Self, runtime_checkable
from parsimony.catalog import Entity
from parsimony.catalog.indexes import IndexBuildContext

@runtime_checkable
class CatalogIndex(Protocol):
    kind: str

    def build(self, entries: list[Entity], *, ctx: IndexBuildContext) -> None: ...

    def save(self, path: Path) -> None: ...

    @classmethod
    def load(cls, path: Path) -> Self: ...
```

- **`kind`** is a class-level string tag (`"bm25"`, `"vector"`, `"hybrid"`).
  Snapshots dispatch on it when reloading an index from disk.
- **`build(entries, *, ctx)`** populates the index from the catalog's entities for the field
  named by `ctx.field`.
- **`values`** is the ordered sequence of distinct indexed texts (value id → text).
- **`score_values(ctx, *, limit)`** returns index-native `{value_id: IndexHit}` scores.
  Magnitudes stay inside the class that understands them; the standardization boundary is
  [`search_index_values`](#scoring-values) below.
- **`save`/`load`** persist and restore a snapshot directory for the three built-in kinds.
  Custom runtime indexes may implement scoring without being serializable — saving one
  raises a clear `TypeError` until core adds a loader.

`CatalogIndex` is `@runtime_checkable`, so `isinstance(some_index, CatalogIndex)` works:

```python
from parsimony.catalog import BM25Index, CatalogIndex

assert isinstance(BM25Index(), CatalogIndex)
```

## Value deduplication

Indexes do not score one document per entry. Instead they collect the **distinct** searchable
strings for the field across all entries and score only those unique values. This keeps the
BM25 corpus and the FAISS matrix small when many entries share categorical values (a
`REF_AREA` of `"Germany"` appears once in the index even if a thousand series use it).

The mapping is built by [`field_values`](entities.md), which resolves `namespace` / `code` /
`title` specially and reads everything else from `metadata`. Nested / non-scalar metadata
is not searchable — indexed and filterable columns must be scalar (see
[field extraction](entities.md#what-an-index-reads-field-extraction)). Snapshots still
persist `postings.parquet` (value → row-id) for the unique-value list; search itself does
**not** fan scores out through those postings. Scored values become a `FieldIn` candidate
filter, then one row scan attaches each row to the value-hit it carries (see
[Building and searching](search.md)).

A value that *equals* the query after case-folding and trimming is flagged `exact` and
ranks ahead of the whole fuzzy band with relevance `1.0` — see
[value-level exactness](ranking-and-fusion.md#value-level-exactness). Equality is the
whole test: nothing softer is credited.

## `BM25Index`

A lexical index over the unique field values, backed by `rank_bm25.BM25Okapi`.

```python
from parsimony.catalog import BM25Index

idx = BM25Index()      # kind == "bm25"
```

`build` deduplicates the field's values, tokenizes each one (see
[`tokenize`](#low-level-helpers-parsimonyindexes) below), and constructs a `BM25Okapi` over
the value tokens. An empty corpus leaves the model unbuilt and scores nothing.

!!! note "Zero-score fallback in tiny corpora"
    In a very small corpus, BM25's IDF can collapse to zero for terms that appear in most of
    the values, so every BM25 score comes back `0`. When that happens, the index falls back to
    raw query-token overlap counts so genuine matches still surface in the ranking. Real
    deployments with many values rarely hit this; tests add filler entries to avoid it.

`save` writes `meta.json`, a `values.parquet` (value-id, text, tokens) and a
`postings.parquet`, all zstd-compressed; `load` rebuilds the `BM25Okapi` from the stored
tokens.

## `VectorIndex`

A dense-vector index over the unique field values, backed by FAISS.

```python
from parsimony.catalog import VectorIndex
from parsimony.embedder import SentenceTransformerEmbedder

idx = VectorIndex(embedder=SentenceTransformerEmbedder())   # kind == "vector"
```

`build` deduplicates the values, embeds them via the build context (so embedding work is
shared — see [the build context](#index-build-context-and-the-vector-cache)), stacks them
into a float32 matrix, and calls `build_faiss`. The `embedder` is keyword-only and may be
`None`; a `None` embedder lazily instantiates a `SentenceTransformerEmbedder` on first use.
See [Embedders](embedders.md) for the provider contract.

At query time `VectorIndex.score_values` embeds the query through the shared
[`QueryContext`](#query-context) memo — one forward pass per distinct embedder identity
across every field in a multi-field search. Callers of `Catalog.search` /
`multi_field_search` / `search_values` never see this; it is orchestration plumbing.

```python
from parsimony.catalog import VectorIndex, Entity
from parsimony.catalog.indexes import IndexBuildContext, QueryContext, search_index_values
from parsimony.embedder import SentenceTransformerEmbedder

entries = [
    Entity(namespace="ns", code="A", title="GDP of Germany"),
    Entity(namespace="ns", code="B", title="CPI of France"),
]
idx = VectorIndex(embedder=SentenceTransformerEmbedder())
idx.build(entries, ctx=IndexBuildContext(field="title", vector_cache={}))

scored = search_index_values(idx, QueryContext(query="German output"), limit=10)
print(scored)  # [ScoredValue(text, relevance, exact, components), ...]
```

!!! note "Catalog extras"
    This example needs the `catalog` extra (FAISS + sentence-transformers) and downloads a
    model on first use.

`save` writes `meta.json` (including the embedder identity), `values.parquet`,
`postings.parquet`, and `vectors.faiss`. `load(path, *, embedder=None)` validates that any
supplied embedder's `(model, dim, normalize)` matches the stored identity and raises
`ValueError` on mismatch; with `embedder=None` it defers to a lazily-constructed embedder
matching the stored identity.

## `HybridIndex`

Fuses a BM25 and a vector component over **one** field. There is no fusion policy to
configure — `HybridIndex` takes only `components=`, and the two components are always
combined by [tie-aware unweighted RRF](ranking-and-fusion.md#fusion-inside-a-hybrid-field)
at query time. One regime, regardless of what the caller composes on top: a field must carry
its own semantic recall, because it knows nothing about how many other fields the caller is
about to weight it against.

```python
from parsimony.catalog import BM25Index, HybridIndex, VectorIndex
from parsimony.embedder import SentenceTransformerEmbedder

idx = HybridIndex(
    components=[BM25Index(), VectorIndex(embedder=SentenceTransformerEmbedder())],
)   # kind == "hybrid"
```

The constructor requires at least one component and rejects two components of the same kind:

```python
from parsimony.catalog import BM25Index, HybridIndex

HybridIndex(components=[])                          # ValueError: requires at least one component
HybridIndex(components=[BM25Index(), BM25Index()])  # ValueError: duplicate component kind 'bm25'
```

`build` builds each component in turn. `HybridIndex.score_values` fuses BM25 positives
with the vector's top-*limit* under equal-weight RRF (`parsimony.indexes.rrf`); see
[Ranking and fusion](ranking-and-fusion.md#fusion-inside-a-hybrid-field). Components must
share an identical value corpus.

`save` records each component under `components/<kind>/`, plus a frozen legacy `fusion`
key in `meta.json` for pre-0.0.2 readers (`load()` ignores it — see
[Ranking and fusion](ranking-and-fusion.md#snapshots-fusion-is-native-not-stored)).
`load` rebuilds the components by their stored `kind`.

## Index build context and the vector cache

`build` receives an `IndexBuildContext` — a transient dataclass shared across every index in
one catalog build:

```python
from parsimony.catalog.indexes import IndexBuildContext

ctx = IndexBuildContext(field="title", vector_cache={})
```

| Field | Type | Meaning |
| --- | --- | --- |
| `field` | `str` | the Entity field the index is being built for |
| `vector_cache` | `dict[tuple[str, int, bool], dict[str, np.ndarray]]` | embeddings keyed by embedder identity, then text |

The context's `embed_texts(embedder, texts)` method batches embedding work in chunks of
256 and memoizes vectors per text. Because the **same** `vector_cache` is shared across all
field indexes in a single catalog build, identical strings appearing in different fields are
embedded only once.

You only construct `IndexBuildContext` directly when driving an index outside a `Catalog`
(as in the examples above). Within [`Catalog.build`](search.md) it is created and shared for
you.

## Query context

`QueryContext` is the query-time twin of the build context — an internal orchestration
detail, not a connector-facing feature. It holds the query text and a lazy memo of query
embeddings keyed by embedder identity. One context per `Catalog.search` /
`multi_field_search` / `search_values` call guarantees one forward pass per distinct model
across every field.

```python
from parsimony.catalog.indexes import QueryContext

ctx = QueryContext(query="German output")
vector = ctx.query_vector(embedder)  # computed once, then cached
```

Retrieval depth stays an explicit `limit=` on `score_values` / `search_index_values`, not a
context field.

## Scoring values

`search_index_values(index, ctx, *, limit)` is the sole field-standardization boundary.
Every index type goes through it:

1. call `index.score_values(ctx, limit=limit)` for index-native hits;
2. reinject any exact value fuzzy retrieval missed;
3. convert the ordering to reciprocal-rank relevance in `(0, 1]` via `parsimony.indexes.rrf`;
4. order by `(exact desc, relevance desc, text)` and truncate.

Returns `ScoredValue` records (text, relevance, exact, components). `Catalog.search_values` and
`multi_field_search` both consume this path — one transform, seen twice.

## Discovery index policy (`parsimony.catalog.policy`)

When you do not want to choose an index by hand, `discovery_indexes` builds a
ready-to-use index map for a typical discovery catalog. Import it from
`parsimony.catalog.policy` (it is not a top-level name).

```python
def discovery_indexes(
    entries: Sequence[Entity],
    *,
    include_description: bool = True,
    embedder: EmbeddingProvider | None = None,
) -> dict[str, CatalogIndex]: ...
```

Index kind follows the field's **role**, never its cardinality — there is no
distinct-value count, no threshold, no per-field weighting to configure:

- `code` → `BM25Index`. An identifier's token "semantics" are noise; lexical exact/prefix
  matching is what you want, regardless of how many codes exist.
- `title`, and `description` when `include_description=True`, → `HybridIndex` (BM25 +
  `VectorIndex`), always. Both are bounded discovery vocabularies — roughly one value
  per catalog entry — so search semantics never depend on how many entries a provider
  happens to publish; a 50-row catalog and a 5-million-row catalog get the same index
  shape for the same field role.

`entries` is accepted only for call-site compatibility (so callers that used to pass
data for cardinality counting keep working) — the role policy never inspects it.

```python
from parsimony.catalog import Catalog, Entity
from parsimony.catalog.policy import discovery_indexes

entries = [
    Entity(namespace="demo", code="gdp", title="Gross domestic product"),
    Entity(namespace="demo", code="cpi", title="Consumer price index"),
]
catalog = Catalog("demo", indexes=discovery_indexes(entries))

print(sorted(discovery_indexes(entries)))                          # ['code', 'description', 'title']
print(sorted(discovery_indexes(entries, include_description=False)))  # ['code', 'title']
print(type(discovery_indexes(entries)["code"]).__name__)   # BM25Index
print(type(discovery_indexes(entries)["title"]).__name__)  # HybridIndex
```

!!! note "Shared default embedder"
    A `None` `embedder` uses a process-global shared `SentenceTransformerEmbedder`,
    instantiated once on first use. It is a module-level singleton, not thread- or
    process-isolated. Only the `title`/`description` `HybridIndex`es touch it, and only
    at build time (their `VectorIndex` component).

## Low-level helpers (`parsimony.indexes`)

Beneath the catalog indexes sits a pure layer of FAISS, tokenizer, and fusion functions
over numpy arrays and score maps. They are imported from `parsimony.indexes` (not top-level).

### `rrf`

```python
from parsimony.indexes import RRF_K, rrf

# One source → reciprocal-rank transform. Several → weighted RRF.
rrf({"lexical": {7: 2.0, 3: 1.0}, "semantic": {7: 0.9}})
```

`RRF_K` defaults to `60`. Weights must be positive and finite; scores must be finite.
Ties share a competition rank. This is the only fusion primitive in the kernel — hybrid
fields and cross-field row composition both use it.

### `tokenize`

```python
from parsimony.indexes import tokenize

print(tokenize("GDP_growth/annual"))   # ['gdp', 'growth', 'annual']
print(tokenize(""))                     # []
```

`tokenize` lowercases the text and splits on any run of non-`[a-z0-9]` characters. This is
why identifier-style strings such as `debt_to_penny` or
`v2/accounting/od/debt_to_penny#tot_pub_debt` break into their constituent words — a query
of `debt_to_penny` then matches, instead of the whole compound key being one opaque token
that never does. `BM25Index` uses it for both documents and queries.

### `build_faiss` and the adaptive index choice

```python
def build_faiss(matrix: np.ndarray, *, dim: int, normalize: bool) -> faiss.Index: ...
```

`build_faiss` picks a FAISS index type by row count `n`, trading build cost and memory for
recall as the catalog grows:

| Row count `n` | FAISS index | Notes |
| --- | --- | --- |
| `n < HNSW_THRESHOLD` (4096) | `IndexFlatIP` | exact, no build cost |
| `HNSW_THRESHOLD ≤ n < IVF_THRESHOLD` | `IndexHNSWFlat` | highest recall, fits in RAM for medium catalogs |
| `n ≥ IVF_THRESHOLD` (500000) | `IndexIVFFlat` | ~3× lower build peak; trades a little recall for headroom at scale |

When `normalize=True`, `build_faiss` L2-normalizes the matrix with inner-product metric so
scores behave as cosine similarity.

!!! warning "`normalize=True` mutates the input matrix"
    `build_faiss(..., normalize=True)` calls `faiss.normalize_L2` **in place**, modifying the
    array you pass. Hand it a `matrix.copy()` if you need the source untouched.

`read_faiss(path, *, expected_rows)` reads an index and raises `ValueError` if
`index.ntotal` disagrees with `expected_rows` (a corrupt or mismatched snapshot). It also
re-applies the HNSW `efSearch` and re-derives the IVF `nprobe` on load, so a tuning change
propagates without re-publishing every snapshot. `write_faiss(index, path, *, dim)` writes
the index, or an empty `IndexFlatIP(dim)` when `index is None`, so an empty `VectorIndex`
still serializes a valid `vectors.faiss`.

### `PARSIMONY_FAISS_IVF_THRESHOLD`

The HNSW→IVF switch-over row count, `IVF_THRESHOLD`, defaults to `500000` and is read from
the `PARSIMONY_FAISS_IVF_THRESHOLD` environment variable **at import time** of
`parsimony.indexes`.

```bash
export PARSIMONY_FAISS_IVF_THRESHOLD=1000000
```

!!! warning "Captured at import"
    Because the threshold is read when `parsimony.indexes` is first imported, setting the
    environment variable *after* import has no effect. Set it before your process starts. See
    [Environment variables](../reference/environment.md) for the full list of tunables.

## See also

- [Building and searching](search.md) — the `Catalog` API and the query methods that drive these indexes.
- [Ranking and fusion](ranking-and-fusion.md) — the RRF fusion `HybridIndex` combines its components with, and how rows are ranked from the result.
- [Embedders](embedders.md) — the `EmbeddingProvider` contract that `VectorIndex` consumes.
- [Snapshots and persistence](snapshots.md) — how indexes are saved to and loaded from a catalog snapshot.
