# Indexes

A [catalog](index.md) holds [`Entity`](entities.md) records, but the actual matching is
done by per-field **indexes**. An index is scoped to one searchable field, knows how to
build itself from the entities, and (through module-level scoring functions) knows how
to score a query against them. Parsimony ships three index types — lexical
(`BM25Index`), dense-vector (`VectorIndex`), and their fusion (`HybridIndex`) — plus a
role-based selection policy (`discovery_indexes`) for a typical discovery catalog.
Spanning several fields under one query is a call-time concern, not an index type:
`Catalog.search`'s `fields=` fuses across whichever indexes you name — see
[Search](search.md).

This page covers the `CatalogIndex` protocol every index satisfies, the three concrete
index types, how values are deduplicated before scoring, the build-time embedding cache,
the discovery index policy in `parsimony.catalog.policy`, and the low-level
FAISS/tokenizer helpers in `parsimony.indexes`.

!!! note "Optional `catalog` extra"
    `BM25Index` lazily imports `rank_bm25`, and `VectorIndex` lazily imports `faiss` and a
    sentence-transformers embedder. Both are pulled in by the `catalog` extra:
    `pip install "parsimony-core[catalog]"`. The imports happen inside `build`/`load`/`search`,
    so a missing dependency surfaces only when you actually run one of those — not at import.
    Index *construction* and the policy *selection* logic below run with only `parsimony-core`.

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
- **`save`/`load`** persist and restore a snapshot directory.

Scoring is not part of the protocol. It happens through module-level functions in
`parsimony.catalog.indexes` — chiefly `search_index_values(index, query, *, limit,
query_vectors=None, lexical_only=False)`, which every index type (including
`HybridIndex`) supports — rather than a method every index implements itself. See
[Ranking and fusion](ranking-and-fusion.md) for how that scoring works.

`CatalogIndex` is `@runtime_checkable`, so `isinstance(some_index, CatalogIndex)` works:

```python
from parsimony.catalog import BM25Index, CatalogIndex

assert isinstance(BM25Index(), CatalogIndex)
```

## Value deduplication and row postings

Indexes do not score one document per entry. Instead they collect the **distinct** searchable
strings for the field across all entries, score only those unique values, then fan each
value-score out to every entry that carries it. This keeps the BM25 corpus and the FAISS
matrix small when many entries share categorical values (a `REF_AREA` of `"Germany"`
appears once in the index even if a thousand series use it).

The mapping is built by [`field_values`](entities.md), which resolves `namespace` / `code` /
`title` specially and reads everything else from `metadata`, flattening lists and dicts into
strings. Each distinct value gets a value-id; a compact postings array records which rows
carry which value. At score time a value's fuzzy score is expanded to all its rows.

A value that is fully contained in the query — every one of its own tokens appears in
the query, or it equals the query verbatim after case-folding — is graded separately as
*consumed*, with `coverage = 1.0`. Consumed values always rank ahead of the fuzzy-score
band in [row ranking](ranking-and-fusion.md#row-ranking-coverage-tiers-by-surface-arity);
there is no inflated sentinel score for an exact hit, coverage does that job instead.

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

A key constraint: at query time `VectorIndex` **never embeds the query itself**. The caller
must precompute query vectors and pass them in `query_vectors`, a dict keyed by the embedder
identity tuple `(model, dim, normalize)`. Embed once per distinct identity with the module
function `embed_query_vectors`:

```python
from parsimony.catalog import VectorIndex, Entity
from parsimony.catalog.indexes import IndexBuildContext, embed_query_vectors, search_index_values
from parsimony.embedder import SentenceTransformerEmbedder

entries = [
    Entity(namespace="ns", code="A", title="GDP of Germany"),
    Entity(namespace="ns", code="B", title="CPI of France"),
]
idx = VectorIndex(embedder=SentenceTransformerEmbedder())
idx.build(entries, ctx=IndexBuildContext(field="title", vector_cache={}))

query_vectors = embed_query_vectors("German output", [idx])
scored = search_index_values(idx, "German output", limit=10, query_vectors=query_vectors)
print(scored)  # [(text, score, coverage, matched), ...] — matched is "semantic" here
```

!!! warning "Vector search needs a precomputed query vector"
    Calling `search_index_values` (or any other scoring entry point) against a
    `VectorIndex` without the matching query vector raises `ValueError: VectorIndex
    search requires a precomputed query vector for its embedder`. Always run
    `embed_query_vectors(query, indexes)` first and pass the result. The
    `query_vectors` dict is keyed by `(model, dim, normalize)`, **not** by field name, so two
    indexes sharing one embedder share one query embedding.

    This example needs the `catalog` extra (FAISS + sentence-transformers) and downloads a
    model on first use.

`save` writes `meta.json` (including the embedder identity), `values.parquet`,
`postings.parquet`, and `vectors.faiss`. `load(path, *, embedder=None)` validates that any
supplied embedder's `(model, dim, normalize)` matches the stored identity and raises
`ValueError` on mismatch; with `embedder=None` it defers to a lazily-constructed embedder
matching the stored identity.

## `HybridIndex`

Fuses a BM25 and a vector component over **one** field. There is no fusion policy to
configure — `HybridIndex` takes only `components=`; how the two components combine is
decided at query time by [the two fusion regimes](ranking-and-fusion.md#two-regimes-picked-by-surface-arity),
which are picked by the *caller* (how many fields `Catalog.search` scores in one call),
not by anything the index itself knows or stores.

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

`build` builds each component in turn. Scoring happens through the same
`search_index_values` entry point as the other index types, with a `lexical_only` flag
that selects which fusion regime applies — `Catalog.search` sets it for you based on
`fields=`; see [Ranking and fusion](ranking-and-fusion.md) for the algorithm.

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

Beneath the catalog indexes sits a pure layer of FAISS and tokenizer functions over numpy
arrays. They are imported from `parsimony.indexes` (not top-level) and are useful when you
build a custom index or want to reason about FAISS index selection.

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

- [Building and searching](search.md) — the `Catalog` API and the structured-vs-broad query DSL that drive these indexes.
- [Ranking and fusion](ranking-and-fusion.md) — the two fusion regimes `HybridIndex` combines its components with, and how rows are ranked from the result.
- [Embedders](embedders.md) — the `EmbeddingProvider` contract that `VectorIndex` consumes.
- [Snapshots and persistence](snapshots.md) — how indexes are saved to and loaded from a catalog snapshot.
