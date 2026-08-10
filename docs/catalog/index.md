# The Catalog

A `Catalog` is Parsimony's discovery layer: a portable, in-memory index over normalized
[`Entity`](entities.md) records. You load entities into it, build per-field indexes, then rank
rows by a text query, filter them exactly, or both. The same catalog can be saved to a local
directory or a Hugging Face dataset and loaded back, fully built and searchable.

A catalog is the natural sink for an [enumerator](../connectors/loaders-and-enumerators.md): an
enumerator discovers entities (series, tickers, datasets), and a catalog makes them searchable.
Both `Catalog` and `Entity` are top-level imports, but for catalog-heavy code the clearest
convention is to import from the submodule:

```python
from parsimony.catalog import BM25Index, Catalog, Entity
```

!!! note "Optional dependencies"
    The catalog runtime is lazy. `import parsimony` and constructing a `Catalog` pull in no
    heavy dependencies. `BM25Index` keyword search works on the base install (`rank-bm25` is a
    base dependency). Only the vector/hybrid backends need the heavy stack — FAISS and an
    embedder — via the `catalog` extra: `pip install "parsimony[catalog]"`. See
    [Installation](../getting-started/installation.md).

## The lifecycle

A catalog moves through a fixed sequence: **construct → load entities → build → search → save**.
Mutations always invalidate the built indexes, so the build step is a gate, not a one-off.

```text
Catalog(name, indexes=...)          construct (starts "dirty")
        │
        ▼
set_entities([...])                 load / replace entities  ── marks dirty
        │
        ▼
catalog.build()               materialize indexes      ── clears dirty
        │
        ├──► catalog.search("query", limit=5)   →  list[CatalogMatch]
        │
        └──► catalog.save("file:///path")        →  snapshot directory
                                                            │
                              Catalog.load("file:///path")  ──►  built, searchable
```

### The build gate

A freshly constructed catalog is *dirty*. So is one whose entities or indexes you have changed.
`search()` and `save()` both refuse to run on a dirty catalog and raise a plain `ValueError`
whose message tells you what to do:

```text
Catalog entries or indexes changed — call catalog.build() before it can be searched
```

The mutating methods that mark a catalog dirty are `set_entities` and `set_indexes`. `get()`
does not require a build and never raises this error. Re-run `catalog.build()` after any
mutation.

!!! warning "Build before you search or save"
    Forgetting to `catalog.build()` is the most common foot-gun. The error is an ordinary
    `ValueError`, not a custom catalog exception, so do not try to catch a special type — fix the
    call order instead. The exact same gate guards `save()` (the message ends in "before it can
    be saved").

## A minimal catalog

This example constructs a catalog with a single BM25 index over the `title` field, loads two
entities, builds, and runs a plain-text (broad) search. `BM25Index` (`rank-bm25`) is in the base
install, so a plain `pip install parsimony` is enough — no extra needed.

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("artifact", indexes={"title": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="series", code="A", title="alpha growth index"),
        Entity(namespace="series", code="B", title="beta consumer prices"),
    ]
)
catalog.build()

matches = catalog.search("alpha", limit=5)
top = matches[0]
print(top.namespace, top.code)  # -> series A
print(top.title, round(top.score, 3))
```

`search()` returns a list of [`CatalogMatch`](search.md) records ordered by descending score.

## Constructing a catalog

The constructor signature is:

```python
Catalog(name: str, *, indexes: dict[str, CatalogIndex] | None = None)
```

| Parameter | Meaning |
| --- | --- |
| `name` | Catalog identity. Normalized to lowercase snake_case via `normalize_namespace` — uppercase or hyphenated names raise `ValueError` (e.g. `"My Catalog"` is rejected; pass `"my_catalog"`). |
| `indexes` | A map of *search-surface name* → [`CatalogIndex`](indexes.md). `None` enables the default index policy (see below). |

A query that names no field targets the `"title"` index by convention: it is searched when
the catalog has one, otherwise the call raises `BroadSearchUnavailableError` and names the
fields that are indexed. Score one named index with `Catalog.search(..., field="...")`. Rank
across several weighted fields with `Catalog.multi_field_search(..., fields={name: weight})`
— `search` no longer accepts `fields=`.

The keys of `indexes` are logical search-surface names — they are the field names you pass to
`field=` / `multi_field_search`'s `fields=` / `filter=` and the names reported by errors. A key
matches an `Entity` field; each index is scoped to exactly one field.

## Index policy: default versus explicit

There are two ways to configure indexes.

**Default index policy** (`indexes=None`). The catalog starts with a placeholder and, at
`build()`, materializes a `BM25Index` for `code`, `title`, and every text/number metadata key
observed across the loaded entities. Nested values and bool flags are skipped (bools remain
filterable facets, not ranking surfaces). This is the zero-configuration path — you get a
searchable catalog over the text fields without naming any index.

```python
from parsimony.catalog import Catalog, Entity

catalog = Catalog("demo")  # indexes=None -> default policy
catalog.set_entities(
    [Entity(namespace="demo", code="a", title="alpha", metadata={"region": "eu"})]
)
catalog.build()
print(sorted(catalog.indexes))  # -> ['code', 'region', 'title']
```

**Explicit indexes**. Pass a dict to take full control. No indexes are ever added silently, and
`set_indexes` permanently switches the catalog off the
default policy. Use this when you want a vector or hybrid backend on a specific field, or want to
restrict search to a known set of surfaces. The available index types — `BM25Index`,
`VectorIndex`, `HybridIndex` — are covered in [Indexes](indexes.md).

## Query and filter at a glance

`search` takes two independent inputs, and the split between them is the central idea of the
API. `query` is **literal text** that *orders* results — never a grammar, so a colon or an
`&&` inside it is just punctuation being matched. `filter` is an exact constraint that
*excludes* rows outright. Pass either, or both.

```python
matches = catalog.search("alpha growth", limit=5)                      # rank by text
matches = catalog.search(filter={"region": ["eu", "us"]}, limit=100)   # enumerate a slice
matches = catalog.search("alpha", filter={"region": "eu"}, limit=5)    # rank within a slice
```

If it matters, filter on it: a constraint written into the query text is only a hint the
ranker may outweigh, while the same constraint as a filter cannot be traded away for
relevance. When you know what a value means but not how the data spells it,
`catalog.search_values("Germany", "geo")` resolves it first so you can filter exactly.

To score several fields at once, declare a weight for each — the caller owns that policy:

```python
matches = catalog.multi_field_search(
    "german growth", fields={"title": 3.0, "description": 1.0}, limit=5,
)
```

The full signatures, the filter forms, the value-resolution primitive, and the search-time
exceptions are documented in [Building and searching](search.md).

## Saving and loading snapshots

A built catalog can be serialized to a directory and reloaded fully built. `save()` and `load()`
both dispatch on a URL scheme:

| Scheme | Example | Notes |
| --- | --- | --- |
| `file://` (or a bare path) | `file:///srv/catalogs/fred` | Local directory snapshot. Works with only `parsimony` plus the index backends used. |
| `hf://` | `hf://acme/economic-catalog` | Hugging Face dataset. Lazily imports `huggingface_hub`; needs the `catalog` extra. |

Any other scheme raises `ValueError`. A snapshot is a directory of `entries.parquet`
(zstd-compressed), an `indexes/<field>/` subtree, and a `meta.json` manifest. Writes are atomic
(staged in a sibling temp directory, then renamed), and `load()` verifies a `content_sha256`
integrity digest and rejects any `schema_version` other than `1`.

```python
from pathlib import Path

from parsimony.catalog import BM25Index, Catalog, Entity

tmp = Path("/tmp/cat-demo")
catalog = Catalog("solo", indexes={"title": BM25Index()})
catalog.set_entities([Entity(namespace="solo", code="A", title="alpha")])
catalog.build()

catalog.save(f"file://{tmp}/snapshot", builder="nightly-job")
loaded = Catalog.load(f"file://{tmp}/snapshot")
print(len(loaded), loaded.entities[0].code)  # -> 1 A
```

!!! note "Loaded catalogs keep exactly what was serialized"
    A loaded catalog is non-dirty and immediately searchable, and the default index policy is
    forced off. Its indexes are precisely what the snapshot stored — calling `build()` on a loaded
    catalog will not re-derive metadata-key indexes. Only `BM25Index`, `VectorIndex`, and
    `HybridIndex` are serializable; any other `CatalogIndex` raises `TypeError` at save time. The
    full layout, integrity model, and the higher-level `load_or_build_catalog` lazy-cache helper
    are in [Snapshots and persistence](snapshots.md).

## The catalog subsystem

This section breaks the catalog down into focused pages:

- **[Entities](entities.md)** — the `Entity` record model, normalization rules, and how
  DataFrames become entities.
- **[Building and searching](search.md)** — the full `Catalog` API, the filter contract,
  `CatalogMatch`, and the search-time exceptions.
- **[Indexes](indexes.md)** — the `CatalogIndex` protocol, the BM25, vector, and hybrid
  backends, and the role-based discovery index policy.
- **[Ranking and fusion](ranking-and-fusion.md)** — how `score` is computed, the fixed RRF
  fusion inside a hybrid field, and where ranking that isn't relevance belongs.
- **[Embedders](embedders.md)** — the `EmbeddingProvider` implementations used by vector and
  hybrid indexes.
- **[Snapshots and persistence](snapshots.md)** — saving, loading, snapshot layout, integrity,
  and the lazy-cache helpers.
- **[Data stores](data-store.md)** — persisting loader output as observations, the loader-side
  counterpart to the catalog.

## See also

- [Entities](entities.md) — the record model a catalog indexes.
- [Building and searching](search.md) — the search API and the filter contract in depth.
- [Indexes](indexes.md) — choosing and configuring index backends.
- [Loaders and enumerators](../connectors/loaders-and-enumerators.md) — how an enumerator feeds a
  catalog.
