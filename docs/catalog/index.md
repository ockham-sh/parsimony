# The Catalog

A `Catalog` is Parsimony's discovery layer: a portable, in-memory index over normalized
[`Entity`](entities.md) records. You load entities into it, build per-field indexes, then run
structured or plain-text searches that return ranked matches. The same catalog can be saved to
a local directory or a Hugging Face dataset and loaded back, fully built and searchable.

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
    embedder — via the `catalog` extra: `pip install "parsimony-core[catalog]"`. See
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
install, so a plain `pip install parsimony-core` is enough — no extra needed.

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
Catalog(name: str, *, indexes: dict[str, CatalogIndex] | None = None, default_field: str | None = None)
```

| Parameter | Meaning |
| --- | --- |
| `name` | Catalog identity. Normalized to lowercase snake_case via `normalize_namespace` — uppercase or hyphenated names raise `ValueError` (e.g. `"My Catalog"` is rejected; pass `"my_catalog"`). |
| `indexes` | A map of *search-surface name* → [`CatalogIndex`](indexes.md). `None` enables the default index policy (see below). |
| `default_field` | The field used for broad (plain-text) search. Defaults to `"title"` when a `title` index exists, otherwise broad search is disabled. |

The keys of `indexes` are logical search-surface names — they are the field names you use in the
query DSL (`FIELD: value`) and the names reported by errors. A key matches an `Entity` field;
each index is scoped to exactly one field.

!!! warning "default_field must have a backing index"
    If you pass an explicit `indexes` dict together with a `default_field` that the dict does not
    cover, the constructor raises `BroadSearchConfigError` (a `ValueError` subclass) immediately —
    not later at `build()`. Under the default index policy the check is deferred to `build()`,
    since the indexes do not exist yet at construction time.

## Index policy: default versus explicit

There are two ways to configure indexes.

**Default index policy** (`indexes=None`). The catalog starts with a placeholder and, at
`build()`, materializes a `BM25Index` for `code`, `title`, and every metadata key observed across
the loaded entities. This is the zero-configuration path — you get a searchable catalog over every
field without naming any index.

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

## Structured versus broad search at a glance

`search(query, limit, *, namespaces=None)` inspects the query string and picks one of two modes.

A query is **structured** when it begins with a `FIELD:` token (it matches `^\s*\w+\s*:`).
Core `Catalog.search` accepts one soft-scored structured clause; comma-separated values within
that clause are OR-merged. Use `filter=` for exact AND constraints. Every referenced field must
have an index, or the parse raises `UnknownIndexedFieldError`.

```python
matches = catalog.search("title: alpha", filter={"region": ["eu", "us"]}, limit=5)
```

Any query that does not start with a `FIELD:` token is a **broad** query, scored against the
`default_field`. If no broad field is configured, `search()` raises `BroadSearchUnavailableError`.

```python
matches = catalog.search("alpha growth", limit=5)
```

The optional `namespaces` argument post-filters results to entities whose namespace is in the given list.
The full DSL, result shape, and the search-time exceptions are documented in
[Building and searching](search.md).

## Saving and loading snapshots

A built catalog can be serialized to a directory and reloaded fully built. `save()` and `load()`
both dispatch on a URL scheme:

| Scheme | Example | Notes |
| --- | --- | --- |
| `file://` (or a bare path) | `file:///srv/catalogs/fred` | Local directory snapshot. Works with only `parsimony-core` plus the index backends used. |
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
- **[Building and searching](search.md)** — the full `Catalog` API, the query DSL,
  `CatalogMatch`, and the search-time exceptions.
- **[Indexes](indexes.md)** — the `CatalogIndex` protocol and the BM25, vector, and hybrid
  backends, plus the adaptive selection policies.
- **[Ranking and fusion](ranking-and-fusion.md)** — `Ranking`, the `Ranker` protocol, and the
  `RRF` / `ZScoreFusion` / `MinMaxScoreFusion` fusion strategies.
- **[Embedders](embedders.md)** — the `EmbeddingProvider` implementations used by vector and
  hybrid indexes.
- **[Snapshots and persistence](snapshots.md)** — saving, loading, snapshot layout, integrity,
  and the lazy-cache helpers.
- **[Data stores](data-store.md)** — persisting loader output as observations, the loader-side
  counterpart to the catalog.

## See also

- [Entities](entities.md) — the record model a catalog indexes.
- [Building and searching](search.md) — the search API and query DSL in depth.
- [Indexes](indexes.md) — choosing and configuring index backends.
- [Loaders and enumerators](../connectors/loaders-and-enumerators.md) — how an enumerator feeds a
  catalog.
