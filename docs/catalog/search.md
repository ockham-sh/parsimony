# Building and searching

A [`Catalog`](index.md) is only searchable after you build it. This page covers the
constructor, the entry and index mutators, the `build` gate, the `search` method, and the
small query DSL that decides between structured (`field: value`) and broad (plain-text)
search.

All of the runnable examples here use [`BM25Index`](indexes.md), which needs the optional
`rank-bm25` backend at search time. Install it with the `catalog` extra:

```bash
pip install "parsimony-core[catalog]"
```

!!! note "Imports"
    `Catalog`, `Entity`, `CatalogMatch`, and `BM25Index` are top-level
    (`from parsimony import Catalog`), but they are also re-exported from
    `parsimony.catalog`. For catalog-heavy code, importing everything from the submodule —
    `from parsimony.catalog import Catalog, Entity, BM25Index, ...` — is the clearest
    convention, and it is the only way to reach the names that are *not* top-level
    (`StructuredQuery`, `parse_query`, and the query error types).

## Constructing a catalog

```python
from parsimony.catalog import Catalog, CatalogIndex

Catalog(
    name: str,
    *,
    indexes: dict[str, CatalogIndex] | None = None,
)
```

| Parameter | Default | Behavior |
|---|---|---|
| `name` | required | Normalized to lowercase snake_case via the namespace rule (`^[a-z][a-z0-9_]*$`). A name like `"My Catalog"` raises `ValueError`. |
| `indexes` | `None` | `None` enables the **default index policy** (see below). A dict gives you full control — only those indexes exist, none are added silently. |

Broad (plain-text) search targets the `"title"` index by convention: if the catalog has one,
that's what a plain-text query searches; if not, a plain-text query raises
`BroadSearchUnavailableError` at query time. Any other search surface is declared per call
with `fields=`.

The keys of the `indexes` dict are *logical search-surface names*. They are what you type in
the DSL (`FIELD: value`) and what appears in error messages. A key matches the
[`Entity`](entities.md) field its index reads (`code`, `title`, or a metadata key) — each
index is scoped to exactly one field.

A freshly constructed catalog is *dirty*: you must call `build()` before searching or saving.

### The default index policy

Passing `indexes=None` defers index selection to build time. At `build()`, the catalog
materializes a [`BM25Index`](indexes.md) for `code`, for `title`, and for **every metadata
key** observed across the current entries (sorted).

```python
from parsimony.catalog import Catalog, Entity

catalog = Catalog("demo")  # indexes=None -> default policy
catalog.set_entities(
    [Entity(namespace="demo", code="a", title="alpha", metadata={"region": "eu"})]
)
catalog.build()
print(sorted(catalog.indexes))  # -> ['code', 'region', 'title']
```

Calling `set_indexes` permanently disables the
default policy — once you take manual control, `build()` will not re-derive metadata-key
indexes.

## Loading entries and managing indexes

These mutators change the catalog in memory and mark it dirty. None of them rebuild indexes —
that always happens in `build()`.

| Method | Effect |
|---|---|
| `set_entities(entries: list[Entity])` | Replace all entries. Entries are upserted by `(namespace, code)`, so duplicate keys overwrite earlier ones rather than appending. |
| `set_indexes(indexes)` | Replace the entire index set. Disables the default policy. |
| `get(namespace, code)` | Look up a single `Entity` by key, or `None`. This does *not* require a build. |

The `entities` and `indexes` properties return copies of the current entries and index map.
`len(catalog)` is the entry count.

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("series", indexes={"title": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="series", code="A", title="alpha title"),
        Entity(namespace="series", code="B", title="beta title"),
    ]
)
catalog.build()
print(len(catalog))                 # -> 2
print(catalog.get("series", "A").title)  # -> alpha title
```

## The build gate

`catalog.build()` validates the configuration and materializes every configured index
over the current entries. The rebuild is guarded by a `threading.Lock`, and a single shared
vector cache is threaded through all indexes in one build, so identical texts across fields
are embedded once.

Construction and every mutator (`set_entities`, `set_indexes`) mark the catalog dirty. While
dirty, `search()` and `save()` raise a plain
`ValueError`:

```text
Catalog entries or indexes changed — call catalog.build() before it can be searched
```

!!! warning "Build before searching or saving"
    The build gate is a plain `ValueError`, not a custom error type. Catch it as `ValueError`
    if you need to. `get()` is the only data method that skips the gate. Any change to entries
    or indexes after a build requires another `build()`.

## Searching

```python
def search(
    self,
    query: str | None = None,
    limit: int = 50,
    *,
    fields: str | Sequence[str] | None = None,
    filter: Mapping[str, Sequence[str]] | None = None,
    top_k_values: int = 50,
) -> list[CatalogMatch]
```

The method returns ranked matches as a list.

| Parameter | Default | Meaning |
|---|---|---|
| `query` | `None` | Free-text or structured `FIELD: value` query. Omit for filter-only search. |
| `limit` | `50` | Maximum results returned. |
| `fields` | `None` | Declare the scoring surface explicitly: one field name for single-field scoring, or several to fuse — see [multi-field search](#multi-field-search-fields) below. Omit to search the catalog's `title` index with DSL resolution. |
| `filter` | `None` | Exact AND filter: `{column: [allowed_values, ...]}`. Can combine with `query`. |
| `top_k_values` | `50` | Per-field cap on the scored-value table. A deliberate noise floor, not only a cost cap — see [Ranking and fusion](ranking-and-fusion.md#top_k_values-a-noise-floor-not-just-a-cost-cap). |

`search()` first calls the build gate, then parses the query to choose between structured and
broad mode (unless `fields=` is given, which bypasses DSL parsing entirely — see below).

Every match's `coverage`, `score`, and `matched` come from one scoring path: how they're
computed, and how rows are ordered from them, is covered in
[Ranking and fusion](ranking-and-fusion.md). The short version: `coverage` is a **fact**
(the fraction of the query's tokens covered by the union of the row's fully *consumed*
field values — cells whose every token is in the query, so a cell that claims anything
extra doesn't count), `score` is a **guess** (relative BM25 + semantic similarity), and
facts outrank guesses. Since that changes how results are ordered: a search across a
single field (broad search, a DSL clause, or `fields=["one_field"]`) ranks an
exact/subset containment hit (`coverage == 1.0`) above every fuzzy-scored row, then
orders the rest by `score` alone; a search across several fields (`fields=[...]`) ranks
by `(coverage desc, score desc)` throughout, since coverage there counts how many of the
named fields the row fully satisfies.

### Broad search

If the query does **not** start with a `field:` prefix, it is a broad query against the
catalog's `"title"` index. The query is scored against that one index.

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("artifact", indexes={"title": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="series", code="A", title="alpha title"),
        Entity(namespace="series", code="B", title="beta title"),
    ]
)
catalog.build()
hits = catalog.search("alpha", limit=1)
print(hits[0].code)  # -> A
```

If the catalog has no `"title"` index, a plain-text query raises `BroadSearchUnavailableError`:

```text
This catalog only supports structured queries. Use 'field: value' syntax. Indexed fields: ['code']
```

### The structured query DSL

A query is *structured* if and only if it matches the regex `^\s*\w+\s*:` — that is, it begins
with a word followed by a colon. The grammar:

- `FIELD: v1, v2` lists values separated by `,`, which are **OR**ed (any value matching
  contributes).
- Core `Catalog.search` accepts one soft-scored structured clause. Use `filter=` for exact
  AND constraints such as dimension/code filters on parquet-backed catalogs.

Each clause field must have a configured index, otherwise the parse raises
`UnknownIndexedFieldError`. Scoring within the clause keeps the maximum positive score per row
across the OR values.

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("test_cat")
catalog.set_indexes(
    {"title": BM25Index(), "ref_area": BM25Index(), "icp_item": BM25Index()}
)
catalog.set_entities(
    [
        Entity(namespace="ns", code="A", title="series a",
               metadata={"ref_area": "Germany", "icp_item": "energy"}),
        Entity(namespace="ns", code="B", title="series b",
               metadata={"ref_area": "Italy", "icp_item": "energy"}),
        Entity(namespace="ns", code="C", title="series c",
               metadata={"ref_area": "Germany", "icp_item": "food"}),
    ]
)
catalog.build()
res = catalog.search("ref_area: Germany", filter={"icp_item": ["energy"]}, limit=5)
print({m.code for m in res})   # -> {'A'}
```

!!! note "A bare field token is still broad"
    The structured trigger requires a colon. A query like `ref_area` (no colon) does not match
    the regex and is treated as a broad query against the `title` index, not a structured one.

The DSL parser is also available directly when you want to inspect or validate a query without
running it:

```python
from parsimony.catalog import parse_query, StructuredQuery

parsed = parse_query("ref_area: Germany, Italy && freq: M", known_fields={"ref_area", "freq"})
print(parsed.clauses)
# -> [('ref_area', ['Germany', 'Italy']), ('freq', ['M'])]

# A plain-text query parses to None (broad).
print(parse_query("inflation", known_fields={"ref_area"}))  # -> None
```

`parse_query(q, known_fields)` returns `None` for a broad query, a `StructuredQuery`
(a frozen dataclass with a `clauses: list[tuple[str, list[str]]]` field) for a structured one,
raises `ValueError` for a malformed clause (empty field, no values), and
`UnknownIndexedFieldError` when a clause names a field not in `known_fields`.

### Multi-field search (`fields=`)

Pass `fields=[...]` to score a query against several named indexes in one call and fuse
the results — for example searching `title` and a `region` metadata field together for
one plain-text query. This bypasses the DSL entirely: `query` is **always literal text**
when `fields=` is given, never parsed as `FIELD: value`.

```python
matches = catalog.search("german growth", fields=["title", "ref_area"], limit=10)
```

Two things differ from single-field search:

- **`score` sums each field's normalized contribution.** Each named field contributes
  `0.0`–`1.0` (its own best match, relative to that field's own top score for this
  query), so a row that agrees across two fields outscores one that only agrees on one,
  and no single field's raw magnitude dominates.
- **`coverage` unions consumed tokens across the named fields**, and row ranking
  switches from the single-field "exact pin, then score" tier to `(coverage desc, score
  desc)` throughout — see [Ranking and fusion](ranking-and-fusion.md#row-ranking-coverage-tiers-by-surface-arity).

A hybrid field named in `fields=` also picks up a different fusion regime for its BM25 +
vector components (lexical-first, semantic void-fill, rather than the RRF used when the
field is searched alone) — see
[the two fusion regimes](ranking-and-fusion.md#two-regimes-picked-by-surface-arity).

A single-name `fields=["title"]` still takes the single-field path (RRF fusion, exact-pin
row ranking) — multiple fields is what changes the regime, not the presence of `fields=`
itself.

### The discovery-connector surface

Provider packages ship their search connectors through `make_local_search_connector`
(`parsimony.catalog.search`), which takes a `search_fields=` parameter — the connector's
declared search surface. The default is the **entity recipe**, `ENTITY_SEARCH_FIELDS =
("title", "description")`: the surface for catalogs whose rows carry curated descriptive
text. Connectors over ontology-shaped catalogs — rows composed of codelist members with no
curated text, e.g. SDMX series — declare their label columns instead. The declaration is
intersected with the loaded catalog's indexes at query time, so a published catalog that
lacks one of the declared indexes still searches. Either way the query is always literal
text — factory connectors expose no `FIELD: value` DSL, and exact reads go through `filter=`
instead. With both `title` and `description` present this is a two-field surface, so it
takes the facet regime above: lexical evidence first, semantic void-fill only where nothing
literal matches.

A field earns its place on a declared text surface only if it is *curated* — it carries
meaning or binding beyond the member labels it already contains. A fabricated concatenation
of labels does not qualify.

Two deliberate consequences:

- **Descriptions are searched, not shipped.** Result rows stay lean — code, title, score,
  provider metadata — while the description index contributes lexical evidence invisibly.
  Shipping paragraph-length descriptions on every page would multiply the context cost of
  discovery many times over, only to hand the agent text the engine has already read with
  exact-token attention.
- **The ranking trio is the receipt for that delegation.** Because evidence can come from
  a field the row doesn't display, none of it is derivable from the visible columns. The
  factory appends `RANKING_COLUMNS` — `coverage`, `score`, `matched` — to every search
  connector's output spec, and the hand-rolled catalog searches reuse the same constants,
  so every ranked page on every provider ends with the same three columns with the same
  meanings. `coverage` is the provable fraction of the query (mostly 0.0 on prose
  catalogs; a 1.0 explains a row pinned above higher scores), `score` the fuzzy
  similarity, `matched` the evidence origin (null on filter-only reads); an
  all-`semantic` page means nothing literal matched anywhere — rephrase rather than
  trust the order. What varies per surface is the distribution of values, never the
  schema or the semantics.

### Reading results

Each match is a `CatalogMatch` — a Pydantic model carrying the resolved entity fields plus
the ranking evidence:

| Field | Type | Notes |
|---|---|---|
| `namespace` | `str` | Re-normalized to lowercase snake_case. |
| `code` | `str` | Trimmed, non-empty. |
| `title` | `str` | Trimmed, non-empty. |
| `score` | `float` | Relative to this query's best hit; higher is better within this result set only — never comparable across queries or catalogs. |
| `coverage` | `float` | Defaults to `0.0`. Fraction of the query's tokens consumed by the row's fully-consumed field value(s); `1.0` is an exact/subset containment hit. |
| `matched` | `"lexical" \| "semantic" \| "both" \| None` | Defaults to `None`. Which component surfaced this row — `None` for a filter-only match. An all-`"semantic"` result page means nothing lexically real matched. |
| `metadata` | `dict[str, Any]` | Shallow copy of the entity's metadata. |

See [Ranking and fusion](ranking-and-fusion.md) for what these three fields mean in
practice and how they interact.

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("demo", indexes={"title": BM25Index()})
catalog.set_entities([Entity(namespace="demo", code="A", title="alpha title")])
catalog.build()
matches = catalog.search("alpha", limit=5)
top = matches[0]
print(top.namespace, top.code, top.title)  # -> demo A alpha title
print(top.coverage, top.matched)           # -> 0.0 lexical  ("alpha title" isn't fully contained in "alpha")
```

## Query errors

Both query errors subclass `ValueError`, so a broad `except ValueError` catches them, or you
can match by type. Import them from `parsimony.catalog`.

| Error | When raised |
|---|---|
| `UnknownIndexedFieldError` | A structured clause references a field with no configured index. Raised during query parsing. |
| `BroadSearchUnavailableError` | A plain-text query is issued but the catalog has no `"title"` index. Raised at search time. |

```python
from parsimony.catalog import (
    BroadSearchUnavailableError,
    UnknownIndexedFieldError,
)

for exc in (UnknownIndexedFieldError, BroadSearchUnavailableError):
    assert issubclass(exc, ValueError)
```

## Sparse fields and empty results

Indexes read entity fields through the catalog's field-extraction rules: entries that are
missing a metadata field, or that have empty-string values for it, contribute no postings to
that index. A broad search over a `description` index therefore returns only entries that
actually have a non-empty description, and an entirely empty index builds fine and returns
`[]`. See [Entities](entities.md) for the exact extraction rules.

## Ad-hoc runtime catalogs

Everything above is the lifecycle for building a catalog: construct, load entities, build,
search, save. For the opposite case — a DataFrame you produced this moment and just want to
find rows in — `auto_catalog` (a top-level `parsimony` import) collapses the whole
lifecycle into one call and hands back an *already-built* catalog:

```python
from parsimony import auto_catalog

cat = auto_catalog(df)                    # one Entity per row, every column indexed
matches = cat.search("unemployment", limit=20)  # already built — no build() needed
row = df.iloc[int(matches[0].code)]             # code is the row position
```

Each row becomes one entity: `code` is the row's positional index (so
`df.iloc[int(match.code)]` recovers the full row), `title` is the joined non-null cell text
(broad search), and every column is stored as metadata (structured `column: value` search).
Indexing is BM25 only under the [default index policy](index.md#index-policy-default-versus-explicit) —
there is no vector mode, because a runtime frame ships no prebuilt vectors and the typical
caller (a sandboxed agent) has no embedder.

This is a convenience for searching data you already hold, **not** the way catalogs are built.
When you need column roles, key grouping, a vector index, or a persistable snapshot, use the
`Catalog` lifecycle directly with [`Result.entities`](entities.md#result-entities).
BM25 works on a bare `pip install parsimony-core` — no extra needed.

## See also

- [The Catalog](index.md) — the catalog lifecycle at a glance
- [Indexes](indexes.md) — `BM25Index`, `VectorIndex`, `HybridIndex`, and the discovery index policy
- [Ranking and fusion](ranking-and-fusion.md) — how `coverage`, `score`, and `matched` are computed
- [Entities](entities.md) — the `Entity` model and how fields become searchable text
- [Snapshots and persistence](snapshots.md) — `save`/`load` and the build gate on save
