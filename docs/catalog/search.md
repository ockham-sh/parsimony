# Building and searching

A [`Catalog`](index.md) is only searchable after you build it. This page covers the
constructor, the entry and index mutators, the `build` gate, and the four query methods —
`search`, `multi_field_search`, `search_values`, and `iter_rows` — along with the one
filter contract they all share.

All of the runnable examples here use [`BM25Index`](indexes.md), which needs the optional
`rank-bm25` backend at search time. Install it with the `catalog` extra:

```bash
pip install "parsimony[catalog]"
```

!!! note "Imports"
    `Catalog`, `Entity`, `CatalogMatch`, `BM25Index`, `F`, and `Filter` are
    top-level (`from parsimony import Catalog, F`). They are also re-exported from
    `parsimony.catalog`. For catalog-heavy code, importing everything from the submodule —
    `from parsimony.catalog import Catalog, Entity, BM25Index, F, ...` — is the clearest
    convention, and it is the only way to reach the names that are *not* top-level
    (`FilterLike`, `all_of` / `as_filter`, the concrete `Field*` predicates, and the
    query error types).

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

A `search` call that omits `field=` targets the `"title"` index by convention: if the
catalog has one, that's what a query is scored against; if not, the call raises
`BroadSearchUnavailableError` and names the fields that *are* indexed, so the caller can
pick one explicitly.

The keys of the `indexes` dict are *logical search-surface names*. They are what you pass to
`field=`, what you name in `fields=`, and what appears in error messages. A key matches the
[`Entity`](entities.md) field its index reads (`code`, `title`, or a metadata key) — each
index is scoped to exactly one field.

A freshly constructed catalog is *dirty*: you must call `build()` before searching or saving.

### The default index policy

Passing `indexes=None` defers index selection to build time. At `build()`, the catalog
materializes a [`BM25Index`](indexes.md) for `code`, for `title`, and for every
**text/number** metadata key observed across the current entries (sorted). Nested
metadata and **bool** flags are skipped — bools stay on the entity for `filter=`
only; ranking `"true"`/`"false"` is not useful.

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
dirty, every query method and `save()` raise a plain `ValueError`:

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
    field: str | None = None,
    filter: FilterLike | None = None,
    top_k_values: int = 50,
) -> list[CatalogMatch]
```

The method returns ranked matches as a list.

| Parameter | Default | Meaning |
|---|---|---|
| `query` | `None` | Literal text to rank by. Omit for a filter-only enumeration. |
| `limit` | `50` | Maximum results returned. |
| `field` | `None` | The one indexed field to score. Omit to use the catalog's default broad field (`title`). To combine several fields, use [`multi_field_search`](#weighted-multi-field-search). |
| `filter` | `None` | Exact AND constraint that excludes non-matching rows — see [Filters](#filters). Combines with `query`. |
| `top_k_values` | `50` | Cap on the scored-value table. A deliberate noise floor, not only a cost cap — see [Ranking and fusion](ranking-and-fusion.md#the-candidate-value-cap-a-noise-floor-not-just-a-cost-cap). |

At least one of `query` and `filter` must be given; a call with neither raises
`InvalidParameterError` rather than quietly enumerating the whole catalog.
A blank or whitespace-only `query` is treated the same as omitting it — so
`search("")` without a filter also raises, while `search("", filter=...)` is a
filter-only enumeration.

### `query` is literal text — always

There is no query grammar. A colon, a comma, or an `&&` inside `query` is punctuation to be
matched, not a field scope or a boolean operator:

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("demo", indexes={"title": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="demo", code="A", title="Quarterly services Germany"),
        Entity(namespace="demo", code="B", title="Quarterly services France"),
    ]
)
catalog.build()
print([m.code for m in catalog.search("services", limit=2)])       # -> ['A', 'B']
print([m.code for m in catalog.search("item: Services", limit=2)]) # -> ['A', 'B']
```

The second query is not a field-scoped lookup: `item` and `services` are simply two tokens
scored against the `title` index, and `item` happens to match nothing. This is the design,
not a limitation. A grammar embedded in a free-text parameter has to guess whether the
caller meant punctuation or syntax, and it guesses wrong on real titles ("GDP: annual
rate"). Anything you want *enforced* is a [`filter`](#filters), which cannot be
misread as text; `query` only orders what the filter left standing.

### Choosing the scored field

`field=` names the one index to score against. Omit it and the catalog's `title` index is
used:

```python
matches = catalog.search("germany", field="title", limit=5)
```

A catalog with no `title` index raises `BroadSearchUnavailableError` on a query that omits
`field=`, listing the indexes it does have:

```text
This catalog has no default search field, so field= is required. Indexed fields: ['code']
```

`field=` requires a non-empty `query=` — pairing it with a filter-only call raises
`InvalidParameterError`, because there would be nothing for the field to score.

### Filter-only enumeration

Omit `query` to enumerate the filtered slice instead of ranking it. There is nothing to
rank — every row satisfies the filter equally — so each match reports `score=None` and
`search_detail=None`:

```python
matches = catalog.search(filter={"geo": "Germany"}, limit=100)
```

When you want the rows rather than result models, prefer [`iter_rows`](#streaming-rows)
— it skips model validation entirely.

## Filters

One filter contract serves `search`, `multi_field_search`, and `iter_rows`. The mapping
shorthand covers the common case: separate keys are ANDed, and a list of values is
membership (OR) within one field.

```python
{"geo": "DE"}                          # geo == "DE"
{"geo": ["DE", "FR"]}                  # geo in ("DE", "FR")
{"geo": "DE", "freq": ["M", "Q"]}      # geo == "DE" AND freq in ("M", "Q")
```

A bare string stays atomic — `{"geo": "DE"}` is one value, never the characters `["D", "E"]`.
An empty value list raises `InvalidParameterError` rather than silently dropping the
constraint, since a filter that matched everything would be indistinguishable from no filter
at all.

For nested Boolean logic, build the tree with the fluent form. `F` names a field, `all_of`
and `any_of` combine, and `&` / `|` are shorthand for the same:

```python
from parsimony.catalog import F, all_of, any_of

flt = all_of(
    F("geo").eq("DE"),
    any_of(F("freq").eq("M"), F("freq").eq("Q")),
)
flt = F("geo").eq("DE") & F("freq").is_in(["M", "Q"])   # equivalent
```

Pattern predicates (prefix / substring / regex) are filter ops too — use the fluent form
or the serializable expression keys `prefix`, `contains`, and `match`. They are **not**
available in the equality shorthand (`{"code": "D.*"}` is still exact equality on that
literal string):

```python
F("code").prefix("D.USD.")
F("code").contains("EUR")
F("key").matches(r"^D\.[A-Z]{3}\.EUR\.")
{"field": "code", "prefix": "D.USD."}
```

There is also a serializable expression form, for filters that arrive over a wire:

```python
{"all": [{"field": "geo", "eq": "DE"},
         {"field": "freq", "in": ["M", "Q"]}]}
```

`all`, `any`, and `field` are reserved at the top of a mapping, so a column literally named
one of those must be filtered through the fluent form.

Whichever spelling you use, the tree compiles once to a single
`pyarrow.dataset.Expression` for pushdown on a parquet-backed catalog, and the identical
tree evaluates over in-memory rows for a row-indexed one — so the two layouts agree by
construction rather than by two implementations happening to match. Field names are
*logical*: `code` and `title` address whatever physical columns the catalog maps them to.

On a row-indexed catalog, a filter naming a field no entity carries raises
`InvalidParameterError` listing the available fields, so a typo cannot silently match
nothing.

## Weighted multi-field search

```python
def multi_field_search(
    self,
    query: str,
    *,
    fields: Mapping[str, float],
    filter: FilterLike | None = None,
    limit: int = 20,
    candidate_values: int = 50,
) -> list[CatalogMatch]
```

Pass `fields={name: weight}` to score one literal query against several indexes and fuse
them with Level-2 Reciprocal Rank Fusion (see [ranking and fusion](ranking-and-fusion.md)).
Weights scale each field's rank contribution — not BM25/cosine magnitudes — so a row
agreeing across two fields outscores one agreeing on a single field:

```python
matches = catalog.multi_field_search(
    "quarterly services germany",
    fields={"freq": 1.0, "item": 1.0, "geo": 1.0},
    limit=10,
)
```

The caller declares the weights, which is the point: `{"title": 3.0, "description": 1.0}`
states that a title hit is worth three description hits. Ranking policy is *declared* rather
than inferred from how many fields happened to be passed, so adding a field to the surface
cannot silently change what the existing fields mean. Every weight must be positive and
finite; anything else raises `InvalidParameterError`, and an unindexed field name raises
`UnknownIndexedFieldError`.

Rows order by `(score desc, namespace, code)`. `Catalog.search` is the one-field case of
this method — `search(q, field="title")` is `multi_field_search(q, fields={"title": 1.0})`.

Candidates are pooled at the **distinct value** level, never by joining per-field pages of
rows, and the candidate rows are scanned exactly once. Thousands of rows can share one
scored value, so a bounded value table is not a bounded row set. `candidate_values` caps
each field's value table; see
[the candidate-value cap](ranking-and-fusion.md#the-candidate-value-cap-a-noise-floor-not-just-a-cost-cap)
for why that bound is a noise floor as well as a cost cap.

## Resolving values

```python
def search_values(
    self,
    query: str,
    field: str,
    *,
    limit: int = 20,
) -> list[CatalogValueMatch]
```

`search_values` is the resolution primitive: it ranks one field's **distinct** indexed
values, not rows. Use it when you know what a value *means* but not how it is spelled in the
data — resolve the value here, read it off the result, then filter exactly on it:

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("demo", indexes={"title": BM25Index(), "geo": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="demo", code="A", title="Unemployment rate", metadata={"geo": "Germany"}),
        Entity(namespace="demo", code="B", title="Unemployment rate", metadata={"geo": "France"}),
    ]
)
catalog.build()

candidates = catalog.search_values("Germany", "geo", limit=5)
print(candidates[0].value, candidates[0].exact)   # -> Germany True

matches = catalog.search("unemployment", filter={"geo": candidates[0].value}, limit=10)
print([m.code for m in matches])                  # -> ['A']
```

Values order by `(exact desc, score desc)`: the value the query literally names outranks
every fuzzy candidate, and near-misses stay visible below it. Prefer this sequence over
pushing `"germany"` into the query text and hoping the ranker prioritizes it — the filter
*enforces* the constraint, while a query can only order what survives one.

Each result is a `CatalogValueMatch`:

| Field | Type | Notes |
|---|---|---|
| `value` | `str` | The distinct indexed value. |
| `score` | `float` | Fuzzy relevance, relative to this query's best value. |
| `exact` | `bool` | Case-folded, whitespace-trimmed equality of query and value. Nothing softer. |
| `search_detail` | `SearchDetail \| None` | Per-value ranking evidence (field/component traces). |
| `linked_value` | `str \| None` | The linked field's value, when the catalog declares a `field_links` entry for this field — e.g. the canonical code behind a human-readable label. |

An unindexed `field` raises `UnknownIndexedFieldError`.

## Streaming rows

```python
def iter_rows(
    self,
    *,
    filter: FilterLike | None = None,
    columns: Sequence[str] | None = None,
) -> Iterator[dict[str, Any]]
```

`iter_rows` streams a filtered slice as plain dict rows — no ranking, no result models. It
is the efficient read path when you want the data rather than a ranked page:

```python
from parsimony.catalog import F

for row in catalog.iter_rows(filter=F("geo").eq("Germany"), columns=["code", "title"]):
    print(row)   # -> {'code': 'A', 'title': 'Unemployment rate'}
```

A parquet-backed catalog compiles `filter` to an Arrow predicate and projects `columns` in
the scan; a row-indexed catalog evaluates the identical filter tree over its entities. Both
`filter` and `columns` address fields by their **logical** names, and each requested column
name is the key you get back — so a caller never has to know which physical parquet column
carries `code` or `title`.

Omit `columns` to receive the native row (parquet column names, or
`namespace`/`code`/`title` plus metadata keys for a row-indexed catalog). An empty
`columns` list raises `InvalidParameterError`. Row ordering is unspecified. The filter is
validated at the call, not on first iteration, so a bad filter raises where you wrote it.

## The discovery-connector surface

Provider packages ship their search connectors through `make_local_search_connector`
(`parsimony.catalog.search`), which takes a `ranking_fields=` parameter — the connector's
declared ranking surface as `{field: positive weight}`, ranked by `multi_field_search`. The
default is the **entity recipe**, `ENTITY_RANKING_FIELDS = {"title": 1.0, "description":
1.0}`: the surface for catalogs whose rows carry curated descriptive text, weighted
uniformly because both are curated prose about the same row and no measurement so far
justifies preferring one. Connectors over ontology-shaped catalogs — rows composed of
codelist members with no curated text, e.g. SDMX series — declare their label fields and
their own weights instead. The declaration is intersected with the loaded catalog's indexes
at query time, so a published catalog that lacks one of the declared indexes still searches.

Either way the query is always literal text — factory connectors expose no query grammar,
and exact reads go through `filter=`.

A field earns its place on a declared text surface only if it is *curated* — it carries
meaning or binding beyond the member labels it already contains. A fabricated concatenation
of labels does not qualify.

Two deliberate consequences:

- **Ranking searches text; the hit table ships what the OutputSpec declares.** Declared
  ranking fields (often including description) contribute lexical evidence whether or not
  they appear as columns. Hit rows stay lean by default — a connector adds description (or
  other bag fields) only when it declares them as ``ColumnRole.METADATA``. Shipping
  paragraph-length text on every page without that curation would multiply discovery context
  cost for little gain.
- **The ranking pair is the receipt for that delegation.** Because evidence can come from a
  field the row doesn't display, none of it is derivable from the visible columns. The
  factory appends `RANKING_COLUMNS` — `score`, `search_detail` — to every search connector's
  output spec, and the hand-rolled catalog searches reuse the same constants, so every
  ranked page on every provider ends with the same two columns with the same meanings.
  `score` is similarity relative to this query's best hit; `search_detail` is optional
  JSON evidence (hidden from `to_llm()`); null on filter-only reads. Ranked rows are a
  shortlist — commit from provider metadata, not ranking evidence. What varies per surface
  is the distribution of values, never the schema or the semantics.

There is no third "fact" column. A weighted surface already states its policy in the
declared field weights, and what counts as an exact hit is a domain question only the
connector can answer — so a connector that has such a fact filters or tiers on it itself
instead of every provider carrying a generic column for it.

## Reading results

Each match is a `CatalogMatch` — a Pydantic model carrying the resolved entity fields plus
the ranking evidence:

| Field | Type | Notes |
|---|---|---|
| `namespace` | `str` | Re-normalized to lowercase snake_case. |
| `code` | `str` | Trimmed, non-empty. |
| `title` | `str` | Trimmed, non-empty. |
| `score` | `float \| None` | Ranked: relative to this query's best hit in `(0, 1]`. Filter-only: `None` (nothing ranked). Never comparable across queries or catalogs. |
| `search_detail` | `SearchDetail \| None` | Defaults to `None`. Typed ranking evidence (fields, weights, component raw scores/ranks, candidate_limit). `None` for filter-only matches. |
| `metadata` | `dict[str, Any]` | Shallow copy of the entity's metadata. |

Ranked rows order by `(score desc, namespace, code)` — deterministic, with no tier the caller did
not ask for. See [Ranking and fusion](ranking-and-fusion.md) for how `score` is computed and
where ranking that isn't relevance belongs.

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("demo", indexes={"title": BM25Index()})
catalog.set_entities([Entity(namespace="demo", code="A", title="alpha title")])
catalog.build()
matches = catalog.search("alpha", limit=5)
top = matches[0]
print(top.namespace, top.code, top.title)  # -> demo A alpha title
print(round(top.score, 3), top.search_detail is not None)    # -> 1.0 True
```

## Query errors

Both query errors subclass `InvalidParameterError`, so a broad `except
InvalidParameterError` catches them along with every other bad-argument rejection
(`limit < 1`, a malformed filter, a query and filter both omitted), or you can match by
type. Import them from `parsimony.catalog`.

| Error | When raised |
|---|---|
| `UnknownIndexedFieldError` | A `field=` or `fields=` name has no configured index. |
| `BroadSearchUnavailableError` | A query omits `field=` but the catalog has no `"title"` index. |

```python
from parsimony.catalog import BroadSearchUnavailableError, UnknownIndexedFieldError
from parsimony.errors import ConnectorError, InvalidParameterError

for exc in (UnknownIndexedFieldError, BroadSearchUnavailableError):
    assert issubclass(exc, InvalidParameterError)
    assert issubclass(exc, ConnectorError)
```

!!! warning "These are `ConnectorError`s, not `ValueError`s"
    `InvalidParameterError` derives from `ConnectorError`, which derives directly from
    `Exception` — so `except ValueError` does **not** catch them. The build gate, by
    contrast, *is* a plain `ValueError`. See [Errors](../connectors/errors.md).

## Sparse fields and empty results

Indexes read entity fields through the catalog's field-extraction rules: entries that are
missing a metadata field, or that have empty-string values for it, contribute no distinct
values to that index. A search over a `description` index therefore returns only entries that
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
(what a `field=`-less query scores against), and every column is stored as metadata, so any
column is available to `field=`, `fields=`, and `filter=`.
Indexing is BM25 only under the [default index policy](index.md#index-policy-default-versus-explicit) —
there is no vector mode, because a runtime frame ships no prebuilt vectors and the typical
caller (a sandboxed agent) has no embedder.

This is a convenience for searching data you already hold, **not** the way catalogs are built.
When you need column roles, key grouping, a vector index, or a persistable snapshot, use the
`Catalog` lifecycle directly with [`Result.entities`](entities.md#result-entities).
BM25 works on a bare `pip install parsimony` — no extra needed.

## See also

- [The Catalog](index.md) — the catalog lifecycle at a glance
- [Indexes](indexes.md) — `BM25Index`, `VectorIndex`, `HybridIndex`, and the discovery index policy
- [Ranking and fusion](ranking-and-fusion.md) — how `score` and `search_detail` are computed
- [Entities](entities.md) — the `Entity` model and how fields become searchable text
- [Snapshots and persistence](snapshots.md) — `save`/`load` and the build gate on save
