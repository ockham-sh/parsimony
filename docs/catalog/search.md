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
    (`SearchDiagnostic`, `StructuredQuery`, `parse_query`, and the query error types).

## Constructing a catalog

```python
from parsimony.catalog import Catalog, CatalogIndex

Catalog(
    name: str,
    *,
    indexes: dict[str, CatalogIndex] | None = None,
    default_field: str | None = None,
)
```

| Parameter | Default | Behavior |
|---|---|---|
| `name` | required | Normalized to lowercase snake_case via the namespace rule (`^[a-z][a-z0-9_]*$`). A name like `"My Catalog"` raises `ValueError`. |
| `indexes` | `None` | `None` enables the **default index policy** (see below). A dict gives you full control — only those indexes exist, none are added silently. |
| `default_field` | `None` | The search-surface name used for broad (plain-text) search. If `None`, broad search falls back to `"title"` when a `"title"` index exists, otherwise broad search is disabled. |

The keys of the `indexes` dict are *logical search-surface names*. They are what you type in
the DSL (`FIELD: value`) and what appears in error messages. By convention a key matches the
[`Entity`](entities.md) field its index reads (`code`, `title`, or a metadata key), but a
composite index such as [`DisMaxIndex`](indexes.md) can expose one surface name while reading
several entity fields internally.

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

Calling any of `set_index`, `set_indexes`, or `update_indexes` permanently disables the
default policy — once you take manual control, `build()` will not re-derive metadata-key
indexes.

!!! warning "`default_field` and an explicit `indexes` dict"
    If you set `default_field` together with an explicit `indexes` dict that does not contain
    it, the catalog raises `BroadSearchConfigError` **at construction time**. With the default
    index policy active (`indexes=None`), that check is deferred to `build()`, where the same
    error is raised if the field is still not covered.

## Loading entries and managing indexes

These mutators change the catalog in memory and mark it dirty. None of them rebuild indexes —
that always happens in `build()`.

| Method | Effect |
|---|---|
| `set_entities(entries: list[Entity])` | Replace all entries. Entries are upserted by `(namespace, code)`, so duplicate keys overwrite earlier ones rather than appending. |
| `delete_many(keys)` | Remove entries by `(namespace, code)` pairs. Returns the count removed (`0` if none matched). |
| `set_index(field, index)` | Replace one field index. Disables the default policy. |
| `update_indexes(indexes)` | Merge field indexes into the current set. Disables the default policy. |
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

Construction and every mutator (`set_entities`, `set_index`, `set_indexes`, `update_indexes`,
`delete_many`) mark the catalog dirty. While dirty, `search()` and `save()` raise a plain
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
    query: str,
    limit: int,
    *,
    namespaces: list[str] | None = None,
) -> tuple[list[CatalogMatch], SearchDiagnostic]
```

`query` and `limit` are positional and both required; `namespaces` is keyword-only. The
method returns a tuple of the ranked matches and a diagnostic describing how the query ran.

- `limit` caps the number of results. (Whole tie-groups can slightly exceed it — see
  [Ranking and fusion](ranking-and-fusion.md).)
- `namespaces`, when given, post-filters the ranking to entries whose normalized namespace is
  in the allowed set.

`search()` first calls the build gate, then parses the query to choose between structured and
broad mode.

### Broad search

If the query does **not** start with a `field:` prefix, it is a broad query against the
catalog's resolved default field. The query is scored against that one index and the
diagnostic reports `mode="broad"`.

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
hits, diag = catalog.search("alpha", limit=1)
print(diag.mode, hits[0].code)  # -> broad A
```

If no default field can be resolved — `default_field` is unset and there is no `"title"`
index — a plain-text query raises `BroadSearchUnavailableError`:

```text
This catalog only supports structured queries. Use 'field: value' syntax. Indexed fields: ['code']
```

### The structured query DSL

A query is *structured* if and only if it matches the regex `^\s*\w+\s*:` — that is, it begins
with a word followed by a colon. The grammar:

- `&&` separates **clauses**, which are **AND**ed together (a result must satisfy every
  clause).
- Within one clause, `FIELD: v1, v2` lists values separated by `,`, which are **OR**ed (any
  value matching contributes).

Each clause field must have a configured index, otherwise the parse raises
`UnknownIndexedFieldError`. Scoring within a clause keeps the maximum positive score per row
across the OR values; across clauses, the surviving rows (the intersection) have their clause
scores summed.

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
res, diag = catalog.search("ref_area: Germany && icp_item: energy", limit=5)
print(diag.mode)               # -> structured
print({m.code for m in res})   # -> {'A'}
```

!!! note "A bare field token is still broad"
    The structured trigger requires a colon. A query like `ref_area` (no colon) does not match
    the regex and is treated as a broad query against the default field, not a structured one.

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

### Reading results

Each match is a `CatalogMatch` — a Pydantic model carrying the resolved entity fields plus the
final relevance score:

| Field | Type | Notes |
|---|---|---|
| `namespace` | `str` | Re-normalized to lowercase snake_case. |
| `code` | `str` | Trimmed, non-empty. |
| `title` | `str` | Trimmed, non-empty. |
| `score` | `float` | The fused/ranked score; higher is better. |
| `metadata` | `dict[str, Any]` | Shallow copy of the entity's metadata. |

The second tuple element is a `SearchDiagnostic`, whose `mode` is the literal `"broad"` or
`"structured"` and whose `notes` is a list of strings (empty by default).

```python
from parsimony.catalog import BM25Index, Catalog, Entity

catalog = Catalog("demo", indexes={"title": BM25Index()})
catalog.set_entities([Entity(namespace="demo", code="A", title="alpha title")])
catalog.build()
matches, diag = catalog.search("alpha", limit=5)
top = matches[0]
print(top.namespace, top.code, top.title)  # -> demo A alpha title
print(diag.mode)                            # -> broad
```

## Query errors

All three query errors subclass `ValueError`, so a broad `except ValueError` catches them, or
you can match by type. Import them from `parsimony.catalog`.

| Error | When raised |
|---|---|
| `UnknownIndexedFieldError` | A structured clause references a field with no configured index. Raised during query parsing. |
| `BroadSearchUnavailableError` | A plain-text query is issued but no broad-search (default) field resolves. Raised at search time. |
| `BroadSearchConfigError` | `default_field` is set but no index covers it. Raised at construction (with an explicit `indexes` dict) or at `build()` (under the default policy). |

```python
from parsimony.catalog import (
    BroadSearchConfigError,
    BroadSearchUnavailableError,
    UnknownIndexedFieldError,
)

for exc in (UnknownIndexedFieldError, BroadSearchUnavailableError, BroadSearchConfigError):
    assert issubclass(exc, ValueError)
```

!!! tip "Config-time vs query-time"
    `BroadSearchConfigError` is a *configuration* error (your `default_field` points at a
    field with no index). `BroadSearchUnavailableError` is a *query-time* error (a plain-text
    query against a catalog that only supports structured search). They are easy to confuse in
    a broad `except ValueError`.

## Sparse fields and empty results

Indexes read entity fields through the catalog's field-extraction rules: entries that are
missing a metadata field, or that have empty-string values for it, contribute no postings to
that index. A broad search over a `description` index therefore returns only entries that
actually have a non-empty description, and an entirely empty index builds fine and returns
`[]`. See [Entities](entities.md) for the exact extraction rules.

## See also

- [The Catalog](index.md) — the catalog lifecycle at a glance
- [Indexes](indexes.md) — `BM25Index`, `VectorIndex`, `HybridIndex`, `DisMaxIndex`, and the selection policies
- [Entities](entities.md) — the `Entity` model and how fields become searchable text
- [Snapshots and persistence](snapshots.md) — `save`/`load` and the build gate on save
