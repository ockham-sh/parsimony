# Core concepts

Parsimony is a connector framework for financial data — typed fetch and a hybrid-search
catalog. This page gives you the mental model that ties the library together: what a
connector is, how the framework wraps its output, how connectors compose and bind, how
plugins are discovered, and how the catalog turns discovered entities into a searchable
index. Once these pieces click, the rest of the docs are just detail.

Parsimony has two pillars:

- **Connectors** — small synchronous callables plus metadata that fetch raw data.
- **The [Catalog](../catalog/index.md)** — a portable, searchable index over normalized
  entity records, used to discover *what* you can fetch.

The core package (`parsimony-core`, version 0.7.0, Python `>=3.11`) ships the framework and
the catalog. It ships **no connectors** — every connector is published as its own
`parsimony-<name>` plugin and discovered at runtime.

## A connector is a function plus metadata

A connector is exactly what its name suggests: a small `def` that fetches data,
decorated with metadata. The function's parameters *are* the connector's parameters — there
is no separate request object. You turn a function into a [`Connector`](../connectors/defining-connectors.md)
with the `@connector` decorator.

```python
import pandas as pd
from parsimony import connector


@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search demo series by keyword."""
    return pd.DataFrame({"code": ["a", "b"], "title": [f"about {query}", "other"]})


result = demo_search(query="gdp")
print(result.provenance.source)  # -> demo_search
print(result.provenance.params)  # -> {'query': 'gdp'}
```

Three rules are worth internalizing now, because everything else follows from them:

- **It must be synchronous (`def`, not `async def`).** Decorating a coroutine function raises
  `TypeError`.
- **A description is mandatory.** It defaults to the function's stripped docstring; if there
  is no docstring you must pass `description=`. An empty description raises `ValueError`. The
  description is what an LLM reads when deciding whether to call the connector.
- **It returns raw data.** A connector returns a DataFrame, Series, scalar, or dict — never a
  `Result`. The *framework* builds the output envelope (see below). Returning a `Result`,
  `TabularResult`, or a `(data, properties)` tuple raises `TypeError`.

!!! note "Why return raw data?"
    Keeping connectors to "fetch and return a DataFrame" makes them trivial to write, test,
    and reason about. Provenance, schema coercion, and the typed-error envelope are
    cross-cutting concerns the framework owns once, so every connector gets them for free and
    none of them can drift between providers.

## The framework wraps the return value into a Result

When you call a connector, the framework calls your function, then wraps the raw return value
into a [`Result` or `TabularResult`](../connectors/results.md) and attaches a
[`Provenance`](../connectors/results.md) record describing the fetch:

```text
connector(**kwargs)
        │
        ▼
   your fn ──returns──> raw DataFrame / Series / scalar / dict
        │
        ▼
   framework wraps:
     DataFrame/Series ──> TabularResult(data=…, output_schema=…)
     scalar/dict      ──> Result(data=…)
   and builds Provenance(source, source_description, params, fetched_at)
```

`Provenance` is **framework-built only** — connectors never construct it. It records the
connector `name` as `source`, the description as `source_description`, a UTC `fetched_at`
timestamp, and the call-time `params`. Connector authors put provider facts in returned
DataFrame columns, not in provenance.

If you attach an [`OutputConfig`](../connectors/results.md) (via `output=`), the framework
applies that declarative schema to a DataFrame/Series return — coercing dtypes and assigning
column roles. A schema or coercion `ValueError` during wrapping surfaces as a typed
[`ParseError`](../connectors/errors.md), so the caller always sees Parsimony's
[typed, agent-facing error taxonomy](../connectors/errors.md) rather than a raw pandas error.

## Binding fixes parameters and hides secrets

`Connector` is a frozen dataclass: every transformation returns a *new* connector rather than
mutating in place. The most important transformation is `bind`, which fixes parameters by
name. Bound parameters disappear from the connector's exposed call surface — and crucially,
from its provenance and its LLM-facing cards.

```python
import pandas as pd
from parsimony import connector


@connector(secrets=("api_key",))
def keyed(query: str, api_key: str) -> dict:
    """Connector with a declared secret parameter."""
    return {"q": query}


bound = keyed.bind(api_key="sk-…")              # fix the secret
print(list(bound.exposed_signature.parameters))  # -> ['query']

result = bound(query="gdp")
print(result.provenance.params)                  # -> {'query': 'gdp'}  (api_key stripped)
```

This is the idiom for injecting credentials and base URLs: declare a parameter in
`secrets=(...)` and `bind` it. Two things then happen. The bound argument no longer appears in
the exposed signature, so an agent inspecting the connector never sees the secret. And any
declared secret — whether supplied via `bind` or at call time — is stripped from the
recorded provenance `params`.

!!! warning "Bound arguments are not recorded in provenance"
    Only *call-time* params are recorded in `provenance.params`; bound arguments never are.
    This is intentional (it is how injected secrets stay out of provenance), but it means
    provenance does not reflect the full argument set passed to your function. `bind` also
    rejects unknown names and re-binding an already-bound name with `TypeError`.

## Loaders and enumerators: two stricter verbs

`@connector` is the general case. Two specializations add output-schema contracts that mark a
connector's *intent* — fetching values versus discovering entities. See
[loaders and enumerators](../connectors/loaders-and-enumerators.md) for the full contracts.

| Verb | Decorator | Output contract (validated at decoration) | Feeds | Tag |
|---|---|---|---|---|
| **Loader** | `@loader(output=…)` | exactly one namespaced `KEY`, `≥1` `DATA`, no `TITLE`/`METADATA` | a [data store](../catalog/data-store.md) | `loader` |
| **Enumerator** | `@enumerator(output=…)` | exactly one namespaced `KEY`, `≥1` `TITLE`, no `DATA`, only `KEY`/`TITLE`/`METADATA` | a [catalog](../catalog/index.md) | `enumerator` |

A **loader** fetches observations — the actual values for a series. An **enumerator**
discovers entities — the catalog of *what is fetchable*. The shape of their output schemas
encodes that difference: a loader produces values keyed by identity; an enumerator produces
titled entity records with no data columns. An enumerator must also annotate a
`pd.DataFrame`/`Series` return (not `list[Entity]`), and its returned columns are checked
against the declared schema at call time.

## Collections compose connectors

A [`Connectors`](../connectors/calling-binding-composing.md) collection is an immutable,
name-keyed registry. You build one from a list and invoke a member with the canonical idiom
`connectors[name](**kwargs)`. Collections compose with the `+` operator — that is how
you merge two bundles; there is no `.merge` method.

```python
from parsimony import Connectors

bundle = Connectors([demo_search]) + Connectors([keyed])
print(bundle.names())            # -> ['demo_search', 'keyed']  (sorted)
print("demo_search" in bundle)   # -> True

result = bundle["demo_search"](query="cpi")
print(len(result.df))            # -> 2
```

Construction (and `+`) raises `ValueError` on duplicate connector names. `__getitem__` looks
up by *name* (a string, not an integer index) and raises a helpful `KeyError` listing the
available names if it is absent. Collections also support `get`, `names`, `filter`, `search`,
collection-wide `bind` (scoped — only connectors that actually have a matching parameter are
bound), and the `describe`/`to_llm` projections that render the whole bundle for a prompt.

## Plugins are discovered through entry points

The core package ships zero connectors. Every connector lives in a separate `parsimony-<name>`
distribution — a **plugin** (or **provider**). A plugin registers itself under the
`parsimony.providers` entry-point group and exports a module-level `CONNECTORS: Connectors`.
At runtime, [`parsimony.discover`](../plugins/discovery.md) enumerates and loads them.

```python
from parsimony import discover

# Metadata only — imports no plugin code:
for provider in discover.iter_providers():
    print(provider.name, provider.version)

# Strict: raises LookupError if a name is not installed:
fred = discover.load("fred")

# Forgiving: load every installed plugin, skipping (and logging) any that fail:
everything = discover.load_all()
```

`iter_providers()` reads distribution metadata without importing any plugin module; it raises
`RuntimeError` if two distributions claim the same provider name. `load(*names)` is strict —
it raises `LookupError` listing the missing and available names. `load_all()` is forgiving —
one broken plugin is logged and skipped, the rest still load. Both return a `Connectors`, so
you compose loaded bundles with `+` exactly like any other collection.

!!! tip "A bare install discovers nothing"
    `pip install parsimony-core` discovers zero providers — there are no in-tree connectors.
    Install at least one `parsimony-<name>` distribution for `discover.load_all()` to return
    anything. See [Installation](installation.md) and [Plugins and providers](../plugins/index.md).

## The catalog is the discovery layer over entities

Where connectors *fetch*, the [`Catalog`](../catalog/index.md) helps you *discover*. A catalog
is a portable, in-memory index over normalized [`Entity`](../catalog/entities.md) records,
with pluggable per-field [indexes](../catalog/indexes.md) and structured plus broad search.
Its lifecycle is a fixed sequence:

```text
Catalog(name, indexes=…)        # construct
   └─ set_entities([Entity, …]) # load records   (marks the catalog dirty)
        └─ build()              # materialize the indexes
             └─ search(q, limit=…)   -> [CatalogMatch, …]
                  └─ save(url)        # persist a snapshot (file:// or hf://)
```

```python
from parsimony import BM25Index, Catalog, Entity

catalog = Catalog("demo", indexes={"title": BM25Index()})
catalog.set_entities([
    Entity(namespace="demo", code="alpha", title="Alpha series"),
    Entity(namespace="demo", code="beta", title="Beta series"),
])
catalog.build()
hits = catalog.search("Alpha", limit=5)
print(hits[0].code, round(hits[0].score, 3))  # -> alpha …
```

!!! note "This catalog example needs the `catalog` extra"
    `BM25Index` builds against `rank-bm25`, which ships in the optional `catalog` extra
    (`pip install "parsimony-core[catalog]"`). Defining connectors and using a data store
    above run with only `parsimony-core` installed; building or searching a catalog requires
    `catalog`. See [Installation](installation.md).

The key invariant is the **build gate**: any mutation (`set_entities`, index changes,
`delete_many`) marks the catalog dirty, and `search()` / `save()` raise a `ValueError` —
whose message tells you to call `catalog.build()` — until you rebuild. Passing
`indexes=None` instead opts into the default index policy: at `build()`, BM25 indexes are
created automatically for `code`, `title`, and every metadata key on the entries.

## How the pieces flow together

Loaders and enumerators are the bridges between the two pillars. An enumerator's output feeds
a catalog; a loader's output feeds a data store:

```text
enumerator ──DataFrame──> OutputConfig.build_entities ──> [Entity, …] ──> Catalog
   (discover what is fetchable)                                            (search it)

loader     ──DataFrame──> InMemoryDataStore.load_result ──> stored DATA columns
   (fetch the values)                                        keyed by (namespace, code)
```

- An **enumerator** returns a discovery frame. `OutputConfig.build_entities` projects that
  frame into `Entity` records using the schema's column roles — the `KEY` column's namespace
  plus the `TITLE` and `METADATA` columns. Those entities go into a `Catalog` via
  `set_entities`, and after `build()` you can search them.
- A **loader** returns an observation frame. A [data store](../catalog/data-store.md) extracts
  the `DATA` columns and persists one DataFrame per distinct entity, keyed by
  `(namespace, code)`. The store's `load_result(table, force=…)` returns a `LoadResult` tally
  of `total` / `loaded` / `skipped` / `errors`:

```python
import pandas as pd
from parsimony import Column, ColumnRole, InMemoryDataStore, OutputConfig, loader

LOAD = OutputConfig(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="demo"),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])


@loader(output=LOAD)
def load_demo(series_id: str) -> pd.DataFrame:
    """Load observations for a demo series."""
    return pd.DataFrame({"date": ["2020", "2020"], "value": [1.0, 2.0]})


store = InMemoryDataStore()
table = load_demo(series_id="x")
stats = store.load_result(table)
print(stats.total, stats.loaded, stats.skipped, stats.errors)  # -> 1 1 0 0
rows = store.get("demo", "2020")
print(list(rows.columns))  # -> ['value']  (KEY consumed for identity, DATA kept)
```

!!! note "There is no `DataStore` protocol yet"
    `InMemoryDataStore` is the only store implementation today. A generic `DataStore` protocol
    will be extracted from its public method set when a second backend (SQLite, Parquet) lands
    — so treat `InMemoryDataStore` as the concrete store, not as one implementation of a
    defined interface.

## Namespaces and codes

Identity in Parsimony is a `(namespace, code)` pair, and the rules are uniform everywhere
entities are stored or keyed.

- A **namespace** is a lowercase snake_case identity scope, matching `^[a-z][a-z0-9_]*$`.
  Constructing an `Entity`, a `Catalog`, or keying a data store with an uppercase or
  hyphenated namespace raises `ValueError`.
- A **code** is the entity's identifier within its namespace. It only has to be non-empty
  after trimming — no case or character-set constraint.

The helpers `normalize_namespace`, `normalize_entity_code`, `code_token`, and `entity_key`
(import from `parsimony.entity`, or the lazy re-exports on `parsimony.catalog`) enforce and
build these keys. `code_token` is handy in plugins for turning arbitrary provider strings into
valid codes. The same `(namespace, code)` key threads through `Entity`, `CatalogMatch`, and
every data-store method, which is what lets an enumerator's catalog entry and a loader's stored
observations refer to the same thing.

!!! tip "Import paths"
    The framework essentials — `connector`, `loader`, `enumerator`, `Connector`, `Connectors`,
    `Result`, `TabularResult`, `OutputConfig`, `Column`, `ColumnRole`, the error types,
    `Catalog`, `Entity`, `BM25Index`, `InMemoryDataStore`, `discover` — are importable from the
    top-level `parsimony` package. The catalog names are lazy re-exports, so for catalog-heavy
    code `from parsimony.catalog import Catalog, Entity, BM25Index, …` is the clearest
    convention. See the [public API & import map](../reference/api.md) for what lives where.

## See also

- [The connector model](../connectors/index.md) — the connector pillar in depth
- [Loaders and enumerators](../connectors/loaders-and-enumerators.md) — the two stricter verbs and their contracts
- [The Catalog](../catalog/index.md) — the discovery pillar, lifecycle, and search
- [Plugins and providers](../plugins/index.md) — how connectors are packaged and discovered
