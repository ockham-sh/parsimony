# Quickstart

This page walks you through three hands-on flows with `parsimony-core` (Python `>=3.11`): define and call your own [connector](../connectors/index.md), compose two connectors into a [collection](../connectors/calling-binding-composing.md), and build a tiny searchable [Catalog](../catalog/index.md). The first two flows run with only the base install; the catalog flow needs one optional extra, which is called out below.

If you have not installed the package yet, start with [Installation](installation.md):

```bash
pip install parsimony-core
```

!!! note "No connectors ship in core"
    The core package is the framework plus the catalog — it contains zero data
    connectors. Every real data source is published as its own
    `parsimony-<name>` distribution (a [provider plugin](../plugins/index.md))
    and discovered at runtime. The examples below define their own connectors
    so they run with nothing but `parsimony-core` installed.

## 1. Define and call a connector

A connector is a small **synchronous** function plus metadata. The function's parameters *are* the connector's call surface, and the function returns **raw data** — a `pandas` DataFrame, Series, scalar, or dict. The framework wraps that raw value into a [`Result`](../connectors/results.md) and attaches framework-built [`Provenance`](../connectors/results.md); connectors never construct those carriers themselves. When `Result.raw` is a DataFrame the result is *tabular* (`result.is_tabular`).

The `@connector` decorator turns a plain `def` into a frozen `Connector`. When you attach an [`OutputSpec`](../connectors/results.md), it is attached to the result verbatim as `result.output_spec` — the framework never inspects, coerces, renames, or reorders the DataFrame you return; it just tags each declared column with a role for later consumers (a catalog, a data store) to read.

```python
import pandas as pd

from parsimony import Column, ColumnRole, OutputSpec, Result, connector

PRICE_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="demo_prices"),
        Column(name="close", role=ColumnRole.DATA),
    ]
)


@connector(output=PRICE_OUTPUT, tags=["demo"])
def daily_close(symbol: str) -> pd.DataFrame:
    """Return a tiny synthetic price series for a ticker symbol."""
    # Replace this with a real HTTP call — see the HTTP transport guide.
    df = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "close": ["185.6", "188.1", "187.2"],
        }
    )
    # OutputSpec never coerces dtypes — parse/cast in the connector body.
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = df["close"].astype(float)
    return df


result = daily_close(symbol="ACME")

assert isinstance(result, Result)
assert result.is_tabular                   # raw is a DataFrame
print(result.raw)                          # exactly what the function returned
print(result.raw["close"].dtype)           # float64 — cast in daily_close, not by the schema
print(result.provenance.source)            # "daily_close" (defaults to the function name)
print(result.provenance.params)            # {"symbol": "ACME"}
print([c.name for c in result.columns if c.role == ColumnRole.DATA])  # ["close"]
```

A few things this example demonstrates, all enforced by the framework:

- **The connector is synchronous.** An `async def` raises `TypeError` at decoration time.
- **A description is mandatory.** It defaults to the stripped docstring; pass `description=` to override. With neither, decoration raises `ValueError`.
- **`provenance.source` is the connector name**, which defaults to `fn.__name__`. `provenance.params` records only the call-time arguments.
- **The `close` column is `float64` because `daily_close` cast it**, not because `OutputSpec` declared a dtype — `Column` has no `dtype=` field.

!!! warning "Connectors must return raw data"
    Returning a `Result` or a `(data, properties)` tuple
    raises `TypeError`. The framework builds the output envelope; your job is
    to return the data, already in the shape and types you want it in.

## 2. Compose connectors and hide secrets

Connectors live in an immutable `Connectors` collection. You merge collections with the `+` operator (there is no `.merge` method), look connectors up **by name** with `[]`, and invoke them with the canonical idiom `collection[name](**kwargs)`.

`bind(**kwargs)` fixes parameter values and returns a *new* connector with those parameters removed from its call surface. This is how you inject a secret or a base URL without exposing it: declare the parameter in `secrets=(...)`, then `bind` it. Bound secrets never appear in the connector's signature, its LLM-facing card, or its provenance.

```python
import pandas as pd

from parsimony import Connectors, connector


@connector
def search_titles(query: str) -> pd.DataFrame:
    """Search a demo index by keyword."""
    return pd.DataFrame({"code": ["A", "B"], "title": [f"{query} alpha", f"{query} beta"]})


@connector(secrets=("api_key",))
def fetch_series(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch one observation for a series id (requires an API key)."""
    return pd.DataFrame({"date": ["2024-01-01"], "value": [1.0]})


# Merge two single-connector collections with the + operator.
bundle = Connectors([search_titles]) + Connectors([fetch_series])
print(bundle.names())          # ["fetch_series", "search_titles"] (sorted)
print("fetch_series" in bundle)  # True
print(len(bundle))             # 2

# Bind the secret across the whole collection. bind is scoped per connector:
# it only fixes parameters a connector actually has, so search_titles is untouched.
wired = bundle.bind(api_key="sk-demo")
print(list(wired["fetch_series"].exposed_signature.parameters))  # ["series_id"]

# Invoke by name.
titles = wired["search_titles"](query="GDP")
print(len(titles.raw))  # 2

series = wired["fetch_series"](series_id="UNRATE")
print(series.provenance.params)  # {"series_id": "UNRATE"} — api_key is stripped
```

!!! tip "Why binding hides secrets"
    Provenance records only the connector's *exposed* (unbound) call-time
    parameters, and even a supplied secret-named argument is stripped. So a
    bound `api_key` is invisible both to provenance and to the
    `describe()` / `to_llm()` cards a connector renders for an agent prompt —
    that is the mechanism, not a convention.

`Connectors` also offers `get`, `names`, `filter`, `search`, `describe`, and `to_llm` — see [Calling, binding, and composing](../connectors/calling-binding-composing.md). Note that `[]` takes a connector **name**, never an integer index: `bundle[0]` raises `KeyError`.

## 3. Build and search a Catalog

A `Catalog` is a portable, in-memory index over normalized [`Entity`](../catalog/entities.md) records. An entity has a `namespace` (lowercase snake_case), a `code` (its identifier within that namespace), a `title`, and arbitrary `metadata`. The lifecycle is fixed: construct the catalog, load entities with `set_entities`, materialize indexes with `build()`, then `search(...)`.

```python
from parsimony.catalog import BM25Index, Catalog, CatalogMatch, Entity

# An explicit BM25 index over the "title" field; broad (plain-text) queries
# target the "title" index by convention.
catalog = Catalog("demo", indexes={"title": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="series", code="UNRATE", title="Unemployment Rate"),
        Entity(namespace="series", code="GDPC1", title="Real Gross Domestic Product"),
    ]
)

catalog.build()  # materialize the indexes; required before searching

matches = catalog.search("unemployment", limit=5)
for match in matches:
    assert isinstance(match, CatalogMatch)
    print(match.namespace, match.code, match.title, round(match.score, 3))
```

`search` returns a `list[CatalogMatch]`. Each `CatalogMatch` carries the entity's `namespace`, `code`, `title`, `metadata`, and a final `score`.

!!! warning "Build before you search"
    Every mutation (`set_entities`, `set_indexes`, …) marks the
    catalog dirty. Calling `search()` or `save()` while dirty raises a plain
    `ValueError` whose message tells you to `catalog.build()` first.
    Re-run `build()` after any change.

!!! note "BM25 needs the `catalog` extra"
    `BM25Index` builds and scores with `rank-bm25`, which ships in the
    `catalog` optional extra, not the base install. Run this flow after
    `pip install "parsimony-core[catalog]"`. That extra also unlocks the
    FAISS vector indexes, the default sentence-transformers embedder, and the
    `hf://` snapshot loader. See [Installation](installation.md).

### The default index policy

If you pass `indexes=None` (the default), the catalog uses the **default index policy**: at `build()` time it creates a BM25 index for `code`, `title`, and every metadata key observed across your entities. This is the quickest way to make a catalog searchable across all its fields:

```python
from parsimony.catalog import Catalog, Entity

catalog = Catalog("demo")  # indexes=None -> default policy
catalog.set_entities(
    [Entity(namespace="demo", code="a", title="alpha", metadata={"region": "eu"})]
)
catalog.build()
print(sorted(catalog.indexes))  # ["code", "region", "title"]
```

For structured queries, snapshot persistence (`save` / `load` over `file://` and `hf://`), and the index types in depth, see [Building and searching](../catalog/search.md), [Indexes](../catalog/indexes.md), and [Snapshots and persistence](../catalog/snapshots.md).

## Using a real provider plugin

The connectors above are synthetic. A real data source is an installed
`parsimony-<name>` distribution that registers itself through the
`parsimony.providers` entry-point group. Once installed, load it at runtime
through `parsimony.discover`:

```python
from parsimony import discover

# discover.load("fred") loads a named provider (LookupError if not installed);
# discover.load_all() loads every installed provider, skipping failures.
providers = discover.load_all()  # -> a Connectors collection
print(providers.names())

# Compose installed providers with your own connectors using +.
# bundle = providers + Connectors([daily_close])
```

`discover.load_all()` returns a `Connectors` collection you can compose with `+` exactly like the ones you built by hand. The [Plugins and providers](../plugins/index.md) section covers installing, discovering, and authoring plugins.

!!! note "Plugins are separate installs"
    The `discover` example above prints an empty list until you install a
    provider, for example `pip install parsimony-fred`. Core never bundles a
    connector, so there is nothing to discover out of the box.

## See also

- [Installation](installation.md) — base install and the optional-extras matrix
- [Core concepts](concepts.md) — the mental model behind connectors and catalogs
- [The connector model](../connectors/index.md) — connectors, loaders, and enumerators in depth
- [The Catalog](../catalog/index.md) — entities, indexes, search, and snapshots
