# Parsimony

Parsimony is a connector framework for financial data — typed fetch and hybrid-search
catalogs. It gives you a small, agent-native data layer: connectors that fetch raw data
through a typed, synchronous call surface, and a portable in-memory catalog that indexes and
searches over the entities those connectors discover.

The distribution is published to PyPI as `parsimony-core` (import name `parsimony`,
Apache-2.0). It runs on Python `>=3.11` (3.11, 3.12, 3.13).

## The two pillars

Parsimony is built around two complementary ideas.

- **Connectors** — a connector is a small **synchronous** Python callable plus metadata. The
  [`@connector`](connectors/defining-connectors.md) decorator (and the stricter
  [`@loader` / `@enumerator`](connectors/loaders-and-enumerators.md) verbs) turn a plain
  `def` into a frozen `Connector`. The function's parameters *are* the connector's
  call surface — there is no bundled `params` object. A connector returns **raw data** (a
  DataFrame, Series, scalar, or dict); the framework wraps it in a
  [`Result`](connectors/results.md) carrying framework-built
  [`Provenance`](connectors/results.md). When `data` is a DataFrame the result
  is *tabular* (`result.is_tabular`). The immutable
  [`Connectors`](connectors/calling-binding-composing.md) collection composes connectors
  and is invoked with `connectors[name](**kwargs)`.

- **Catalog** — a [`Catalog`](catalog/index.md) is a portable, in-memory, searchable index
  over normalized [`Entity`](catalog/entities.md) records. It supports
  [pluggable per-field indexes](catalog/indexes.md) (BM25, FAISS vectors, hybrid fusion,
  DisMax), [structured and broad search](catalog/search.md), and
  [snapshot persistence](catalog/snapshots.md) to local paths or Hugging Face datasets.

!!! note "Connectors ship as separate plugins"

    No connectors ship inside the core package. Every connector is published as its own
    `parsimony-<name>` distribution and discovered at runtime through the
    `parsimony.providers` entry-point group. The core library is the framework plus the
    catalog. See [Plugins and providers](plugins/index.md).

Two design choices show up throughout the code and are worth knowing up front: connectors
expose **flat, top-level parameters** (the conformance suite forbids bundling them into a
single `params: SomeModel` object), and connector errors are
[typed and agent-facing](connectors/errors.md) — default messages embed directives like
"DO NOT retry" so an LLM driving the connector can act on them. Connectors can also render
themselves for prompts via `to_llm()`.

## Install

```bash
pip install parsimony-core
```

The base install pulls only a small kernel (pydantic, pandas, pyarrow, httpx,
platformdirs). The heavy catalog runtime (FAISS, sentence-transformers, Hugging Face Hub)
is an optional extra that loads lazily — a plain `import parsimony` never imports torch or
faiss.

```bash
pip install "parsimony-core[catalog]"
```

See [Installation](getting-started/installation.md) for the full optional-extras matrix.

## A 60-second taste

This runs with only `parsimony-core` installed. Define a `@connector`, attach an output
schema, call it, and read the typed `Result`.

```python
import pandas as pd

from parsimony import Column, ColumnRole, OutputConfig, connector

OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="demo"),
        Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
    ]
)


@connector(output=OUTPUT, tags=["demo"])
def demo_fetch(series_id: str) -> pd.DataFrame:
    """Fetch a tiny demo time series by series_id."""
    return pd.DataFrame({"date": ["2020-01-01", "2020-04-01"], "value": [1.0, 2.0]})


result = demo_fetch(series_id="GDP")
print(result.data)                     # the validated DataFrame
print(result.provenance.source)        # 'demo_fetch'
print(result.provenance.params)        # {'series_id': 'GDP'}
```

A few things this shows:

- The connector is a plain `def`; an `async def` would raise `TypeError` at decoration time.
- The docstring becomes the connector's required `description` — omit both and decoration
  raises `ValueError`.
- The function returns a **raw** DataFrame. The framework applies the
  [`OutputConfig`](connectors/results.md) schema and wraps the result in a `Result`
  with `Provenance`. Returning a `Result` or a `(data, properties)` tuple instead would
  raise `TypeError`.
- `result.provenance` is built by the framework — connectors never construct it. Its
  `params` record only the call-time arguments (with any declared `secrets` stripped).

!!! tip "Composing connectors"

    Merge collections with the `+` operator, then invoke a member by name:

    ```python
    from parsimony import Connectors

    bundle = Connectors([demo_fetch]) + Connectors([another_connector])
    result = bundle["demo_fetch"](series_id="GDP")
    print(result.data)
    ```

    There is no `.merge` method — `+` is the composition primitive. See
    [Calling, binding, and composing](connectors/calling-binding-composing.md).

## A taste of the catalog

The catalog indexes [`Entity`](catalog/entities.md) records so you can search them. A
catalog must be built before it can be searched. This example uses a keyword-only
[`BM25Index`](catalog/indexes.md), which loads `rank-bm25` lazily on first build —
that ships in the base install, so no extra is needed for keyword search. (The
[`catalog` extra](getting-started/installation.md) is only for *vector* search.)

```python
from parsimony import BM25Index, Catalog, Entity

catalog = Catalog(name="demo", indexes={"title": BM25Index()})
catalog.set_entities(
    [
        Entity(namespace="demo", code="gdp", title="Gross domestic product"),
        Entity(namespace="demo", code="cpi", title="Consumer price index"),
    ]
)
catalog.build()                          # required before searching
matches = catalog.search("price", limit=5)
for match in matches:
    print(match.code, match.title, match.score)
```

`catalog.search(...)` returns a list of [`CatalogMatch`](catalog/search.md) records.
Mutating a built catalog marks it dirty; search and save raise until you
rebuild. See [The Catalog](catalog/index.md) for the full lifecycle.

## Using a real provider

Core ships no connectors, so the runnable examples above define their own. In practice you
install a provider plugin and discover it at runtime:

```bash
pip install parsimony-fred
```

```python
from parsimony import discover

bundle = discover.load_all()       # composes every installed parsimony-<name> plugin
print(bundle.names())
```

`discover.load_all()` is forgiving (it logs and skips a plugin that fails to import);
`discover.load("fred")` is strict and raises if a name is missing. See
[Discovering installed providers](plugins/discovery.md). You can also list what is
installed from the shell with [`parsimony list`](cli.md).

## Where to go next

- **[Installation](getting-started/installation.md)** — the optional-extras matrix
  (`catalog`, `standard-onnx`, `litellm`, `all`) and what each pulls in.
- **[Quickstart](getting-started/quickstart.md)** — hands-on flows: a custom connector, a
  composed collection, and a small in-memory catalog.
- **[Core concepts](getting-started/concepts.md)** — the mental model that ties connectors,
  results, entities, and the catalog together.
- **[The connector model](connectors/index.md)** — connectors in depth: defining, the
  loader/enumerator verbs, calling and binding, results, errors, and HTTP transport.
- **[The Catalog](catalog/index.md)** — entities, building and searching, indexes, ranking
  and fusion, embedders, snapshots, and data stores.
- **[Plugins and providers](plugins/index.md)** — discovering, authoring, and conformance-
  testing your own `parsimony-<name>` distribution.

## See also

- [Quickstart](getting-started/quickstart.md) — the fastest path from install to a first result.
- [Core concepts](getting-started/concepts.md) — how the pieces fit together.
- [The connector model](connectors/index.md) — the connector abstraction in full.
- [Public API & import map](reference/api.md) — what to import from where.
