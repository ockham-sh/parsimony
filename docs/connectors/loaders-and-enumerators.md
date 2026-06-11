# Loaders and enumerators

A plain [`@connector`](defining-connectors.md) returns whatever raw data you give it. The two verb decorators — `@loader` and `@enumerator` — narrow that contract: they require a declarative [`OutputConfig`](results.md) with a specific role shape so the framework knows, structurally, whether a connector produces **values** (observations to persist) or **entities** (records to discover). Picking the right verb makes a connector's output directly consumable by a [data store](../catalog/data-store.md) or a [catalog](../catalog/index.md) with no glue code.

Both are thin specializations of `@connector`: they validate the schema at decoration time, prepend a tag (`loader` or `enumerator`), then delegate to `connector(...)`. Import them from `parsimony.connector` or the package root.

```python
from parsimony import loader, enumerator
# or: from parsimony.connector import loader, enumerator
```

## Which verb to use

| You are fetching… | Use | Output shape | Consumed by |
|---|---|---|---|
| Observation/value data (a time series, prices, a panel) | `@loader` | exactly one namespaced KEY + ≥1 DATA, no TITLE/METADATA | [`InMemoryDataStore.load_result`](../catalog/data-store.md) |
| Discoverable entities (what series exist, their titles) | `@enumerator` | exactly one namespaced KEY + ≥1 TITLE, no DATA | [`Catalog`](../catalog/search.md) via [`build_entities`](../catalog/entities.md) |
| Anything else (scalars, dicts, ad-hoc frames) | `@connector` | free-form, optional schema | you, directly |

The split is structural, not advisory: an enumerator literally cannot declare a DATA column, and a loader literally cannot declare a TITLE column. That guarantee is what lets the data store and the catalog trust the shape of what they receive.

## Loaders

`@loader` decorates an async function that fetches actual observations. The decorator is **keyword-only** and `output` is **required**:

```python
import asyncio
import pandas as pd
from parsimony import loader
from parsimony.result import Column, ColumnRole, OutputConfig

LOAD_OUTPUT = OutputConfig(columns=[
    Column(name="series_code", role=ColumnRole.KEY, namespace="demo_series"),
    Column(name="date", role=ColumnRole.DATA, dtype="date"),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])

@loader(output=LOAD_OUTPUT, tags=["demo"])
async def load_observations(series_id: str) -> pd.DataFrame:
    """Load observations for one or more demo series."""
    return pd.DataFrame({
        "series_code": ["unrate", "unrate", "gdpc1"],
        "date": ["2020-01-01", "2020-02-01", "2020-01-01"],
        "value": ["3.5", "3.6", "21000"],
    })

result = asyncio.run(load_observations(series_id="batch"))
assert load_observations.tags == ("loader", "demo")
assert list(result.df.columns) == ["series_code", "date", "value"]
```

`@loader` prepends `"loader"` to your tags, so `load_observations.tags == ("loader", "demo")`. The function still returns **raw data** — a DataFrame — and the framework wraps it into a [`TabularResult`](results.md), applies the schema (coercing `value` to numeric, `date` to dates), and attaches framework-built [`Provenance`](results.md). You never construct a `Result` yourself.

### The loader output contract

`@loader` validates `output` at **decoration time** via the loader rules below. A violation raises `ValueError` immediately, when the module is imported — not when the connector is called.

| Rule | Violation message (excerpt) |
|---|---|
| Exactly one KEY column | `Loader output must define exactly one KEY column for identity; found N` |
| The KEY column declares a non-empty `namespace=` | `Loader KEY column must declare a non-empty namespace=...` |
| At least one DATA column | `Loader output must include at least one DATA column` |
| No TITLE columns | `Loader output must not include TITLE columns; remove or reassign roles for: [...]` |
| No METADATA columns | `Loader output must not include METADATA columns; remove or reassign roles for: [...]` |

The KEY namespace is mandatory because the data store derives each entity's identity from it. A loader without a namespaced KEY cannot feed [`load_result`](../catalog/data-store.md).

!!! warning "The 'at most one KEY' error fires earlier than the loader rules"
    Declaring **two** KEY columns fails during `OutputConfig(...)` construction itself — its role validator allows at most one KEY and one TITLE — so you see `Output config must have at most one KEY column` before `@loader` ever runs. The loader-specific messages ("exactly one KEY", namespace, DATA-required, no-TITLE, no-METADATA) cover the remaining cases.

### Feeding a data store

A loader's output is shaped precisely so [`InMemoryDataStore.load_result`](../catalog/data-store.md) can persist it. The store groups rows by the KEY value, derives the namespace from the KEY column's `namespace=`, and persists the DATA columns per entity:

```python
import asyncio
from parsimony import loader
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.stores import InMemoryDataStore

LOAD_OUTPUT = OutputConfig(columns=[
    Column(name="series_code", role=ColumnRole.KEY, namespace="demo_series"),
    Column(name="date", role=ColumnRole.DATA, dtype="date"),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])

@loader(output=LOAD_OUTPUT)
async def load_observations(series_id: str) -> pd.DataFrame:
    """Load observations for one or more demo series."""
    return pd.DataFrame({
        "series_code": ["unrate", "unrate", "gdpc1"],
        "date": ["2020-01-01", "2020-02-01", "2020-01-01"],
        "value": ["3.5", "3.6", "21000"],
    })

async def main() -> None:
    result = await load_observations(series_id="batch")
    store = InMemoryDataStore()
    stats = await store.load_result(result)
    print(stats.model_dump())          # {'total': 2, 'loaded': 2, 'skipped': 0, 'errors': 0}
    print(await store.get("demo_series", "unrate"))

import pandas as pd
asyncio.run(main())
```

Two distinct KEY values (`unrate`, `gdpc1`) become two stored entities. By default `load_result` skips entities already present; pass `force=True` to upsert them all. See [Data stores](../catalog/data-store.md) for `LoadResult`, `upsert`, `get`, `delete`, and `exists`.

## Enumerators

`@enumerator` decorates an async function that **discovers** what entities exist — typically the metadata catalog a provider exposes (every series, its title, its frequency). It is the entity-discovery counterpart to a loader.

```python
import asyncio
import pandas as pd
from parsimony import enumerator
from parsimony.result import Column, ColumnRole, OutputConfig

ENUMERATE_OUTPUT = OutputConfig(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="demo_series"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="frequency", role=ColumnRole.METADATA),
])

@enumerator(output=ENUMERATE_OUTPUT, name="list_series")
async def list_series(prefix: str = "") -> pd.DataFrame:
    """Discover demo series matching a prefix."""
    return pd.DataFrame({
        "code": ["unrate", "gdpc1"],
        "title": ["Unemployment", "Real GDP"],
        "frequency": ["monthly", "quarterly"],
    })

result = asyncio.run(list_series(prefix="g"))
assert list(result.df.columns) == ["code", "title", "frequency"]
```

`@enumerator` prepends `"enumerator"` to your tags (so `list_series.tags == ("enumerator",)`) and sets `role="enumerator"` on the returned :class:`~parsimony.connector.Connector`. As with loaders, the function returns a **raw DataFrame**; the framework wraps it.

### The enumerator output contract

The schema is validated at **decoration time** with the enumerator rules:

| Rule | Violation message (excerpt) |
|---|---|
| Exactly one KEY column | `Enumerator output must define exactly one KEY column; found N` |
| The KEY column declares a non-empty `namespace=` | `Enumerator KEY column must declare a non-empty namespace=...` |
| At least one TITLE column | `Enumerator output must include at least one TITLE column` |
| No DATA columns | `Enumerator output must not include DATA columns; remove: [...]` |
| Only KEY / TITLE / METADATA roles | `Enumerator output has invalid column roles: [...]` |

An enumerator describes identities, not measurements — hence no DATA columns. Every discovered entity needs a human-readable title, hence the mandatory TITLE.

### Return-type annotation is required

Unlike a plain connector, an enumerator's wrapped function **must annotate** a `pd.DataFrame` (or `pd.Series`) return type. This is checked at decoration time:

```python
# raises ValueError: "enumerator must annotate return type pd.DataFrame"
@enumerator(output=ENUMERATE_OUTPUT)
async def missing_annotation():
    ...

# raises ValueError: "enumerator return must be pd.DataFrame"
from parsimony.entity import Entity
@enumerator(output=ENUMERATE_OUTPUT)
async def returns_entities() -> list[Entity]:
    ...
```

The check has two stages. First, the annotation must mention `DataFrame` or `Series`; a `list[Entity]` return mentions neither, so it raises
`ValueError("<name>: enumerator return must be pd.DataFrame")`. Second, even an annotation that *does* mention a frame must **not** also mention `Entity` or `list[` — an annotation such as `pd.DataFrame | list[Entity]` raises the distinct
`ValueError("<name>: enumerator must not return list[Entity]")`. Either way the outcome is the same rule: an enumerator returns the raw discovery frame; the framework — not your function — turns it into entities. Returning `list[Entity]` directly is forbidden.

### Column shape is enforced at call time

The schema checks above run at decoration. There is one further check that runs **every call**: after the framework applies your schema, the resulting frame's columns must **exactly** match the declared schema columns (the `"*"` wildcard column, if present, is excluded from this check). A missing or extra declared column raises `ValueError`, which the connector surface re-raises as a typed [`ParseError`](errors.md):

```python
import asyncio
import pandas as pd
from parsimony import enumerator
from parsimony.errors import ParseError
from parsimony.result import Column, ColumnRole, OutputConfig

OUT = OutputConfig(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="demo"),
    Column(name="title", role=ColumnRole.TITLE),
])

@enumerator(output=OUT, name="broken")
async def broken() -> pd.DataFrame:
    """Returns a frame missing the declared title column."""
    return pd.DataFrame({"code": ["a"]})   # 'title' is missing

try:
    asyncio.run(broken())
except ParseError as exc:
    print(exc)   # references "Enumerator DataFrame missing declared columns: ['title']"
```

!!! note "Enumerators drop unmapped columns; loaders keep them"
    A normal connector or loader folds any returned column you did not declare into a fresh DATA column (`merge_unmapped_as_data=True`). Enumerators do the opposite: unmapped columns are **dropped** before the exact-match check. So a returned `junk` column you forgot to declare is silently discarded — it will not appear in the result and will not raise. Declare every column you intend to keep.

### Feeding a catalog

An enumerator's output is shaped to become [`Entity`](../catalog/entities.md) records directly. The same `OutputConfig` you pass to the decorator can extract entities from the returned frame via [`build_entities`](../catalog/entities.md):

```python
import asyncio
import pandas as pd
from parsimony import enumerator
from parsimony.result import Column, ColumnRole, OutputConfig

ENUMERATE_OUTPUT = OutputConfig(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="demo_series"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="frequency", role=ColumnRole.METADATA),
])

@enumerator(output=ENUMERATE_OUTPUT, name="list_series")
async def list_series() -> pd.DataFrame:
    """Discover demo series."""
    return pd.DataFrame({
        "code": ["unrate", "gdpc1"],
        "title": ["Unemployment", "Real GDP"],
        "frequency": ["monthly", "quarterly"],
    })

result = asyncio.run(list_series())
entities = ENUMERATE_OUTPUT.build_entities(result.df)
for e in entities:
    print(e.namespace, e.code, e.title, e.metadata)
# demo_series unrate Unemployment {'frequency': 'monthly'}
# demo_series gdpc1 Real GDP {'frequency': 'quarterly'}
```

`build_entities` groups rows by the KEY value, uses the KEY column's `namespace=` as the entity namespace, the TITLE column for `title`, and METADATA columns (including a `"*"` wildcard for "every column not otherwise claimed") for `metadata`. Those `Entity` records are exactly what you load into a `Catalog`. See [Entities](../catalog/entities.md) for the full mapping rules and the "metadata varies within key" error.

### Per-row namespaces with `__row__`

Usually one enumerator covers one namespace, fixed by the KEY column's `namespace=`. When a single enumerator discovers entities across **several** namespaces, set the KEY namespace to the sentinel `"__row__"` and add an `entity_namespace` METADATA column carrying each row's namespace. This is enforced at decoration time:

```python
import asyncio
import pandas as pd
from parsimony import enumerator
from parsimony.result import Column, ColumnRole, OutputConfig

MULTI_NS = OutputConfig(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="__row__"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="entity_namespace", role=ColumnRole.METADATA),
])

@enumerator(output=MULTI_NS, name="discover_mixed")
async def discover_mixed() -> pd.DataFrame:
    """Discover entities across several namespaces."""
    return pd.DataFrame({
        "code": ["unrate", "aapl"],
        "title": ["Unemployment", "Apple Inc"],
        "entity_namespace": ["fred_series", "stock_ticker"],
    })

result = asyncio.run(discover_mixed())
for e in MULTI_NS.build_entities(result.df):
    print(e.namespace, e.code)
# fred_series unrate
# stock_ticker aapl
```

If you set `namespace="__row__"` but omit the `entity_namespace` METADATA column, decoration fails with `Enumerator with namespace="__row__" requires entity_namespace METADATA column`. At entity-build time, `build_entities` reads each row's namespace from that column (and each must be valid lowercase snake_case, like any [namespace](../catalog/entities.md)).

## Validation timing summary

Knowing *when* each rule fires saves debugging time — most failures surface at import, not at runtime.

| Check | When | Raises |
|---|---|---|
| Loader/enumerator output role shape | decoration (module import) | `ValueError` |
| Enumerator return-type annotation | decoration | `ValueError` |
| `OutputConfig` "≤1 KEY / ≤1 TITLE" base rule | `OutputConfig(...)` construction | `ValueError` |
| `secrets=` names match real parameters | decoration | `ValueError` |
| Function must be `async` | decoration | `TypeError` |
| Enumerator returned-frame exact column match | every call | `ValueError` → [`ParseError`](errors.md) |
| Connector returned `Result`/`TabularResult`/tuple | every call | `TypeError` |

Everything `@connector` does — [binding](calling-binding-composing.md), `secrets=` stripping from provenance, [`Connectors`](calling-binding-composing.md) composition with `+`, `describe()` / `to_llm()` cards — applies unchanged to loaders and enumerators. The verbs only add the schema contract on top.

## See also

- [Defining connectors](defining-connectors.md) — the base `@connector` decorator the verbs specialize
- [Results and output schemas](results.md) — `OutputConfig`, `Column`, `ColumnRole`, and `TabularResult`
- [Entities](../catalog/entities.md) — `build_entities`, the `Entity` model, and namespace rules
- [Data stores](../catalog/data-store.md) — persisting loader output with `InMemoryDataStore`
- [Errors](errors.md) — `ParseError` and the typed exception taxonomy
