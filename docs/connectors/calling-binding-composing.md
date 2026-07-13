# Calling, binding, and composing

A connector is a callable plus metadata, so you invoke it and the framework
hands back a [`Result`](results.md). Binding fixes parameters
ahead of time — the idiom for injecting secrets and base URLs without leaking
them — and the immutable `Connectors` collection lets you merge and filter
connector bundles. This page covers all three.

## Calling a connector

Call a connector with keyword (or positional) arguments; the framework wraps
the connector's raw return value in a [`Result`](results.md) with
framework-built [`Provenance`](results.md). There is one result type: when the
return is a DataFrame the result is tabular (`result.is_tabular`), otherwise the
value lands on `result.data` as-is.

```python
import pandas as pd
from parsimony.connector import connector


@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search demo series by keyword."""
    return pd.DataFrame({"id": ["A", "B"], "title": [f"Series about {query}", "Another"]})


result = demo_search(query="GDP")
print(result.data)                    # the returned DataFrame (no output= schema here; also .frame / .df)
print(result.provenance.source)       # "demo_search"
print(result.provenance.params)       # {"query": "GDP"}
```

Arguments are bound against the connector's **exposed signature** (see
[binding](#binding-parameters) below) and defaults are applied, so a connector
called with no arguments still runs if all its parameters are optional. Invalid
call-time arguments raise `TypeError` with a message naming the connector — for
example `Invalid parameters for connector 'demo_search': ...` for an unknown
keyword, and the underlying "missing a required argument" message when a
required parameter is omitted.

!!! warning "Connectors must return raw data"
    A connector returns a DataFrame, Series, scalar, or dict — never a
    pre-built envelope. Returning a `Result` or a `(data, properties)` tuple
    raises `TypeError` at call time, because the framework builds the execution
    envelope. Wrapping never transforms the data: any declared
    [`OutputSpec`](results.md) is attached as annotation only.

### The exposed signature

`Connector.exposed_signature` is the `inspect.Signature` callers actually see.
For an unbound connector it equals the wrapped function's signature; after
[binding](#binding-parameters), the fixed parameters are removed. This signature
drives argument binding at call time and the parameter listing in
`describe()` / `to_llm()`.

```python
import inspect
print(list(demo_search.exposed_signature.parameters))   # ['query']
```

### Calling without wrapping: `call_raw`

`connector.call_raw(**kwargs)` invokes the underlying function and returns
its **raw** value — no `Result`, no `Provenance`. Note that
`call_raw` does not bind against the exposed signature or apply defaults; you
pass the full merged argument set yourself. Use it when you want the data only,
for example inside another connector or a test.

```python
raw = demo_search.call_raw(query="CPI")
assert isinstance(raw, pd.DataFrame)
```

## Binding parameters

`Connector.bind(**kwargs)` returns a **new** connector with the named parameters
fixed. The bound names disappear from `exposed_signature` (and therefore from
`describe()` / `to_llm()` cards), and they are **not** recorded in
`provenance.params`. This is the mechanism for injecting credentials and
configuration that the caller — or an LLM driving the connector — should never
see or have to supply.

```python
import os
import pandas as pd
from parsimony.connector import connector


@connector(secrets=("api_key",))
def fetch_series(series_id: str, api_key: str, base_url: str = "https://api.example.com") -> pd.DataFrame:
    """Fetch a series by id from the example provider."""
    return pd.DataFrame({"series": [series_id]})


# Inject the secret and base URL once; the agent only ever sees series_id.
ready = fetch_series.bind(api_key=os.environ.get("EXAMPLE_API_KEY", "demo-key"))

print(list(ready.exposed_signature.parameters))   # ['series_id', 'base_url']

result = ready(series_id="GDP")
print(result.provenance.params)               # {'series_id': 'GDP', 'base_url': '...'} — no api_key
```

!!! note "`secrets=` and binding are independent"
    Declaring a parameter in `secrets=` strips it from `provenance.params`
    whether you fix it with `bind()` or pass it at call time. Binding hides a
    parameter from the **call surface** regardless of whether it is a secret.
    Combine them — `secrets=("api_key",)` plus `bind(api_key=...)` — so the key
    is both invisible to callers and absent from provenance. See
    [Defining connectors](defining-connectors.md) for the `secrets=` declaration
    and [Errors](errors.md) for the agent-facing error contract.

Binding is composable: each `bind()` call returns a new connector, accumulating
fixed parameters and shrinking the exposed signature.

```python
step1 = fetch_series.bind(api_key="k")
step2 = step1.bind(base_url="https://api.test")
print(list(step2.exposed_signature.parameters))   # ['series_id']
```

`bind()` validates its arguments and rejects two mistakes with `TypeError`:

| Mistake | Example | Raised |
|---|---|---|
| Binding a name that is not a parameter | `fetch_series.bind(nope=1)` | `TypeError: ... received unexpected bind arguments: ['nope']` |
| Re-binding an already-bound parameter | `step1.bind(api_key="k2")` | `TypeError: ... received already-bound arguments: ['api_key']` |

Calling `bind()` with no keyword arguments returns the same connector
unchanged. Because `Connector` is a frozen dataclass, `bind()` never mutates in
place — it always returns a fresh instance.

## Composing with `Connectors`

`Connectors` is an immutable, composable collection of `Connector` instances
keyed by name. Construct one from a sequence of connectors; the constructor
freezes the input and raises `ValueError` if two connectors share a name.

```python
from parsimony.connector import connector, Connectors


@connector
def ping() -> dict:
    """Return a tiny payload."""
    return {"ok": True}


bundle = Connectors([demo_search, ping])
print(bundle.names())        # ['demo_search', 'ping'] (sorted)
print(len(bundle))           # 2
print("ping" in bundle)      # True
```

### Calling by name

The canonical execution idiom is `connectors[name](**kwargs)`. Lookup is
by connector **name** (a string), not by position.

```python
from parsimony.connector import Connectors

bundle = Connectors([demo_search, ping])


result = bundle["demo_search"](query="GDP")
print(len(result.data))
```

!!! warning "Indexing is by name, not by integer"
    `connectors[0]` raises `KeyError`, not `IndexError`. `Connectors` is keyed
    by connector name. A missing name raises a helpful
    `KeyError: No connector 'x'. Available: [...]` listing the available names.
    Use `get(name)` when you want `None` instead of an exception for an absent
    connector.

### Merging collections with `+`

To merge two collections, use the `+` operator. There is **no `.merge` method** —
concatenation is the composition primitive. The result re-checks for duplicate
names and raises `ValueError` on a collision.

```python
from parsimony.connector import Connectors

search_tools = Connectors([demo_search])
health_tools = Connectors([ping])

all_tools = search_tools + health_tools
print(all_tools.names())     # ['demo_search', 'ping']
```

This is how you assemble a working set from multiple provider plugins — load
each plugin's `CONNECTORS` and add them together. See
[Discovering installed providers](../plugins/discovery.md) for loading plugin
bundles.

### Collection-wide binding

`Connectors.bind(**kwargs)` binds matching parameters across every connector,
**scoped per connector**: for each connector only the keyword arguments that
appear in that connector's exposed signature are applied; connectors lacking a
given parameter are left untouched. This lets you inject a shared secret across
a heterogeneous bundle in one call.

```python
# Suppose every FRED connector takes api_key but the demo ones do not.
wired = all_tools.bind(api_key="shared-key")   # binds api_key only where it exists
```

### Inspecting and filtering

| Method | Returns | Behavior |
|---|---|---|
| `get(name)` | `Connector \| None` | lookup by name, `None` if absent |
| `__getitem__(name)` | `Connector` | lookup; raises `KeyError` listing available names if absent |
| `__contains__(name)` | `bool` | `True` if a connector has that name (`False` for non-`str`) |
| `names()` | `list[str]` | sorted connector names |
| `__len__()` / `__iter__()` | `int` / iterator | count and iteration over the connectors |
| `filter(predicate=, tags=)` | `Connectors` | connectors matching the predicate and/or carrying all the given tags |

```python
# Keep only the loaders.
loaders = all_tools.filter(tags=["loader"])

# Predicate and tags compose; both are optional.
fred_loaders = all_tools.filter(lambda c: c.name.startswith("fred_"), tags=["loader"])
```

`filter(predicate=None, *, tags=None)` keeps a connector when the given `tags`
are a **subset** of the connector's tags and `predicate(connector)` is true
(each check is skipped when its argument is omitted). `filter` and `bind` both
return new `Connectors`; the original is never modified.

### Discovery over large collections

`filter`, `names()`, `describe()`, and name lookup cover collection filtering.
When an agent needs to *find* the right connector among hundreds by free-text
query, use explicit catalog-backed discovery: `to_entities()` converts each
connector into one catalogable [`Entity`](../catalog/entities.md)
(namespace `"connectors"` by default; description, tags, params, and declared
output columns land in `metadata`), and you build and search the
[`Catalog`](../catalog/index.md) yourself:

```python
import pandas as pd
from parsimony import Catalog, connector

entities = all_tools.to_entities()   # one Entity per connector
catalog = Catalog("connectors")
catalog.set_entities(entities)
catalog.build()

@connector(tags=["search"])
def connectors_search(query: str) -> pd.DataFrame:
    """Search the available connector operations."""
    matches = catalog.search(query, limit=10)
    return pd.DataFrame([m.model_dump() for m in matches])
```

`to_entities()` is conversion only — index policy, embedding model, and
lifecycle stay explicit in your code, exactly like any other catalog.

### Rendering for prompts and humans

A `Connectors` collection renders itself two ways, mirroring the per-connector
projections. Bound parameters (including bound secrets) never appear in either,
because both read the **exposed** signature.

- `describe()` — a numbered, human-readable listing (`Connectors (N):`, or
  `Connectors (empty)` when there are none).
- `to_llm(*, header="", heading="Connectors")` — a compact prompt section: an
  optional leading `header`, then a `## {heading} (N)` line, then one
  `to_llm()` card per connector. Returns an empty string when the collection is
  empty **and** no `header` is given.

```python
prompt = all_tools.to_llm(header="Available tools:", heading="Connectors")
print(prompt)
print(all_tools.describe())
```

See [Defining connectors](defining-connectors.md) for what goes into each card
and [The connector model](index.md) for the bigger picture.

## See also

- [Defining connectors](defining-connectors.md) — the `@connector` decorator, `secrets=`, and namespace hints
- [Loaders and enumerators](loaders-and-enumerators.md) — the two stricter connector verbs you compose into bundles
- [Results and output specs](results.md) — the `Result` envelope every call returns
- [Errors](errors.md) — the typed exceptions connectors raise
- [Discovering installed providers](../plugins/discovery.md) — loading plugin `CONNECTORS` bundles to compose with `+`
