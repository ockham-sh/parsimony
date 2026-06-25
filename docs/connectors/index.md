# The connector model

A **connector** is a small Python callable plus metadata. You write a
`def` that fetches data and returns a plain `pandas` object (or a scalar
or dict); the `@connector` decorator turns it into a frozen `Connector`, and the
framework wraps whatever you return into a [`Result`](results.md)
with framework-built [`Provenance`](results.md). The function's parameters *are*
the connector's parameters — there is no wrapper request object — which is what
makes connectors directly callable by agents.

This page is the map for the connector subsection. It covers what a connector
is, the `@connector` decorator at a glance, the rules every connector obeys, and
one compact end-to-end example. The deeper pages drill into each piece.

## What a connector is

`Connector` is a frozen dataclass that pairs a `name`, a `description`, the
wrapped `fn`, its `inspect.Signature`, and optional metadata (`tags`,
`properties`, an `output` schema, declared `secrets`, namespace hints). You do
not construct it directly — you decorate a function:

```python
import pandas as pd
from parsimony.connector import connector


@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search demo series by keyword."""
    return pd.DataFrame(
        {"id": ["A", "B"], "title": [f"Series about {query}", "Another"]}
    )


result = demo_search(query="GDP")
print(result.provenance.source)   # demo_search
print(result.provenance.params)   # {'query': 'GDP'}
print(len(result.data))           # 2
```

Calling a connector returns its result directly. The return value is always a
`Result`. When the connector returns a DataFrame or Series the result is
*tabular* (`result.is_tabular` is `True`); otherwise `data` carries the scalar
or dict unchanged.

!!! note "Import path"
    `connector`, `loader`, `enumerator`, `Connector`, and `Connectors` are
    re-exported at the package root, so `from parsimony import connector` also
    works.

## The decorator at a glance

`@connector` works bare or called with options:

```python
from parsimony.connector import connector
from parsimony.result import Column, ColumnRole, OutputConfig

OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="demo"),
        Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
    ]
)


@connector(
    name="demo_fetch",
    output=OUTPUT,
    tags=["demo"],
    secrets=("api_key",),
)
def fetch(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch demo observations for a series."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `name` | `fn.__name__` | Connector identity; becomes `provenance.source`. |
| `description` | stripped `fn.__doc__` | **Required.** Human/agent-facing summary. |
| `output` | `None` | An [`OutputConfig`](results.md) schema applied to a DataFrame/Series return. |
| `tags` | `()` | Free-form labels (`loader`/`enumerator` set them automatically). |
| `properties` | `{}` | Read-only metadata you can filter the [collection](calling-binding-composing.md) on. |
| `secrets` | `()` | Parameter names stripped from provenance; validated against the signature. |

[`loader` and `enumerator`](loaders-and-enumerators.md) are stricter factories
built on the same machinery — they add output-schema contracts and a verb tag.

## The rules every connector follows

These invariants are enforced by the framework — most at decoration time, so a
malformed connector fails as soon as the module imports.

### Parameters are the call surface

A connector exposes flat, top-level parameters. There is no bundled
`params: SomeModel` object — the conformance suite (see
[conformance testing](../plugins/conformance.md)) actively forbids annotating a
public `params` parameter as a Pydantic `BaseModel`. Scalar parameters keep the
call surface legible to an agent and let the framework record exactly which
arguments produced a result.

### The function must be synchronous

The decorator rejects an `async def` at decoration time:

```python
from parsimony.connector import connector

try:
    @connector
    async def not_sync(q: str) -> str:  # async is rejected
        """Async functions are rejected."""
        return q
except TypeError as exc:
    print(exc)   # not_sync: connector function must be synchronous; ...
```

### A description is mandatory

`description` defaults to the stripped docstring; pass `description=` to override
it. If both are empty the decorator raises `ValueError`:

```python
try:
    @connector
    def undocumented(q: str) -> str:
        return q
except ValueError as exc:
    print(exc)   # undocumented: add a docstring or pass description= ...
```

### Return raw data — never a Result

A connector returns raw data: a `pandas.DataFrame`/`Series`, a scalar, or a
dict. The framework builds the output envelope (the `Result` and its
`Provenance`); a connector that tries to build it itself fails at call time.

!!! warning "Returning the wrong shape raises `TypeError`"
    Returning a `Result`, or a `(data, properties)` tuple, raises `TypeError`
    from inside the call — put provider facts in DataFrame columns and let the
    framework wrap them. When an `output` schema is set, a schema/coercion
    `ValueError` during wrapping is re-raised as a typed [`ParseError`](errors.md).

Why raw data? It keeps the connector author's job to one thing — fetch and shape
the data — while the framework owns provenance, secret stripping, and the
declarative [output schema](results.md). It also means a connector cannot
fabricate provenance: `Provenance.source`, `fetched_at`, and `params` are always
framework-built.

### Secrets are declared, not exposed

Names listed in `secrets=` must be real parameters (an unknown name raises
`ValueError` at decoration time). Their values are stripped from
`provenance.params` whether they were supplied at call time or fixed with
`bind`. Combined with `bind`, this is how you inject an API key without it
appearing in agent-facing cards or recorded provenance — see
[calling, binding, and composing](calling-binding-composing.md).

## A compact end-to-end example

Define a connector, attach an output schema, bind a secret, collect it into a
`Connectors` bundle, and call it by name. This runs with only `parsimony-core`
installed — no network, no plugins, no optional extras.

```python
import pandas as pd
from parsimony.connector import Connectors, connector
from parsimony.result import Column, ColumnRole, OutputConfig

OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="demo"),
        Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
    ]
)


@connector(output=OUTPUT, tags=["demo"], secrets=("api_key",))
def demo_fetch(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch demo observations for a series id."""
    # A real connector would call an HTTP API here using api_key.
    return pd.DataFrame({"date": ["2020-01-01", "2020-02-01"], "value": ["1", "2"]})


# Bind the secret once; it disappears from the call surface and provenance.
wired = demo_fetch.bind(api_key="s3cr3t")
bundle = Connectors([wired])

print(bundle.names())                       # ['demo_fetch']
result = bundle["demo_fetch"](series_id="X1")

print(result.provenance.source)             # demo_fetch
print(result.provenance.params)             # {'series_id': 'X1'} — api_key stripped
print(result.data["value"].tolist())        # [1, 2] — coerced to numeric
```

A few things to notice:

- `bundle["demo_fetch"](...)` is the canonical execution pattern:
  `connectors[name](**kwargs)`. `Connectors.__getitem__` takes a connector
  **name**, not an integer index.
- `bind` returns a *new* connector — `Connector` is frozen — with `api_key`
  fixed and removed from the public signature.
- The `OutputConfig` coerced the string `value` column to numeric because the
  column declared `dtype="numeric"`.

You merge bundles with the `+` operator (`bundle_a + bundle_b`); there is no
`.merge` method. The collection also gives you `filter`, `search`,
collection-wide `bind`, and the `describe()` / `to_llm()` prompt projections.

## The connector subsection

| Page | What it covers |
| --- | --- |
| [Defining connectors](defining-connectors.md) | The `@connector` decorator in depth: defaults, validation timings, namespace hints, and the `describe()` / `to_llm()` projections. |
| [Loaders and enumerators](loaders-and-enumerators.md) | The two stricter verbs — fetching observation values vs discovering entities — and their output-schema contracts. |
| [Calling, binding, and composing](calling-binding-composing.md) | Invoking connectors, `bind`, and the immutable `Connectors` collection. |
| [Results and output schemas](results.md) | `Result` (tabular when `data` is a DataFrame), `OutputConfig` / `Column` / `ColumnRole`, `Provenance`, and the dtype coercion rules. |
| [Errors](errors.md) | The typed, agent-facing exception taxonomy connector authors raise. |
| [HTTP transport](http-transport.md) | The HTTP layer (`HttpClient`, retry policy, redaction) that connector authors build on. |

## See also

- [Defining connectors](defining-connectors.md) — the decorator and its rules in full
- [Calling, binding, and composing](calling-binding-composing.md) — `bind` and the `Connectors` collection
- [Results and output schemas](results.md) — what a connector's return value becomes
- [Plugins and providers](../plugins/index.md) — how connectors ship as `parsimony-<name>` distributions
