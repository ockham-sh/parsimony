# Defining connectors

The `@connector` decorator turns a `def` into a frozen `Connector`: a small
callable plus the metadata Parsimony needs to call it, validate its output, render it for an
LLM, and stamp its results with provenance. This page covers the decorator in depth — its two
call forms, every keyword, the defaults it derives, the validation it runs at decoration time,
and what happens to your return value when the connector is called.

All three names come from the package root:

```python
from parsimony import connector, loader, enumerator
```

`loader` and `enumerator` are stricter variants built on top of `connector`; they get their own
page. This page is about the general `@connector`. See
[loaders and enumerators](loaders-and-enumerators.md) for the two specialized verbs.

## The two decorator forms

`@connector` is overloaded so it works bare or called.

```python
import pandas as pd
from parsimony import connector

@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search for test series by keyword."""
    return pd.DataFrame({"id": ["A", "B"], "title": [f"Series about {query}", "Other"]})

# Called form, with keyword options:
@connector(name="ecb_search", tags=["search"])
def search(query: str) -> pd.DataFrame:
    """Search ECB series by keyword."""
    return pd.DataFrame({"id": ["X"], "title": [query]})
```

Both produce a `Connector` instance. The bare form passes your function straight to the
decorator; the called form returns a decorator that wraps it. Use `@connector()` (called, no
arguments) only if you want the called form without options — `@connector` is the idiomatic
bare spelling.

The decorated object is a frozen dataclass, not your original function. Its parameters *are* the
connector's call surface — there is no separate `params: SomeModel` wrapper, and the conformance
suite forbids one. Pass flat, top-level scalar parameters.

!!! note "Connectors are always synchronous"
    The wrapped function must be a plain function. An `async def` raises
    `TypeError("<name>: connector function must be synchronous; ...")` at decoration time.
    Calling a connector is also synchronous — see [Calling](#calling) below.

## Name and description

```python
@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search for test series by keyword."""
    ...

assert demo_search.name == "demo_search"
assert demo_search.description == "Search for test series by keyword."
```

- **`name`** defaults to `fn.__name__`. An explicit `name=` overrides it. The name becomes the
  connector's identity in a [`Connectors`](calling-binding-composing.md) collection and is
  recorded as `provenance.source` on every result.
- **`description`** defaults to the stripped `fn.__doc__`. You can override it with
  `description=`. A description is **required** — if both the docstring and `description=` are
  empty, decoration raises
  `ValueError("<name>: add a docstring or pass description= ...")`.

The description is not decoration: it is the text an LLM reads to decide whether to call the
connector, so write it as a precise capability statement.

!!! warning "Empty description is a hard error"
    ```python
    @connector
    def no_doc(x: str) -> dict:  # no docstring, no description=
        return {}
    # ValueError: no_doc: add a docstring or pass description= (connector description is required)
    ```

## Decorator keywords

| Keyword | Type | Default | Purpose |
|---|---|---|---|
| `name` | `str \| None` | `fn.__name__` | Connector identity; becomes `provenance.source`. |
| `description` | `str \| None` | stripped `fn.__doc__` | Required capability text. |
| `output` | `OutputConfig \| None` | `None` | Declarative output schema applied to DataFrame/Series returns. |
| `tags` | `list[str] \| None` | `()` | Free-form labels used by `Connectors.search`/`filter`. |
| `properties` | `dict[str, Any] \| None` | `{}` | Exact-match metadata used by `Connectors.search`. |
| `secrets` | `tuple[str, ...]` | `()` | Parameter names to strip from provenance. |

`tags` and `properties` are stored as read-only views (`tags` as a tuple, `properties` as a
`MappingProxyType`). `output` is an [`OutputConfig`](results.md); when present and the return is
a DataFrame/Series, the schema is applied. The `secrets` and `output` keywords are covered in
their own sections below.

```python
@connector(tags=["finance"], properties={"region": "us"})
def fetch(q: str) -> dict:
    """Fetch a finance value."""
    return {"q": q}

assert fetch.tags == ("finance",)
assert dict(fetch.properties) == {"region": "us"}
```

## Declaring secrets

`secrets=` names parameters whose values must never appear in
[provenance](results.md#provenance). At decoration time the names are validated against the
function's actual parameters — an unknown name raises
`ValueError("secrets references unknown parameters: [...]")`.

```python
import pandas as pd
from parsimony import connector

@connector(secrets=("api_key",))
def keyed(query: str, api_key: str) -> pd.DataFrame:
    """Fetch data using an API key."""
    return pd.DataFrame({"q": [query]})

result = keyed(query="GDP", api_key="sk-secret")
assert result.provenance.params == {"query": "GDP"}  # api_key stripped
```

A declared secret is stripped from `provenance.params` whether you supply it at call time (as
above) or fix it with `bind()`. Binding additionally removes the parameter from the connector's
*exposed* call surface, so it never shows up in `describe()`/`to_llm()` cards either. That is the
canonical idiom for injecting credentials and base URLs without leaking them to an agent — see
[binding](calling-binding-composing.md#binding-parameters).

!!! warning "Stripping is name-based"
    Only the exact declared parameter names are removed. A sensitive value passed under a
    parameter that is not listed in `secrets=` is recorded in provenance verbatim.

## Namespace hints

Annotate a parameter with `Annotated[T, "ns:<namespace>"]` to declare which catalog
[namespace](../catalog/entities.md) its values belong to. The framework parses these into
`namespace_hints` and surfaces them in the LLM-facing cards.

```python
from typing import Annotated
import pandas as pd
from parsimony import connector
from parsimony.result import Column, ColumnRole, OutputConfig

OUT = OutputConfig(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
    Column(name="value", role=ColumnRole.DATA),
])

@connector(output=OUT)
def fred_fetch(series_id: Annotated[str, "ns:fred_series"]) -> pd.DataFrame:
    """Fetch FRED time series observations by series_id."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})

assert dict(fred_fetch.namespace_hints) == {"series_id": "fred_series"}
assert "[ns:fred_series]" in fred_fetch.to_llm()
```

A hint tells a downstream agent that `series_id` accepts codes drawn from the `fred_series`
namespace — the same namespace a sibling [enumerator](loaders-and-enumerators.md) would populate
in a [catalog](../catalog/index.md). An empty hint (`"ns:"`) is ignored.

## Calling

Calling a connector binds your arguments against the *exposed* signature,
applies defaults, calls your function, and wraps the raw return value into a
[`Result`](results.md).

```python
from parsimony import connector
import pandas as pd

@connector
def demo_search(query: str) -> pd.DataFrame:
    """Search for test series by keyword."""
    return pd.DataFrame({"id": ["A", "B"], "title": [query, "Other"]})

result = demo_search(query="GDP")
print(result.df)                      # the DataFrame you returned
print(result.provenance.source)       # 'demo_search'
print(result.provenance.params)       # {'query': 'GDP'}
```

Invalid call-time arguments raise `TypeError("Invalid parameters for connector '<name>': ...")`
(e.g. a missing required argument or an unexpected keyword), so callers get a clear,
connector-named error rather than a raw binding failure.

## How return values are wrapped

You return **raw data**; the framework builds the result envelope. The rules:

| Return value | `output=` set? | Result |
|---|---|---|
| `DataFrame` / `Series` | yes | `TabularResult` with the schema applied via `OutputConfig.build_table_result` |
| `DataFrame` / `Series` | no | bare `TabularResult` (no schema) |
| scalar / `dict` / any other | — | `Result` (the value lands on `result.data`) |
| `tuple` | — | **`TypeError`** |
| `Result` / `TabularResult` | — | **`TypeError`** |

Returning a `(data, properties)` tuple, a `Result`, or a `TabularResult` is rejected with
`TypeError(... must return raw data ...)`. The framework — not your connector — owns the
execution envelope and the [provenance](results.md#provenance) on it. Put provider facts in
DataFrame columns, not in a side-channel tuple.

```python
@connector
def bad(q: str) -> tuple:
    """Returns a forbidden tuple."""
    return (pd.DataFrame({"a": [1]}), {"prop": 1})

# bad(q="x")
# TypeError: connector 'bad': must return raw data, not (data, properties) tuples; ...
```

Provenance is always framework-built: `source` is the connector name, `source_description` is
the description, `fetched_at` is the current UTC time, `params` is the call-time arguments with
declared secrets removed, and `properties` is empty. Bound arguments are *never* recorded as
provenance params — only call-time arguments are.

### Applying an output schema

When `output=` is an [`OutputConfig`](results.md) and you return a DataFrame/Series, the schema
maps your columns into roles (`KEY`/`TITLE`/`DATA`/`METADATA`), coerces dtypes, and produces a
schema-applied `TabularResult`. For a plain `@connector` (and a `@loader`), any DataFrame column
not named in the schema is folded in as a `DATA` column. (An `@enumerator` is the exception — it
enforces an exact column match instead.)

```python
import pandas as pd
from parsimony import connector
from parsimony.result import Column, ColumnRole, OutputConfig

OUT = OutputConfig(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="demo"),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])

@connector(output=OUT)
def fetch(series_id: str) -> pd.DataFrame:
    """Fetch demo observations."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0], "extra": ["z"]})

result = fetch(series_id="X")
assert [c.name for c in result.columns] == ["date", "value", "extra"]
assert result.columns[2].role == ColumnRole.DATA      # 'extra' merged as DATA
```

### Schema failures become `ParseError`

A `ValueError` raised while wrapping the result — typically a dtype coercion failure such as an
all-non-numeric column declared `dtype="numeric"` — is re-raised as a typed
[`ParseError`](errors.md), carrying the connector name as `provider`. This is what
`Connector.__call__` raises on a schema mismatch, so callers see one consistent operational error
type rather than a raw pandas/pydantic exception.

```python
import pandas as pd
from parsimony import connector
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.errors import ParseError

OUT = OutputConfig(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="demo"),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])

@connector(output=OUT)
def fetch(series_id: str) -> pd.DataFrame:
    """Fetch demo observations."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": ["not-a-number"]})

try:
    fetch(series_id="X")
except ParseError as exc:
    print(exc.provider)   # 'fetch'
```

!!! note "Operational errors only"
    `ParseError` and its siblings are for *operational* failures — bad upstream data, auth, rate
    limits. Programmer errors (a forbidden tuple return, an unknown argument, a bad
    `OutputConfig`) stay as `TypeError` / `ValueError` / pydantic `ValidationError`. See
    [Errors](errors.md) for the full taxonomy.

## Projections: `describe()` and `to_llm()`

Every connector renders itself two ways. Both operate on the *exposed* signature, so bound
parameters (including bound secrets) are invisible in both.

- **`describe()`** — a multi-line, human-readable block: header, description, a `Parameters`
  section (each parameter's type, `required`/`optional`, and any `namespace=` hint), an
  `Output Schema` section (column name + role + namespace) when `output=` is set, and `Tags` /
  `Properties` lines.
- **`to_llm()`** — a compact, token-efficient card for system prompts: `### <name> [tags]`, the
  collapsed description (with a `Returns: col1, col2.` line when an `OutputConfig` declares
  columns), then one `- <param>?: <type> [ns:x]` line per exposed parameter (`?` marks optional,
  `[ns:...]` marks a namespace hint).

```python
print(fred_fetch.to_llm())
# ### fred_fetch
# Fetch FRED time series observations by series_id. Returns: date, value.
# - series_id: Annotated [ns:fred_series]

print(fred_fetch.describe())
# Connector: fred_fetch
# ─────────────────────
#
# Fetch FRED time series observations by series_id.
#
# Parameters:
#   series_id: Annotated (required)  —  namespace='fred_series'
#
# Output Schema:
#   date   KEY         namespace='fred_series'
#   value  DATA
```

!!! tip "Secrets and `*args`/`**kwargs` stay out of cards"
    Bound parameters are dropped from `exposed_signature`, so a bound secret never reaches a
    card. Variadic `*args` / `**kwargs` parameters are also skipped by both projections.

## Inspecting a connector

`Connector` is a frozen dataclass. The fields and helpers most useful when defining connectors:

| Member | Kind | What it gives you |
|---|---|---|
| `name` | field | The connector's identity. |
| `description` | field | The required capability text. |
| `tags` | field | `tuple[str, ...]` of labels. |
| `properties` | field | Read-only metadata mapping. |
| `namespace_hints` | field | Read-only `{param: namespace}` mapping from `Annotated` hints. |
| `secrets` | field | `tuple[str, ...]` of secret parameter names. |
| `output_config` | field | The `OutputConfig` or `None`. |
| `exposed_signature` | property | The post-binding `inspect.Signature` callers and cards see. |
| `describe()` / `to_llm()` | method | Human and LLM projections. |

Because the dataclass is frozen, you never mutate a connector; `bind()` and `with_callback()`
return new instances. Those, plus composing connectors into a collection, are covered in
[Calling, binding, and composing](calling-binding-composing.md).

## See also

- [Loaders and enumerators](loaders-and-enumerators.md) — the two stricter connector verbs and their output contracts.
- [Calling, binding, and composing](calling-binding-composing.md) — calling connectors, fixing parameters with `bind`, and `Connectors` collections.
- [Results and output schemas](results.md) — `OutputConfig`, `Column`, `ColumnRole`, `Provenance`, and dtype coercion.
- [Errors](errors.md) — the typed exception taxonomy connectors raise and `ParseError` translation.
