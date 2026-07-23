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
| `output` | `OutputSpec \| None` | `None` | Declarative column-role schema, attached to every result unchanged. |
| `tags` | `list[str] \| None` | `()` | Free-form labels used by `Connectors.search`/`filter`. |
| `properties` | `dict[str, Any] \| None` | `{}` | Exact-match metadata used by `Connectors.search`. |
| `secrets` | `tuple[str, ...]` | `()` | Parameter names to strip from provenance. |
| `requires` | `tuple[str, ...]` | `()` | Env-var names that must resolve for a call to succeed; surfaced in the cards, never read by core. |

`tags` and `properties` are stored as read-only views (`tags` as a tuple, `properties` as a
`MappingProxyType`). `output` is an [`OutputSpec`](results.md); when present, it is attached to
every result on `result.output_spec` — the framework never inspects or applies it to the data
itself. The `secrets`, `requires`, and `output` keywords are covered in their own sections below.

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

## Declaring required env vars

`secrets=` and `requires=` answer two different questions, and they are independent:

- **`secrets=` — must this value be *hidden*?** It names *parameters* whose call-time values
  are redacted from provenance and error surfaces. It says nothing about whether the connector
  can run without the value.
- **`requires=` — must this value *exist*?** It names *environment variables* that must resolve
  for a call to succeed. A name in `requires=` is exactly the env var that
  [`UnauthorizedError`](errors.md#unauthorizederror-401403-bad-credentials) names when the
  connector is called with nothing configured.

`requires=` is a **static declaration, never resolution** — core stores the tuple verbatim and
never reads `os.environ` from it (nor validates the names against the signature; an env var need
not correspond to any parameter). Resolving the value, and fast-failing when it is absent, is
the connector body's job — typically via `require_key` (from
`parsimony.transport.helpers`), which raises `UnauthorizedError(env_var=...)` before any
network call. `requires=` is the matching *declaration* of that runtime behaviour, so tooling
can report what a connector needs without importing and calling it.

```python
import os
import pandas as pd
from parsimony import connector
from parsimony.transport.helpers import require_key

@connector(secrets=("api_key",), requires=("FRED_API_KEY",))
def fred_fetch(series_id: str, api_key: str = "") -> pd.DataFrame:
    """Fetch FRED observations by series_id."""
    key = require_key(api_key, env_var="FRED_API_KEY", provider="fred")  # fast-fails if absent
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})
```

Because the two axes are independent, all four combinations occur:

| | `requires` non-empty | `requires` empty |
|---|---|---|
| **`secrets` non-empty** | required key — the call cannot succeed unconfigured (`fred`, `eia`) | optional key — a secret-marked param the call does *not* require; the key only lifts a quota (`riksbank`, `bls`) |
| **`secrets` empty** | a required env var with no parameter at all — e.g. `sec_edgar`'s `SEC_EDGAR_USER_AGENT` header | keyless (`treasury`, `boc`) |

Neither field derives from the other. For an *optional* key, the env var the connector reads
when the parameter is omitted lives only in the docstring/prose — `requires=` stays empty
because the call still succeeds without it.

## Namespace hints

Annotate a parameter with `Annotated[T, "ns:<namespace>"]` to declare which catalog
[namespace](../catalog/entities.md) its values belong to. The framework parses these into
`namespace_hints` and surfaces them in the LLM-facing cards.

```python
from typing import Annotated
import pandas as pd
from parsimony import connector
from parsimony.result import Column, ColumnRole, OutputSpec

OUT = OutputSpec(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
    Column(name="value", role=ColumnRole.DATA),
])

@connector(output=OUT, requires=("FRED_API_KEY",))
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
print(result.raw)                     # the DataFrame you returned (also result.frame)
print(result.provenance.source)       # 'demo_search'
print(result.provenance.params)       # {'query': 'GDP'}
```

Invalid call-time arguments raise `TypeError("Invalid parameters for connector '<name>': ...")`
(e.g. a missing required argument or an unexpected keyword), so callers get a clear,
connector-named error rather than a raw binding failure.

## How return values are wrapped

You return **raw data**; the framework builds the result envelope. There is one
result type, `Result`; a DataFrame return is simply a `Result` whose `raw` is the
frame (`result.is_tabular` is then `True`). The rules:

| Return value | Result |
|---|---|
| `DataFrame` / `Series` | `Result` carrying the frame unchanged, plus `output_spec` if `output=` was set (`is_tabular`) |
| scalar / `dict` / any other | `Result` (the value lands on `result.raw`) |
| `tuple` | **`TypeError`** |
| `Result` | **`TypeError`** |

Returning a `(data, properties)` tuple or a `Result` is rejected with
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

### The output schema is attached, not applied

When `output=` is an [`OutputSpec`](results.md), it is attached to the result as
`result.output_spec` **verbatim** — the framework never inspects your returned DataFrame's
columns against it, never coerces a dtype, never renames or reorders a column, and never drops or
adds one. Whatever columns you return are exactly the columns on `result.raw`.

```python
import pandas as pd
from parsimony import connector
from parsimony.result import Column, ColumnRole, OutputSpec

OUT = OutputSpec(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="demo"),
    Column(name="value", role=ColumnRole.DATA),
])

@connector(output=OUT)
def fetch(series_id: str) -> pd.DataFrame:
    """Fetch demo observations."""
    return pd.DataFrame({"date": ["2020-01-01"], "value": [1.0], "extra": ["z"]})

result = fetch(series_id="X")
assert list(result.raw.columns) == ["date", "value", "extra"]  # returned unchanged, incl. "extra"
assert [c.name for c in result.output_spec.columns] == ["date", "value"]  # schema is unrelated
```

`result.columns` (the schema's declared `Column`s, if you attached one) and `result.raw.columns`
(the DataFrame's actual columns) are two independent things now — nothing keeps them in sync for
you. Anything that needs both aligned — an [entity projection](results.md#entity-projection), a
[data store load](../catalog/data-store.md) — validates that alignment itself, at the point it is
used, and raises `ValueError` if a declared column is missing from the data.

!!! tip "No dtype coercion, ever"
    `Column` has no `dtype=` field. If a provider hands you string-typed dates or numbers,
    `pd.to_datetime(...)` / `pd.to_numeric(...)` (or equivalent) in the connector body, before you
    return — see [Results and output schemas](results.md#column) for the rationale.

!!! note "Programmer errors stay plain exceptions"
    A forbidden tuple/`Result` return, an unknown call argument, or a malformed `OutputSpec` stay
    as `TypeError` / `ValueError` / pydantic `ValidationError` — they are not part of the
    [`ParseError`](errors.md) operational-error taxonomy. See [Errors](errors.md) for the full
    taxonomy and when a connector should raise `ParseError` itself.

## Projections: `describe()` and `to_llm()`

Every connector renders itself two ways. Both operate on the *exposed* signature, so bound
parameters (including bound secrets) are invisible in both.

- **`describe()`** — a multi-line, human-readable block: header, description, a `Parameters`
  section (each parameter's type, `required`/`optional`, and any `namespace=` hint), a
  `Requires: <ENV_VAR>, ...` line when `requires=` is non-empty, an
  `Output Schema` section (column name + role + namespace) when `output=` is set, and `Tags` /
  `Properties` lines.
- **`to_llm()`** — a compact, token-efficient card for system prompts: `### <name> [tags] (needs <ENV_VAR>)`
  (the `(needs …)` suffix appears only when `requires=` is non-empty), the
  collapsed description (with a `Returns: <col> (ROLE ns:x), ...` line when an `OutputSpec`
  declares columns — one `name (ROLE)` token per LLM-visible column, `ns:` appended for KEY/METADATA
  columns that declare a namespace, columns with `exclude_from_llm_view` omitted), then one
  `- <param>?: <type> [ns:x]` line per exposed parameter (`?` marks optional, `[ns:...]` marks a
  namespace hint).

```python
print(fred_fetch.to_llm())
# ### fred_fetch (needs FRED_API_KEY)
# Fetch FRED time series observations by series_id. Returns: date (KEY ns:fred_series), value (DATA).
# - series_id: str [ns:fred_series]

print(fred_fetch.describe())
# Connector: fred_fetch
# ─────────────────────
#
# Fetch FRED time series observations by series_id.
#
# Parameters:
#   series_id: str (required)  —  namespace='fred_series'
#
# Requires: FRED_API_KEY
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
| `requires` | field | `tuple[str, ...]` of env-var names the call needs to succeed. |
| `output_spec` | field | The `OutputSpec` or `None`. |
| `exposed_signature` | property | The post-binding `inspect.Signature` callers and cards see. |
| `describe()` / `to_llm()` | method | Human and LLM projections. |

Because the dataclass is frozen, you never mutate a connector; `bind()` returns a new
instance. That, plus composing connectors into a collection, is covered in
[Calling, binding, and composing](calling-binding-composing.md).

## See also

- [Loaders and enumerators](loaders-and-enumerators.md) — the two stricter connector verbs and their output contracts.
- [Calling, binding, and composing](calling-binding-composing.md) — calling connectors, fixing parameters with `bind`, and `Connectors` collections.
- [Results and output schemas](results.md) — `OutputSpec`, `Column`, `ColumnRole`, `Provenance`, and the entity projection.
- [Errors](errors.md) — the typed exception taxonomy connectors raise and `ParseError` translation.
