# Authoring a provider plugin

A provider plugin is a `parsimony-<name>` distribution that exposes one or more
connectors to anyone who installs it. Core ships zero connectors; you publish your own
package, register it under the `parsimony.providers` entry-point group, and export a
module-level `CONNECTORS: Connectors` that consumers load through
[discovery](discovery.md). This page walks through the package layout, the entry-point
declaration, and a minimal conformant module you can copy.

The contract is intentionally small. A plugin is *just* a Python module that defines
connectors with the standard decorators and binds them into a `Connectors` collection
named `CONNECTORS`. Everything else — typed errors, HTTP transport, identity helpers —
is reused from `parsimony-core`, so your plugin stays thin.

## What the kernel requires

A conformant plugin satisfies exactly these conditions:

| Requirement | Enforced by |
|---|---|
| The distribution declares a `parsimony.providers` entry point | discovery (`iter_providers`) |
| The entry-point value is a dotted module path | discovery |
| Importing that module yields a module-level `CONNECTORS` | `Provider.load()` raises `TypeError` otherwise |
| `CONNECTORS` is a `parsimony.connector.Connectors` instance | `Provider.load()` / conformance |
| `CONNECTORS` is non-empty | [conformance](conformance.md) |
| Every connector has a description (20–800 chars) | conformance |
| Connector parameters are flat (no bundled `params: BaseModel`) | conformance |

Validate all of this with the [conformance toolkit](conformance.md) before you publish.

## Package layout

A plugin is an ordinary Python package. The connector module that exports `CONNECTORS`
can be the package's `__init__.py` or any submodule — the entry point points at whichever
you choose.

```text
parsimony-acme/
├── pyproject.toml
├── src/
│   └── parsimony_acme/
│       ├── __init__.py        # exports CONNECTORS
│       └── connectors.py      # (optional) the actual @connector defs
└── tests/
    └── test_conformance.py    # uses parsimony.testing
```

!!! note "Naming convention"
    Distributions are named `parsimony-<name>` on PyPI (e.g. `parsimony-fred`,
    `parsimony-sdmx`), and the importable package uses an underscore
    (`parsimony_fred`). The *provider name* — the entry-point key — is a short
    identifier such as `fred`; it is what callers pass to
    `parsimony.discover.load("fred")` and what `parsimony list` shows.

## Declaring the entry point

The only packaging requirement is a `[project.entry-points."parsimony.providers"]`
table mapping a provider name to the dotted module path that exports `CONNECTORS`. The
group name `parsimony.providers` is fixed; the kernel queries exactly this group.

```toml
# pyproject.toml
[project]
name = "parsimony-acme"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["parsimony-core>=0.0.1"]

[project.urls]
Homepage = "https://github.com/acme/parsimony-acme"

[project.entry-points."parsimony.providers"]
acme = "parsimony_acme"
```

Here `acme` is the provider name and `parsimony_acme` is the module imported on
`Provider.load()`. The `Homepage` URL (or a `[project.urls]` entry whose key is
case-insensitively `homepage`) surfaces on the discovered `Provider.homepage` property —
declaring it is optional but recommended.

!!! warning "One name per provider, globally"
    `iter_providers()` raises `RuntimeError` if two installed distributions register the
    same provider name — the kernel refuses to guess which one wins. Pick a name unlikely
    to collide and keep it stable across releases.

## Building the connectors

Inside the connector module, use the standard decorators from `parsimony.connector` and
the schema types from `parsimony.result`. There is no plugin-specific base class to
subclass — a connector is a `def` plus metadata.

```python
import pandas as pd

from parsimony.connector import connector, enumerator, Connectors
from parsimony.result import Column, ColumnRole, OutputSpec
```

Three rules govern every connector you write:

- **Functions must be synchronous.** An `async def` raises `TypeError` at decoration time.
- **Parameters are the call surface.** Expose flat, top-level scalar parameters — never a
  single bundled `params: SomeModel` object. The conformance suite forbids the bundled
  idiom (the check fires on any public parameter literally named `params` annotated as a
  pydantic `BaseModel`).
- **Return raw data.** A connector returns a `DataFrame`, `Series`, scalar, or `dict`; the
  framework wraps it in a [`Result`](../connectors/results.md) with framework-built
  [`Provenance`](../connectors/results.md), attaching `output=` verbatim as `result.output_spec`
  — never coercing dtypes or touching a column. Returning a `Result` or a `(data, props)` tuple
  raises `TypeError`.

Pick the decorator that matches the verb:

| Decorator | Output contract | Feeds |
|---|---|---|
| `@connector` | optional `output=`, attached unchanged, never applied to the data | anything |
| `@loader(output=...)` | exactly one namespaced KEY, ≥1 DATA, no TITLE/METADATA | a [data store](../catalog/data-store.md) |
| `@enumerator(output=...)` | exactly one namespaced KEY, ≥1 TITLE, no DATA; must annotate a `pd.DataFrame` return | a [catalog](../catalog/index.md) |

See [defining connectors](../connectors/defining-connectors.md) and
[loaders and enumerators](../connectors/loaders-and-enumerators.md) for the full
contracts and validation timing.

### Injecting secrets with `bind`

Declare credential parameters in `secrets=(...)`. Declared secret names are validated
against the function's real parameters at decoration time (an unknown name raises
`ValueError`) and are stripped from the recorded provenance. To supply a key without
exposing it to an LLM or a downstream caller, fix it with `bind` — the bound parameter
disappears from `exposed_signature`, from the `describe()`/`to_llm()` cards, and from
provenance.

```python
import os

# acme_fetch declares secrets=("api_key",); bind hides it from the call surface
wired = acme_fetch.bind(api_key=os.environ["ACME_API_KEY"])
# wired.exposed_signature now omits api_key entirely
```

This is the standard idiom for wiring a base URL or API key into a connector before
handing the collection to an agent. See
[calling, binding, and composing](../connectors/calling-binding-composing.md).

## Using the HTTP transport

Build your HTTP calls on `parsimony.transport` rather than hand-rolling `httpx`. The
helpers in `parsimony.transport.helpers` construct a configured `HttpClient`, and
`fetch_json` performs a GET, raises for status, maps `httpx` errors to the typed
[connector errors](../connectors/errors.md), and returns parsed JSON in one call.

```python
from parsimony.transport.helpers import fetch_json, make_api_key_client

# default API-key query param is "apikey"; default helper timeout is 15s
client = make_api_key_client("https://api.example.com", provider="acme", api_key="...", api_key_param="apikey")

def _fetch(series_id: str) -> dict:
    return fetch_json(
        client,
        path="series",
        params={"id": series_id},     # None-valued params are dropped
        op_name="fetch_series",
    )
```

`make_http_client(base_url, *, provider, query_params=None, headers=None, timeout=15.0)` builds a
client without a default API key. `HttpClient.request` returns the raw response and does
**not** check its status itself — `fetch_json` calls `check_status` for you and translates
failures. For enumerator loops and fan-out fetches, reuse one pooled connection via the
`pooled_client` context manager. The full transport surface is documented under
[HTTP transport](../connectors/http-transport.md).

## Raising typed errors

Surface operational failures as the typed exceptions from `parsimony.errors`, not raw
`httpx` exceptions or bare strings. Each carries a `provider` and a default, agent-facing
message embedding directives like "DO NOT retry"; the `fetch_json` helper already maps
upstream HTTP failures to the right type.

```python
from parsimony.errors import EmptyDataError, UnauthorizedError

def _fetch(series_id: str, *, api_key: str) -> pd.DataFrame:
    if not api_key:
        # the env_var argument tells the agent which variable to set
        raise UnauthorizedError("acme", env_var="ACME_API_KEY")
    rows = _query(series_id, api_key)
    if not rows:
        raise EmptyDataError("acme", query_params={"series_id": series_id})
    return pd.DataFrame(rows)
```

!!! warning "Operational errors only"
    The typed taxonomy is for *operational* failures (bad credentials, rate limits,
    empty results, upstream errors). Programmer errors stay as `TypeError`, `ValueError`,
    or pydantic `ValidationError`. If you override a default message with `message=`, you
    own the agent-facing text — keep it free of URLs, tokens, and upstream prose that
    could leak credentials or carry prompt-injection vectors.

## Identity conventions

Entities live in a `(namespace, code)` space. The namespace is a lowercase snake_case
identity scope; the code is the entity's identifier within it. Normalize provider-derived
strings with the helpers in `parsimony.entity` (also re-exported from `parsimony.catalog`)
so your codes and namespaces are valid and stable.

```python
from parsimony.entity import code_token, normalize_namespace

code_token("US.Real GDP-2024")     # -> "us_real_gdp_2024"
normalize_namespace("acme_series")  # -> "acme_series" (raises if not snake_case)
```

`code_token` lowercases, collapses separators to single underscores, drops disallowed
characters, and prefixes a leading-digit token with `v_`. `normalize_namespace` enforces
the `^[a-z][a-z0-9_]*$` pattern and raises `ValueError` on anything else. Declare the KEY
column's namespace on your loader/enumerator `OutputSpec`, and tie a parameter to a
namespace for LLM cards with an `Annotated[str, "ns:<namespace>"]` hint. See
[entities](../catalog/entities.md) for how these flow into a catalog.

## Shipping agent guidance (skills)

When your connectors share procedural knowledge that belongs to no single verb — how to
combine them into one workflow, a recovery ladder, a cardinality table — ship it as a native
[Anthropic `SKILL.md`](https://docs.claude.com/en/docs/agents-and-tools/agent-skills/overview)
inside your package:

```text
parsimony_acme/
└── skills/
    └── acme_key_resolution/
        └── SKILL.md        # YAML frontmatter (name, description) + the markdown playbook
```

It is just a file — no registration, no API, no `Connectors` coupling. The skill belongs to the
package, so anything that uses your connectors can find it:

- **Claude Code / Cursor** discover it on disk (the frontmatter `description` is the menu line,
  the body loads on demand).
- **parsimony-agents** injects the body into its prompt whenever any of your connectors are in
  the active bundle — package-presence, resolved from each connector's defining module.

Make sure the `skills/` tree ships in your wheel (with hatchling's default `packages = [...]`
layout it does; verify with `unzip -l dist/*.whl`). The frontmatter `name` should match the
skill's directory name.

## A minimal conformant module

This module exports a fetch connector and an enumerator, binds them into `CONNECTORS`,
and passes all five conformance checks. It runs with only `parsimony-core` installed (no
network is involved — the bodies return synthetic frames).

```python
import pandas as pd

from parsimony.connector import connector, enumerator, Connectors
from parsimony.result import Column, ColumnRole, OutputSpec

FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="acme"),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

ENUM_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="acme"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


@connector(output=FETCH_OUTPUT, tags=["acme", "tool"], secrets=("api_key",))
def acme_fetch(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch a series of observations for the given series_id from the ACME API."""
    df = pd.DataFrame(
        {
            "key": [series_id, series_id],
            "date": ["2024-01-01", "2024-02-01"],
            "value": [1.0, 2.0],
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


@enumerator(output=ENUM_OUTPUT, tags=["acme"])
def acme_enumerate(limit: int = 10) -> pd.DataFrame:
    """Enumerate up to ``limit`` catalog entries available from the ACME provider."""
    return pd.DataFrame([{"code": f"s{i}", "title": f"Series {i}"} for i in range(limit)])


CONNECTORS = Connectors([acme_fetch, acme_enumerate])
```

Two details worth noting:

- `acme_fetch` declares `api_key` as a secret; a consumer fixes it via
  `CONNECTORS.bind(api_key=...)` (binding is scoped per-connector — only connectors that
  actually expose `api_key` receive it).
- `acme_enumerate` is an enumerator, so its schema has a namespaced KEY and a TITLE, no
  DATA columns, and the function annotates a `pd.DataFrame` return. That shape is checked
  at decoration time; the returned frame's actual columns are only checked later, if and
  when something accesses `result.entities` on its output.

## Validate before you publish

Run the conformance toolkit against your imported module — fail-fast, raising on the
first violated check:

```python
import parsimony_acme
from parsimony.testing import assert_plugin_valid

assert_plugin_valid(parsimony_acme)  # raises ConformanceError on the first failure
```

Or, for a pytest-native suite, subclass `ProviderTestSuite` and (optionally) verify the
entry-point registration once the package is installed. The [conformance](conformance.md)
page covers the five checks, `ConformanceError`'s structured fields, and how
`parsimony list --strict` reuses the same checks from the [CLI](../cli.md).

## See also

- [Conformance testing](conformance.md) — validate your `CONNECTORS` surface before publishing
- [Discovering installed providers](discovery.md) — how consumers find and load your plugin
- [Defining connectors](../connectors/defining-connectors.md) — the `@connector` decorator in depth
- [Loaders and enumerators](../connectors/loaders-and-enumerators.md) — the two stricter verbs
- [HTTP transport](../connectors/http-transport.md) — the HTTP layer to build on
