# Authoring a connector

When no installed or installable connector covers the source you need (see the Route section of
`SKILL.md`), wrap it as a **project-local connector**: the same typed `Result`, provenance, and
error taxonomy as the published connectors, and a reusable artifact the project keeps.

Author the connector first, then run the analysis through it. A local connector is code the user
owns — show it to them before relying on it.

Not every source needs the catalog and search machinery `SKILL.md` describes. That apparatus
earns its place only when a source exposes many series with no other way to find one. If you
just query an endpoint by parameters, wrap it and return a DataFrame — nothing more.

## The contract

A connector is a synchronous function decorated with `@connector`. Its parameters are the call
surface; its docstring (required) is the description an agent routes by; it returns a
`pandas.DataFrame`, which the framework wraps into a typed `Result` with provenance.

```python
import pandas as pd
from parsimony import connector
from parsimony.transport.helpers import fetch_json, make_http_client

_http = make_http_client("https://api.example.org/v1", provider="example")

@connector
def example_fetch(series_id: str, start: str | None = None) -> pd.DataFrame:
    """Fetch a timeseries from Example.org by series id (e.g. 'ABC123')."""
    payload = fetch_json(_http, path="series", params={"id": series_id, "from": start}, op_name="example_fetch")
    return pd.DataFrame(payload["observations"])
```

Make the first line of the docstring name the source and what the verb does — that line is what
routing reads in `connectors.describe()`. Full details:
[Defining connectors](https://docs.parsimony.dev/connectors/defining-connectors/).

## Transport and errors

The transport helpers (`make_http_client` + `fetch_json` / `fetch_csv` / `fetch_text`, from
`parsimony.transport.helpers`) are why an authored connector behaves like an official one: they
retry transient failures, redact secrets from logs, and surface HTTP failures as the typed error
taxonomy from `SKILL.md` instead of raw exceptions. When the provider needs something beyond
them (POST bodies, sessions, unusual status semantics), see
[HTTP transport](https://docs.parsimony.dev/connectors/http-transport/) and
[Errors](https://docs.parsimony.dev/connectors/errors/) — keep failures mapped to the typed
taxonomy either way.

When a provider fights back — blocked clients, auth statuses that mean two things, errors
carried inside a 200 body, sources that can only be enumerated creatively — the connectors
repo's [guidebook](https://github.com/ockham-sh/parsimony-connectors/tree/main/docs/_guidebook)
collects the tactics the published connectors use. It is long; navigate by its table of contents
and read the section that matches your problem.

## Verify completeness

Providers can return partial data behind an HTTP 200: row caps, page limits, silent truncation
of long series. Before trusting the connector, read the provider's documentation on pagination
and limits, then confirm against a live call that a large fetch returns the full series — for
example by comparing the rows you got against a total or count field in the response, or against
a range you can bound another way. Paginate to the true total where the provider caps rows per
call.

## Credentials

Two independent declarations describe a keyed connector, answering two different questions:

- **`secrets=` — must this value be hidden?** Take the key as a parameter and list it in
  `secrets=` so it stays out of provenance and error surfaces.
- **`requires=` — must this value exist?** If the verb cannot succeed without the key, declare
  the env var it needs as a literal tuple, `requires=("EXAMPLE_API_KEY",)`. Use the *same*
  literal name you pass to `require_key`/`UnauthorizedError`, so the declaration matches the
  fast-fail. A verb that runs fine unconfigured leaves `requires=()`.

```python
from parsimony.transport.helpers import require_key

@connector(secrets=("api_key",), requires=("EXAMPLE_API_KEY",))
def example_fetch(series_id: str, api_key: str | None = None) -> pd.DataFrame:
    """Fetch a series from Example.org by id. Needs an Example.org API key."""
    key = require_key(api_key, env_var="EXAMPLE_API_KEY", provider="example")  # fast-fails if absent
    ...
```

The user binds it once — `example_fetch.bind(api_key=...)` — which also keeps the key off the
call surface. More in
[Calling, binding, composing](https://docs.parsimony.dev/connectors/calling-binding-composing/).

## Declare the output (optional)

An `OutputSpec` binds semantics to the returned columns — it drives the schema shown in
`.describe()`, the governed `result.to_llm()` preview, and entity extraction:

```python
from parsimony import Column, ColumnRole, OutputSpec

@connector(output=OutputSpec(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="example"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="*", role=ColumnRole.DATA),
]))
def example_list(...) -> pd.DataFrame: ...
```

A spec is validated when the module imports: at most one `KEY` column, and `namespace=` is
allowed only on that `KEY`. A second searchable value goes under `METADATA`, not a second key.

For a plain data fetch, skipping the spec is fine. If the source exposes many series and no way
to search them, an `@enumerator` (a listing function whose output declares a namespaced KEY, a
TITLE, and searchable metadata) plus a `Catalog` built from its entities gives you local search
over them — see the [catalog docs](https://docs.parsimony.dev/catalog/). Full details on output
schemas: [Results and output schemas](https://docs.parsimony.dev/connectors/results/).

## Test before you use

Put the tests in the project's test suite, not a throwaway cell: a live smoke test calling the
connector with known-good params (assert the expected columns and a plausible row count), and a
test for the largest fetch you'll realistically make (assert the row count against the bound you
established above). Offline tests can inject `httpx.MockTransport` via
`HttpClient(..., _transport=...)`.

The live smoke test is what decides done: a connector is not finished until a real call against
the upstream returns a real `Result`. If the source is unreachable, stop and say so — never stub
or mock the data path to make an unproven connector look complete.

## The local library convention

Local connectors live in a `connectors/` package inside the project, exporting one bundle — the
same shape a published plugin exports:

```python
# myproject/connectors/__init__.py
from parsimony import Connectors
from .example import example_fetch, example_list

CONNECTORS = Connectors([example_fetch, example_list])
```

```python
from parsimony import discover
from myproject.connectors import CONNECTORS as LOCAL

connectors = discover.load_all() + LOCAL   # one bundle: installed + local, same idiom everywhere
```

`+` merges bundles and raises on duplicate names — prefix local names with the source
(`kaggle_fetch`, not `fetch`). After the merge, everything downstream (`describe()`, `bind()`,
`[...]`) treats local and installed connectors identically. Next task in this project: check the
local library (branch 1 of routing) before authoring again.

## Publishing (a user decision)

A local connector becomes an installable package by adding packaging, not by rewriting: a
`pyproject.toml` whose entry point registers the module —

```toml
[project.entry-points."parsimony.providers"]
example = "myproject_example"
```

— where the module exports `CONNECTORS`. From then on `discover.load_all()` finds it like any
official connector; `parsimony.testing.assert_plugin_valid(module)` is the conformance bar, and
the [plugin authoring docs](https://docs.parsimony.dev/plugins/authoring/) cover the rest. If a
connector seems broadly useful, *suggest* contributing it to the community
(https://github.com/ockham-sh/parsimony-connectors) and leave the decision to the user. That
repo's [authoring guide](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/contributing/authoring-a-connector.md)
is the manual for package-grade work: full-universe catalogs, completeness verification, and the
contribution process.
