# Author your first connector

This is the orienting guide for connector authors. It walks the whole path
end to end — pick a distribution model, scaffold from the template, build one
working connector, prove it conforms, publish it, and watch it surface to
agents — then hands you off to the deeper references for the parts that need
detail.

What this guide is **not**: the contract. The
[Plugin Contract](contract.md) is the authoritative, load-bearing spec — the
only mechanism the kernel knows about. Where this guide and the contract
disagree, the contract wins. This page is the narrative; the contract is the
law.

Three documents, three jobs:

| Document | Job |
|---|---|
| **This page** | The map. One worked example, end to end. |
| [Connector Implementation Walkthrough](connector-implementation-guide.md) | The deep dive — provider research, schema design, error mapping, catalogs. |
| [Building a private connector](building-a-private-connector.md) | Internal databases (Postgres, Snowflake, S3) and private-PyPI distribution. |
| [Plugin Contract](contract.md) | The authoritative spec. Every export, every symbol, every stability marking. |

---

## 1. Pick where the connector lives

Every connector ships as its own PyPI distribution (`parsimony-<name>`) and
registers with the kernel through one `parsimony.providers` entry point. The
kernel has no in-tree connectors and cannot tell an official connector from a
private one. What differs is *where it lives and who can install it*.

| Model | When | Distribution | Continue at |
|---|---|---|---|
| **Official monorepo** | A public data source useful to everyone (a central bank, a public statistics API). | Public PyPI, Apache-2.0, maintained in [`parsimony-connectors`](https://github.com/ockham-sh/parsimony-connectors). | [parsimony-connectors CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md) |
| **Standalone public** | A public source you want to own and release yourself. | Your own repo, your own PyPI release cadence. | This guide + the [Walkthrough](connector-implementation-guide.md). |
| **Private / internal** | An internal database, a feed under restrictive ToS, anything firewalled. | Private PyPI / Artifactory / a wheel. Never redistributed. | [Building a private connector](building-a-private-connector.md). |

The contract is identical across all three. If you are contributing a public
source to the official monorepo, follow its `CONTRIBUTING.md` for the merge
gate and review process — the rest of this guide still applies to *how you
write the code*. The worked example below builds a **standalone public**
connector.

---

## 2. Scaffold from the template

```bash
uvx cookiecutter gh:ockham-sh/parsimony-plugin-template
```

Answer the prompts (provider name, slug, description). The
[`parsimony-plugin-template`](https://github.com/ockham-sh/parsimony-plugin-template)
cookiecutter writes a working package: a `pyproject.toml` with the entry
point already wired, a placeholder `CONNECTORS` export, a release-blocking
conformance test, and CI. It builds and passes conformance before you change a
line.

The layout it produces:

```text
parsimony-<name>/
├── parsimony_<name>/
│   ├── __init__.py         CONNECTORS  (+ optional CATALOGS / RESOLVE_CATALOG)
│   ├── connectors.py       @connector / @enumerator / @loader functions
│   └── py.typed
├── tests/
│   ├── test_conformance.py          assert_plugin_valid — release-blocking
│   └── test_<name>_connectors.py    happy path + error mapping
└── pyproject.toml          entry-point registration + [project.urls] Homepage
```

The two load-bearing entries in `pyproject.toml`:

```toml
[project.urls]
Homepage = "https://your-provider.example"

[project.entry-points."parsimony.providers"]
<your-name> = "parsimony_<your_name>"
```

Provider metadata (homepage, version, description) lives in `pyproject.toml`
and is read on demand via `importlib.metadata`. There is **no** module-level
`ENV_VARS`, `PROVIDER_METADATA`, or `__version__` — see
[contract §4](contract.md#4-plugin-module-exports).

---

## 3. The contract in brief

Enough to read the worked example. The full spec is
[`contract.md`](contract.md); section links below go straight to it.

- **`@connector`** ([§4.5](contract.md#45-connectorenv)) — decorates a typed
  `async def`. Its first argument is a Pydantic params model; keyword-only
  arguments after `*` are injected dependencies. The function returns a
  DataFrame (or a `Result`); the decorator wraps it.
- **Env injection** ([§4.5](contract.md#45-connectorenv)) — `env={"api_key":
  "FOO_API_KEY"}` maps each keyword-only dep to an environment variable. The
  value is read at *bind* time and **never** serialized into provenance, logs,
  or the agent-facing projection.
- **The `Connectors` collection** ([§4](contract.md#4-plugin-module-exports)) —
  your module must export `CONNECTORS`, an immutable `parsimony.Connectors`
  built from your decorated functions. Consumers call `.bind_env()` to resolve
  credentials from `os.environ`, or `.bind(...)` to inject them by hand.
- **`Result` and provenance** — every call returns a `Result` carrying the
  data plus a `Provenance` record (`source`, `params`, `fetched_at`).
  Provenance is generated for you from the params model.
- **Keep-but-unbound** ([§4.6](contract.md#46-keep-but-unbound-credentialing))
  — a connector whose env var is missing is *kept* in the collection, not
  dropped. Calling it raises `UnauthorizedError` naming the missing variable.
- **`@enumerator` / `@loader`** (optional) — `@enumerator` populates a
  searchable catalog (KEY/TITLE/METADATA, no DATA); `@loader` persists
  observations into a `DataStore`. Most first connectors need neither — start
  with `@connector` and add them later. Catalog publishing is
  [contract §6](contract.md#6-catalog-publishing-catalogs-resolve_catalog).

---

## 4. Worked example — a complete minimal connector

A standalone public connector for a fictional `MySource` time-series API. One
`@connector`, one params model, one `OutputConfig`. This is the whole package
body — drop it into the scaffold's `parsimony_my_source/connectors.py`.

```python
"""Connectors for the MySource public time-series API."""
from __future__ import annotations

import httpx
import pandas as pd
from pydantic import BaseModel, Field, field_validator

from parsimony import Column, ColumnRole, OutputConfig, connector
from parsimony.transport import HttpClient, map_http_error

_BASE_URL = "https://api.my-source.example.com/v1"


class MySourceFetchParams(BaseModel):
    """Parameters for fetching a time series from MySource."""

    series_id: str = Field(..., description="Series identifier, e.g. CPI.TOTAL")
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_id must be non-empty")
        return v


FETCH_OUTPUT = OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY,
           param_key="series_id", namespace="my_source"),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])


@connector(
    output=FETCH_OUTPUT,
    env={"api_key": "MY_SOURCE_API_KEY"},
    tags=["my_source", "tool"],
)
async def my_source_fetch(
    params: MySourceFetchParams,
    *,
    api_key: str,
) -> pd.DataFrame:
    """Fetch time series observations by series_id from MySource."""
    async with HttpClient(_BASE_URL, default_params={"api_key": api_key}) as http:
        try:
            response = await http.get(
                f"/series/{params.series_id}/observations",
                params={"start": params.start_date} if params.start_date else None,
            )
        except httpx.HTTPStatusError as exc:
            raise map_http_error(exc, provider="my_source", op_name="fetch") from exc
    return pd.DataFrame(response.json().get("observations", []))
```

And the package's `parsimony_my_source/__init__.py` — the **required**
`CONNECTORS` export:

```python
from parsimony import Connectors
from parsimony_my_source.connectors import my_source_fetch

CONNECTORS = Connectors([my_source_fetch])
```

What each piece does:

- **`env={"api_key": "MY_SOURCE_API_KEY"}`** binds the keyword-only `api_key`
  to that environment variable. `CONNECTORS.bind_env()` reads it; the value
  never reaches provenance.
- **`tags=["my_source", "tool"]`** — `"my_source"` is the domain category;
  `"tool"` opts the connector into the MCP tool surface (see §7). Omit
  `"tool"` for connectors agents call programmatically rather than
  interactively.
- **`map_http_error`** translates upstream HTTP failures into typed errors:
  `401/403` → `UnauthorizedError`, `402` → `PaymentRequiredError`, `429` →
  `RateLimitError`, anything else → `ProviderError`.
- **`OutputConfig`** declares each column's semantic role — exactly one `KEY`,
  the rest `DATA`. Columns in the DataFrame not declared here become `DATA`
  automatically.

Run it locally:

```python
import asyncio
from parsimony_my_source import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()           # reads MY_SOURCE_API_KEY
    result = await connectors["my_source_fetch"](series_id="CPI.TOTAL")
    print(result.data.tail())
    print(result.provenance)                     # source, params, fetched_at

asyncio.run(main())
```

For provider research, pagination, multi-namespace providers, reserved-keyword
aliasing, `dtype` traps, and the `@enumerator` catalog path, the
[Connector Implementation Walkthrough](connector-implementation-guide.md)
covers each in depth.

---

## 5. Prove it conforms

A plugin is **contract-compliant** iff
`parsimony.testing.assert_plugin_valid(module)` raises nothing. The scaffold
ships this test already:

```python
# tests/test_conformance.py — release-blocking
import parsimony_my_source
from parsimony.testing import assert_plugin_valid

def test_plugin_conforms() -> None:
    assert_plugin_valid(parsimony_my_source)
```

It checks three things ([contract §7](contract.md#7-conformance)):
`CONNECTORS` is exported and non-empty; every connector has a non-empty
description; every `env_map` key names a real keyword-only dependency.

Then add a happy-path test and the two credential tests every connector with
an `api_key` / `token` must have — `401 → UnauthorizedError` and
`429 → RateLimitError`, each asserting the key never leaks into the error:

```python
# tests/test_my_source_connectors.py
import httpx, pytest, respx
from parsimony.errors import UnauthorizedError
from parsimony_my_source import CONNECTORS

@respx.mock
@pytest.mark.asyncio
async def test_fetch_happy_path():
    respx.get("https://api.my-source.example.com/v1/series/CPI/observations").mock(
        return_value=httpx.Response(200, json={"observations": [
            {"series_id": "CPI", "date": "2024-01-01", "value": 100.0},
        ]})
    )
    bound = CONNECTORS.bind(api_key="test-key")
    result = await bound["my_source_fetch"](series_id="CPI")
    assert result.provenance.source == "my_source"
    assert len(result.data) == 1

@respx.mock
@pytest.mark.asyncio
async def test_fetch_401_maps_to_unauthorized():
    respx.get("https://api.my-source.example.com/v1/series/X/observations").mock(
        return_value=httpx.Response(401, json={"error": "bad key"})
    )
    bound = CONNECTORS.bind(api_key="live-looking-key")
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound["my_source_fetch"](series_id="X")
    assert "live-looking-key" not in str(exc_info.value)   # no key leak
```

The full happy-path / error-mapping shape is in
[testing-template.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/testing-template.md).
Run everything before a release:

```bash
pytest tests/ -v && ruff check . && mypy parsimony_my_source/
parsimony list --strict          # conformance suite; non-zero exit on any failure
```

`parsimony list --strict --json` produces a machine-readable artefact for
security review. All four gates — pytest, ruff, mypy, conformance — must pass
to cut a release.

---

## 6. Publish to PyPI

The kernel discovers your connector through the `parsimony.providers` entry
point at runtime — no registry, no manifest. Publishing is just a normal PyPI
release.

1. Configure [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/)
   for your repo (one-time — no tokens in secrets).
2. Copy a release workflow from an existing plugin (e.g.
   `parsimony-fred/.github/workflows/release.yml`).
3. Tag and push: `git tag v0.1.0 && git push --tags`. GitHub Actions
   publishes via OIDC.
4. Verify in a fresh virtualenv:

   ```bash
   pip install parsimony-core parsimony-my-source
   parsimony list                 # your provider appears, with its declared catalogs
   ```

Pin the kernel by range, not by exact version:

```toml
dependencies = ["parsimony-core>=0.4,<0.5"]
```

A kernel MAJOR release cannot break a **stable** symbol you depend on without
a deprecation window ([contract §8](contract.md#8-versioning-and-deprecation-policy)).

For a **private** connector, the build step is identical — only the upload
target changes (private PyPI / Artifactory / a wheel file). See
[Building a private connector](building-a-private-connector.md#publish-to-your-private-index).

---

## 7. How the connector reaches agents

Once installed, the connector is discoverable everywhere — no extra
registration step. Agent-facing consumers (the ockham terminal,
`parsimony-agents`, and any custom downstream that calls
`discover.load_all().bind_env()`) pick up every installed `parsimony-*`
package automatically and expose each connector tagged `"tool"` to the
underlying LLM.

Two design consequences for the author:

- **The connector docstring's first sentence becomes the agent-facing
  tool description.** Write it for an LLM. Tool-tagged connectors should
  have a description of ≥40 characters.
- **Pydantic field descriptions appear verbatim in the agent's tool schema.**
  Write them as instructions to a model, not prose for a human.

A connector *without* the `"tool"` tag is still discovered and still
callable — agents reach it through their code interpreter via
`discover.load_all().bind_env()` — it just isn't pushed as an interactive
tool. Tag connectors a model calls *interactively to discover or search*;
leave `"tool"` off bulk-fetch connectors a model calls *programmatically*.

Because discovery is entry-point based, installing your package is the
only integration step — the kernel and every downstream consumer see it
automatically.

---

## Checklist before `v0.1.0`

- [ ] `CONNECTORS` exported (and `CATALOGS` / `RESOLVE_CATALOG` if the plugin publishes catalogs)
- [ ] `@connector(env={...})` covers every keyword-only dep
- [ ] `[project.urls] Homepage` set; entry point registered under `parsimony.providers`
- [ ] `parsimony.testing.assert_plugin_valid(module)` passes
- [ ] Tool-tagged connectors have ≥40-character descriptions
- [ ] Tests cover happy path + `401` + `429`, with key-leak assertions
- [ ] `parsimony list --strict`, `ruff check`, `mypy` all green
- [ ] `README.md` covers install, credential setup, one example
- [ ] `LICENSE` present (Apache-2.0 for official plugins)

---

## Where to go next

- [Connector Implementation Walkthrough](connector-implementation-guide.md) —
  provider research, schema design, `dtype` reference, error mapping,
  pagination, catalog integration.
- [Building a private connector](building-a-private-connector.md) — internal
  databases, sync-SDK wrapping, private-index distribution, security review.
- [Plugin Contract](contract.md) — the authoritative spec. Read it before a
  release; it is the only reference that the kernel itself is built against.
- [Recipes](cookbook.md) — composing connectors, callbacks, catalog search.
- Contributing a public source to the official monorepo:
  [parsimony-connectors CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md).
</content>
</invoke>
