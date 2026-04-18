# Plugin Contract

> How external packages expose connectors to `parsimony`. Authoritative spec for plugin authors — the `parsimony.testing` conformance suite enforces everything below.

`parsimony` is a kernel. Connectors live in separate packages (official or community) discovered at runtime via Python entry points. A plugin is any installed distribution that declares an entry point in the `parsimony.providers` group pointing at a module that follows this contract.

Users compose their data surface by installing plugins:

```bash
pip install parsimony parsimony-fred parsimony-sdmx
```

`parsimony` then discovers both plugins automatically — no registration code, no PRs to the core repo.

---

## 1. Package shape

- **Distribution name on PyPI:** `parsimony-<name>` for official plugins. Community plugins are encouraged to follow the same convention but are not required to.
- **Import name:** `parsimony_<name>` (underscore — standard PEP 8 module naming).
- **License:** Apache-2.0 is the official-plugin default. Community plugins may use any OSI-approved license.
- **Python:** `>=3.11`.
- **`parsimony` dependency pin:** range-style, e.g. `parsimony>=0.3,<0.5`. Pin the compatible minor range, not a single patch.

---

## 2. Module exports

Your plugin's entry-point target module MUST export:

### `CONNECTORS: Connectors` — required

An immutable `parsimony.Connectors` collection of bound `@connector` / `@enumerator` / `@loader` decorated functions:

```python
from parsimony import Connectors, connector, enumerator

@connector(tags=["macro", "tool"])
async def foo_search(params: FooSearchParams, *, api_key: str) -> Result:
    """Short, specific description. The first sentence is shown to LLMs."""
    ...

@connector(tags=["macro"])
async def foo_fetch(params: FooFetchParams, *, api_key: str) -> Result:
    """Fetch observations for a Foo series."""
    ...

CONNECTORS = Connectors([foo_search, foo_fetch])
```

### `ENV_VARS: dict[str, str]` — optional

Maps each connector dependency name to the environment variable that supplies it:

```python
ENV_VARS: dict[str, str] = {"api_key": "FOO_API_KEY"}
```

Semantics:

- Every key MUST correspond to a keyword-only argument on at least one connector in `CONNECTORS`.
- At composition time (`parsimony.connectors.build_connectors_from_env`), missing env vars cause the plugin to be silently skipped — the expected behavior when a user has not configured that provider.
- Plugins without any credential dependency omit `ENV_VARS` or set it to `{}`.

### `PROVIDER_METADATA: dict[str, Any]` — optional

Free-form dict for plugin-level metadata. Reserved top-level keys:

- `"homepage"` — provider's docs URL
- `"rate_limits"` — short human-readable description
- `"pricing"` — `"free"`, `"freemium"`, `"paid"`, or a short string

Other keys are ignored by core but may be surfaced in `parsimony list-plugins --verbose`.

---

## 3. Entry-point registration

In your `pyproject.toml`:

```toml
[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"
```

The left-hand name is the **provider key** (lowercase snake_case, must match `^[a-z][a-z0-9_]*$`). It's used in logs and `parsimony list-plugins` output. Keep it short and match your import name.

The right-hand value is the **module path** to import. It must point at the module that exports `CONNECTORS` (and optional `ENV_VARS` / `PROVIDER_METADATA`). You can point at a subpackage if your plugin ships multiple independent provider modules:

```toml
[project.entry-points."parsimony.providers"]
foo_rest = "parsimony_foo.rest"
foo_stream = "parsimony_foo.stream"
```

Each entry point is loaded independently. Order is not guaranteed.

---

## 4. Tool-tag convention

Connectors that should be exposed as direct MCP tools (agent-callable) carry `tags=["tool", ...]`:

```python
@connector(tags=["macro", "tool"])
async def foo_search(...) -> Result: ...
```

Conventions:

- **Search / discovery / enumeration** connectors: tool-tag them. LLMs call these directly.
- **Fetch / bulk data** connectors: usually do NOT tool-tag. They're called from code executors where the agent already has the key. Exposing them as tools bloats the MCP tool list.
- **Descriptions** for tool-tagged connectors must be ≥40 characters and written to be readable by an LLM (the first sentence is the MCP tool description).

`parsimony mcp serve --tool-only` filters by this tag. Core does not assign the tag for you.

---

## 5. Compatibility with `parsimony` core

- Plugins pin a range: `parsimony>=X.Y,<X.(Y+2)` (allow two minor releases before forcing a plugin update).
- Breaking changes to the plugin contract bump `parsimony`'s **minor** version (pre-1.0) or **major** (post-1.0).
- `parsimony.testing` provides the source-of-truth conformance test suite. Pin it to the same range as `parsimony` itself.

---

## 6. Testing your plugin

Add a single test:

```python
# tests/test_conformance.py
import parsimony_foo
from parsimony.testing import assert_plugin_valid

def test_conforms_to_parsimony_plugin_contract() -> None:
    assert_plugin_valid(parsimony_foo)
```

Or use the pytest plugin fixture if you have multiple plugin modules:

```python
def test_conforms(parsimony_plugin) -> None:
    # parsimony_plugin is parametrized over every discovered plugin module
    ...
```

The assertion checks:

- `CONNECTORS` is a non-empty `Connectors` collection.
- Every connector has a non-empty description.
- Every connector with `tags=["tool", ...]` has a description ≥40 characters.
- `ENV_VARS` keys (if present) all correspond to a dep name declared on at least one connector.
- No duplicate connector names within `CONNECTORS`.
- Connector names and `ENV_VARS` keys do not overlap (accidental shadowing).
- Every connector either has an `OutputConfig` or explicitly opts out via `result_type != "dataframe"`.

Individual checks can be skipped with `assert_plugin_valid(module, skip=["check_name"])` — the escape hatch is there for pragmatic cases, but every skip should be justified in a comment.

For release CI, make the conformance test blocking.

---

## 7. Versioning your plugin

- `v0.x.0` during alpha — breaking changes between minor versions allowed.
- `v1.0.0` signals stable contract.
- Bump the MINOR version when:
  - Adding new connectors to `CONNECTORS`.
  - Adding optional env vars.
  - Widening connector parameters (backwards-compatible).
- Bump the MAJOR version when:
  - Removing / renaming connectors.
  - Tightening parameter constraints.
  - Changing `OutputConfig` column names or roles.
- Bump the PATCH version for bug fixes.

---

## 8. Publishing

- PyPI publish via trusted publishing (GitHub Actions OIDC, no API tokens).
- Upload on tag push (`v*`), after CI green + conformance tests pass.
- Community plugin authors: see `ockham-sh/parsimony-fred` as the reference template until the official cookiecutter lands (deferred one quarter).

---

## 9. Discovery semantics (for plugin authors to understand)

At `build_connectors_from_env()` time, `parsimony` calls `importlib.metadata.entry_points(group="parsimony.providers")` and for each entry point:

1. Imports the target module.
2. Reads `CONNECTORS`, `ENV_VARS` (default `{}`), `PROVIDER_METADATA` (default `{}`).
3. Resolves env vars from the caller's `env` dict (or `os.environ`).
4. If any connector dep is unresolved:
   - For **required deps** (keyword-only arg without default): plugin is silently skipped.
   - For **optional deps** (keyword-only arg with default): bind proceeds without that dep.
5. Calls `connectors.bind_deps(**resolved)` to produce a dependency-bound copy.
6. Composes the result into the returned `Connectors` collection.

If your plugin needs to *require* env vars and raise on absence rather than silently skipping, raise in your module's top-level code or expose a custom composition function — but prefer the silent-skip default. It matches user expectations.

---

## 10. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `parsimony list-plugins` doesn't show your plugin | Editable install missed entry-point metadata — run `uv sync` or `pip install -e .` again |
| `CONNECTORS` present but connectors don't appear in `build_connectors_from_env()` output | Required env var not set — check `parsimony list-plugins` env var column |
| Conformance test fails with "duplicate connector name" | Two modules in the same plugin exported connectors with the same name — namespace them per-agency or per-endpoint |
| Conformance test fails with "tool-tagged description too short" | Rewrite the docstring first sentence to be LLM-friendly (≥40 chars) |
| `ImportError` at discovery time | Your module raises on import — keep module-level code side-effect-free; do I/O inside connector functions |

---

## 11. Example: minimal plugin

`parsimony-foo/src/parsimony_foo/__init__.py`:

```python
"""Foo data source plugin for parsimony."""

from __future__ import annotations

from pydantic import BaseModel, Field

from parsimony import Connectors, connector
from parsimony.result import Result, Provenance
from parsimony.transport.http import HttpClient

ENV_VARS: dict[str, str] = {"api_key": "FOO_API_KEY"}

PROVIDER_METADATA: dict = {
    "homepage": "https://example.com/foo",
    "pricing": "freemium",
}


class FooSearchParams(BaseModel):
    query: str = Field(..., min_length=1)


@connector(tags=["tool"])
async def foo_search(params: FooSearchParams, *, api_key: str) -> Result:
    """Search Foo's public catalog by keyword. Returns top 20 matches with ids and titles."""
    http = HttpClient("https://api.foo.example", query_params={"api_key": api_key})
    response = await http.request("GET", "/search", params={"q": params.query})
    response.raise_for_status()
    return Result.from_dataframe(
        pd.DataFrame(response.json()["results"]),
        Provenance(source="foo", params={"query": params.query}),
    )


CONNECTORS = Connectors([foo_search])
```

`parsimony-foo/pyproject.toml`:

```toml
[project]
name = "parsimony-foo"
version = "0.1.0"
dependencies = ["parsimony>=0.3,<0.5"]

[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"
```

That's the whole contract.
