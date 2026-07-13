# Plugin contract

This document is the authoritative specification for `parsimony.providers` plugins —
the `parsimony-<name>` distributions that register connectors at runtime. The kernel
(`parsimony-core`) ships zero connectors; every data source is a separate installable
package that implements this contract.

For step-by-step authoring guidance, see [Authoring a provider](plugins/authoring.md).
For the conformance toolkit, see [Conformance testing](plugins/conformance.md).

## Distribution and discovery

| Requirement | Detail |
|---|---|
| PyPI name | `parsimony-<name>` (hyphenated) |
| Python package | `parsimony_<name>` (underscored) |
| Entry point | `[project.entry-points."parsimony.providers"]` mapping a **provider name** → dotted module path |
| Module export | `CONNECTORS: parsimony.Connectors` (non-empty) |
| Kernel pin | `parsimony-core>=0.7,<0.8` (contract-version pin; use `[catalog]` extra only when the package needs hybrid search) |

Consumers discover plugins via `parsimony.discover`:

```python
from parsimony import discover

bundle = discover.load("fred")      # strict: LookupError if not installed
everything = discover.load_all()    # forgiving: skips broken plugins
```

`parsimony list --strict` imports every plugin and runs the conformance suite.

## Connector functions

A connector is a **synchronous** Python function plus metadata. There is no plugin base
class to subclass.

| Rule | Enforcement |
|---|---|
| **Synchronous** | `def`, not `async def` — `async def` raises `TypeError` at decoration time |
| **Description required** | Docstring or `description=` on the decorator (20–800 chars for conformance) |
| **Return raw data** | `pd.DataFrame`, `Series`, scalar, or `dict` — never a `Result` or `(data, props)` tuple |
| **Flat parameters** | Top-level scalar parameters are the call surface; no bundled `params: BaseModel` |
| **Secrets in provenance** | Declare credential parameters via `secrets=(...)`; bound or call-time secret values are stripped from provenance and LLM cards |

Pick the decorator that matches the verb:

| Decorator | Output contract | Typical use |
|---|---|---|
| `@connector` | `output=` optional | General-purpose fetch |
| `@enumerator(output=...)` | exactly one namespaced `KEY`, ≥1 `TITLE`, **no** `DATA`; `pd.DataFrame` return | Catalog entity discovery |
| `@loader(output=...)` | exactly one namespaced `KEY`, ≥1 `DATA`, **no** `TITLE`/`METADATA` | Observation loading into a data store |

Column roles (`KEY`, `TITLE`, `DATA`, `METADATA`) and `OutputSpec` are documented in
[Results and output specs](connectors/results.md).

## Credentials

Auth belongs in the connector implementation, not decorator metadata beyond `secrets=`.

- Use `Connector.bind(api_key=...)` (or `Connectors.bind(...)`) to fix credentials and
  remove them from the exposed call surface.
- Env-var fallback (e.g. `FRED_API_KEY`) lives inside the connector via
  `parsimony.transport.helpers.require_key`.
- Optional provider helpers such as `load(api_key=...)` that return bound `Connectors`
  are a convention, not a kernel requirement.

## Errors

Operational failures raise `ConnectorError` subclasses (`UnauthorizedError`,
`RateLimitError`, `ProviderError`, …). Default messages embed agent-facing retry
directives. Programmer mistakes (`TypeError`, `ValueError`) are not wrapped.

Use `parsimony.transport.check_status` / `parsimony.transport.helpers.fetch_json` to
translate `httpx` errors. **Never** let API keys appear in exception messages,
provenance, or `to_llm()` projections — the connectors repo enforces this with a
shared secret-canary test suite.

## Search connectors (`*_search`)

Not all `_search` connectors work the same way:

- **Provider-native search** (e.g. `fred_search`) calls the upstream API directly; no
  catalog is required.
- **Catalog-backed search** (e.g. `riksbank_search`, `sdmx_search`) loads a published
  hybrid-search `Catalog` snapshot (typically from Hugging Face). These require
  `parsimony-core[catalog]` at runtime and fail clearly when the snapshot is missing or
  has an unsupported `schema_version`.

Document which pattern your connector uses in its README.

## Catalogs (maintainer workflow)

Catalog build/push is **operator tooling**, not part of the runtime plugin contract.

- Build scripts live under `packages/<name>/scripts/build_catalog.py` in
  `parsimony-connectors`.
- Snapshots use `schema_version: 1` (the initial and only supported version).
- Published catalogs are saved with `catalog.save(...)` to a local path or `hf://...`.
- The user-facing module exports only `CONNECTORS`; do not download catalogs or build
  indexes at import time.

## Conformance gate

Every plugin must pass `parsimony.testing.assert_plugin_valid(module)` before merge or
release. In pytest:

```python
from parsimony.testing import ProviderTestSuite
import parsimony_foo

class TestFoo(ProviderTestSuite):
    module = parsimony_foo
    entry_point_name = "foo"
```

The suite checks `CONNECTORS` export, non-empty descriptions, flat parameters, and
entry-point registration. Per-connector happy-path and error-mapping tests live in the
plugin repo (see [parsimony-connectors CONTRIBUTING](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md)).

## See also

- [Defining connectors](connectors/defining-connectors.md)
- [Loaders and enumerators](connectors/loaders-and-enumerators.md)
- [HTTP transport](connectors/http-transport.md)
- [Catalog](catalog/index.md)
- [Public API](reference/api.md)
