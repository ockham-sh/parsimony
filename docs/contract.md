# The Parsimony Plugin Contract

**Status:** Contract v1.
**Author tutorials:** [`building-a-private-connector.md`](building-a-private-connector.md),
[`guide-new-plugin.md`](guide-new-plugin.md).

This document is the load-bearing surface of the framework. It is the only
mechanism the kernel knows about for composing connectors and publishing
catalogs. Every external package — officially-maintained connectors in the
[parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)
monorepo, vendor-published connectors, customer-private internal connectors —
implements exactly this contract and nothing more.

Where this document and any docstring disagree, **this document wins**.

---

## 1. Scope

The contract covers:

1. **The entry-point registration** plugins declare in their `pyproject.toml`.
2. **The module exports** the kernel reads from a discovered plugin module.
3. **The kernel API surface** plugins may import, with a stability marking on
   every symbol.
4. **The discovery record** (`DiscoveredProvider`) the kernel exposes to
   consumers that walk discovered plugins directly.
5. **The conformance entry point** plugins MUST pass to be considered
   contract-compliant.
6. **The catalog-publish shape** (`CATALOGS` / `RESOLVE_CATALOG`) the kernel's
   `parsimony publish` command reads from the plugin module.
7. **Versioning and deprecation policy**.

Everything not enumerated here is private. The kernel reserves the right to
change, rename, or delete private symbols in any release.

---

## 2. Versioning

The contract is versioned alongside the kernel distribution (`parsimony-core`
on PyPI). Stability is signalled per-symbol in §5, not through a separate
contract-version classifier.

### Stability markings

Every symbol named in §5 (the kernel API surface table) is marked:

- **stable** — Cannot break without a MAJOR version bump and a deprecation
  window. The bar for marking a symbol `stable` is evidence it has been used
  unchanged by at least one minor release cycle and there is no active
  redesign in flight.
- **provisional** — May change in a MINOR version, but only with a
  `DeprecationWarning` in the preceding minor. Shipped for external use, but
  the shape is still under evaluation. Default marking for newly-added
  contract surface.
- **private** — Internal. Starts with an underscore, or lives in a module
  not named in §5. May be changed or removed in any release. Plugins
  importing private symbols are accepting breakage.

Plugins depending on **stable** surface are protected across MAJOR boundaries
by the deprecation window. Plugins depending on **provisional** surface must
track minor releases; the kernel will not silently change provisional shapes,
but MINOR is the bump they warrant.

### Kernel version pin

Plugins declare their dependency on the kernel via a standard PEP 621 range
pin. There is no separate contract-version classifier.

```toml
[project]
dependencies = ["parsimony-core>=0.3,<0.5"]
```

The kernel ships a single public distribution (`parsimony-core`); the bare
`parsimony` PyPI name is squatted. Imports remain `from parsimony import ...`
regardless of the distribution name.

---

## 3. Entry-point registration

Plugins register under the `parsimony.providers` entry-point group. Exactly
one entry point per provider module.

```toml
[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"
```

- **Group name:** `parsimony.providers`. **Stable.** No other group is read.
- **Provider key** (LHS): lowercase snake_case matching `^[a-z][a-z0-9_]*$`.
  Appears in logs and `parsimony list`. **Stable.**
- **Module path** (RHS): dotted Python module path pointing at a module that
  satisfies §4. **Stable.**
- **Multiple entries per distribution** are supported. Each is loaded
  independently.

---

## 4. Plugin module exports

Every module named in a `parsimony.providers` entry point MUST satisfy this
shape.

### Required

#### `CONNECTORS: Connectors`
An immutable `parsimony.Connectors` collection of bound `@connector` /
`@enumerator` / `@loader` decorated functions. **Stable.** Non-empty;
duplicate connector names within the collection are a contract violation.

### Optional

#### `ENV_VARS: dict[str, str]`
Maps each connector dependency name (keyword-only arg on at least one
connector in `CONNECTORS`) to the environment variable supplying it.
**Stable.** Default `{}`. Missing env vars cause the plugin to be silently
skipped at `build_connectors_from_env` time — the expected behaviour when a
user has not configured that provider.

#### `PROVIDER_METADATA: dict[str, Any]`
Free-form plugin-level metadata. **Stable** as a shape. Reserved top-level
keys (all **provisional**):

- `"homepage"` — provider's docs URL.
- `"rate_limits"` — short human-readable description.
- `"pricing"` — `"free"`, `"freemium"`, `"paid"`, or a short string.

Plugins may add arbitrary keys; the kernel ignores unknown keys.

#### `CATALOGS`
See §6 — the catalog-publish contract. Optional. Plugins that do not publish
catalogs omit this.

#### `RESOLVE_CATALOG: Callable[[str], Callable | None]`
See §6 — optional reverse-lookup for `parsimony publish --only`.

---

## 5. Kernel API surface

The table below enumerates every symbol a plugin may import. Absence from this
table implies **private**.

### `parsimony` (root)

Plugins should import the public surface from the root module; submodule paths
are **private** unless otherwise noted.

| Symbol | Stability | Notes |
|---|---|---|
| `Connector` | stable | Frozen dataclass wrapping one decorated function. |
| `Connectors` | stable | Immutable collection; `+`, `.filter()`, `.bind_deps()`, `.with_callback()`. |
| `connector` | stable | Decorator for fetch connectors. |
| `enumerator` | stable | Decorator for catalog-population connectors. |
| `loader` | stable | Decorator for observation-loading connectors. |
| `ResultCallback` | provisional | Post-fetch hook type used by `with_callback`. |
| `Result` | stable | Connector return type. Carries optional `output_schema: OutputConfig`. |
| `OutputConfig` | stable | Tabular output configuration. |
| `Provenance` | stable | Fetch provenance record. |
| `Column` | stable | Output column definition. |
| `ColumnRole` | stable | `KEY` / `TITLE` / `DATA` / `METADATA`. |
| `CatalogBackend` | provisional | Structural Protocol — `add(entries)`, `search(query, limit, *, namespaces=None)`. Custom backends match this shape. |
| `Catalog` | provisional | Canonical `CatalogBackend` implementation — Parquet rows + FAISS + BM25 + RRF. Requires `parsimony-core[standard]`. |
| `EmbeddingProvider` | provisional | Structural Protocol for embedders. |
| `SentenceTransformerEmbedder` | provisional | Default local embedder (`[standard]`). |
| `LiteLLMEmbeddingProvider` | provisional | Hosted-API embedder (`[litellm]`). |
| `EmbedderInfo` | provisional | Persisted identity of a catalog's embedder. |
| `SeriesEntry` | stable | Catalog row. |
| `SeriesMatch` | stable | Catalog search match. |
| `IndexResult` | provisional | Outcome of a catalog ingest. |
| `InMemoryDataStore` | provisional | Dict-backed `DataStore` for `@loader`. |
| `LoadResult` | provisional | Outcome of a `DataStore.load_result` call. |
| `ConnectorError` | stable | Base connector error. |
| `EmptyDataError` | stable | Upstream returned no data. |
| `ParseError` | stable | Upstream payload could not be parsed. |
| `ProviderError` | stable | Upstream returned a non-success status. |
| `PaymentRequiredError` | stable | Upstream requires payment/subscription. |
| `RateLimitError` | stable | Upstream rate-limit response; carries `retry_after`. |
| `UnauthorizedError` | stable | Upstream credentials rejected. |
| `catalog_key` | stable | Canonical `(namespace, code)` key. |
| `code_token` | stable | Provider-side helper: slugify any string into a valid code. |
| `normalize_code` | stable | Validate lowercase snake_case namespace strings. |
| `normalize_entity_code` | stable | Validate non-empty trimmed entity-code strings. |
| `parse_catalog_url` | stable | Split a `scheme://root[/sub]` URL into `(scheme, root, sub)`. |
| `series_match_from_entry` | provisional | Build a `SeriesMatch` from a stored `SeriesEntry`. |
| `client` | provisional | Lazy-composed `Connectors` surface (reads env on first access). |
| `__version__` | stable | Installed kernel package version string. |

### `parsimony.discovery`

| Symbol | Stability | Notes |
|---|---|---|
| `DiscoveredProvider` | stable | Record returned from discovery. Fields: `name`, `module_path`, `connectors`, `env_vars`, `provider_metadata`, `distribution_name`, `version`, `module`. |
| `discovered_providers` | stable | Enumerate all discovered plugins. Cached. |
| `iter_entry_points` | stable | Low-level iteration over `parsimony.providers` entry points. |
| `load_provider` | stable | Load and validate one entry point. |
| `build_connectors_from_env` | stable | Compose every discovered provider, binding env deps. |
| `PluginError` | stable | Base class for discovery errors. |
| `PluginImportError` | stable | Target module failed to import. |
| `PluginContractError` | stable | Target module violated §4. |

### `parsimony.transport`

| Symbol | Stability | Notes |
|---|---|---|
| `HttpClient` | stable | Shared async HTTP client with credential-redacted logging. |
| `pooled_client` | provisional | Connection-pooled `httpx.AsyncClient` context manager for burst workloads. |
| `map_http_error` | provisional | Translate `httpx.HTTPStatusError` → typed `parsimony.errors` exception. |
| `map_timeout_error` | provisional | Translate `httpx.TimeoutException` → `ProviderError`. |
| `redact_url` | provisional | Strip sensitive query-param values from a URL before logging. |
| `parse_retry_after` | provisional | Extract retry-after seconds from a 429 response. |

### `parsimony.publish`

| Symbol | Stability | Notes |
|---|---|---|
| `publish` | stable | Build one catalog per namespace from a plugin module and push each. |
| `publish_provider` | stable | Like `publish`, but takes the provider name string and looks up its module. |
| `collect_catalogs` | provisional | Iterate a plugin module's declared catalogs without publishing. |
| `PublishReport` | provisional | Outcome of a publish run. |

### `parsimony.testing`

| Symbol | Stability | Notes |
|---|---|---|
| `assert_plugin_valid` | stable | Raise `ConformanceError` if the module fails any conformance check. |
| `ConformanceError` | stable | Subclass of `AssertionError`; carries `check`, `reason`, `module_path`, `next_action`. |
| `ProviderTestSuite` | stable | Pytest-native base class (4 `test_*` methods). |
| `iter_check_names` | provisional | Enumerate conformance-check identifiers. |

### `parsimony.errors`

Re-exports of the root error hierarchy; same stability as at root.

---

## 6. Catalog publishing — `CATALOGS` / `RESOLVE_CATALOG`

Plugins that publish catalogs export one of the following shapes on the
module. The `parsimony publish` CLI reads these to build one
`parsimony.Catalog` per declared namespace and push each to
`target_template.format(namespace=...)`.

### `CATALOGS` — required to publish

Two shapes are accepted. Pick the one that fits your namespace discipline.

**Static list** — when the namespace set is known at import time:

```python
from parsimony import Connectors
from parsimony_fred.connectors import fred_enumerate

CATALOGS: list[tuple[str, Callable[[], Awaitable[Result]]]] = [
    ("fred", fred_enumerate),
]
```

**Async generator** — when namespaces are discovered at build time (e.g. SDMX
iterates agencies and dataflows on the wire):

```python
from functools import partial
from typing import AsyncIterator, Awaitable, Callable

async def CATALOGS() -> AsyncIterator[tuple[str, Callable[[], Awaitable]]]:
    for static_ns, fn in _STATIC_CATALOGS:
        yield static_ns, fn
    async for agency in _fetch_agencies():
        async for flow in _fetch_dataflows(agency):
            ns = f"sdmx_series_{agency.lower()}_{flow.id.lower()}"
            yield ns, partial(enumerate_sdmx_series, agency=agency, dataset_id=flow.id)
```

**Contract:**

- Each entry is `(namespace: str, fn: Callable)` where `fn` is zero-arg,
  returns an `Awaitable[Result]`, and the `Result` carries an `OutputConfig`
  whose KEY column identifies the series in that namespace.
- Namespaces must be lowercase snake_case (validated by `normalize_code`).
- `Column(role=KEY).namespace` MAY be omitted; if so, the catalog name
  defaults to the declared namespace string.

**Stable** as a contract shape. The return-value type of the enumerator is
**provisional** while the `Result` pipeline settles.

### `RESOLVE_CATALOG` — optional reverse lookup

```python
def RESOLVE_CATALOG(namespace: str) -> Callable | None:
    """Return the enumerator for *namespace* without iterating CATALOGS."""
```

When present, `parsimony publish --only NAMESPACE` tries this first. If it
returns a callable, the publisher skips walking `CATALOGS` entirely — useful
for large async generators where the targeted namespace is known up front.
Return `None` for unknown namespaces; the publisher will fall back to the
generator.

**Stable** as a contract shape.

---

## 7. Conformance

A plugin is **contract-compliant** iff
`parsimony.testing.assert_plugin_valid(module)` raises no exception.

The suite runs three checks:

1. `check_connectors_exported` — module exports `CONNECTORS`, a non-empty
   `parsimony.Connectors`.
2. `check_descriptions_non_empty` — every connector has a non-empty
   description (no silently empty LLM tool schemas).
3. `check_env_vars_map_to_deps` — every key in `ENV_VARS` names a real
   keyword-only dependency on at least one connector in `CONNECTORS`
   (catches typos and renames).

The same suite runs as:

- **Merge gate** in the `parsimony-connectors` monorepo CI (every PR).
- **Release gate** for each officially-published connector.
- **Security-review artefact**, via the CLI:

  ```bash
  parsimony list --strict
  ```

  Exits non-zero on any conformance failure; the report is a single JSON
  object per plugin in `--json` mode.

Individual checks can be skipped via
`assert_plugin_valid(module, skip=[...])`; the check names are **stable**.

---

## 8. Versioning and deprecation policy

### Kernel version bumps

| Change | Required bump |
|---|---|
| Break a **stable** symbol (remove, rename, tighten input, loosen output) | Kernel MAJOR, with one-minor-cycle deprecation window. |
| Break a **provisional** symbol | Kernel MINOR with `DeprecationWarning` on the removed shape for one minor cycle. |
| Add a new symbol marked **provisional** | Kernel MINOR (non-breaking). |
| Promote **provisional** → **stable** | Kernel MINOR. |
| Demote **stable** → **provisional** | Not allowed. Once stable, stays stable until MAJOR. |
| Mark any new symbol **stable** on first introduction | Discouraged. Default new surface to **provisional**. |

### Grace period for pre-1.0

Until the first `1.0` release, the stability markings in §5 are a commitment
**about** the markings themselves: no symbol loses its marking during the
`0.x` cycle. Specific symbols marked **stable** may nevertheless change shape
before `1.0` if a genuine flaw is found — in which case the shape changes,
the marking stays, and the next alpha tag ships with the fix.

Post-1.0, the markings are contractually binding and the bump table above
applies strictly.

---

## 9. What is explicitly **not** contract

These are available in `parsimony.*` but are **not** stable-or-provisional
surface, and plugins MUST NOT import them. They may change or disappear in
any kernel release:

- Anything with a leading underscore (e.g. `parsimony.catalog._write_faiss`).
- Private modules not named in §5 (`parsimony.indexes.*`,
  `parsimony.embedder` internals beyond the Protocol, etc.).
- Any `Catalog` implementation detail beyond the `CatalogBackend` Protocol
  surface, unless re-exported from the public table in §5.

There is no in-tree `parsimony.connectors` package: every connector ships as
its own `parsimony-<name>` distribution discovered via the
`parsimony.providers` entry-point group.

---

*This is contract v1.*
