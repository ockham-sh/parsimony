# The Parsimony Plugin Contract

**Status:** Contract v1.
**Binding design:** `DESIGN-distribution-model.md`.
**Author tutorial:** `docs/building-a-private-connector.md`.

This document is the load-bearing surface of the framework. It is the only
mechanism the kernel knows about for composing connectors. Every external
package — officially-maintained connectors in the `parsimony-connectors`
monorepo, vendor-published connectors, customer-private internal connectors —
implements exactly this contract and nothing more.

Where this document and any docstring disagree, **this document wins**.

---

## 1. Scope

The contract covers:

1. **The entry-point registration** plugins declare in their `pyproject.toml`.
2. **The module exports** the kernel reads from a discovered plugin module.
3. **The kernel ABI pin** each plugin declares in its entry-point metadata, and the semantics the kernel applies when checking it.
4. **The kernel API surface** plugins may import, with a stability marking on every symbol.
5. **The discovery record** (`DiscoveredProvider`) the kernel exposes to consumers that walk discovered plugins directly.
6. **The conformance entry point** plugins MUST pass to be considered contract-compliant.
7. **Deprecation and versioning policy**.

Everything not enumerated here is private. The kernel reserves the right to change, rename, or delete private symbols in any release.

---

## 2. Versioning

The contract is versioned alongside the kernel distribution (`parsimony` on PyPI, currently `parsimony-core`). The contract version tracks `MAJOR.MINOR` of the kernel.

### Stability markings

Every symbol named in §6 (the kernel API surface table) is marked:

- **stable** — Cannot break without a MAJOR version bump and a **one-year deprecation window** during which the symbol continues to work with a `DeprecationWarning`. The bar for marking a symbol `stable` is evidence it has been used unchanged by at least one minor release cycle and there is no active redesign in flight.
- **provisional** — May change in a MINOR version, but only with a `DeprecationWarning` in the preceding minor. Shipped for external use, but the shape is still under evaluation. Default marking for newly-added contract surface.
- **private** — Internal. Starts with an underscore. May be changed or removed in any release. Plugins importing private symbols are accepting breakage.

Plugins depending on **stable** surface are protected across MAJOR boundaries by the deprecation window. Plugins depending on **provisional** surface must track minor releases; the kernel will not silently change provisional shapes, but MINOR is the bump they warrant.

### The kernel ABI version

Every kernel release exposes `parsimony.CONTRACT_VERSION` as a string (e.g. `"1"`, `"2"`) — this is the **contract major version**, not the kernel package version. It bumps only when a stable symbol breaks. A kernel that ships `parsimony 0.7.3` may expose `CONTRACT_VERSION = "1"`; a later `parsimony 0.8.0` that removes a stable symbol bumps `CONTRACT_VERSION = "2"`.

Plugins pin `CONTRACT_VERSION` ranges (see §4), not kernel package versions.

---

## 3. Entry-point registration

Plugins register under the `parsimony.providers` entry-point group. Exactly one entry point per provider module.

```toml
[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"
```

- **Group name:** `parsimony.providers`. **Stable.** No other group is read.
- **Provider key** (LHS): Lowercase snake_case matching `^[a-z][a-z0-9_]*$`. Appears in logs and `parsimony list-plugins`. **Stable.**
- **Module path** (RHS): Dotted Python module path pointing at a module that satisfies §4. **Stable.**
- **Multiple entries per distribution** are supported. Each is loaded independently.

---

## 4. Plugin module exports

Every module named in a `parsimony.providers` entry point MUST satisfy this shape.

### Required

#### `CONNECTORS: Connectors`
An immutable `parsimony.Connectors` collection of bound `@connector` / `@enumerator` / `@loader` decorated functions. **Stable.** Non-empty; duplicate connector names within the collection are a contract violation.

### Optional

#### `ENV_VARS: dict[str, str]`
Maps each connector dependency name (keyword-only arg on at least one connector in `CONNECTORS`) to the environment variable supplying it. **Stable.** Default `{}`. Missing env vars cause the plugin to be silently skipped at `build_connectors_from_env` time — the expected behavior when a user has not configured that provider.

#### `PROVIDER_METADATA: dict[str, Any]`
Free-form plugin-level metadata. **Stable** as a shape. Reserved top-level keys (all **provisional**):

- `"homepage"` — provider's docs URL.
- `"rate_limits"` — short human-readable description.
- `"pricing"` — `"free"`, `"freemium"`, `"paid"`, or a short string.

Plugins may add arbitrary keys; the kernel ignores unknown keys.

### Kernel ABI pin — declared in `pyproject.toml`

Each plugin declares the contract version range it supports, in its distribution metadata. **Stable** interface.

```toml
[project]
# Standard PEP 621 range pin against the kernel distribution:
dependencies = ["parsimony-core>=0.1.0a0,<0.3"]

# Contract-version signal via PEP 621 keyword. Propagates to dist-info
# METADATA; readable via importlib.metadata without importing the plugin.
keywords = ["parsimony", "connector", "parsimony-contract-v1"]
```

**Why a keyword instead of a Trove classifier?** A classifier like
`Framework :: Parsimony :: Contract 1` would be rejected by build backends
that validate classifiers against the upstream `trove-classifiers`
package (hatchling, setuptools with strict mode). Registering
`Framework :: Parsimony` requires an upstream PR. Keywords have no
validation and propagate the same way. The kernel reads the
`Keywords:` header from `.dist-info/METADATA`, splits on whitespace or
commas, and looks for the first token matching `parsimony-contract-v*`.

**Kernel behaviour on ABI mismatch.** At discovery time, before importing
the plugin module, the kernel reads the plugin distribution's
`Keywords:` metadata. A plugin that declares no `parsimony-contract-v*`
keyword is refused. A plugin whose declared version does not match
`parsimony.CONTRACT_VERSION` is refused. In both cases the kernel logs
a structured diagnostic and skips the plugin:

```
parsimony: plugin 'parsimony-foo' targets contract '2'; this kernel supports '1' — skipping
```

---

## 5. Conformance

A plugin is **contract-compliant** iff `parsimony.testing.assert_plugin_valid(module)` raises no exception. The same suite runs as:

- **Merge gate** in the `parsimony-connectors` monorepo CI (every PR).
- **Release gate** for each officially-published connector.
- **Regulated-finance security-review artefact**, via the dedicated CLI:
  ```
  parsimony conformance verify <distribution-name>
  ```

The CLI is the only tooling the kernel ships specifically for external plugins. Its exit code (0 pass / 1 fail) and machine-readable JSON output are **stable**. The specific checks it runs are versioned alongside `CONTRACT_VERSION`: a kernel bump to `CONTRACT_VERSION = "2"` may tighten conformance checks without otherwise breaking stable symbols.

Individual conformance checks can be skipped via `assert_plugin_valid(module, skip=[...])`. Every skip is tracked by the check name; the check names are **stable**.

---

## 6. Kernel API surface

The table below enumerates every symbol a plugin may import. Absence from this table implies **private**.

### `parsimony` (root)

| Symbol | Stability | Notes |
|---|---|---|
| `Connector` | stable | Connector class. |
| `Connectors` | stable | Immutable connector collection. |
| `connector` | stable | Decorator for fetch connectors. |
| `enumerator` | stable | Decorator for catalog-oriented connectors. |
| `loader` | stable | Decorator for data-oriented connectors. |
| `Namespace` | stable | Parameter annotation for catalog namespace. |
| `ResultCallback` | provisional | Post-fetch hook type. |
| `Result` | stable | Connector return type. |
| `SemanticTableResult` | provisional | Tabular result variant. |
| `OutputConfig` | stable | Tabular output configuration. |
| `Provenance` | stable | Fetch provenance record. |
| `Column` | stable | Output column definition. |
| `ColumnRole` | stable | Column role enum (KEY / TITLE / DATA / METADATA). |
| `Catalog` | provisional | Catalog orchestrator. |
| `CatalogStore` | provisional | Catalog store ABC. |
| `DataStore` | provisional | Data-persistence ABC. |
| `InMemoryDataStore` | provisional | In-memory DataStore implementation. |
| `LoadResult` | provisional | DataStore.load_result outcome. |
| `SeriesEntry` | stable | Catalog row. |
| `SeriesMatch` | stable | Catalog search match. |
| `IndexResult` | provisional | Catalog.index_result outcome. |
| `EmbeddingProvider` | provisional | Embedding provider ABC. |
| `ConnectorError` | stable | Base connector error. |
| `EmptyDataError` | stable | Upstream returned no data. |
| `ParseError` | stable | Upstream payload could not be parsed. |
| `ProviderError` | stable | Upstream returned a non-success status. |
| `PaymentRequiredError` | stable | Upstream requires payment/subscription. |
| `RateLimitError` | stable | Upstream rate-limit response. |
| `UnauthorizedError` | stable | Upstream credentials rejected. |
| `build_embedding_text` | provisional | Embedding-text helper. |
| `code_token` | stable | Catalog code normaliser. |
| `normalize_code` | stable | Catalog code normaliser. |
| `normalize_series_catalog_row` | provisional | Catalog row normaliser. |
| `series_match_from_entry` | provisional | SeriesMatch constructor. |
| `client` | provisional | Lazy-composed Connectors surface. |
| `__version__` | stable | Installed kernel package version. |
| `CONTRACT_VERSION` | stable | Integer contract-version string (new in contract v1). |

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

### `parsimony.transport.http`

| Symbol | Stability | Notes |
|---|---|---|
| `HttpClient` | stable | Shared async HTTP client with retry / backoff / query-param injection. |

### `parsimony.testing`

| Symbol | Stability | Notes |
|---|---|---|
| `assert_plugin_valid` | stable | Conformance-suite assertion. |
| `ConformanceError` | stable | Raised by `assert_plugin_valid` on failure. |

### `parsimony.errors`

Re-exports of the root error hierarchy; same stability as at root.

---

## 7. Versioning and deprecation policy

### Contract version bumps

| Change | Required bump |
|---|---|
| Break a **stable** symbol (remove, rename, tighten input, loosen output) | `CONTRACT_VERSION` major bump **and** kernel MAJOR. |
| Break a **provisional** symbol | Kernel MINOR with `DeprecationWarning` on the removed shape for one minor cycle. |
| Add a new symbol marked **provisional** | Kernel MINOR (non-breaking). |
| Promote **provisional** → **stable** | Kernel MINOR. |
| Demote **stable** → **provisional** | Not allowed. Once stable, stays stable until MAJOR. |
| Mark any new symbol **stable** on first introduction | Discouraged. Default new surface to **provisional**. |

### Deprecation windows

- **Stable symbol removal:** one-year window minimum between the first kernel release emitting `DeprecationWarning` and the kernel release where the symbol is gone. Document the planned removal in `CHANGELOG.md` at both ends.
- **Provisional symbol removal:** one minor release cycle emitting `DeprecationWarning`.

### Kernel ABI gate

The kernel refuses to import a plugin module whose `parsimony-contract-v<N>` keyword does not match `parsimony.CONTRACT_VERSION`. The kernel logs a structured diagnostic naming the plugin and its declared version. This prevents silent breakage; plugins with mismatched versions fail loudly rather than raising mid-fetch.

---

## 8. What is explicitly **not** contract

These are available in `parsimony.*` but are **not** stable-or-provisional surface, and plugins MUST NOT import them. They may change or disappear in any kernel release:

- Anything with a leading underscore (e.g. `parsimony.discovery._scan`).
- Submodules not named in §6 (`parsimony.bundles`, `parsimony.catalog.arrow_adapters`, `parsimony.mcp.*`, `parsimony.stores.*` except `DataStore` / `InMemoryDataStore` / `LoadResult` / `CatalogStore`, and anything else not listed above).
- Anything under `parsimony-core`'s optional extras that is not explicitly re-exported from the public surface (e.g. the SQLite-vec / litellm path under `[search]`, the MCP server scaffolding under `[mcp]`).

There is no in-tree `parsimony.connectors` package: every connector ships as its own `parsimony-<name>` distribution discovered via the `parsimony.providers` entry-point group.

---

## 9. Grace period for pre-1.0

Until the first non-alpha kernel release (tracked at `CONTRACT_VERSION = "1"` stabilisation), the stability markings in §6 are a commitment **about** the markings themselves: no symbol loses its marking during the alpha cycle. Specific symbols marked **stable** in this draft may nevertheless change shape before `1.0` if a genuine flaw is found — in which case the shape changes, the marking stays, and the next alpha tag ships with the fix.

Post-1.0, the markings are contractually binding and §7 applies strictly.

---

*This is contract v1.*
