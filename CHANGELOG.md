# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.4.0]

### Breaking changes

- **New `parsimony.discover` module** replaces `parsimony.discovery`. Three
  functions (`iter_providers`, `load`, `load_all`) plus one frozen dataclass
  (`Provider`). Metadata-only enumeration, no cache, no singleton, no
  import-time side effects. `Provider.load()` returns the plugin's
  `CONNECTORS` export — no more `DiscoveredProvider`, `ENV_VARS`, or
  `PROVIDER_METADATA` in the kernel record.
- **Deleted `parsimony.discovery`** — `DiscoveredProvider`, `discovered_providers`,
  `load_provider`, `iter_entry_points`, `build_connectors_from_env`, the
  per-process cache, `PluginError`, `PluginImportError`, `PluginContractError`.
  Replaced by `parsimony.discover.Provider` + `iter_providers` +
  `TypeError`/`LookupError`/`RuntimeError` for the three failure modes.
- **Deleted `parsimony.client` lazy singleton** and the `load_dotenv()`
  auto-import shim on `parsimony.__getattr__`. Silent side effects on
  attribute access were a recurring source of surprise; agents assemble
  their own `Connectors` via `discover.load_all().bind_env()` now. The
  `python-dotenv` dependency moved with the shim to `parsimony-mcp`.
- **`@connector(env=...)` replaces module-level `ENV_VARS`**. The
  decorator now accepts a keyword-only `env: dict[str, str] | None` mapping
  dep names to env-var names (`{"api_key": "FRED_API_KEY"}`). Stored on
  the resulting `Connector` as the read-only `env_map: Mapping[str, str]`.
  `@enumerator` and `@loader` inherit the kwarg through their delegation to
  `connector(...)`.
- **`[project.urls] Homepage` in `pyproject.toml` replaces
  `PROVIDER_METADATA`**. Homepage is resolved at runtime from distribution
  metadata via `importlib.metadata` — no per-module dict duplicating
  PEP 621 data.
- **`Connectors` verbs**: new `merge(*others)`, `bind_env(overrides=None)`,
  `env_vars()`, `replace(name, connector)`, `unbound` property, and a
  `filter(predicate)` overload. `bind_deps()` is renamed to `bind()` with
  no backwards-compat alias. `__add__` is kept internal as the merge
  engine; public callers use `.merge()`.
- **Keep-but-unbound credentialing.** `Connectors.bind_env()` does not silent-
  drop connectors whose required env var is missing; it marks the clone
  `bound=False` and preserves it in the collection. Calling an unbound
  connector raises `parsimony.errors.UnauthorizedError` naming the missing
  env var. Unbound names are surfaced via `Connectors.unbound: tuple[str, ...]`.
- **`Connector.bound: bool` and `Connector.env_map: Mapping[str, str]`** are
  new fields on the frozen dataclass. `bound` defaults to `True`; it flips
  to `False` only on clones produced by `bind_env()` for connectors whose
  required env var was unresolved. `env_map` defaults to the empty mapping.
- **Conformance checks renamed**: `_check_env_vars_map_to_deps` →
  `_check_env_map_matches_deps` (walks each `Connector.env_map` instead of
  reading `module.ENV_VARS`). `ProviderTestSuite.test_env_vars_map_to_deps`
  renamed to `test_env_map_matches_deps`.
- **CLI `parsimony list` output.** Dropped per-plugin `env_vars_present` /
  `env_vars_missing` / `provider_metadata` keys. Env-var aggregation happens
  at the collection level via `CONNECTORS.env_vars()` — the JSON payload now
  has a single top-level `env_vars: [...]` array. Metadata-only by default;
  `--strict` imports each plugin for the conformance check.
- **`parsimony.publish`** now uses the connector's `env_map` directly
  via `Connectors.bind_env()`; the `env_vars` parameter on `publish()` is
  gone.

## [0.3.1]

### Added

- `parsimony.transport.redact_url(url)` — strip sensitive query-param values
  before logging or embedding a URL in an exception message.
- `parsimony.transport.parse_retry_after(response, *, default=60.0)` — extract
  retry-after seconds from a 429 response, with `X-Ratelimit-Reset`
  Unix-epoch fallback.
- `parsimony.transport.map_http_error(exc, *, provider, op_name)` — translate
  `httpx.HTTPStatusError` into a typed `parsimony.errors` exception
  (401/403→`UnauthorizedError`, 402→`PaymentRequiredError`,
  429→`RateLimitError`, else→`ProviderError`). Replaces the per-connector
  `_raise_mapped_error` helpers duplicated across six packages.
- `parsimony.transport.map_timeout_error` and `pooled_client` — timeout →
  `ProviderError` mapping and a context-managed pooled `httpx.AsyncClient`
  for burst workloads.

### Changed

- `parsimony.http` renamed to `parsimony.transport` (the module now covers
  more than just the `HttpClient`). `parsimony.http` remains as a
  deprecation shim through the 0.3.x line.
- Publish optimisation: when `--only` is set and a plugin exports
  `RESOLVE_CATALOG`, the publisher tries the resolver first and skips the
  `CATALOGS` walk entirely if every requested namespace resolves.

## [0.3.0]

### Breaking changes (kernel refactor)

- **Flat module layout.** Subpackages `bundles/`, `catalog/`, `_standard/`,
  `discovery/`, `stores/`, `transport/`, `cli/` replaced by flat modules at
  the `parsimony.*` top level: `catalog.py`, `embedder.py`, `indexes.py`,
  `publish.py`, `discovery.py`, `stores.py`, `http.py` (renamed to
  `transport.py` in 0.3.1), `cli.py`.
- **`BaseCatalog` ABC → `CatalogBackend` Protocol** (two methods:
  `add(entries)` and `search(query, limit, namespaces=...)`). The
  canonical `Catalog` class still ships but as the reference
  implementation, not a nominal base.
- **Catalog method renames.** `upsert()` → `add()`. `index_result()` →
  `add_from_result()`. `entries_from_table_result()` →
  `entries_from_result()`.
- **`Column(role=KEY).namespace` is now optional.** When omitted,
  `Catalog.add_from_result()` uses the catalog's own `name` as the default.
- **Catalog publishing.** `@enumerator(catalog=CatalogSpec(...))` replaced
  by exporting `CATALOGS: list[(ns, fn)]` or `async def CATALOGS(): ...`
  on the plugin module. Optional `RESOLVE_CATALOG(ns) -> fn | None` for
  on-demand builds. `CatalogSpec` / `CatalogPlan` / `to_async` removed.
- **`SemanticTableResult` merged into `Result`.** Results carry an optional
  `output_schema: OutputConfig | None`; no separate subclass. Schema-aware
  accessors (`entity_keys`, `data_columns`, `metadata_columns`) return
  empty sequences when the schema is absent.
- **Namespace templates removed.** `Column(namespace="x_{agency}")` and
  `resolve_namespace_template()` are gone — plugin authors build namespace
  strings directly in Python.
- **`Namespace` annotation class removed.** Use
  `Annotated[str, "ns:<name>"]` sentinel.
- **`LazyNamespaceCatalog` removed.** Ships as a userland recipe instead.
- **`ResultCallback` + `Connector.with_callback()` + `Connectors.with_callback()`
  PRESERVED** — observer semantics unchanged (exceptions logged, not raised).
- **CLI verbs: 4 → 2.** `parsimony list [--strict|--json]` (merges
  `list-plugins` + `conformance verify` + `bundles list`) and
  `parsimony publish --provider NAME --target 'url/{namespace}'` (replaces
  `bundles build`). `--force` flag removed.
- **Conformance checks: 7 → 3.** Kept: `connectors_exported`,
  `descriptions_non_empty`, `env_vars_map_to_deps`. Dropped:
  `tool_tag_description_length`, `env_vars_shape`,
  `name_env_var_collisions`, `provider_metadata_shape`.
- **Removed symbols.** `BundleNotFoundError`, `CONTRACT_VERSION`,
  `parsimony-contract-v1` keyword reading, `ProviderCatalogURL`,
  namespace manifest + resume + content hashing + upload retry,
  `BaseCatalog`, `SemanticTableResult`, `Namespace`,
  `LazyNamespaceCatalog`, `CatalogSpec`, `CatalogPlan`, `to_async`,
  `DataStore` (alias).
- **`EmbeddingProvider` ABC → Protocol.** Three bundled implementations
  unchanged: `SentenceTransformerEmbedder`, `LiteLLMEmbeddingProvider`.

### Metrics

- Kernel LOC: ~4,035 (from ~5,838). 13 flat modules from 31 files across
  8 subpackages.

## [0.1.0a1] — 2026-04-10

### Added

- `@connector` and `@enumerator` decorators for typed data source wrappers
- `Connectors` composition with `+` operator and `bind_deps()`
- `Result` with provenance tracking
- `OutputConfig` with `Column` roles (KEY, TITLE, METADATA, DATA)
- Built-in connectors: FRED, SDMX, FMP, FMP Screener, SEC Edgar, Polymarket, EODHD
  (all subsequently extracted to the `parsimony-connectors` monorepo)
- `with_callback()` for post-fetch hooks on connectors and collections
- Typed error hierarchy: `ConnectorError`, `UnauthorizedError`,
  `PaymentRequiredError`, `RateLimitError`, `ProviderError`,
  `EmptyDataError`, `ParseError`
- `HttpClient` with credential redaction in logs
