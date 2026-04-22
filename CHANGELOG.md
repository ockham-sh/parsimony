# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

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
