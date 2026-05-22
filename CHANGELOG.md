# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.7.0]

### Breaking changes

- **Decoupled `embedder` parameter from `Catalog`**: `Catalog` constructor and `Catalog.load` no longer accept `embedder`. Instead, the `embedder` is managed at the `VectorIndex` level.
- **Removed `CatalogCache` and the in-process LRU**: Catalog reuse is now the caller's responsibility (a one-time `Catalog.load(url)` plus a module-level singleton or small dict). The kernel no longer carries hidden global cache state.
- **Unified `Catalog.load` and `catalog.save`**: Removed `Catalog.from_url` and `Catalog.push`, replacing them with unified `Catalog.load(url_or_path)` and `catalog.save(url_or_path)`.
- **Removed `CatalogBackend`**: Dropped the obsolete `CatalogBackend` protocol and references.
- **Dropped v2 migration shim**: Removed the v2-to-v3 snapshot loader migration shim. `SCHEMA_VERSION` is pinned strictly to its current value.
- **Removed mutable/legacy methods**: Deleted `Catalog.{add_entries, add_indexes, set_default_field, delete, exists, list_namespaces, list_entries}` and the `indexes` property.
- **Tabular factories moved to `TabularResult`**: `from_dataframe`, `from_arrow`, and `from_parquet` are no longer on `Result`; import and call them on `TabularResult` instead.
- **Explicit connector secrets**: `@connector`, `@enumerator`, and `@loader` accept `secrets=(...)`. Declared names are omitted from `Provenance.params`. Name-based secret conformance (`check_unbound_secret_params`) is removed — connector packages must declare `secrets=` for auth-bearing parameters.
- **Flat public connector parameters**: Connectors must expose flat function parameters only. Bundled `params: BaseModel` signatures and kernel field-splat binding are removed. Use Pydantic models internally for validation if helpful.

### Added

- **BM25 Token Persistence**: `BM25Index.save` now writes a compressed `tokens.parquet` file. `BM25Index.load` synchronously reads tokens and rebuilds the BM25 model, enabling completely offline/self-contained keyword search.
- **Snapshot Integrity Check**: Added required `content_sha256` hash under `BuildInfo` to verify snapshot contents (excluding `meta.json`) on load.
- **Graceful Structured Search Fallback**: If the first field in a structured query does not have a configured index, the query falls back to a broad search against `default_field`.
- **Local Cache Subdirectories Split**: Added `parsimony.cache.staging_dir(provider)` for staging local connector builds.
- **`Connector.secrets`**: Tuple of parameter names excluded from provenance at decoration time; validated against the wrapped function signature.

### Changed

- **`Provenance.safe_dump()`**: Truncates oversize `params` / `properties` blobs only; it no longer name-redacts entries by parameter name.
- **Conformance checks**: Four checks remain (`check_connectors_exported`, `check_descriptions_non_empty`, `check_enumerator_return_type`, `check_flat_public_params`).

### Removed

- **`SECRET_NAME_PATTERN` and `REDACTED`** exports from `parsimony.result`.
- **`safe_dump_provenance()`** module helper (use `Provenance.safe_dump()`).
- **`HttpClient.aclose()`** no-op.

## [0.5.0]

### Breaking changes

- **`Provenance` is now framework-only.** `source` and `source_description`
  are required; `extra="forbid"`. Removed `title`, `description`, `tags`
  (curation lives at the artifact layer downstream). Connectors that built
  a `Provenance` directly will have it overwritten by the framework — use
  `Result.with_properties(**extras)` to contribute source-specific extras.
- **`Result.from_dataframe(df, provenance=...)` no longer accepts
  `provenance=`.** The framework owns authorship.
- **`OutputConfig.build_table_result(df, provenance=..., params=...)` no
  longer accepts `provenance=` / `params=`.** Pure schema application; the
  framework's `Connector._wrap_result` is the only path that authors
  provenance.
- **Connectors are plain async callables.** Function signatures are the
  connector parameter contract; Pydantic models are ordinary parameters,
  not a required first argument.
- **Framework env binding is removed.** Connector implementations own any
  env fallback they support, and operators use `Connector.bind(...)` /
  `Connectors.bind(...)` to create variants with fixed values hidden from
  the exposed call surface and call-time provenance.
- **Tool schema export is a projection.** `Connector.to_json_schema()`
  derives JSON Schema from the current exposed signature and fails at that
  boundary for unsupported public parameters. Required secret-shaped
  parameters must be bound before tool export; optional secret-shaped
  parameters are omitted from the schema.

### Added

- `Provenance.safe_dump()` and `safe_dump_provenance()` — single canonical
  redactor for every wire/disk exit. Key-redacts secret-named entries in
  `params` / `properties` with the `«redacted»` sentinel; oversize fields
  are replaced with a structured `{"truncated": True, "byte_length": N,
  "field": ...}` marker (never a partial prefix that could leak the head
  of an unredacted secret).
- `Provenance.data_object_path: str | None` — pointer to a content-
  addressed snapshot of the bytes returned by this fetch, populated by an
  external persister callback.
- `Result.with_properties(**kwargs)` — connector-only channel for
  contributing source-specific extras to provenance. Multiple calls merge
  cumulatively.
- `SECRET_NAME_PATTERN` and `REDACTED` constants exported from
  `parsimony.result` for downstream redactors.
- `Connector._wrap_result` is the single authoring site for every
  `Provenance` field: `source` (from `fn.__name__`),
  `source_description` (from `fn.__doc__`), `fetched_at`, `params`, and
  `properties` (from any `Result.with_properties` calls).

### Removed

- **`FragmentEmbeddingCache`** and its public re-export from
  `parsimony.__all__`. The fragment-composition embedding strategy is no
  longer used by any in-tree connector; downstream catalogs index a single
  composite document per entry directly via `EmbeddingProvider.embed_texts`.
- **`parsimony.cache.embeddings_dir()`** and the `embeddings/` named
  subdirectory under the cache root. The remaining named subdirs are
  `catalogs/`, `models/`, and `connectors/`.

## [0.4.2]

### Changed

- **Typed `ConnectorError` defaults now carry agent-loop directives.**
  `UnauthorizedError`, `PaymentRequiredError`, `RateLimitError`,
  `ProviderError`, and `ParseError` default messages embed explicit
  `DO NOT retry` / `pick a different connector` / `inform the user`
  prose sized to the class semantics. `ProviderError` defaults branch
  on `status_code` (408 / 5xx / other 4xx). The `message=` keyword
  remains as an escape hatch for connector authors who carry agent-
  useful context the kernel cannot construct (e.g. an upstream
  `error_code`); when overridden, authors take responsibility for the
  agent-facing text being free of URLs, tokens, and upstream payloads.
  `EmptyDataError` keeps its no-`DO NOT retry` default — empty is a
  valid outcome, the agent SHOULD be free to adjust parameters and
  retry. The locked contract lives in `tests/test_errors.py`.

### Added

- `UnauthorizedError.env_var: str | None = None` — names the environment
  variable the agent should set to fix the failure. The kernel default
  message embeds it when present.
- `Connector.__call__` now passes `env_var=` automatically when raising
  `UnauthorizedError` for an unbound connector, deriving the variable
  name(s) from the connector's `env_map`.

## [0.4.1]

### Fixed

- Removed unused `toon-format` runtime dependency. The kernel never imported it
  — it was declared for downstream consumers (`parsimony-mcp`, future
  `parsimony-agents`) which now declare it themselves. Eliminates the
  `--prerelease=allow` requirement that propagated to every project depending
  on `parsimony-core` due to `toon-format>=0.9.0b1` being a beta.

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
  reading `module.ENV_VARS`).
- **CLI `parsimony list` output.** Dropped per-plugin `env_vars_present` /
  `env_vars_missing` / `provider_metadata` keys. Env-var aggregation happens
  at the collection level via `CONNECTORS.env_vars()` — the JSON payload now
  has a single top-level `env_vars: [...]` array. Metadata-only by default;
  `--strict` imports each plugin for the conformance check.
- **Catalog lifecycle simplified.** Providers expose lazy `Catalog`
  declarations; maintainer scripts call `build()` and `push()` directly.
  The old framework publisher module is gone.

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
- Catalog publication optimisation in the legacy publisher improved targeted
  maintainer builds.

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
- **Catalog method renames.** `upsert()` became explicit entry mutation APIs.
  `entries_from_table_result()` became `entries_from_result()`.
- **`Column(role=KEY).namespace` is required for catalog results.** `Catalog.name`
  identifies the snapshot artifact only.
- **Catalog publishing.** Legacy catalog declaration hooks replaced
  `@enumerator(catalog=CatalogSpec(...))`. `CatalogSpec` / `CatalogPlan` /
  `to_async` removed.
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
  `parsimony cache`. `--force` flag removed.
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
