# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.3.1]

### Added

- `parsimony.http.redact_url(url)` — strip sensitive query-param values
  before logging or embedding a URL in an exception message.
- `parsimony.http.parse_retry_after(response, *, default=60.0)` — extract
  retry-after seconds from a 429 response, with `X-Ratelimit-Reset`
  Unix-epoch fallback.
- `parsimony.http.map_http_error(exc, *, provider, op_name)` — translate
  `httpx.HTTPStatusError` into a typed `parsimony.errors` exception
  (401/403→`UnauthorizedError`, 402→`PaymentRequiredError`,
  429→`RateLimitError`, else→`ProviderError`). Replaces the per-connector
  `_raise_mapped_error` helpers duplicated across six packages.

## [0.3.0]

### Breaking changes (kernel refactor)

- **Flat module layout.** Subpackages `bundles/`, `catalog/`, `_standard/`,
  `discovery/`, `stores/`, `transport/`, `cli/` replaced by flat modules at
  the `parsimony.*` top level: `catalog.py`, `embedder.py`, `indexes.py`,
  `publish.py`, `discovery.py`, `stores.py`, `http.py`, `cli.py`.
- **`BaseCatalog` ABC → `CatalogBackend` Protocol** (two methods:
  `add(entries)` and `search(query, limit, namespaces=...)`). The
  canonical `Catalog` class still ships but as the reference
  implementation, not a nominal base.
- **Catalog method renames.** `upsert()` → `add()`. `index_result()` →
  `add_from_result()`. `entries_from_table_result()` → `entries_from_result()`.
- **`Column(role=KEY).namespace` is now optional.** When omitted,
  `Catalog.add_from_result()` uses the catalog's own `name` as the default.
- **Catalog publishing.** `@enumerator(catalog=CatalogSpec(...))` replaced
  by exporting `CATALOGS: list[(ns, fn)]` or `async def CATALOGS(): ...` on
  the plugin module. Optional `RESOLVE_CATALOG(ns) -> fn | None` for
  on-demand builds. `CatalogSpec` / `CatalogPlan` / `to_async` removed.
- **`SemanticTableResult` merged into `Result`.** Results carry an optional
  `output_schema: OutputConfig | None`; no separate subclass.
- **Namespace templates removed.** `Column(namespace="x_{agency}")` and
  `resolve_namespace_template()` are gone — plugin authors build namespace
  strings directly in Python.
- **`Namespace` annotation class removed.** Use
  `Annotated[str, "ns:<name>"]` sentinel.
- **`LazyNamespaceCatalog` removed.** Ships as a userland recipe instead.
- **`ResultCallback` + `Connector.with_callback()` + `Connectors.with_callback()`
  PRESERVED** — observer semantics unchanged (exceptions logged, not raised).
- **Transport module removed.** `HttpClient` moves from
  `parsimony.transport.http` to `parsimony.http`.
  `parsimony.transport.json_helpers` deleted.
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
  namespace manifest + resume + content hashing + upload retry.
- **`EmbeddingProvider` ABC → Protocol.** Three bundled implementations
  unchanged: `SentenceTransformerEmbedder`, `LiteLLMEmbeddingProvider`.
- **`DataStore` alias removed.** Use `InMemoryDataStore` directly.

### Metrics

- Kernel LOC: ~4,035 (from ~5,838). 13 flat modules from 31 files across
  8 subpackages.
- Test suite: 215 tests, 87.97% coverage.

## [Unreleased]

### Breaking changes

- **Connectors ship as separate `parsimony-<name>` packages.** Every
  connector is published from
  [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)
  as its own PyPI distribution and discovered via the
  `parsimony.providers` entry-point group. The kernel is a thin shell
  with no in-tree connectors. Import connectors from their own package
  (`from parsimony_fred import CONNECTORS`). See
  [`DESIGN-distribution-model.md`](DESIGN-distribution-model.md) and
  [`docs/contract.md`](docs/contract.md) for the binding spec.
- **Allow-list default for discovery.** Officially-maintained
  `parsimony-<name>` packages (listed in the bundled
  `OFFICIAL_PLUGINS.json`) load without opt-in. Non-official plugins
  require `PARSIMONY_TRUST_PLUGINS=<name1>,<name2>` to load. Set
  `PARSIMONY_TRUST_PLUGINS=*` to bypass allow-list checks entirely
  (developer escape hatch; logs a warning). See
  [`docs/contract.md`](docs/contract.md) §7.
- **ABI gate.** Plugins declare their target contract version via the
  `parsimony-contract-v<N>` keyword in `[project] keywords`. The
  kernel refuses to import plugins whose declared version does not
  match `parsimony.CONTRACT_VERSION`, or that omit the keyword.
- **Heavy catalog dependencies moved to `[catalog]` extra.** `faiss-cpu`,
  `sentence-transformers`, and `huggingface_hub` are no longer mandatory.
  Users who need HF-bundle catalog search install via
  `pip install parsimony-core[catalog]`.
- **Catalog distribution uses HuggingFace Parquet + FAISS bundles**
  (`HFBundleCatalogStore`). `SQLiteCatalogStore` remains for local-only
  use.
- **`Catalog.search` requires an explicit non-empty `namespaces=[...]`
  list.** Each namespace ships with its own embedding model; implicit
  cross-namespace search is unsound and now rejected with `ValueError`.

### Added (distribution model)

- `parsimony.discovery` — plugin discovery + composition public API:
  `build_connectors_from_env`, `discovered_providers`,
  `iter_entry_points`, `load_provider`, `DiscoveredProvider`.
- `parsimony.CONTRACT_VERSION` export. Used by the ABI gate;
  documented in [`docs/contract.md`](docs/contract.md) §2.
- `parsimony conformance verify <package>` CLI subcommand. Release-gate
  and regulated-finance security-review artefact. JSON output; exit 0
  on pass, 1 on fail, 2 on not-installed.
- Structured logging on the `parsimony.discovery` logger for every
  ABI-gate and trust-gate decision.

### Changed

- **Distribution name is now `parsimony-core` on PyPI.** The `parsimony` name on PyPI
  is currently held by an unrelated squatted project. Import path is unchanged
  (`from parsimony import ...`). The distribution will migrate to the bare `parsimony`
  name once it becomes available; a shim `parsimony-core` will then depend on `parsimony`
  for backwards compatibility.
- `pyproject.toml`: explicit `[tool.hatch.build.targets.wheel]` and `sdist` sections added
  so the `parsimony/` package is unambiguously included in the built distributions.

### Added

- `HFBundleCatalogStore` — reads Parquet + FAISS bundles from HuggingFace Hub; path-confined downloads, SHA-256 integrity checks, size caps, revision pinning via `PARSIMONY_CATALOG_PIN`, single-flight loading for both `try_load_remote` and `refresh`, atomic `refresh()` swap, LRU-bounded `_loaded` cache (`PARSIMONY_MAX_LOADED_BUNDLES`, default 16), O(1) `get`/`list` via pre-built `code_index`, structured per-query `catalog.search` log line. `status()` and `refresh()` return plain dicts — JSON-serializable out of the box.
- `SentenceTransformersEmbeddingProvider` — LRU-bounded process-wide model cache (size 2) keyed on `(repo_id, revision)`, prefix allowlist on model ids, per-instance embedding semaphore (`PARSIMONY_EMBED_CONCURRENCY`) preventing torch from saturating the asyncio default thread pool.
- `BundleManifest` Pydantic model (minimal fields — only what changes per bundle), `entries.parquet` schema, allowlisted filenames. Error hierarchy: `BundleError`, `BundleNotFoundError`, `BundleIntegrityError` (3 classes; message is the discriminator).
- `parsimony.stores.hf_bundle.builder` CLI — local bundle builder that runs enumerators, embeds, writes Parquet + FAISS + manifest, and uploads to HuggingFace Hub. Publish CLI adds `--dry-run`, `--yes` (required for destructive upload), `--allow-shrink` (bypass >50% entry_count drop guard), and `--keep-dir` (copy bundle out before tempdir cleanup). Token-shape regex redactor + urllib3/requests DEBUG-logger silencing during upload.
- `CatalogStore.try_load_remote` hook; default implementation returns `False`.
- Supply-chain audit job in CI (`uv tool run pip-audit`) + committed `uv.lock`.
- Opt-in retrieval-quality eval fixture (`tests/test_hf_bundle_retrieval_eval.py`, gated on `PARSIMONY_EVAL_HF=1`).

### Changed (post-review hardening)

- `Catalog.search` no longer double-embeds the query; the store owns `embed_query` and receives the raw query string.
- `Catalog.ingest` narrows its retryable-exception catch to `httpx.TransportError`. `RuntimeError` / `OSError` / programmer-error signals now propagate instead of being silently counted into the errors bucket.
- `parsimony.stores` uses the lazy `__getattr__` pattern matching `parsimony.catalog` and `parsimony.embeddings`, so `SQLiteCatalogStore` consumers don't pay pyarrow/huggingface_hub import cost.
- HF bundle publish/build targets removed from `Makefile` — use the CLI directly: `python -m parsimony.stores.hf_bundle.builder publish <ns> --yes`.



- `@loader` decorator for observation-persistence connectors
- `DataStore` abstract class and `InMemoryDataStore` implementation
- `LoadResult` statistics model for data loading
- MCP (Model Context Protocol) server integration (`parsimony.mcp`)
- Provider registry pattern (`ProviderSpec`) for declarative connector wiring
- `Connectors.filter()` for tag/property-based connector filtering
- `Connectors.to_llm()` and `Connector.to_llm()` for LLM-ready descriptions
- `Connector.describe()` for human-readable documentation
- `Result.to_table()` for late schema application
- `Result.entity_keys`, `Result.data_columns`, `Result.metadata_columns` accessors
- Arrow/Parquet serialization (`Result.to_arrow()`, `Result.to_parquet()`)
- `Namespace` annotation for catalog-aware parameter fields
- `Catalog` lazy namespace population (GitHub download + live enumerator fallback)
- `Catalog.embed_pending()` for backfilling embeddings
- Hybrid search (FTS5 BM25 + vec0 cosine via Reciprocal Rank Fusion) in `SQLiteCatalogStore`
- New connectors: CoinGecko, Finnhub, Tiingo, Alpha Vantage, EIA, BLS, US Treasury
- Central bank connectors: SNB, RBA, Riksbank, BDE, BOJ, BOC, BDP, BDF, Destatis
- Financial reports connector
- `Makefile` with common development commands
- `CLAUDE.md` for AI-assisted development conventions
- `py.typed` marker for PEP 561

### Changed

- `build_connectors_from_env()` now driven by declarative `PROVIDERS` registry
- Improved error messages for missing dependencies and validation failures
- `RateLimitError.retry_after` now validates against epoch timestamp misuse

### Fixed

- `from parsimony import client` no longer crashes without API keys (uses `lenient=True`)
- Bare `Exception` catches narrowed to specific operational types (EC-1 convention)
- Fixed import ordering in riksbank, snb, coingecko, tiingo modules
- All ruff linting issues resolved (E501, E402, F841, B904, B905, SIM)

## [0.1.0a1] - 2026-04-10

### Added

- `@connector` and `@enumerator` decorators for typed data source wrappers
- `Connectors` composition with `+` operator and `bind_deps()`
- `Result` and `SemanticTableResult` with provenance tracking
- `Catalog` with optional vector-searchable catalog
- `SQLiteCatalogStore` implementation with FTS5 and optional vector search
- `LiteLLMEmbeddingProvider` for catalog embeddings
- `OutputConfig` with `Column` roles (KEY, TITLE, METADATA, DATA)
- Built-in connectors: FRED, SDMX, FMP, FMP Screener, SEC Edgar, Polymarket, EODHD
- `with_callback()` for post-fetch hooks on connectors and collections
- Typed error hierarchy: `ConnectorError`, `UnauthorizedError`, `PaymentRequiredError`,
  `RateLimitError`, `ProviderError`, `EmptyDataError`, `ParseError`
- `HttpClient` with credential redaction in logs
