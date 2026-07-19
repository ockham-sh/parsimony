# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Changed

- **BREAKING — `OutputConfig` is renamed `OutputSpec` and no longer transforms
  data.** It is now a purely declarative labeling of column roles: no `dtype=`
  coercion, no `mapped_name=` renaming, no eager validation against the
  returned DataFrame. `Column` drops both fields — cast and rename in the
  connector body instead, before you return. `OutputSpec.build_entities(df)`,
  `build_table_result`, and the module-level `entities_from_dataframe` are
  removed. A `KEY` column may now omit `namespace=` at declaration time (e.g.
  a per-call dynamic namespace); it is only required once something actually
  projects entities — at that point a missing namespace raises `ValueError`,
  not at `OutputSpec` construction. `@loader` and `@enumerator` still enforce
  it eagerly at decoration time, since feeding a store/catalog is their entire
  purpose (closes #72, #73).
- **BREAKING — `Result.data` is renamed `Result.raw`.** The field name now
  states the framework's core guarantee — this is exactly what the connector
  returned, untouched — rather than a name that collided with the new
  entity-keyed `data` accessor below.
- **BREAKING — entity projection is two parallel `Mapping` views, not a
  method.** `Result.to_entities()` and `EntityResult` are removed. A tabular
  `Result` now exposes `entities: Mapping[EntityRef, Entity]` (identity: that
  entity's `TITLE` + `METADATA`) and `data: Mapping[EntityRef, pd.DataFrame]`
  (that entity's `DATA` columns only), sharing one key type and one set of
  keys (`result.entities.keys() == result.data.keys()`). `EntityRef` is a new
  two-field `NamedTuple` (`namespace`, `code`) that compares and hashes equal
  to a plain tuple. Both properties are computed from `result.raw` against
  `result.output_spec` on every access — uncached, so mutating `result.raw`
  and re-reading either property sees the update. Feed a catalog with
  `catalog.set_entities(result.entities.values())`; a data store loads from
  `result.data` directly.
- **BREAKING — `Result.df` alias is removed.** Use `Result.frame` (raises
  `TypeError` if the payload is not tabular).
- **BREAKING — `Catalog.search` replaces `field=` with `fields=`.** One
  parameter declares the scoring surface: a single indexed field name
  (`fields="title"`, exactly the old `field=` behavior) or several to fuse
  (`fields=["title", "ITEM_label", ...]` — fuzzy score is the row's best
  per-field score over the already-built indexes), so a caller can declare a
  bare-query surface such as title + dimension labels without rebuilding
  catalogs. Requires `query=`, which is then literal text — never parsed as
  the ``FIELD: value`` DSL. `search_values` keeps its single `field` argument
  (#69).
- **BREAKING — search results rank by (coverage, score); the exact-match
  sentinel is gone.** `CatalogMatch` and `CatalogValueMatch` gain
  `coverage: float`: the fraction of the query's tokens consumed by the union
  of the row's fully-consumed field values (a field value counts only when all
  its tokens appear in the query; case-insensitive string equality covers
  values that tokenize to nothing). An exact value hit is coverage 1.0 and
  ranks first WITHOUT suppressing fuzzy near-misses ("Annual rate of change"
  vs "Annual average rate of change" stay adjacent), value-level term
  repetition ("Current account, Current transfers") gates to 0.0 instead of
  outranking the consumed value, and a verbose query decomposes across a
  multi-field surface ("current account … euro area … quarterly" → ITEM +
  REF_AREA + FREQ). `EXACT_MATCH_SCORE = 1_000_000.0` no longer exists, and no
  magic magnitude ever reaches a caller: `score` is the sum over searched fields of the row's
  normalized per-field relevance (each field votes 0..1 of its own best
  match), computed for every candidate row in one backend pass — agreeing
  evidence across fields accumulates, no single field's raw magnitude
  dominates, and a deserving row can never be lost to per-field pool
  truncation. Chosen empirically over raw-max/min-max/RRF/DisMax fusion and
  over hard auto-filtering on a 37-case, 17-class query-taxonomy battery
  (MRR 0.81 vs 0.45 for the previous title-only search) (#69).
- **BREAKING — `DisMaxIndex` is removed.** It had no consumers: its per-field
  sub-indexes were byte-identical to standalone indexes (no blended term
  statistics, no precomputation), it forced a uniform component kind, and it
  could not ride the parquet path. Query-time `fields=` is the one multi-field
  mechanism; the `dis_max` kind is no longer valid in catalog metadata (#69).

- **BREAKING — transport error mapping now decides from the status code, not a
  raised `httpx.HTTPStatusError`.** `parsimony.transport.map_http_error`,
  `map_timeout_error`, `map_transport_error`, and the `classify=` callback are
  removed. In their place, `check_status(response, *, provider, op_name,
  env_var=None)` maps a non-2xx response to a typed error directly from
  `response.status_code` — no chained `HTTPStatusError`, so no query-string
  credential can leak through `__cause__`. Transport failures (timeout,
  connection refused, DNS, protocol error) are no longer mapped by a separate
  function at all — `HttpClient.request(...)` now maps them internally (timeout
  → `status_code=408`, any other failure → `status_code=503`), so no raw
  `httpx` exception ever escapes `request()`.
- **BREAKING — `HttpClient`, `make_http_client`, and `make_api_key_client` now
  require `provider=`** (also exposed as `HttpClient.provider`), and
  `HttpClient.request(...)` now requires `op_name=`. Both were previously
  threaded through per-call to the removed mapper functions; they're now
  supplied once, up front, and read by `check_status` and the internal
  transport-failure mapping.
- **BREAKING — `fetch_json` / `fetch_text` / `fetch_csv` dropped their
  `provider=` argument.** The provider slug is read from `http.provider`
  instead.
- The value-indexed (parquet) catalog lifecycle now fails loud instead of
  silently corrupting state: `attach_parquet_rows()` raises if indexes are
  unbuilt or rows were already attached, and `build()` / `set_entities()`
  raise after attaching (previously `build()` silently rebuilt the indexes
  over the emptied entity list). A snapshot's persisted backend kind is now
  derived from the actual row store, so `meta.json` can never disagree with
  the payload. Docstrings and the catalog docs now describe the two layouts
  honestly: row-indexed (the entities are the rows) vs value-indexed (the
  entities are codelist members; each parquet row is a composition of
  members). The persisted schema-v1 surface is untouched — all published
  snapshots load unchanged (closes #84).

### Removed

- **BREAKING — `InMemoryRowBackend` and the `RowBackend` protocol are gone.**
  The class was scaffolding: memory-mode search always iterated the catalog's
  own entity list directly, and nothing but an internal row count ever
  consulted it. `Catalog` now holds `ParquetRowBackend | None` — a backend
  object exists only when value-indexed parquet rows are attached.
  `ParquetRowBackend` no longer takes a `config=` argument (it never read it)
  and exposes its `path`.
- **BREAKING — dead `Catalog` surface deleted (zero callers):**
  `delete_many`, `set_index`, `update_indexes`, `set_field_links`, and the
  `field_links` property. `set_indexes` remains the one way to replace the
  index set, and `field_links` is still accepted at construction and
  persisted.
- **BREAKING — `CatalogBackendConfig.field_links` is removed.** No backend
  code ever read it; snapshots persist links from the catalog's own
  `field_links` as before, and loading passes them straight back to the
  constructor. The persisted `BackendMeta.field_links` field is unchanged.
- `BuildInfo.parsimony_version` (never assigned; outside the digest payload)
  and `validate_catalog_meta` (unreachable behind the pydantic `Literal`
  parse) are removed.

### Fixed

- `Catalog.load(<parquet snapshot>).save(...)` no longer raises. The rows
  path was tracked in shadow state that only `attach_parquet_rows()` set;
  save now reads it from the attached backend, so load → save → load
  round-trips.

## [0.7.5] - 2026-06-19

### Fixed

- **BM25 search now works on a bare `pip install parsimony-core`.** `rank-bm25` moved from the
  `[catalog]` extra to the base dependencies — it is pure-Python on top of numpy (already a base
  dep), and the base, top-level `auto_catalog` / `BM25Index` features must not require an extra.
  They previously failed with `ModuleNotFoundError: No module named 'rank_bm25'` anywhere
  `parsimony-core` was installed without `[catalog]` (e.g. the sandboxed agent kernel). The
  `[catalog]` extra now covers only the vector stack (FAISS, sentence-transformers,
  huggingface_hub).

## [0.7.4] - 2026-06-19

### Added

- **`auto_catalog(df)`** — a top-level convenience that wraps a DataFrame in an
  already-built, BM25-searchable `Catalog` in memory: one entity per row (`code` is the row
  position, so `df.iloc[int(match.code)]` recovers the row), the joined cell text as `title` for
  broad search, and every column as metadata for structured `column: value` search. For finding
  rows in data you already hold — not a way to build curated catalogs; for those, use the
  `Catalog` lifecycle with `entities_from_dataframe`. BM25 only (needs the `catalog` extra); no
  vector mode, since a runtime frame ships no prebuilt vectors.
- **Parquet-backed catalogs for large datasets.** `Catalog.attach_parquet_rows(path, config)` binds
  a flat parquet file as a lazy row backend (`ParquetRowBackend`): the index entities are the
  scoring surface while the actual rows stream on demand with filter pushdown. `CatalogBackendConfig`
  (memory|parquet, code/title columns, field links) captures the backend shape and is persisted in
  the snapshot manifest.
- **Release-ready snapshot validation.** Snapshots carry a `content_sha256` integrity digest plus a
  `manifest_contract_sha256`, both verified on load; validation tolerates Hugging Face
  `snapshot_download` symlinked files.
- **Richer search.** `Catalog.search()` adds `field=` (single-field soft scoring), `filter=` (exact
  AND constraints) and `top_k_values=`, and accepts a query-less filter-only search.
  `Catalog.search_values()` returns distinct indexed values as `CatalogValueMatch`.

### Changed

- **BREAKING — `Catalog.search()` returns `list[CatalogMatch]`** instead of
  `tuple[list[CatalogMatch], SearchDiagnostic]`. Drop the diagnostic from the unpack:
  `matches = cat.search(...)`.

### Removed

- **BREAKING — `SearchDiagnostic`.** `search()` no longer returns per-query diagnostics.
- **BREAKING — `Catalog.load_entities_only`.** Load a full snapshot with `Catalog.load()`.

## [0.7.3] - 2026-06-18

### Changed

- **Unified result type + honest governed `to_llm()`.** `TabularResult` is collapsed into a single
  `Result` (`data: Any` + optional `output_schema`; `is_tabular` / `frame` accessors), so tabular and
  opaque payloads are one type. `Result.to_llm()` renders an honest header (real
  `N rows × M columns [K hidden]`), a governed per-column schema, and the first N rows — never a
  head/tail sample masquerading as the whole. `governed_view()` and `shape_descriptor()` are the single
  source of truth for `exclude_from_llm_view` governance + schema annotation, paired to the frame by
  position (robust to integer and duplicate column labels).

### Removed

- **`TabularResult`** (breaking) — use `Result`; a result is tabular when `data` is a DataFrame. The
  head/tail `to_llm` sampler is gone.

## [0.7.2] - 2026-06-15

### Added

- **Governed `to_llm()` result views.** `Result.to_llm()` renders a compact, depth-limited
  structural preview of an opaque payload; `TabularResult.to_llm()` overrides it with a governed,
  schema-aware view (shape, per-column dtype + role + namespace, head/tail sample) honoring
  `exclude_from_llm_view`. `Column.llm_annotation()` is the single owner of role/namespace rendering,
  shared by the connector card's `Returns:` line and the result preview.
- **`fetch_csv` / `fetch_text` transport helpers** join `fetch_json` atop a shared `_get`: same
  typed-error mapping and secret redaction, parsing the body into a `DataFrame` / `str`. A body that
  cannot be parsed in the requested shape surfaces as a typed `ParseError` (CSV: `EmptyDataError` on
  an empty body), never a raw `json`/`pandas` exception.
- **`map_transport_error`** maps a non-timeout `httpx.TransportError` (connection refused, DNS,
  protocol error) to `ProviderError(status_code=503)`, embedding only the exception type name; the
  fetch helpers catch it so no raw `httpx` exception escapes.
- **Pinned `hf://` snapshots.** `hf://<org>/<repo>@<revision>` pins a catalog load to a Hugging Face
  git revision for a reproducible, tamper-resistant remote load.
- **`parsimony list --strict` lists declared secrets.** A `SECRETS` column shows the union of each
  plugin's credential parameter names (`?` when not inspected, `-` when none declared).

### Changed

- The connector card's `Returns:` line now annotates each LLM-visible column with its role and
  namespace (`name (ROLE ns:x)`) and omits `exclude_from_llm_view` columns.
- `Connectors.bind(**kwargs)` now raises `TypeError` when a keyword matches no connector in the
  collection (previously a silent no-op warning), so a typo'd credential name fails loudly —
  matching the per-connector `Connector.bind` contract.

### Removed

- **Dropped the unused post-fetch callback surface** (`ResultCallback`, `Connector.with_callback`,
  `Connectors.with_callback`). No consumer existed, and an observer that swallows exceptions is the
  wrong layer for persistence.

### Fixed

- **`HybridIndex` applies its configured fusion.** `score_candidates` previously ranked by the max
  raw component score and discarded the fused scores, so `ZScoreFusion` / `MinMaxScoreFusion` / `RRF`
  weights had no effect on fuzzy queries. The configured fusion now drives the ranking; exact-match
  sentinels are kept verbatim; centred (z-score) scores are shifted into the positive range so
  below-mean candidates are not silently dropped.
- Redact the `registrationkey` query parameter in transport logs (previously emitted in cleartext).
- Snapshot save coerces non-JSON-native catalog metadata (datetime, Decimal) to its string form
  (`json.dumps(default=str)`) instead of crashing the whole save.

## [0.7.1] - 2026-06-14

### Added

- **`parsimony.Namespace` marker** — a typed marker for connector parameters whose legal
  values come from a same-namespace catalog search, surfaced on connector cards as an
  agent/human hint.
- **Authoritative plugin contract** — `docs/contract.md` consolidates the distribution,
  connector, credential, error, search, catalog, and conformance rules in one place.

### Changed

- **Actionable catalog `schema_version` mismatch** — loading a catalog with an unsupported
  schema version now raises `Unsupported catalog schema_version N; expected 1` instead of a
  cryptic Pydantic validation error.
- **`parsimony list` shows connector counts by default** — the per-provider connector count
  no longer requires `--strict`; full conformance checks still do.
- **Embedder uses `get_embedding_dimension()`** with a fallback — silences the
  `sentence-transformers` `FutureWarning` on newer versions.

### Removed

- **Dropped the back-compat policy aliases** `hybrid_field_index` and `macro_discovery_indexes`;
  use the canonical `adaptive_field_index` and `discovery_indexes`.

### Docs

- Synced all connector-contract documentation to the synchronous calling surface
  (connectors are `def`, not `async def`).

## [0.7.0] - 2026-05-28

### Breaking changes

- **Renamed `[standard]` extra to `[catalog]`** — the cohesive catalog runtime extra is now
  `parsimony-core[catalog]`. `standard-onnx` still depends on it.
- **Removed the unused `[s3]` extra** — `s3://` catalog URLs were never wired.
- **Catalog query errors are `ConnectorError` subclasses** — `UnknownIndexedFieldError`,
  `BroadSearchUnavailableError`, and `BroadSearchConfigError` now participate in the typed error
  taxonomy.
- **`OutputConfig.build_table_result` fails on missing declared columns** instead of logging a
  warning.
- **HTTP client no longer retries 429** — rate limits surface immediately for agent-facing handling.
- **`discover.load_all()` raises `RuntimeError` when every installed provider fails to load.**

### Added

- **Actionable catalog dependency errors** — missing `parsimony-core[catalog]` raises a
  `ProviderError` with an install hint instead of a raw `ImportError`.
- **`map_http_error` / `fetch_json` accept optional `env_var`** for `UnauthorizedError` hints.
- **`Connectors.bind` warns on zero-match keys.**

### Changed

- **Response HTTP logging redacts URLs** like the request path already did.
- **`fetch_json` maps non-JSON 200 responses to `ParseError`.**
- **Catalog integrity failures include a cache-clear hint.**

## [0.6.0]

### Breaking changes

- **Decoupled `embedder` parameter from `Catalog`**: `Catalog` constructor and `Catalog.load` no longer accept `embedder`. Instead, the `embedder` is managed at the `VectorIndex` level.
- **Removed `CatalogCache` and the in-process LRU from the kernel**: Catalog reuse is now the caller's responsibility (a one-time `Catalog.load(url)` plus a module-level singleton or small dict). Provider packages use `parsimony.catalog.search.CatalogLRU` when needed.
- **Unified `Catalog.load` and `catalog.save`**: Removed `Catalog.from_url` and `Catalog.push`, replacing them with unified `Catalog.load(url_or_path)` and `catalog.save(url_or_path)`.
- **Removed `CatalogBackend`**: Dropped the obsolete `CatalogBackend` protocol and references.
- **Dropped v2 migration shim**: Removed the v2-to-v3 snapshot loader migration shim. `SCHEMA_VERSION` is pinned strictly to its current value.
- **Removed mutable/legacy methods**: Deleted `Catalog.{add_entries, add_indexes, set_default_field, delete, exists, list_namespaces, list_entries}` and the `indexes` property.
- **Tabular factories moved to `TabularResult`**: `from_dataframe`, `from_arrow`, and `from_parquet` are no longer on `Result`; import and call them on `TabularResult` instead.
- **Explicit connector secrets**: `@connector`, `@enumerator`, and `@loader` accept `secrets=(...)`. Declared names are omitted from `Provenance.params`. Name-based secret conformance (`check_unbound_secret_params`) is removed — connector packages must declare `secrets=` for auth-bearing parameters.
- **Flat public connector parameters**: Connectors must expose flat function parameters only. Bundled `params: BaseModel` signatures and kernel field-splat binding are removed. Use Pydantic models internally for validation if helpful.
- **`Connectors.merge` and `Connectors.replace` removed**: Combine collections with `a + b` or `Connectors([*a, *b])`. Substring search uses `connectors.search(query)` instead of `connectors.filter(name=...)`.
- **`@enumerator` requires `output=`**: Enumerators return raw `pd.DataFrame`; entity projection happens in catalog build via `entities_from_connector` / `entities_from_raw`.

### Added

- **BM25 Token Persistence**: `BM25Index.save` now writes a compressed `tokens.parquet` file. `BM25Index.load` synchronously reads tokens and rebuilds the BM25 model, enabling completely offline/self-contained keyword search.
- **Snapshot Integrity Check**: Added required `content_sha256` hash under `BuildInfo` to verify snapshot contents (excluding `meta.json`) on load.
- **Local catalog search helpers**: `parsimony.catalog.search.make_local_search_connector` and related types for provider catalog search connectors.
- **Local Cache Subdirectories Split**: Added `parsimony.cache.staging_dir(provider)` for staging local connector builds.
- **`Connector.secrets`**: Tuple of parameter names excluded from provenance at decoration time; validated against the wrapped function signature.
- **`InvalidParameterError`**: Typed error for call-time parameter validation failures.

### Changed

- **`Provenance.safe_dump()`**: Truncates oversize `params` / `properties` blobs only; it no longer name-redacts entries by parameter name.
- **Conformance checks**: Five checks remain (`check_connectors_exported`, `check_descriptions_non_empty`, `check_enumerator_decorator`, `check_enumerator_return_type`, `check_flat_public_params`).
- **Catalog dirty-state errors**: `search()` and `save()` on an unbuilt catalog now raise with an actionable message pointing to `await catalog.build()`.

### Removed

- **`SECRET_NAME_PATTERN` and `REDACTED`** exports from `parsimony.result`.
- **`safe_dump_provenance()`** module helper (use `Provenance.safe_dump()`).
- **`HttpClient.aclose()`** no-op.
- **`ResultCallback`** from top-level `parsimony` exports (still available from `parsimony.connector`).

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
- **Tool schema export is a projection.** Required secret-shaped parameters must be bound before tool export; optional secret-shaped parameters are omitted from the schema.

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
