# Changelog

All notable changes to parsimony will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

- **Declarative credential requirement**: `@connector`, `@loader`, and `@enumerator`
  accept `requires=(...)` — the env-var names that must resolve for a call to
  succeed (the names `UnauthorizedError` would cite if the connector were called
  with nothing configured). A static declaration, never resolution: nothing reads
  `os.environ`. Orthogonal to `secrets=` (redaction). When non-empty,
  `describe()` renders a `Requires: FRED_API_KEY` line after Parameters and the
  `to_llm()` header carries a `(needs FRED_API_KEY)` token.
- The registry manifest (`schema_version: 1`) rows carry `requires: list[str]`
  (the env vars a package's connectors need); `keyless` is not on the wire —
  `InstallableConnector` derives it as `not requires`. Any other
  `schema_version` still fails loudly.
- `CatalogMatch.matched` — `"lexical" | "semantic" | "both"`: which component
  surfaced the row's evidence (`None` on filter-only matches). The trap
  signal: an all-`"semantic"` result page means nothing lexically real
  matched — rephrase the query rather than trust the order.
  `search_index_values` returns the same evidence kind per value.
- Every catalog-backed search connector now ends with the same ranking trio —
  `coverage`, `score`, `matched` — defined once as `RANKING_COLUMNS`
  (`COVERAGE_COLUMN` / `SCORE_COLUMN` / `MATCHED_COLUMN` in
  `parsimony.catalog.search`) and appended automatically to every
  `make_local_search_connector` output spec; hand-rolled search connectors
  reuse the same constants. Identical meanings on every provider; what varies
  per surface is only the distribution of values (graded coverage on facet
  surfaces, mostly-0.0-with-exact-pins on prose catalogs).
- `CatalogValueMatch.matched` — `Catalog.search_values` now reports the
  evidence kind per value (it was already computed for fusion), so value-level
  consumers can emit the same trio.

### Changed

- **BREAKING — `Connectors.bind_env()` and `Connectors.unbound` are removed.**
  Both were inert stubs since 0.5.0 (return `self` / `()`), with zero callers.
- `Connectors.env_vars()` now returns the union of the env-var names its
  connectors declare via `requires=` (still no `os.environ` read); it always
  returned an empty `frozenset` before.
- `parsimony list` replaces the SECRETS column with REQUIRES — the union of
  each provider's declared `requires` env vars, sorted ("?" outside `--strict`,
  "-" when a provider declares none). The `--json` payload key `secrets` is
  renamed `requires` with the same sourcing.
- Catalog fetches are now much faster and no longer silent. A bundle is ~100
  small files, so a cold fetch was bound by sequential per-file round-trips
  rather than bandwidth — 45s to move under 1MB. Files are now downloaded
  concurrently, and a branch revision is resolved to a commit sha once so
  already-cached files skip their HEAD request entirely. Measured against
  `hf://parsimony-dev/sdmx`: a cold fetch drops ~45s → ~7s, and a warm
  re-resolution ~19s → ~0.4s. Pinning the listing and the downloads to one
  commit also stops a mid-fetch republish from assembling a bundle out of two
  different commits. Set `HF_HUB_ENABLE_HF_TRANSFER` to keep downloads serial.
- Catalog fetches log how many files and how many MB are about to transfer
  *before* downloading them, and the elapsed time after, on
  `parsimony.catalog.remote`; the embedding-model download logs the same pair on
  `parsimony.embedder`. The order is the point: the size has to arrive before
  the wait for a caller to judge whether a stall is proportionate, and a start
  with no matching finish is what marks a long wait as progress rather than a
  hang. Both lines are skipped when every file is already cached, so a
  revalidation never claims a download that did not happen. Previously the
  single fetch line was gated on a cached `meta.json` — one file of ~100 —
  which silenced exactly the largest downloads (a republish, an interrupted
  pull, any post-TTL revalidation).
- A cold catalog resolve announces itself *before* listing the repo, not only
  before downloading. The listing is a network round-trip that can stall for
  minutes on a large repo, and it runs before the file count and size are
  knowable, so it was previously silent for its whole duration. A revalidation
  — anything with a snapshot already on disk — stays quiet.
- Loading the embedding model logs a start/finish pair on `parsimony.embedder`.
  It costs seconds on the first semantic search of every process even when the
  model is fully cached, and suppressing the `Loading weights` bar removed the
  only signal it had. Unlike the bar, these lines survive redirection off a tty.
- Per-request HTTP tracing moved from INFO to DEBUG on `parsimony.transport`.
  It emits two lines per call, so a paginated fetch buried the events a caller
  enabled INFO to see — a catalog download, a model load — under hundreds of
  lines of chatter. Retry/backoff warnings are unchanged; they name a real
  blocking wait. This matches where `httpx`, `urllib3` and `requests` log
  theirs; recover the old behaviour with DEBUG on `parsimony.transport`.
- Catalog builds, ONNX export, ONNX quantization, and FAISS IVF training each
  log a matching completion line with elapsed time. All four previously
  announced a slow step and then went silent, which is the one shape a log
  cannot recover from: no way to tell still-working from died-halfway.

- Internal: the catalog's layout branch (in-memory entities vs attached
  parquet rows) is now decided in one place per concern — row iteration for
  scored search lives in a single candidate-row source, and the remaining
  layout checks test backend presence directly. No behavior change; closes
  out the #84 razor review.

- **BREAKING — factory search connectors declare the discovery surface.**
  `make_local_search_connector` ranked queries search the connector's declared
  surface — a new `search_fields=` parameter, defaulting to the entity recipe
  `ENTITY_SEARCH_FIELDS = ("title", "description")`, intersected with the
  loaded catalog's indexes — as literal text, and each connector's description
  states its declared surface. Consequences: the `FIELD: value` DSL no longer applies to
  factory-built connectors (exact reads use `filter=`); description evidence
  now ranks lexically (the description index was previously built and shipped
  but never searched on this path); and the two-field surface takes the
  lexical-first facet regime instead of single-field RRF, so a short generic
  title can no longer out-rank a row whose description carries the query's
  words.

- **BREAKING — catalog search is one native path; the fusion library is
  gone.** Hybrid fields combine their components by surface arity: on a
  multi-field facet surface, lexical-first with semantic void-fill (the
  vector ranks a field only when BM25 fully abstains); on a single-field
  surface, tie-aware unweighted Reciprocal Rank Fusion (k=60) over the BM25
  and vector-top-k rankings. Row ordering: multi-field surfaces keep
  `(coverage desc, score desc)`; single-field surfaces rank
  `(coverage == 1.0, score desc)` — an exactly-consumed value still pins
  rank 1, but partial containment no longer orders (on one long-text field
  it proxied title brevity, not relevance). `HybridIndex` no longer takes
  `fusion=`; snapshots write a frozen legacy `fusion` key that `load()`
  ignores, so old snapshots load unchanged and no republish is needed.
- **BREAKING — index policy is role-based.** `adaptive_field_index` and the
  `HYBRID_UNIQUE_VALUE_LIMIT` / `HYBRID_BM25_WEIGHT` / `HYBRID_VECTOR_WEIGHT`
  constants are removed; `discovery_indexes` picks index kind by field role
  (identifiers BM25-only; title/description always hybrid) and accepts an
  optional `embedder=`.
- **BREAKING — `Catalog.search` drops `namespaces=`** (dead parameter, zero
  callers anywhere).
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

- **BREAKING — `Catalog(backend=...)` constructor parameter is gone** (zero
  external callers; snapshot loading sets the config internally). The one way
  to give a catalog a backend config is `attach_parquet_rows(config=...)`,
  which is where the parquet file it describes actually arrives.
- **BREAKING — `Catalog.is_parquet_backend` is gone** (zero callers anywhere,
  including this codebase once its last internal read was inlined). The
  layout shows itself behaviorally: a value-indexed catalog has empty
  `entities` while `len()` counts the attached parquet rows.
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

- The `Loading weights` progress bar no longer leaks to stderr on every fresh
  process that loads the default embedder (transformers 5.x). Suppressed via
  transformers' own flag for the duration of the model load, deliberately not
  `disable_progress_bar()` — that also disables huggingface_hub's bars
  globally, which would take the catalog and model *download* progress with it.
- `Catalog.load(<parquet snapshot>).save(...)` no longer raises. The rows
  path was tracked in shadow state that only `attach_parquet_rows()` set;
  save now reads it from the attached backend, so load → save → load
  round-trips.

### Removed

- **BREAKING — `Catalog(default_field=...)` and `BroadSearchConfigError`.**
  Broad (no-`fields=`) search targets the `title` index by convention — every
  production caller passed `default_field="title"`, the exact value the
  fallback already resolved. Nothing validates a broad field at construction
  or build any more; a plain-text query on a catalog with no title index
  still raises `BroadSearchUnavailableError` at query time, and any other
  surface is declared per call with `fields=`. The persisted `default_field`
  snapshot key remains parsed and digest-checked (written as `null` by new
  saves), ignored at runtime.
- `parsimony/ranking.py` and the package exports `RRF`, `Ranker`, `Ranking`,
  `ZScoreFusion`, `MinMaxScoreFusion`; the `score_candidates()` / `ranking()`
  methods on index classes. Released `parsimony-sdmx` wheels import
  `ZScoreFusion` at module level — upgrade core and the sdmx package
  together (the paired connectors release floor-bumps its pin).

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
