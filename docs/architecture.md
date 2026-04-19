# parsimony Architecture

**Version**: 0.1.0  
**Audience**: Contributors, integrators, and developers who need to extend the library

This document describes the internal design of parsimony: the connector pattern and its three decorator variants, the catalog abstraction, the HTTP transport layer, the typed error hierarchy, and how the 24 connector modules are organized and composed.

---

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Module Organization](#module-organization)
3. [The Connector Pattern](#the-connector-pattern)
4. [Decorator Variants and Schema Contracts](#decorator-variants-and-schema-contracts)
5. [Dependency Injection Flow](#dependency-injection-flow)
6. [Result and Schema Layer](#result-and-schema-layer)
7. [Catalog Subsystem](#catalog-subsystem)
8. [HTTP Transport Layer](#http-transport-layer)
9. [Error Hierarchy](#error-hierarchy)
10. [Connector Implementations: 24 Modules](#connector-implementations-24-modules)
11. [Connector Composition and Injection](#connector-composition-and-injection)
12. [Data Flow Diagrams](#data-flow-diagrams)
13. [Dependency Graph](#dependency-graph)
14. [Key Design Decisions](#key-design-decisions)

---

## Design Philosophy

parsimony is built around three core principles:

**Uniform interface**. Every data source — whether it is FRED's REST API, an SDMX provider, or a central bank endpoint — exposes the same calling convention: an async function that accepts a Pydantic model and returns a typed DataFrame wrapped in a `Result`.

**Immutability**. The `Connector` and `Connectors` types are frozen dataclasses. Every operation that would modify them — binding dependencies, attaching callbacks, filtering — returns a new instance. The original is never mutated.

**Pluggable backends**. The catalog and data store are defined as abstract base classes (`BaseCatalog`, `DataStore`). The standard `parsimony.Catalog` (Parquet + FAISS + BM25 + RRF) is the bundled, production-grade implementation; custom backends subclass `BaseCatalog` directly.

---

## Module Organization

```
parsimony/
├── __init__.py               # Public API surface (__all__)
├── connector.py              # Connector, Connectors, decorators
├── errors.py                 # Typed error hierarchy (ConnectorError and subclasses)
├── result.py                 # Result, SemanticTableResult, OutputConfig, Column, ColumnRole
├── catalog/
│   ├── catalog.py            # BaseCatalog ABC + ingest/index_result orchestration
│   ├── models.py             # SeriesEntry (with embedding_text()), SeriesMatch, IndexResult
│   ├── embedder_info.py      # EmbedderInfo: persisted embedder identity
│   └── identity_from_params.py  # Namespace field extraction utilities
├── _standard/                # Private: the canonical Catalog implementation
│   ├── catalog.py            # Catalog: Parquet rows + FAISS vectors + BM25 + RRF
│   ├── embedder.py           # EmbeddingProvider protocol + SentenceTransformerEmbedder
│   ├── indexes.py            # FAISS / BM25 / RRF helpers
│   ├── meta.py               # CatalogMeta, BuildInfo, file constants
│   └── sources/              # URL-scheme dispatch: file://, hf://, s3:// (planned)
├── stores/
│   ├── data_store.py         # DataStore ABC, LoadResult
│   └── memory_data.py        # InMemoryDataStore
├── transport/
│   ├── http.py               # HttpClient wrapping httpx; API key log redaction
│   └── json_helpers.py       # json_to_df(), interpolate_path()
└── connectors/
    ├── __init__.py           # build_connectors_from_env(), PROVIDERS registry
    ├── fred.py               # 3 connectors: fred_search, fred, enumerate_fred_release
    ├── sdmx.py               # 5 connectors + enumerate_sdmx_dataset_codelists()
    ├── fmp.py                # 18 connectors
    ├── fmp_screener.py       # 1 connector: fmp_screener (fan-out pattern)
    ├── sec_edgar.py          # SEC filing connectors
    ├── eodhd.py              # EODHD market data connectors
    ├── polymarket.py         # 2 connectors: polymarket_gamma, polymarket_clob
    ├── finnhub.py            # Finnhub news/events connectors
    ├── coingecko.py          # CoinGecko crypto connectors
    ├── tiingo.py             # Tiingo market data connectors
    ├── alpha_vantage.py      # Alpha Vantage connectors
    ├── treasury.py           # US Treasury connectors
    ├── bls.py, eia.py, ...   # Government data (BLS, EIA, Destatis)
    ├── snb.py, rba.py, ...   # Central banks (SNB, RBA, Riksbank, BDE, BOJ, BOC, BDP, BDF)
    ├── financial_reports.py  # Financial Reports API connector
    └── mcp/
        └── server.py         # MCP server with typed error handling
```

The dependency graph is a directed acyclic graph. The two most widely imported modules are `result.py` and `catalog/models.py`. No circular dependencies exist.

---

## The Connector Pattern

The central abstraction is the `Connector` frozen dataclass defined in `connector.py`.

```python
@dataclass(frozen=True)
class Connector:
    name: str
    description: str
    tags: frozenset[str]
    param_type: type[BaseModel]       # Pydantic model for params validation
    dep_names: frozenset[str]         # Required keyword-only deps (must be bound)
    optional_dep_names: frozenset[str]# Optional keyword-only deps (may be absent)
    fn: Callable                      # The wrapped async function (partial after bind_deps)
    output_config: OutputConfig | None
    callbacks: tuple[ResultCallback, ...]
```

The following diagram shows the complete execution flow from decorator application through to the returned result.

```mermaid
graph TD
    classDef decorator fill:#4A90E2,stroke:#2E5C8A,color:#fff
    classDef process fill:#6C8EBF,stroke:#4A6A9F,color:#fff
    classDef result fill:#50C878,stroke:#2E7D50,color:#fff
    classDef decision fill:#F5A623,stroke:#C07B10,color:#fff

    A["@connector() / @enumerator() / @loader()"]:::decorator
    B["Inspect function signature"]:::process
    C["Extract param_type, dep_names,<br/>output_config, description"]:::process
    D["Replace function with Connector instance"]:::process
    E["connector(params_dict_or_model)"]:::process
    F{"OutputConfig<br/>present?"}:::decision
    G["Pydantic validates params"]:::process
    H["fn(params, bound_deps) called"]:::process
    I["HTTP request or SDK call"]:::process
    J["pd.DataFrame returned"]:::process
    K["Result wrapping"]:::process
    L["SemanticTableResult"]:::result
    M["Result"]:::result
    N["Callbacks fired"]:::process

    A --> B --> C --> D
    E --> G --> H --> I --> J --> K
    K --> F
    F -->|"Yes"| L
    F -->|"No"| M
    L --> N
    M --> N
```

When a developer writes a connector function using the `@connector()` decorator, the decorator inspects the function signature to extract:

- `param_type`: the Pydantic `BaseModel` subclass that is the first positional parameter.
- `dep_names`: keyword-only parameters (after `*`) that have no default value and are not yet bound.
- `output_config`: taken from the `output=` argument to the decorator.
- `description`: from the function docstring.

The decorator replaces the function with a `Connector` instance. Calling the `Connector` with a params dict or model instance triggers Pydantic validation, calls the wrapped function, and wraps the return value in a `Result` or `SemanticTableResult`.

### Immutability pattern

All mutation operations on `Connector` return a new instance:

```python
# bind_deps returns a new Connector with fn replaced by a partial
bound = connector.bind_deps(api_key="secret")

# with_callback returns a new Connector with callbacks extended
logged = connector.with_callback(my_callback)

# Original connector is unchanged
assert connector.dep_names == frozenset({"api_key"})
assert bound.dep_names == frozenset()
```

`Connectors` follows the same pattern. The `+` operator, `.filter()`, `.bind_deps()`, and `.with_callback()` all return new `Connectors` instances.

---

## Decorator Variants and Schema Contracts

Three decorator factories are provided, each enforcing a different column-role contract on the `OutputConfig`.

| Decorator | KEY required | TITLE required | DATA allowed | METADATA allowed | Primary use case |
|-----------|:-----------:|:--------------:|:------------:|:----------------:|-----------------|
| `@connector()` | No | No | Yes | Yes | General search, profile, fetch |
| `@enumerator(output)` | Yes (with namespace) | Yes | No | Yes | Catalog population: list series IDs |
| `@loader(output)` | Yes (with namespace) | No | Yes | No | Observation loading: time series data |

The `output` parameter is mandatory for `@enumerator` and `@loader`; it is optional for `@connector`. When `output` is present, the connector automatically wraps its return value in a `SemanticTableResult` instead of a plain `Result`.

```python
# @enumerator enforces KEY + TITLE, no DATA
@enumerator(output=OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY,   dtype="str", namespace="fred"),
    Column(name="title",     role=ColumnRole.TITLE, dtype="str"),
]))
async def enumerate_fred_release(params: FredReleaseParams, *, api_key: str) -> pd.DataFrame:
    ...

# @loader enforces KEY + DATA, no TITLE/METADATA
@loader(output=OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY,  dtype="str", namespace="fred"),
    Column(name="date",      role=ColumnRole.DATA, dtype="date"),
    Column(name="value",     role=ColumnRole.DATA, dtype="float64"),
]))
async def fred(params: FredFetchParams, *, api_key: str) -> pd.DataFrame:
    ...
```

The schema contracts exist to make catalog indexing and data loading reliable. An `@enumerator` result can always be safely passed to `Catalog.index_result()` because it is guaranteed to have identifiable series codes. A `@loader` result can always be safely passed to `DataStore.load_result()` because it is guaranteed to have data columns.

---

## Dependency Injection Flow

The dependency injection mechanism allows connector functions to declare API keys or other runtime dependencies as keyword-only parameters, without coupling the connector to any specific credential store.

```
connector_fn(params, *, api_key: str)     # declares dep: api_key
    ↓
Connector(dep_names=frozenset({"api_key"}), fn=connector_fn)
    ↓ .bind_deps(api_key=os.getenv("FRED_API_KEY"))
Connector(dep_names=frozenset(), fn=partial(connector_fn, api_key=...))
    ↓ connector({"series_id": "GDP"})
1. Pydantic validates params dict → FredFetchParams(series_id="GDP")
2. fn(params, **bound_deps) called
3. Return value wrapped in Result or SemanticTableResult
4. Callbacks fired with the Result
    ↓
Result returned to caller
```

Dependencies bound via `bind_deps()` become part of the function partial. They are never stored in `Provenance` (which only contains the Pydantic params model fields). This ensures API keys do not appear in lineage records, logs, or serialized results.

The factory function `build_connectors_from_env()` calls `bind_deps()` internally:

```python
# Internal pattern used by factory functions
fred_connectors = Connectors([fred_search, fred, enumerate_fred_release])
fred_bound = fred_connectors.bind_deps(api_key=os.environ["FRED_API_KEY"])
```

---

## Result and Schema Layer

The `result.py` module defines the output contract for all connectors. It is a standalone module with no internal dependencies beyond pandas and pyarrow.

```
Raw API response (JSON/CSV)
    ↓ connector implementation
pd.DataFrame
    ↓ Connector.__call__()
Result(data=df, provenance=Provenance(...))
    ↓ (when OutputConfig is present)
SemanticTableResult(data=df, provenance=..., output_schema=OutputConfig(...))
```

`SemanticTableResult` extends `Result` with a required `output_schema`. It exposes typed column groups:

- `entity_keys`: `ColumnRole.KEY` columns (series identifiers with namespace)
- `data_columns`: `ColumnRole.DATA` columns (numeric observations)
- `metadata_columns`: `ColumnRole.METADATA` columns (ancillary context)

Both types support Arrow and Parquet serialization. The `OutputConfig` schema is stored in Arrow table metadata, enabling schema recovery on deserialization.

If a connector returns a plain `Result` but the caller needs schema information, they can call `result.to_table(output_config)` to produce a `SemanticTableResult` without re-fetching.

---

## Catalog Subsystem

The catalog subsystem manages the lifecycle of series metadata: ingestion, deduplication, embedding, hybrid search, and distribution.

### `BaseCatalog`: the contract (`parsimony/catalog/catalog.py`)

`BaseCatalog` is the ABC every catalog implements. It defines two layers:

* **Abstract** — `upsert`, `get`, `exists`, `delete`, `search`, `list`, `list_namespaces`. Implementations own the storage layout *and* the embedder, because index-time and query-time embeddings must come from the same model.
* **Concrete orchestration** — `ingest` (dedupe + batched upsert with `dry_run` / `force`) and `index_result` (extracts `SeriesEntry` rows from a `SemanticTableResult` and calls `ingest`).

| Component | Responsibility |
|-----------|---------------|
| `BaseCatalog` | ABC for catalogs; ingest/index_result orchestration |
| `SeriesEntry` | Pydantic record with `embedding_text()` for index/query text composition |
| `SeriesMatch` / `IndexResult` | Search hits and ingest summaries |
| `EmbedderInfo` | Persisted embedder identity (model, dim, normalize); used by `Catalog.save`/`load` |
| `identity_from_params` | Extracts `(namespace, code)` from a params model via `Namespace` annotation |

### `Catalog`: the standard implementation (`parsimony._standard/`)

The framework ships exactly one production-grade implementation, exposed at the top level as `parsimony.Catalog`. It is gated behind the `parsimony-core[standard]` extra (FAISS, BM25, sentence-transformers, huggingface-hub) and lazy-loaded so `import parsimony` stays cheap.

* **Storage** — three-file directory snapshot: `meta.json` (`CatalogMeta` + `EmbedderInfo`), `entries.parquet` (zstd-compressed rows + embeddings), `embeddings.faiss` (FAISS index).
* **Indices** — Flat IP for small catalogs, HNSW above 4 096 rows; BM25Okapi rebuilt in-memory from parquet on load (kept out of the on-disk contract for forward compatibility).
* **Search** — pulls candidates from BM25 and FAISS independently, fuses via Reciprocal Rank Fusion (`k = 60`), filters by namespace afterwards.
* **Embedder** — owned by the catalog. Default is `SentenceTransformerEmbedder("BAAI/bge-small-en-v1.5")`; pass `embedder=...` to override. Two implementations ship: `SentenceTransformerEmbedder` (local, `parsimony-core[standard]`) and `LiteLLMEmbeddingProvider` (hosted API, `parsimony-core[litellm]`). `EmbeddingProvider` is the public contract for custom backends; there is no entry-point plugin axis.
* **Distribution** — `Catalog.from_url(...)` and `Catalog.push(url)` dispatch on URL scheme to `parsimony._standard/sources/`: `file://`, `hf://`, `s3://` (planned). Schemes are resolved in-process; not a plugin axis.

### Indexing pipeline

When `catalog.index_result(table)` is called:

1. `_entries_from_table_result` reads the KEY column's `Namespace` annotation and assembles one `SeriesEntry` per unique key, copying `title`, `tags` (provenance source + `extra_tags`), and METADATA columns.
2. `ingest` batches entries (default 100), calls `exists()` to dedupe (skipped unless `force=True`), and `upsert()`s the survivors.
3. `Catalog.upsert()` calls `embedder.embed_texts(entry.embedding_text())` for any entry without a precomputed vector and rebuilds the FAISS + BM25 indices.
4. Returns an `IndexResult(total, indexed, skipped, errors)`. With `dry_run=True`, the dedupe pass runs but nothing is written.

### Identity resolution

The `Namespace` metadata annotation on a Pydantic field is the bridge between connector params and catalog identity:

```python
class FredFetchParams(BaseModel):
    series_id: Annotated[str, Namespace("fred")]
```

Exactly one `Namespace`-annotated field may exist per params model.

### Search routing

`Catalog.search(query, limit, namespaces=None)` runs BM25 and FAISS in parallel (each over a wider candidate pool than `limit`), fuses via RRF, then applies the optional namespace filter. There is no `semantic=` flag — hybrid retrieval is the only mode, because owning both retrievers is what makes the embedder–catalog contract enforceable.

### Custom backends

Subclass `BaseCatalog` directly when the standard layout is wrong (Postgres+pgvector, OpenSearch, an in-memory mock for tests, …). Override the seven abstract methods; you inherit `ingest` and `index_result` for free. Override `from_url` / `push` if your backend needs URL-based loading.

---

## HTTP Transport Layer

The `transport/` directory provides two utilities used by connector implementations.

### `HttpClient` (`transport/http.py`)

A thin wrapper around `httpx.AsyncClient`. Connectors instantiate `HttpClient` with a base URL and optional default query parameters (including the API key).

**Key behaviors**:

- A new `httpx.AsyncClient` is created per request. This avoids event loop sharing issues that arise when callers use multiple `asyncio.run()` calls in sequence, at the cost of connection setup overhead per request.
- Query parameter values whose names appear in `_SENSITIVE_QUERY_PARAM_NAMES` are redacted to `"***REDACTED***"` in structured HTTP logs.
- The redacted names include: `api_key`, `apikey`, `token`, `access_token`, `refresh_token`, `id_token`, `client_secret`, `secret`, `password`, `authorization`, and any name ending in `_token`.

If a new connector introduces an API key with a non-standard query parameter name, that name must be added to `_SENSITIVE_QUERY_PARAM_NAMES`.

### `json_helpers.py`

Two utility functions used by FMP and Polymarket connectors:

- `json_to_df(data, ...)`: converts a JSON response body (list of dicts, indexed dict, or date-keyed dict) to a pandas DataFrame. Handles nested dict detection and `TableRef` references for nested content.
- `interpolate_path(path_template, params)`: substitutes path parameters from a params model into a URL path template (e.g. `"/profile/{symbol}"` with `FmpCompanyProfileParams(symbol="AAPL")` → `"/profile/AAPL"`).

---

## Error Hierarchy

The `errors.py` module defines typed exceptions for connector operational errors. These are distinct from programmer errors (which remain `TypeError`, `ValueError`, or Pydantic `ValidationError`).

```
ConnectorError(provider: str)
├── UnauthorizedError      (401/403 — bad credentials)
├── PaymentRequiredError   (402 — plan restriction)
├── RateLimitError         (429 — burst or quota)
│   ├── retry_after: float
│   └── quota_exhausted: bool
├── ProviderError          (5xx / unexpected status)
├── EmptyDataError         (200 but no rows)
└── ParseError             (200 but unparseable)
```

Every subclass carries a `provider: str` attribute so callers can identify the source without parsing message strings. The MCP server maps these to appropriate MCP error responses. `RateLimitError` distinguishes between burst limits (retryable after `retry_after` seconds) and quota exhaustion (terminal, do not retry).

---

## Connector Implementations: 24 Modules

Each data-source module in `connectors/` follows the same structure:

1. Param models (one `BaseModel` subclass per connector).
2. One or more `OutputConfig` constants defining the column schema.
3. Connector functions decorated with `@connector`, `@enumerator`, or `@loader`.
4. A module-level `CONNECTORS` constant (a `list` or `Connectors` instance) and optionally `ENV_VARS`, used by the `PROVIDERS` registry.

### Module summary

| Module | Category | Key dependency |
|--------|----------|---------------|
| `fred.py` | Public (macro) | `FRED_API_KEY` + httpx |
| `sdmx.py` | Public (multi-agency) | `sdmx1` package |
| `treasury.py` | Public (US fiscal) | httpx only |
| `bls.py`, `eia.py`, `destatis.py` | Public (government stats) | httpx (keys optional/varied) |
| `boe.py`, `riksbank.py`, `snb.py`, `rba.py` | Public (central banks) | httpx only |
| `bde.py`, `boc.py`, `boj.py`, `bdf.py`, `bdp.py` | Public (central banks) | httpx (some need keys) |
| `fmp.py` | Commercial | `FMP_API_KEY` + httpx |
| `fmp_screener.py` | Commercial | `FMP_API_KEY` + httpx (fan-out) |
| `alpha_vantage.py` | Commercial | `ALPHA_VANTAGE_API_KEY` + httpx |
| `coingecko.py` | Commercial (crypto) | `COINGECKO_API_KEY` + httpx |
| `eodhd.py` | Commercial | `EODHD_API_KEY` + httpx |
| `finnhub.py` | Commercial | `FINNHUB_API_KEY` + httpx |
| `tiingo.py` | Commercial | `TIINGO_API_KEY` + httpx |
| `sec_edgar.py` | Commercial (filings) | `edgartools` (sync, wrapped) |
| `polymarket.py` | Commercial (predictions) | httpx only |
| `financial_reports.py` | Commercial | `FINANCIAL_REPORTS_API_KEY` + SDK |

### Async transport strategies per module

- **FRED, FMP, EODHD, Polymarket, Finnhub, CoinGecko, Tiingo, Alpha Vantage, central banks**: use `HttpClient` (httpx-based async).
- **SDMX**: uses the `sdmx1` library's own async transport; `sdmx` yields a `pandasdmx` DataMessage.
- **SEC Edgar**: uses `edgartools`, a synchronous library. The connector wraps it in `_SecEdgarEngine`, a dispatch class that runs synchronous calls.
- **Financial Reports**: uses the `financial-reports-generated-client` SDK, which provides its own async client.

### FMP Screener fan-out pattern

`fmp_screener` performs three concurrent API calls for each batch of screener results:

1. `/company-screener` — primary filter results.
2. `/key-metrics-ttm/{symbol}` — per-symbol key metrics (concurrent, semaphore-limited to 10).
3. `/financial-ratios-ttm/{symbol}` — per-symbol financial ratios (concurrent, semaphore-limited to 10).

The semaphore limit (`_SEMAPHORE_LIMIT = 10`) prevents overwhelming the FMP rate limit. Results are merged by symbol. If the `where_clause` parameter is set, it is applied as a `DataFrame.query()` filter on the merged DataFrame.

---

## Connector Composition and Injection

A single factory in `connectors/__init__.py` composes the per-module connector lists into one bundle and binds API keys:

```
connectors/__init__.py
    PROVIDERS = (ProviderSpec(...), ...)   # declarative registry of provider modules

    build_connectors_from_env(*, env, lenient=False)
        → iterates PROVIDERS
        → reads env vars (FRED_API_KEY, FMP_API_KEY, EODHD_API_KEY, etc.)
        → for each provider whose env vars are present: binds keys to the module's CONNECTORS
        → concatenates all bound bundles into one Connectors instance
        → returns Connectors
```

`FRED_API_KEY` is required by default; pass `lenient=True` to skip it when missing. All other credentialed providers are optional — if their env vars are absent, they are silently excluded. Consumers needing only the MCP/search surface filter with `connectors.filter(tags=["tool"])`.

Connectors for SDMX (no key needed), SEC Edgar (no key needed), and Polymarket (no key needed) are always included in `build_connectors_from_env()` when their respective package dependencies are installed.

---

## Data Flow Diagrams

The following narrative describes the data flow for a typical connector call.

**Fetch call flow**:

```
Caller
  → connectors["fred"]({"series_id": "GDP"})
  → Connector.__call__()
      → Pydantic: validate dict → FredFetchParams(series_id="GDP")
      → fn(params, api_key="...") called
          → HttpClient.get("/series/observations", params={...})
          → JSON response parsed to pd.DataFrame
          → DataFrame returned
      → Result.from_dataframe(df, Provenance(...)) created
      → SemanticTableResult wrapping applied (OutputConfig present)
      → callbacks fired
  → SemanticTableResult returned to caller
```

The diagram below maps this narrative to the concrete types involved at each step.

```mermaid
graph LR
    classDef caller fill:#4A90E2,stroke:#2E5C8A,color:#fff
    classDef connector fill:#6C8EBF,stroke:#4A6A9F,color:#fff
    classDef transport fill:#9B6B9B,stroke:#7A4A7A,color:#fff
    classDef result fill:#50C878,stroke:#2E7D50,color:#fff

    A["Caller\nparams dict"]:::caller
    B["Connector.__call__()"]:::connector
    C["Pydantic validation\nFredFetchParams"]:::connector
    D["fn(params, api_key=...)"]:::connector
    E["HttpClient.get()"]:::transport
    F["JSON → pd.DataFrame"]:::transport
    G["Result / SemanticTableResult"]:::result
    H["Callbacks fired"]:::result

    A --> B --> C --> D --> E --> F --> G --> H
```

**Catalog indexing flow**:

```
SemanticTableResult (from connector)
  → Catalog.index_result(result)
      → KEY columns extracted from output_schema
      → Namespace annotation read: namespace="fred"
      → Each row → SeriesEntry(namespace="fred", code="GDP", title="...", ...)
      → CatalogStore.exists() checked per entry (unless force=True)
      → build_embedding_text(entry) called
      → EmbeddingProvider.embed(texts) called (if embed=True)
      → CatalogStore.upsert(entries) called
  → IndexResult(total, indexed, skipped, errors) returned
```

The diagram below shows the catalog indexing pipeline from a `SemanticTableResult` through to the persisted `IndexResult`.

```mermaid
graph TD
    classDef input fill:#4A90E2,stroke:#2E5C8A,color:#fff
    classDef pipeline fill:#6C8EBF,stroke:#4A6A9F,color:#fff
    classDef store fill:#9B6B9B,stroke:#7A4A7A,color:#fff
    classDef output fill:#50C878,stroke:#2E7D50,color:#fff
    classDef decision fill:#F5A623,stroke:#C07B10,color:#fff

    A["SemanticTableResult"]:::input
    B["Extract KEY columns\nfrom output_schema"]:::pipeline
    C["Read Namespace annotation\nnamespace + code per row"]:::pipeline
    D["Convert rows to SeriesEntry\nobjects"]:::pipeline
    E{"force=False?\nCheck CatalogStore.exists()"}:::decision
    F["Skip unchanged entries"]:::pipeline
    G["build_embedding_text(entry)\ntitle + tags + description"]:::pipeline
    H{"embed=True and\nEmbeddingProvider set?"}:::decision
    I["EmbeddingProvider.embed(texts)\nbatch call"]:::store
    J["CatalogStore.upsert(entries)"]:::store
    K["IndexResult\ntotal / indexed / skipped / errors"]:::output

    A --> B --> C --> D --> E
    E -->|"exists"| F
    E -->|"new or changed"| G
    G --> H
    H -->|"Yes"| I --> J
    H -->|"No"| J
    J --> K
```

---

## Dependency Graph

The internal dependency structure (simplified, showing import direction):

```
result.py           ← (standalone: pandas, pyarrow)
errors.py           ← (standalone: no internal deps)
catalog/models.py   ← (standalone: pydantic)

connector.py        ← result.py, errors.py
catalog/store.py    ← catalog/models.py
catalog/catalog.py  ← catalog/store.py, catalog/models.py, catalog/embeddings.py, result.py
data_store.py       ← catalog/models.py, result.py

stores/memory.py         ← catalog/models.py, catalog/store.py
stores/memory_data.py    ← catalog/models.py, data_store.py
embeddings/litellm.py    ← catalog/embeddings.py, litellm (optional)

transport/http.py        ← httpx
transport/json_helpers.py← pandas

connectors/fred.py       ← connector.py, result.py, transport/http.py
connectors/sdmx.py       ← connector.py, result.py (sdmx1 optional)
connectors/fmp.py        ← connector.py, result.py, transport/http.py, transport/json_helpers.py
connectors/__init__.py   ← all connector modules
```

No circular dependencies. `result.py` and `catalog/models.py` have the highest in-degree (most modules depend on them). Changes to these two files have the widest blast radius.

---

## Key Design Decisions

### Why frozen dataclasses for Connector and Connectors?

Immutability makes connector bundles safe to share across coroutines without locks. It also makes the `bind_deps()` and `with_callback()` patterns composable: you can create variations of a bundle without affecting other parts of the codebase that hold references to the original.

### Why create a new httpx.AsyncClient per request?

A shared `AsyncClient` across multiple `asyncio.run()` calls (each of which creates a new event loop) would raise `RuntimeError: Event loop is closed`. The per-request client avoids this at the cost of TCP connection setup overhead. The comment in `transport/http.py` documents this rationale explicitly.

### Why separate Result from SemanticTableResult?

Not all connectors have fully declared schemas. Keeping `Result` as the base type allows connectors to return raw DataFrames without requiring every implementer to write a complete `OutputConfig`. `SemanticTableResult` is opt-in and adds semantic meaning when needed (catalog indexing, agent tool generation, column filtering).

### Why are dep_names and optional_dep_names separate?

`dep_names` must be bound before the connector is callable. `optional_dep_names` may be absent at call time without error. This distinction allows connectors that have optional credentials (e.g. `SEC_EDGAR_USER_AGENT`) to express that the dep is present-if-available rather than required.

### Why is the Supabase backend external to this package?

parsimony is a client library. It does not manage any schema or runtime infrastructure. The `CatalogStore` and `DataStore` ABCs define the contracts; production backends are injected by the consuming application. This keeps the package self-contained and testable with in-memory implementations.

### Why separate MCP tools from client data connectors?

parsimony draws a hard line between two access paths to the same data sources:

| | MCP Tools (search/discovery) | Client Connectors (fetch/load) |
|---|---|---|
| **Tagged** | `"tool"` | No `"tool"` tag |
| **Caller** | Agent via MCP protocol | Agent-written Python code |
| **Result size** | Small — fits comfortably in a context window | Large — full datasets, thousands of rows |
| **Purpose** | Figure out *what* to fetch | Fetch the actual data |
| **Examples** | `fred_search`, `sdmx_dsd`, `fmp_screener` | `fred`, `sdmx`, `eodhd_eod` |

**The core problem is context window economics.** When an agent calls an MCP tool, the result is injected into its context window alongside the system prompt, conversation history, and reasoning. A 10,000-row DataFrame returned as an MCP tool response would consume most of the context budget and crowd out the agent's ability to reason about the data. Worse, the agent doesn't need all that data in its context — it needs it in a variable it can manipulate with code.

**The workflow this enables:** discover → fetch → analyze.

1. **Discover** (MCP tool): the agent calls `fred_search("unemployment rate")` as an MCP tool. It receives a compact table of series metadata — IDs, titles, frequencies — that fits in a few hundred tokens.
2. **Fetch** (client code): the agent writes and executes Python code that calls `await client['fred'](series_id='UNRATE')`. The resulting DataFrame lands in the code execution environment, not in the context window.
3. **Analyze** (client code): the agent operates on the DataFrame programmatically — computing statistics, plotting, joining with other data — and only surfaces the conclusions back into the conversation.

This mirrors a broader principle in the MCP ecosystem: **MCP is for point-in-time, scoped answers; APIs (and client libraries) are for bulk data.** They are complements, not competitors. The MCP tool gives the agent a quick, context-friendly answer to "what data exists?" The client connector gives it the full dataset to work with — through code, not through the context window.

**Implementation.** The `"tool"` tag is the only mechanism. Connectors tagged `"tool"` are filtered by the MCP server at startup (`connectors.filter(tags=["tool"])`) and registered as MCP tools. All other connectors remain accessible via `from parsimony import client` for programmatic use. Adding or removing `"tool"` from a connector's tags is the single switch that moves it between the two access paths.
