# parsimony API Reference

**Version**: 0.1.0  
**License**: Apache-2.0  
**Python**: 3.11 – 3.12

This reference covers every public symbol exported by `parsimony`, all 33 connector functions across 9 data-source modules, the catalog API, the factory functions, and all public types.

---

## Table of Contents

1. [Core Connector Framework](#core-connector-framework)
2. [Schema and Output Types](#schema-and-output-types)
3. [Catalog API](#catalog-api)
4. [Data Store API](#data-store-api)
5. [In-Memory Implementations](#in-memory-implementations)
6. [Embedding Providers](#embedding-providers)
7. [Utility Functions](#utility-functions)
8. [Factory Functions](#factory-functions)
9. [Connector Inventory](#connector-inventory)
   - [FRED Connectors](#fred-connectors)
   - [SDMX Connectors](#sdmx-connectors)
   - [FMP Connectors](#fmp-connectors)
   - [FMP Screener Connector](#fmp-screener-connector)
   - [SEC Edgar Connector](#sec-edgar-connector)
   - [EODHD Connector](#eodhd-connector)
   - [Polymarket Connectors](#polymarket-connectors)
   - [Financial Reports Connector](#financial-reports-connector)

---

## Core Connector Framework

Import from `parsimony`:

```python
from parsimony import Connector, Connectors, connector, enumerator, loader
```

### `Connector`

A frozen dataclass wrapping a single async data-fetching function.

```python
@dataclass(frozen=True)
class Connector:
    name: str
    description: str
    tags: frozenset[str]
    param_type: type[BaseModel]
    dep_names: frozenset[str]
    optional_dep_names: frozenset[str]
    fn: Callable
    output_config: OutputConfig | None
    callbacks: tuple[ResultCallback, ...]
```

**Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `__call__` | `async (params_dict_or_model, **kwargs) -> Result` | Validate params, inject deps, call fn, fire callbacks |
| `bind_deps` | `(self, **deps) -> Connector` | Return new `Connector` with dependencies pre-bound; returns self if no matching dep_names |
| `with_callback` | `(self, cb: ResultCallback) -> Connector` | Return new `Connector` with callback appended |
| `to_llm` | `(self) -> str` | Serialize connector as a plain-text tool description for LLM prompts |
| `describe` | `(self) -> str` | Return a short human-readable description |

**Notes**:
- All mutation methods return a **new** `Connector` instance; the original is not modified.
- `dep_names` are required keyword-only function arguments that must be bound before the connector is callable. `optional_dep_names` may be absent at call time.
- Params are validated via Pydantic v2 at every `__call__` invocation.

### `Connectors`

An immutable ordered collection of `Connector` instances.

```python
from parsimony import Connectors
```

**Constructor**: `Connectors(connectors: Sequence[Connector])`

**Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `__add__` | `(self, other: Connectors) -> Connectors` | Combine two bundles; returns new `Connectors` |
| `filter` | `(self, tags: Iterable[str]) -> Connectors` | Return new bundle containing only connectors whose tags intersect the given set |
| `bind_deps` | `(self, **deps) -> Connectors` | Apply `bind_deps` to every connector; returns new `Connectors` |
| `with_callback` | `(self, cb: ResultCallback) -> Connectors` | Apply `with_callback` to every connector; returns new `Connectors` |
| `to_llm` | `(self) -> str` | Concatenate all connector tool descriptions into one prompt block |
| `describe` | `(self) -> str` | Multi-line summary of all connectors |
| `__iter__` | `(self) -> Iterator[Connector]` | Iterate over contained connectors |
| `__len__` | `(self) -> int` | Number of connectors |
| `__getitem__` | `(self, name: str) -> Connector` | Look up a connector by name |

### `connector` decorator

```python
from parsimony import connector

connector(
    name: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    output: OutputConfig | None = None,
) -> Callable[[AsyncFunc], Connector]
```

Decorator factory for **general-purpose** connectors. The decorated function must be `async def` and accept a single Pydantic `BaseModel` as its first positional argument.

- `name`: overrides the function name as the connector's identifier.
- `description`: overrides the docstring as the connector's description.
- `tags`: list of string tags for filtering via `Connectors.filter()`.
- `output`: if provided, the return value is wrapped in `SemanticTableResult`; otherwise wrapped in `Result`.

**Schema contract**: no restrictions on `ColumnRole` usage.

### `enumerator` decorator

```python
from parsimony import enumerator

enumerator(output: OutputConfig) -> Callable[[AsyncFunc], Connector]
```

Decorator for **catalog-population** connectors. Enforces that the `OutputConfig` contains exactly one KEY column and at least one TITLE column. DATA columns are not permitted.

Use `enumerator` for functions that list series identifiers for later catalog ingestion.

### `loader` decorator

```python
from parsimony import loader

loader(output: OutputConfig) -> Callable[[AsyncFunc], Connector]
```

Decorator for **observation-loading** connectors. Enforces that the `OutputConfig` contains at least one KEY column and at least one DATA column. TITLE and METADATA columns are not permitted.

Use `loader` for functions that fetch time-series observations for a specific identifier.

### `ResultCallback`

```python
ResultCallback = Callable[[Result], Awaitable[None] | None]
```

Type alias for callbacks attached via `Connector.with_callback()`. Callbacks receive the `Result` after a connector call completes. They may be sync or async.

---

## Schema and Output Types

```python
from parsimony import (
    Namespace, Column, ColumnRole, OutputConfig,
    Provenance, Result, SemanticTableResult,
)
```

### `Namespace`

```python
Namespace(name: str)
```

Pydantic field metadata annotation that links a param model field to a catalog namespace. Used with `Annotated`:

```python
from typing import Annotated
from parsimony import Namespace

class FredFetchParams(BaseModel):
    series_id: Annotated[str, Namespace("fred")]
```

A connector whose param model has a `Namespace`-annotated field supports automatic `(namespace, code)` identity extraction for catalog indexing.

### `ColumnRole`

```python
from parsimony import ColumnRole

class ColumnRole(str, Enum):
    KEY      = "key"       # Series identifier (namespace + code)
    DATA     = "data"      # Numeric observation values
    TITLE    = "title"     # Human-readable label
    METADATA = "metadata"  # Ancillary context; excluded from data analysis
```

### `Column`

```python
Column(
    name: str,
    role: ColumnRole,
    dtype: str,
    mapped_name: str | None = None,
    param_key: str | None = None,
    description: str | None = None,
    exclude_from_llm_view: bool = False,
    namespace: str | None = None,
)
```

Describes a single column in a connector's output DataFrame. Applied via `OutputConfig`.

- `name`: column name in the output DataFrame.
- `role`: semantic role of the column.
- `mapped_name`: optional alias used when the raw API response column has a different name.
- `param_key`: links this column back to a param field (used in KEY columns).
- `namespace`: namespace string for KEY columns; enables catalog identity resolution.

### `OutputConfig`

```python
OutputConfig(columns: list[Column])
```

Declares the full schema for a connector's output. Passed to the `output` argument of `@connector()`, `@enumerator()`, or `@loader()`. When an `OutputConfig` is present, the connector returns `SemanticTableResult`; otherwise it returns `Result`.

### `Provenance`

```python
Provenance(
    source: str,
    source_description: str | None = None,
    params: dict,
    fetched_at: datetime,
    title: str | None = None,
    properties: dict | None = None,
)
```

Immutable data lineage record attached to every `Result`. `params` holds the serialized Pydantic params model; keyword-only dependency arguments (API keys) are never included.

### `Result`

```python
Result(
    data: pd.DataFrame,
    provenance: Provenance,
    output_schema: OutputConfig | None = None,
)
```

Base result type returned by any connector.

**Class methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `from_dataframe` | `(df, provenance) -> Result` | Wrap a raw DataFrame |
| `from_arrow` | `(table: pa.Table) -> Result` | Deserialize from Arrow table |
| `from_parquet` | `(path: str \| Path) -> Result` | Deserialize from Parquet file |

**Instance methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `to_table` | `(output_config: OutputConfig) -> SemanticTableResult` | Promote to `SemanticTableResult` with the given schema |
| `to_arrow` | `() -> pa.Table` | Serialize to Arrow table (schema stored in metadata) |
| `to_parquet` | `(path: str \| Path) -> None` | Write to Parquet file |

### `SemanticTableResult`

```python
SemanticTableResult(
    data: pd.DataFrame,
    provenance: Provenance,
    output_schema: OutputConfig,   # required (not optional)
)
```

Subclass of `Result` with a required `OutputConfig`. Produced by connectors decorated with an `output=` argument, or by calling `result.to_table(config)`.

**Properties**:

| Property | Type | Description |
|----------|------|-------------|
| `entity_keys` | `pd.DataFrame` | DataFrame subset containing KEY columns |
| `data_columns` | `list[Column]` | Columns with `role == DATA` |
| `metadata_columns` | `list[Column]` | Columns with `role == METADATA` |

All `Result` serialization methods (`to_arrow`, `to_parquet`, `from_arrow`, `from_parquet`) are inherited and work with the full schema.

---

## Catalog API

```python
from parsimony import (
    Catalog, CatalogStore, SeriesEntry, SeriesMatch,
    IndexResult, EmbeddingProvider,
)
from parsimony.stores import HFBundleCatalogStore, SQLiteCatalogStore
from parsimony.embeddings.sentence_transformers import SentenceTransformersEmbeddingProvider
```

### `Catalog`

```python
Catalog(
    store: CatalogStore,
    *,
    embeddings: EmbeddingProvider | None = None,
    connectors: Any | None = None,
)
```

Orchestration layer over a `CatalogStore`. Lazy-populates requested
namespaces (via the store's `try_load_remote`, falling back to a live
`@enumerator` when `connectors` is supplied) and dispatches search.

**Methods**:

| Method | Signature | Description |
|---|---|---|
| `search` | `async (query: str, limit=10, *, namespaces: list[str]) -> list[SeriesMatch]` | Vector search scoped to the given namespaces (required, non-empty) |
| `index_result` | `async (result: SemanticTableResult, *, embed=True, batch_size=100, extra_tags=None, dry_run=False, force=False) -> IndexResult` | Extract entries from a result and ingest them |
| `ingest` | `async (entries: list[SeriesEntry], *, embed=True, batch_size=100, force=False) -> IndexResult` | Dedupe, optionally embed, and upsert pre-built entries |
| `embed_pending` | `async (limit: int \| None = None, *, namespace: str \| None = None) -> int` | Backfill embeddings for entries that don't have them |
| `list_namespaces` | `async () -> list[str]` | All loaded namespaces |
| `list_entries` | `async (*, namespace=None, q=None, limit=50, offset=0) -> tuple[list[SeriesEntry], int]` | Paginated listing with optional substring filter |
| `get_entry` | `async (namespace: str, code: str) -> SeriesEntry \| None` | Retrieve one entry |
| `delete_entry` | `async (namespace: str, code: str) -> None` | Delete one entry (store must support writes) |
| `upsert_entries` | `async (entries: list[SeriesEntry]) -> None` | Bulk upsert (store must support writes) |
| `refresh` | `async (namespace: str) -> dict` | Force a fresh HEAD check and reload (store must support it) |

### `CatalogStore`

Abstract base class for catalog persistence backends.

**Abstract methods**:

| Method | Signature | Description |
|---|---|---|
| `upsert` | `async (entries: list[SeriesEntry]) -> None` | Insert or update entries (read-only stores raise `NotImplementedError`) |
| `get` | `async (namespace: str, code: str) -> SeriesEntry \| None` | Retrieve a single entry |
| `delete` | `async (namespace: str, code: str) -> None` | Remove an entry |
| `exists` | `async (keys: list[tuple[str, str]]) -> set[tuple[str, str]]` | Return the subset of `(namespace, code)` pairs that already exist |
| `search` | `async (query: str, limit: int, *, namespaces: list[str] \| None = None, query_embedding: list[float] \| None = None) -> list[SeriesMatch]` | Vector or keyword search |
| `list_namespaces` | `async () -> list[str]` | Distinct namespaces |
| `list` | `async (*, namespace=None, q=None, limit=50, offset=0) -> tuple[list[SeriesEntry], int]` | Paginated listing |

**Optional hook**:

| Method | Description |
|---|---|
| `try_load_remote` | Attempt to populate a namespace from a remote source (HF bundle stores override this; default returns `False`). |

### `HFBundleCatalogStore`

Read-only store backed by Parquet + FAISS bundles on HuggingFace Hub.

```python
HFBundleCatalogStore(
    *,
    embeddings: EmbeddingProvider,
    cache_dir: Path | str | None = None,
    pin: str | None = None,
    repo_id_for_namespace: Callable | None = None,
)
```

| Parameter | Description |
|---|---|
| `embeddings` | Provider whose `model_id` / `revision` / `dimension` must match every loaded manifest |
| `cache_dir` | On-disk cache root; defaults to `PARSIMONY_CACHE_DIR` or a platformdirs user-cache path |
| `pin` | 40-char HF commit SHA pinning every namespace; overrides `PARSIMONY_CATALOG_PIN` |
| `repo_id_for_namespace` | Override for the HF repo id (default: `parsimony-dev/<namespace>`) |

`upsert` and `delete` raise `NotImplementedError` — bundles are
published by rebuilding. `refresh(namespace)` returns a dict
`{namespace, updated, old_revision, new_revision}`. `status()` returns a
dict snapshot of the cache and loaded bundles.

### `SQLiteCatalogStore`

Local FTS5 + vec0 store for custom catalogs ([search] extra).

```python
SQLiteCatalogStore(path: str | Path)
```

### `SeriesEntry`

```python
SeriesEntry(
    namespace: str,
    code: str,
    title: str,
    tags: list[str] = [],
    description: str | None = None,
    metadata: dict = {},
    properties: dict = {},
    embedding: list[float] | None = None,
)
```

### `SeriesMatch`

```python
SeriesMatch(
    namespace: str,
    code: str,
    title: str,
    similarity: float,
    tags: list[str] = [],
    description: str | None = None,
    metadata: dict = {},
    properties: dict = {},
)
```

`similarity` is clamped to `[0, 1]`; higher is more relevant.

### `IndexResult`

```python
IndexResult(total: int, indexed: int, skipped: int, errors: int)
```

Summary returned by `Catalog.index_result()` and `ingest()`.

### `EmbeddingProvider`

Abstract base class for text-to-vector embedding backends.

**Abstract methods**:

| Method | Signature | Description |
|---|---|---|
| `dimension` | `@property -> int` | Output vector dimension |
| `embed_texts` | `async (texts: list[str]) -> list[list[float]]` | Batch embed; returns L2-normalized vectors |
| `embed_query` | `async (query: str) -> list[float]` | Single-query embed |

### Bundle exceptions

```python
from parsimony.stores.hf_bundle import (
    BundleError, BundleNotFoundError, BundleIntegrityError,
)
```

| Class | When it fires |
|---|---|
| `BundleNotFoundError` | No HF bundle published for this namespace (`try_load_remote` returns `False` in this case rather than raising) |
| `BundleIntegrityError` | Any other failure: download, manifest corrupt, SHA / shape / size mismatch, model dim mismatch, pinned revision unavailable |
| `BundleError` | Base — catch to handle any bundle failure |

---

## Data Store API

```python
from parsimony import DataStore, LoadResult
```

### `DataStore`

Abstract base class for observation (DataFrame) persistence backends.

**Abstract methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `upsert` | `async (namespace: str, code: str, result: Result) -> None` | Store or replace a result |
| `get` | `async (namespace: str, code: str) -> Result \| None` | Retrieve a stored result |
| `delete` | `async (namespace: str, code: str) -> None` | Remove a stored result |
| `exists` | `async (namespace: str, code: str) -> bool` | Check existence |
| `load_result` | `async (entries: list[SeriesEntry], connector: Connector) -> LoadResult` | Fetch and store observations for a list of catalog entries using the given connector |

### `LoadResult`

```python
LoadResult(
    total: int,
    loaded: int,
    skipped: int,
    errors: int,
)
```

Summary returned by `DataStore.load_result()`.

---

## Data Store Implementations

```python
from parsimony import InMemoryDataStore
```

### `InMemoryDataStore`

Dict-backed implementation of `DataStore`. Suitable for development and testing.

```python
data_store = InMemoryDataStore()
```

---

## Embedding Providers

Two providers ship in-tree:

### `SentenceTransformersEmbeddingProvider` (default)

```python
from parsimony.embeddings.sentence_transformers import SentenceTransformersEmbeddingProvider

provider = SentenceTransformersEmbeddingProvider(
    model_id="sentence-transformers/all-MiniLM-L6-v2",
    revision="c9745ed1d9f207416be6d2e6f8de32d1f16199bf",
    expected_dim=384,
)
```

| Parameter | Description |
|---|---|
| `model_id` | HF repo id; must start with `sentence-transformers/` or `parsimony-dev/` |
| `revision` | 40-char HF commit SHA pinning the model weights |
| `expected_dim` | Output dim; validated against the model's actual dim on first embed |

- Process-wide LRU cache (size 2) keyed on `(model_id, revision)` so
  multiple stores share one set of weights.
- Per-instance `asyncio.Semaphore` caps concurrent embed calls at
  `PARSIMONY_EMBED_CONCURRENCY` (default `os.cpu_count()`).
- Vectors are L2-normalized so FAISS inner-product search == cosine similarity.

### `LiteLLMEmbeddingProvider` (optional, [search] extra)

```python
from parsimony.embeddings.litellm import LiteLLMEmbeddingProvider

provider = LiteLLMEmbeddingProvider(model="gemini/text-embedding-004", dimension=768)
```

Wraps `litellm.aembedding()` for hosted embedding APIs. Requires
`pip install parsimony-core[search]`. Batches at 100 texts per call. Applies
L2 normalization.

```python
from parsimony import Catalog
from parsimony.embeddings.litellm import LiteLLMEmbeddingProvider
from parsimony.stores import SQLiteCatalogStore

provider = LiteLLMEmbeddingProvider(model="gemini/text-embedding-004", dimension=768)
catalog = Catalog(SQLiteCatalogStore(":memory:"), embeddings=provider)
```

---

## Utility Functions

```python
from parsimony import (
    build_embedding_text,
    code_token,
    normalize_code,
    normalize_series_catalog_row,
    series_match_from_entry,
)
```

| Function | Signature | Description |
|----------|-----------|-------------|
| `build_embedding_text` | `(entry: SeriesEntry) -> str` | Compose a plain-text string from entry fields for embedding; format affects search quality |
| `code_token` | `(code: str) -> str` | Normalize a code string to a single slug token (removes separators) |
| `normalize_code` | `(code: str) -> str` | Lowercase normalization of namespace/code identifiers |
| `normalize_series_catalog_row` | `(row: dict) -> SeriesEntry` | Convert a raw dict (e.g. from a database row) to a `SeriesEntry` |
| `series_match_from_entry` | `(entry: SeriesEntry, similarity: float) -> SeriesMatch` | Construct a `SeriesMatch` from an existing `SeriesEntry` |

---

## Factory Functions

These functions are not in `__all__` but are the primary entry points for production use. Import from `parsimony.connectors`.

```python
from parsimony.connectors import build_connectors_from_env
```

### `build_connectors_from_env`

```python
build_connectors_from_env(*, env: dict[str, str] | None = None) -> Connectors
```

Single factory that composes every discovered `parsimony.providers` plugin into one `Connectors` bundle, binding each plugin's declared env vars from `os.environ`.

- `env`: optional dict of environment variables; defaults to `os.environ` if `None`.
- Providers whose required env vars are absent are silently skipped.

**Required env vars**: `FRED_API_KEY`  
**Optional env vars**: `FMP_API_KEY`, `EODHD_API_KEY`, `FINNHUB_API_KEY`, `TIINGO_API_KEY`, `COINGECKO_API_KEY`, `EIA_API_KEY`, `ALPHA_VANTAGE_API_KEY`, `FINANCIAL_REPORTS_API_KEY`, and others

Connectors whose optional env var is absent are silently excluded from the returned bundle.

To get only the interactive agent tools (search, discovery, reference lookups), filter the result:

```python
tools = connectors.filter(tags=["tool"])
```

---

## Connector Inventory

All 33 connector functions are listed below. Each connector is `async` and accepts a Pydantic params model as its first argument. Unless otherwise noted, connectors return `SemanticTableResult` (when `output=` is set) or `Result`.

The diagram below shows how the 33 functions are distributed across the 9 connector modules and which external dependency each module requires.

```mermaid
graph LR
    classDef module fill:#4A90E2,stroke:#2E5C8A,color:#fff
    classDef count fill:#6C8EBF,stroke:#4A6A9F,color:#fff
    classDef dep fill:#9B6B9B,stroke:#7A4A7A,color:#fff
    classDef nodep fill:#50C878,stroke:#2E7D50,color:#fff

    subgraph Modules["Connector Modules"]
        FRED["parsimony-fred plugin\n3 functions"]:::module
        SDMX["parsimony-sdmx plugin\n3 functions"]:::module
        FMP["fmp.py\n18 functions"]:::module
        FMPS["fmp_screener.py\n1 function"]:::module
        SEC["sec_edgar.py"]:::module
        EOD["eodhd.py"]:::module
        POLY["polymarket.py\n2 functions"]:::module
        FIN["financial_reports.py"]:::module
        MORE["+ 15 more modules"]:::module
    end

    subgraph Deps["External Dependencies"]
        D1["FRED_API_KEY"]:::dep
        D2["sdmx1 package"]:::dep
        D3["FMP_API_KEY"]:::dep
        D4["edgartools package"]:::dep
        D5["EODHD_API_KEY"]:::dep
        D7["None required"]:::nodep
        D8["FINANCIAL_REPORTS_API_KEY\n+ SDK"]:::dep
    end

    FRED --> D1
    SDMX --> D2
    FMP --> D3
    FMPS --> D3
    SEC --> D4
    EOD --> D5
    POLY --> D7
    FIN --> D8
```

### FRED Connectors

**Module**: `parsimony.connectors.fred`  
**Required dependency**: `FRED_API_KEY` environment variable

#### `fred_search`

| Field | Value |
|-------|-------|
| Tags | `["macro"]` |
| Decorator | `@connector` |
| Required env | `FRED_API_KEY` |

**Params model** (`FredSearchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `str` | Search text for FRED series |
| `limit` | `int` | Maximum results to return |

Search FRED for macroeconomic series matching a text query. Returns a DataFrame of matching series identifiers and titles.

#### `fred_fetch`

| Field | Value |
|-------|-------|
| Tags | `["macro"]` |
| Decorator | `@loader` |
| Required env | `FRED_API_KEY` |

**Params model** (`FredFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `series_id` | `Annotated[str, Namespace("fred")]` | FRED series identifier (e.g. `"GDP"`) |
| `observation_start` | `str \| None` | Start date in `YYYY-MM-DD` format |
| `observation_end` | `str \| None` | End date in `YYYY-MM-DD` format |

Fetch time-series observations for a FRED series. Returns a `SemanticTableResult` with KEY and DATA columns.

#### `enumerate_fred_release`

| Field | Value |
|-------|-------|
| Tags | `["fred"]` |
| Decorator | `@enumerator` |
| Required env | `FRED_API_KEY` |

**Params model** (`FredReleaseParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `release_id` | `int` | FRED release identifier |

Enumerate all series in a FRED release. Returns a `SemanticTableResult` with KEY and TITLE columns. Suitable for bulk catalog indexing.

---

### SDMX Connectors

**Package**: `parsimony-sdmx` (separate plugin — `pip install parsimony-sdmx`)
**Module**: `parsimony_sdmx`
**Required dependency**: none — SDMX endpoints are public.

The plugin exposes three connectors via the `parsimony.providers` entry
point; they are discovered automatically by
`build_connectors_from_env()` when the plugin is installed.

Dataset and series discovery is handled by the kernel's generic
`Catalog.search` surface against two namespace families shipped as
HuggingFace FAISS bundles:

- `sdmx_datasets` — one entry per `(agency, dataset_id)` tuple across
  all supported agencies (ECB, ESTAT, IMF_DATA, WB_WDI).
- `sdmx_series_{agency}_{dataset_id}` — one per-dataset bundle of
  series keys, selected by namespace template resolution.

#### `sdmx_fetch`

| Field | Value |
|-------|-------|
| Tags | `["sdmx"]` |
| Decorator | `@connector` |

**Params model** (`parsimony_sdmx.connectors.fetch.SdmxFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset_key` | `str` | `{AGENCY}-{dataset_id}` (e.g. `"ECB-YC"`). Agency must be in the allowlist. |
| `series_key` | `str` | Dot-separated SDMX series key. |
| `start_period` | `str \| None` | Start period (e.g. `"2020-01"`). |
| `end_period` | `str \| None` | End period. |

Fetch observations for an SDMX series from the upstream agency endpoint.

#### `enumerate_sdmx_datasets`

Enumerator that populates the `sdmx_datasets` catalog namespace from
each agency's flat `datasets.parquet` output. Emits KEY=`{AGENCY}|{dataset_id}`
+ TITLE + METADATA(`agency`, `dataset_id`).

#### `enumerate_sdmx_series`

Enumerator with template namespace
`sdmx_series_{agency}_{dataset_id}`. Resolved per row, it populates
one catalog namespace per dataset with the flat series table.

---

### FMP Connectors

**Module**: `parsimony.connectors.fmp`  
**Required dependency**: `FMP_API_KEY` environment variable

All FMP connectors share the tag `["equity"]` unless otherwise noted. Utility connectors have the additional tag `"utility"`.

#### `fmp_search`

| Tags | `["equity", "utility"]` | Decorator | `@connector` |
|------|-------------------------|-----------|--------------|

**Params** (`FmpSearchParams`): `query: str`, `limit: int`

Search FMP for companies and securities matching a text query.

#### `fmp_taxonomy`

| Tags | `["equity", "utility"]` | Decorator | `@connector` |
|------|-------------------------|-----------|--------------|

**Params** (`FmpTaxonomyParams`): `exchange: str | None`, `sector: str | None`, `industry: str | None`

Retrieve the FMP taxonomy of exchanges, sectors, and industries.

#### `fmp_quotes`

| Tags | `["equity"]` | Decorator | `@loader` |
|------|--------------|-----------|-----------|

**Params** (`FmpQuotesParams`): `symbol: Annotated[str, Namespace("fmp")]`

Fetch the latest quote (price, volume, change) for a single ticker symbol.

#### `fmp_prices`

| Tags | `["equity"]` | Decorator | `@loader` |
|------|--------------|-----------|-----------|

**Params** (`FmpPricesParams`): `symbol: Annotated[str, Namespace("fmp")]`, `from_date: str | None`, `to_date: str | None`

Fetch historical daily price data for a ticker symbol.

#### `fmp_company_profile`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpCompanyProfileParams`): `symbol: Annotated[str, Namespace("fmp")]`

Fetch company profile (name, description, sector, country, market cap, etc.) for a ticker symbol.

#### `fmp_peers`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpPeersParams`): `symbol: Annotated[str, Namespace("fmp")]`

Fetch a list of peer companies for a ticker symbol.

#### `fmp_income_statements`

| Tags | `["equity"]` | Decorator | `@loader` |
|------|--------------|-----------|-----------|

**Params** (`FmpIncomeStatementsParams`): `symbol: Annotated[str, Namespace("fmp")]`, `period: str` (`"annual"` or `"quarter"`), `limit: int`

Fetch historical income statements for a ticker symbol.

#### `fmp_balance_sheet_statements`

| Tags | `["equity"]` | Decorator | `@loader` |
|------|--------------|-----------|-----------|

**Params** (`FmpBalanceSheetStatementsParams`): `symbol: Annotated[str, Namespace("fmp")]`, `period: str`, `limit: int`

Fetch historical balance sheet statements for a ticker symbol.

#### `fmp_cash_flow_statements`

| Tags | `["equity"]` | Decorator | `@loader` |
|------|--------------|-----------|-----------|

**Params** (`FmpCashFlowStatementsParams`): `symbol: Annotated[str, Namespace("fmp")]`, `period: str`, `limit: int`

Fetch historical cash flow statements for a ticker symbol.

#### `fmp_corporate_history`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpCorporateHistoryParams`): `symbol: Annotated[str, Namespace("fmp")]`

Fetch corporate history events (splits, dividends, name changes) for a ticker symbol.

#### `fmp_event_calendar`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpEventCalendarParams`): `from_date: str | None`, `to_date: str | None`

Fetch the earnings event calendar for a date range.

#### `fmp_analyst_estimates`

| Tags | `["equity"]` | Decorator | `@loader` |
|------|--------------|-----------|-----------|

**Params** (`FmpAnalystEstimatesParams`): `symbol: Annotated[str, Namespace("fmp")]`, `period: str`, `limit: int`

Fetch analyst consensus estimates for a ticker symbol.

#### `fmp_news`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpNewsParams`): `tickers: list[str]`, `limit: int`

Fetch recent news articles for one or more ticker symbols.

#### `fmp_insider_trades`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpInsiderTradesParams`): `symbol: Annotated[str, Namespace("fmp")]`, `limit: int`

Fetch insider transaction filings for a ticker symbol.

#### `fmp_institutional_positions`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpInstitutionalPositionsParams`): `symbol: Annotated[str, Namespace("fmp")]`, `limit: int`

Fetch institutional ownership positions for a ticker symbol (13F filings).

#### `fmp_earnings_transcript`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpEarningsTranscriptParams`): `symbol: Annotated[str, Namespace("fmp")]`, `year: int`, `quarter: int`

Fetch earnings call transcript text for a ticker symbol, year, and quarter.

#### `fmp_index_constituents`

| Tags | `["equity"]` | Decorator | `@enumerator` |
|------|--------------|-----------|---------------|

**Params** (`FmpIndexConstituentsParams`): `index: Annotated[str, Namespace("fmp_index")]`

Enumerate the constituent symbols of a market index (e.g. `"sp500"`, `"nasdaq100"`). Returns KEY and TITLE columns.

#### `fmp_market_movers`

| Tags | `["equity"]` | Decorator | `@connector` |
|------|--------------|-----------|--------------|

**Params** (`FmpMarketMoversParams`): `direction: str` (`"gainers"` or `"losers"`), `limit: int`

Fetch the top market movers (largest price changers) for the current session.

---

### FMP Screener Connector

**Module**: `parsimony.connectors.fmp_screener`  
**Required dependency**: `FMP_API_KEY`

#### `fmp_screener`

| Field | Value |
|-------|-------|
| Tags | `["equity"]` |
| Decorator | `@connector` |
| Required env | `FMP_API_KEY` |

**Params model** (`FmpScreenerParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `market_cap_more_than` | `int \| None` | Minimum market cap filter |
| `market_cap_lower_than` | `int \| None` | Maximum market cap filter |
| `sector` | `str \| None` | Sector name filter |
| `industry` | `str \| None` | Industry name filter |
| `exchange` | `str \| None` | Exchange filter |
| `country` | `str \| None` | Country filter |
| `is_etf` | `bool \| None` | Filter for ETFs |
| `is_actively_trading` | `bool \| None` | Filter for actively trading securities |
| `limit` | `int` | Maximum results |
| `where_clause` | `str \| None` | pandas query string applied after merging results |

Screens companies using FMP's screener endpoint, then concurrently fetches key metrics and financial ratios for each result. Merges all three data sources by symbol. Concurrent API calls are rate-limited internally by a semaphore (`_SEMAPHORE_LIMIT = 10`).

> **Security note**: `where_clause` is passed directly to `DataFrame.query()`. Do not expose this parameter to untrusted user input.

---

### SEC Edgar Connector

**Module**: `parsimony.connectors.sec_edgar`  
**Required dependency**: `edgartools` package (install separately: `pip install edgartools`)  
**No API key required** (optional `SEC_EDGAR_USER_AGENT` / `EDGAR_IDENTITY` for request identification)

#### `sec_edgar_fetch`

| Field | Value |
|-------|-------|
| Tags | `["sec_edgar"]` |
| Decorator | `@connector` |
| Required env | None (optional: `SEC_EDGAR_USER_AGENT` or `EDGAR_IDENTITY`) |

**Params model** (`SecEdgarFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `ticker` | `str` | Company ticker symbol |
| `form_type` | `str` | SEC form type (e.g. `"10-K"`, `"10-Q"`, `"8-K"`) |
| `limit` | `int` | Maximum filings to return |

Fetch SEC Edgar filings for a company. Uses the `edgartools` synchronous library wrapped in an async dispatch class.

---

### EODHD Connector

**Module**: `parsimony.connectors.eodhd`  
**Required dependency**: `EODHD_API_KEY` environment variable

#### `eodhd_fetch`

| Field | Value |
|-------|-------|
| Tags | `["eodhd"]` |
| Decorator | `@loader` |
| Required env | `EODHD_API_KEY` |

**Params model** (`EodhdFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | `Annotated[str, Namespace("eodhd")]` | EODHD ticker symbol (e.g. `"AAPL.US"`) |
| `from_date` | `str \| None` | Start date (`YYYY-MM-DD`) |
| `to_date` | `str \| None` | End date (`YYYY-MM-DD`) |
| `period` | `str` | Bar period: `"d"` (daily), `"w"` (weekly), `"m"` (monthly) |

Fetch historical end-of-day price data from EODHD.

---

### Polymarket Connectors

**Module**: `parsimony.connectors.polymarket`  
**No API key or additional dependencies required**

#### `polymarket_gamma_fetch`

| Field | Value |
|-------|-------|
| Tags | `["polymarket"]` |
| Decorator | `@connector` |

**Params model** (`PolymarketGammaFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `market_id` | `str \| None` | Polymarket market ID |
| `query` | `str \| None` | Text search query |
| `limit` | `int` | Maximum results |

Fetch prediction market data from Polymarket's Gamma API.

#### `polymarket_clob_fetch`

| Field | Value |
|-------|-------|
| Tags | `["polymarket"]` |
| Decorator | `@connector` |

**Params model** (`PolymarketClobFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `token_id` | `str` | Polymarket CLOB token ID |

Fetch order book and trade data from Polymarket's Central Limit Order Book (CLOB) API.

---

### Financial Reports Connector

**Module**: `parsimony.connectors.financial_reports`  
**Required dependencies**: `FINANCIAL_REPORTS_API_KEY` environment variable + `financial-reports-generated-client` SDK (install separately)

#### `financial_reports_fetch`

| Field | Value |
|-------|-------|
| Tags | `["financial_reports"]` |
| Decorator | `@connector` |
| Required env | `FINANCIAL_REPORTS_API_KEY` |

**Params model** (`FinancialReportsFetchParams`):

| Parameter | Type | Description |
|-----------|------|-------------|
| `report_type` | `str` | Report type identifier (maps to a specific SDK client method) |
| `symbol` | `str \| None` | Ticker symbol (where applicable) |
| `year` | `int \| None` | Report year (where applicable) |

Fetch financial reports using the `financial-reports-generated-client` SDK. Supports up to 20 distinct report types dispatched via an internal if/elif chain.
