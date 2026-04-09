# ockham

[![PyPI version](https://img.shields.io/pypi/v/ockham)](https://pypi.org/project/ockham/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/ockham)](https://pypi.org/project/ockham/)
[![CI](https://github.com/espinetandreu/ockham/actions/workflows/test.yml/badge.svg)](https://github.com/espinetandreu/ockham/actions)

Typed, composable data connectors with searchable catalogs for Python.

### Why ockham?

Every financial data project starts the same way: write API wrappers, parse responses into DataFrames, track where data came from. `ockham` replaces that boilerplate with a declarative `@connector` system that gives you Pydantic-validated parameters, standardized `Result` outputs with provenance tracking, composable routing across sources, and an optional vector-searchable catalog for entity discovery.

## Quick Start

```python
from ockham.connectors.fred import CONNECTORS as FRED

client = FRED.bind_deps(api_key="your-fred-key")

result = await client["fred_fetch"](series_id="GDPC1", observation_start="2020-01-01")
print(result.data)        # pandas DataFrame
print(result.provenance)  # source="fred_fetch", params={...}
```

## Installation

```bash
pip install ockham
```

## Built-in Data Sources

| Source | Connectors | API Key |
|--------|-----------|---------|
| **FRED** (Federal Reserve Economic Data) | `fred_search`, `fred_fetch` | Free ([register](https://fred.stlouisfed.org/docs/api/api_key.html)) |
| **SDMX** (ECB, Eurostat, IMF, World Bank, …) | `sdmx_fetch`, `sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys` | None |
| **FMP** (Financial Modeling Prep) | `fmp_stock_quote`, `fmp_income_statements`, `fmp_balance_sheet_statements`, `fmp_historical_prices`, `fmp_company_profile` | Paid |
| **SEC Edgar** | `sec_edgar_fetch` | None |
| **Polymarket** | `polymarket_clob_fetch`, `polymarket_gamma_fetch` | None |
| **EODHD** | `eodhd_fetch` | Paid |
| **IBKR** (Interactive Brokers) | `ibkr_fetch` | Gateway required |
| **Financial Reports** | `financial_reports_fetch` | Paid |

### SDMX discovery workflow

Use the bundled SDMX connectors (no API key) in order: list dataflows for a source id → inspect dimensions → resolve codelists → optionally list valid series keys → fetch observations.

```python
from ockham.connectors.sdmx import CONNECTORS as SDMX

# agency: ECB, ESTAT, IMF_DATA, WB_WDI, … (see sdmx1 registered sources)
datasets = await SDMX["sdmx_list_datasets"](agency="ECB")
dsd = await SDMX["sdmx_dsd"](dataset_key="ECB-YC")
codes = await SDMX["sdmx_codelist"](dataset_key="ECB-YC", dimension="FREQ")
keys = await SDMX["sdmx_series_keys"](dataset_key="ECB-YC", filters={})
obs = await SDMX["sdmx_fetch"](
    dataset_key="ECB-YC",
    series_key="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
)
```

`sdmx_series_keys` can return large tables for big datasets; `sdmx_codelist` returns the full DSD codelist (not filtered by actual data availability). For World Bank flows, upstream quirks may require extra care; see connector docstrings.

## Features

The framework centers on three **decorator primitives**, all producing the same runtime type (`Connector`):

- **`@connector`** — typed fetch/search/etc.; `output=` is optional.
- **`@enumerator`** — catalog population: KEY + TITLE + METADATA, no DATA; requires `output=`.
- **`@loader`** — observation persistence: KEY + DATA only (no TITLE/METADATA); requires `output=` and a `DataStore` when you wire `load_result` as a callback.

Identity for catalog and data is always `(namespace, code)` from the KEY column.

---

## Step 1 — Connectors and Routing

A connector wraps an async function behind a typed boundary:

- **`params`** — the first argument must be a Pydantic model. This is the public contract: it validates and coerces external input and emits a JSON Schema for AI agents and API contracts.
- **`*deps`** — keyword-only arguments after `*` for trusted dependencies (API keys, HTTP clients, DB pools). Bound by the host application, not validated by Pydantic.

```python
import httpx
import pandas as pd
from pydantic import BaseModel
from ockham import connector, Connectors

class FactorParams(BaseModel):
    ticker: str
    factor: str

@connector()
async def fetch_factor(params: FactorParams, *, db_client: httpx.AsyncClient) -> pd.DataFrame:
    """Fetch proprietary alpha factors from our internal quant database."""
    resp = await db_client.get(
        "https://internal.example.com/factors",
        params=params.model_dump(),
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json()["data"])
```

Bundle connectors from multiple sources into a single routing layer. Bind dependencies once and dispatch by name:

```python
from ockham.connectors.fred import CONNECTORS as FRED

all_connectors = (
    Connectors([fetch_factor]).bind_deps(db_client=my_client)
    + FRED.bind_deps(api_key=fred_key)
)

factor = await all_connectors["fetch_factor"](ticker="AAPL", factor="momentum")
macro  = await all_connectors["fred_fetch"](series_id="UNRATE")
```

Each call returns a `Result` — the data plus provenance:

```python
factor.df           # pandas DataFrame
factor.provenance   # source name, params, fetched_at
```

---

## Step 2 — Building a Catalog

The routing layer answers "how do I call source X?" The catalog answers "what entities exist across all my sources, and how do I find them by description?"

### Enumerating entities

To populate the catalog, you need to list what a source contains — without fetching actual time-series values. This is what `@enumerator` is for. An enumerator produces one row per entity, carrying only its key, title, and metadata. No DATA columns — the entire output is what gets indexed.

The `OutputConfig` declares the column roles. The `namespace=` tag on the `KEY` column scopes identifiers to their source, preventing collisions:

```python
import pandas as pd
from pydantic import BaseModel
from ockham import Column, ColumnRole, OutputConfig, enumerator

class ReleaseParams(BaseModel):
    release_id: int

FRED_ENUM_SCHEMA = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY,      namespace="fred"),
        Column(name="title",     role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="units",     role=ColumnRole.METADATA),
    ]
)

@enumerator(output=FRED_ENUM_SCHEMA)
async def enumerate_fred_release(params: ReleaseParams, *, api_key: str) -> pd.DataFrame:
    """List all series in a FRED release — one row per entity."""
    ...
```

`@enumerator` enforces the contract at definition time: `output=` is required, DATA columns are rejected, the KEY column must declare `namespace=`, and exactly one TITLE column is required. The result is still a `Connector` — `bind_deps`, `with_callback`, and `Connectors` composition all work identically.

### Indexing and searching

```python
from ockham import SeriesCatalog, InMemoryCatalogStore, LiteLLMEmbeddingProvider

catalog = SeriesCatalog(
    store=InMemoryCatalogStore(),
    embeddings=LiteLLMEmbeddingProvider(model="text-embedding-3-small", dimension=1536),
)

enum = enumerate_fred_release.bind_deps(api_key=fred_key)
result = await enum(release_id=10)
await catalog.index_result(result)  # embeds TITLE + METADATA, upserts by (namespace, code)

matches = await catalog.search("quarterly real GDP", limit=5)
for m in matches:
    print(f"[{m.namespace}:{m.code}] {m.title}")
# [fred:GDPC1] Real Gross Domestic Product
# [fred:GDPPOT] Real Potential Gross Domestic Product
```

Catalog identity is `(namespace, code)`. Namespace comes from the schema; code comes from each row's KEY value. No manual mapping.

---

## Step 3 — Fetch Connectors That Also Index

An enumerator populates the catalog upfront. But when you actually *fetch* a specific series, the response already carries its key, title, and metadata — the same information the catalog needs. Adding an `OutputConfig` to a regular `@connector` applies the same schema mechanism: the connector returns a `SemanticTableResult`, and the catalog can index entities from it exactly as it does from an enumerator. DATA columns (the time-series observations) are returned to the caller but skipped during indexing.

```python
from typing import Annotated
from pydantic import BaseModel
from ockham import Column, ColumnRole, OutputConfig, Namespace, connector

class FetchParams(BaseModel):
    series_id: Annotated[str, Namespace("fred")]  # valid values live in the "fred" catalog namespace
    observation_start: str | None = None

FETCH_SCHEMA = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY,      namespace="fred"),
        Column(name="title",     role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="date",      role=ColumnRole.DATA),   # returned to caller, not indexed
        Column(name="value",     role=ColumnRole.DATA),   # returned to caller, not indexed
    ]
)

@connector(output=FETCH_SCHEMA)
async def fred_fetch(params: FetchParams, *, api_key: str) -> pd.DataFrame:
    """Fetch FRED observations — returns time-series data and identifies the series for the catalog."""
    ...
```

Attaching a callback makes every successful call automatically update the catalog:

```python
async def auto_index(result):
    await catalog.index_result(result)

bound = fred_fetch.bind_deps(api_key=fred_key).with_callback(auto_index)
await bound(series_id="GDPC1")  # fetches observations + indexes [fred:GDPC1]
```

When wiring callbacks across a bundle that mixes schema-aware and plain connectors, guard on the result type — plain connectors (no `output=`) return a raw `Result` and pass through silently:

```python
from ockham import Result, SemanticTableResult

async def auto_index(result: Result) -> None:
    if isinstance(result, SemanticTableResult):
        await catalog.index_result(result)

indexed_bundle = all_connectors.with_callback(auto_index)
```

### Linking fetch parameters to the catalog

The `Namespace("fred")` annotation on `series_id` above does more than document intent. It appears in the connector's JSON Schema (`connector.param_schema`):

```json
{
  "properties": {
    "series_id": { "type": "string", "namespace": "fred" }
  }
}
```

This completes the loop: the enumerator populates the `fred` namespace; the `Namespace("fred")` annotation tells AI agents and downstream tools to query that namespace when they need valid values for `series_id` before calling the fetch connector.

---

## Step 4 — Loading Data into a Store

The enumerator indexes **metadata** into the catalog. A **`@loader`** is the counterpart for **observations**: the schema declares only KEY (with `namespace=`) and DATA columns — no TITLE or METADATA (those stay in the catalog). The result is still a `Connector`; you persist with a `DataStore` via the same callback pattern as `SeriesCatalog.index_result`.

Define a loader schema (reuse the same KEY column as your enumerator/fetch connector so `(namespace, code)` lines up):

```python
from pydantic import BaseModel
from ockham import Column, ColumnRole, OutputConfig, loader

class FetchParams(BaseModel):
    series_id: str
    observation_start: str | None = None
    observation_end: str | None = None

FRED_LOAD_SCHEMA = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="date",  dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric",  role=ColumnRole.DATA),
    ]
)

@loader(output=FRED_LOAD_SCHEMA)
async def load_fred_series(params: FetchParams, *, api_key: str) -> pd.DataFrame:
    """Fetch FRED observations for persistence (same HTTP logic as fetch, different output schema)."""
    ...
```

Wire an `InMemoryDataStore` (or your own `DataStore` implementation) and attach `load_result` as a callback — same pattern as catalog indexing:

```python
from ockham import InMemoryDataStore, Connectors

data_store = InMemoryDataStore()

bound = load_fred_series.bind_deps(api_key=fred_key).with_callback(data_store.load_result)
await bound(series_id="GDPC1")  # persists observations for [fred:GDPC1]

# Or compose into a Connectors bundle
all_connectors = Connectors([bound])
```

Retrieve stored observations by catalog identity:

```python
df = await data_store.get("fred", "GDPC1")
# DataFrame with DATA columns only (date, value)
```

`DataStore.load_result` deduplicates by default: if `(namespace, code)` already has data, that entity is skipped unless you pass `force=True`. For multi-entity tables (multiple distinct KEY values in one result), the store upserts one observation frame per entity.

---

### Batch embedding backfill

Embedding backfill for a concrete persistence backend is an **application** concern: implement
`list_codes_missing_embedding` / `update_embeddings` (or equivalent) on your store, then run
embedding batches with your chosen :class:`~ockham.catalog.embeddings.EmbeddingProvider`.
The Ockham Terminal application wires a Supabase-backed store in ``server.catalog.supabase_store``.

## Persistent Storage

`InMemoryCatalogStore` is the reference implementation for tests and local tooling. Production
deployments should provide their own :class:`~ockham.catalog.store.CatalogStore` (e.g.
Postgres/Supabase in the Ockham Terminal app layer — not shipped inside ``ockham``).

---

## Troubleshooting

### Common issues

**`SEC_EDGAR_USER_AGENT` warning**: SEC Edgar requires an identifying user agent. Set the `SEC_EDGAR_USER_AGENT` environment variable to `"YourName your-email@example.com"`.

**SDMX timeouts**: Some SDMX providers (especially Eurostat) can be slow for large datasets. Use specific filters in your query parameters to reduce response size.

**Missing API key errors**: Most connectors require API keys passed via `bind_deps()`. FRED keys are free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html). SDMX connectors require no API key.

**Import errors for optional dependencies**: Install the relevant extra: `pip install ockham[sdmx]` for SDMX support, `pip install ockham[embeddings]` for catalog vector search.

## Related Packages

- **[ockham-agents](../ockham-agents)** — Build AI agents that use these connectors to discover, fetch, and analyze data automatically.

## License

Apache 2.0
