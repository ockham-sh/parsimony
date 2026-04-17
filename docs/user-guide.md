# parsimony User Guide

**Version**: 0.1.0
**Audience**: Python developers building data pipelines, notebooks, or agent integrations

parsimony is a Python library that gives you a single, consistent async interface for fetching financial and macroeconomic data from FRED, SDMX providers (ECB, Eurostat, IMF, World Bank, BIS), Financial Modeling Prep, SEC Edgar, EODHD, Polymarket, and more. All results are typed pandas DataFrames with provenance metadata and optional column-role schemas.

---

## Table of Contents

1. [Installation](#installation)
2. [Environment Setup](#environment-setup)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [Fetching Data from FRED](#fetching-data-from-fred)
6. [Querying SDMX Providers](#querying-sdmx-providers)
7. [Working with FMP Equity Data](#working-with-fmp-equity-data)
8. [Using the Catalog](#using-the-catalog)
9. [Creating Custom Connectors](#creating-custom-connectors)
10. [Working with Results](#working-with-results)
11. [Filtering and Composing Connector Bundles](#filtering-and-composing-connector-bundles)
12. [Common Patterns](#common-patterns)
13. [MCP Server (Coding Agent Integration)](#mcp-server-coding-agent-integration)

---

## Installation

```bash
pip install parsimony-core
```

### Optional extras

SDMX support (ECB, Eurostat, IMF, World Bank, BIS) is included in the base install.

| Extra | Install command | What it enables |
|-------|----------------|-----------------|
| `search` | `pip install "parsimony-core[search]"` | Semantic catalog search via LiteLLM embeddings + sqlite-vec |
| `sec` | `pip install "parsimony-core[sec]"` | SEC Edgar connector via edgartools |
| `mcp` | `pip install "parsimony-core[mcp]"` | MCP server for AI agents |
| `all` | `pip install "parsimony-core[all]"` | Everything |

Two connectors require separately-installed packages:

```bash
pip install edgartools                        # SEC Edgar connector
pip install financial-reports-generated-client  # Financial Reports connector
```

### Python version

parsimony requires **Python 3.11+**.

---

## Environment Setup

Different connectors require different credentials. Set the variables for the data sources you intend to use.

| Variable | Required | Used By |
|----------|----------|---------|
| `FRED_API_KEY` | Yes | All FRED connectors |
| `FMP_API_KEY` | Optional | All FMP connectors and FMP Screener |
| `EODHD_API_KEY` | Optional | EODHD connector |
| `FINNHUB_API_KEY` | Optional | Finnhub connector |
| `TIINGO_API_KEY` | Optional | Tiingo connector |
| `COINGECKO_API_KEY` | Optional | CoinGecko connector |
| `EIA_API_KEY` | Optional | EIA connector |
| `ALPHA_VANTAGE_API_KEY` | Optional | Alpha Vantage connector |
| `FINANCIAL_REPORTS_API_KEY` | Optional | Financial Reports connector |

SDMX, Polymarket, SEC Edgar, US Treasury, and central bank connectors require no credentials.

### .env file example

```bash
# .env
FRED_API_KEY=your-fred-api-key
FMP_API_KEY=your-fmp-api-key       # optional
EODHD_API_KEY=your-eodhd-key       # optional
```

Load into your shell with `export $(cat .env | xargs)` or use a library like `python-dotenv`.

To obtain a FRED API key, register at [https://fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html).

---

## Quick Start

The fastest way to get started is to call `build_connectors_from_env()`. This reads your environment variables, injects the appropriate API keys, and returns a ready-to-use `Connectors` bundle.

The diagram below shows the complete data flow from a connector call through to the `pd.DataFrame` you use in your code.

```mermaid
graph TD
    classDef user fill:#4A90E2,stroke:#2E5C8A,color:#fff
    classDef framework fill:#6C8EBF,stroke:#4A6A9F,color:#fff
    classDef transport fill:#9B6B9B,stroke:#7A4A7A,color:#fff
    classDef output fill:#50C878,stroke:#2E7D50,color:#fff

    A["connectors[name](series_id='GDP')"]:::user
    B["Pydantic validates kwargs\nto typed model"]:::framework
    C["Bound API key injected\nvia bind_deps()"]:::framework
    D["HTTP request\nHttpClient / SDK"]:::transport
    E["Raw JSON or CSV\nresponse"]:::transport
    F["JSON parsed to\npd.DataFrame"]:::framework
    G["Result or\nSemanticTableResult"]:::output
    H["Callbacks fired\n(optional)"]:::framework
    I["result.data\npd.DataFrame"]:::output
    J["result.provenance\nsource + params + fetched_at"]:::output

    A --> B --> C --> D --> E --> F --> G --> H
    G --> I
    G --> J
```

```python
import asyncio
from parsimony.connectors import build_connectors_from_env

async def main():
    # Build the full connector bundle from environment variables
    connectors = build_connectors_from_env()

    # Fetch US GDP data from FRED using keyword arguments
    result = await connectors["fred_fetch"](series_id="GDP")

    print(result.data.tail())
    print(result.provenance)

asyncio.run(main())
```

Expected output (example):

```
         date     value
2023-01-01  26408.4
2023-04-01  26960.4
2023-07-01  27357.8
2023-10-01  27940.4
2024-01-01  28269.7

Provenance(source='fred', params={'series_id': 'GDP'}, fetched_at=datetime(...))
```

---

## Core Concepts

### Connectors

A `Connector` is an async function wrapped with metadata: a name, description, Pydantic params model, and optional output schema. Call it with keyword arguments (`await conn(series_id="GDP")`) or a typed Pydantic model. Dependencies like API keys are injected via `bind_deps()` and never appear in provenance or logs.

### Connectors (the collection)

`Connectors` is an immutable collection of `Connector` instances. Look up by name with `connectors["fred_fetch"]`, compose with `+`, filter with `.filter(tags=["macro"])`, and attach hooks with `.with_callback()`.

### Results

Every connector call returns a `Result` with `.data` (usually a pandas DataFrame) and `.provenance` (source, parameters, timestamp). When a connector declares an `OutputConfig`, the result is a `SemanticTableResult` with typed columns (KEY, TITLE, DATA, METADATA roles) suitable for catalog indexing and LLM display.

### Provenance

`Provenance` records where data came from: the connector name, the user-facing parameters, and when it was fetched. API keys injected via `bind_deps` are excluded. Provenance is serialized with Arrow/Parquet round-trips.

---

## Fetching Data from FRED

### Search for a series

```python
import asyncio
from parsimony.connectors import build_connectors_from_env

async def search_fred():
    connectors = build_connectors_from_env()

    result = await connectors["fred_search"](search_text="consumer price index")
    print(result.data[["id", "title"]].head(5))

asyncio.run(search_fred())
```

### Fetch a specific series with a date range

```python
async def fetch_unemployment():
    connectors = build_connectors_from_env()

    result = await connectors["fred_fetch"](
        series_id="UNRATE",
        observation_start="2020-01-01",
        observation_end="2024-12-31",
    )

    df = result.data
    print(f"Fetched {len(df)} observations")
    print(df.tail(3))
```

### Enumerate all series in a FRED release

```python
async def enumerate_release():
    connectors = build_connectors_from_env()

    # FRED Release 10 is the Employment Situation
    result = await connectors["enumerate_fred_release"](release_id=10)

    print(f"Found {len(result.data)} series in this release")
    print(result.data.head())
```

---

## Querying SDMX Providers

SDMX connectors are included in the base install.

No API key is required. SDMX providers include ECB, Eurostat (ESTAT), IMF, World Bank (WB_WDI), BIS, and others.

```python
async def sdmx_examples():
    connectors = build_connectors_from_env()

    # List all ECB datasets
    datasets = await connectors["sdmx_list_datasets"](agency="ECB")
    print(datasets.data.head(10))

    # Get the Data Structure Definition for the ECB exchange rate dataset
    dsd = await connectors["sdmx_dsd"](dataset_key="ECB-EXR")
    print(dsd.data)

    # Fetch daily USD/EUR spot rate observations
    rates = await connectors["sdmx_fetch"](
        dataset_key="ECB-EXR",
        series_key="D.USD.EUR.SP00.A",
        start_period="2023-01-01",
        end_period="2024-12-31",
    )
    print(rates.data.tail())

    # List available series keys (filterable for catalog ingestion)
    keys = await connectors["sdmx_series_keys"](
        dataset_key="ESTAT-namq_10_gdp",
    )
    print(f"Found {len(keys.data)} series keys")
```

The `dataset_key` format is always `AGENCY-DATASET_ID` (e.g., `ECB-EXR`, `ESTAT-namq_10_gdp`, `IMF_DATA-IFS`).

---

## Working with FMP Equity Data

FMP connectors require `FMP_API_KEY`. The calling pattern is the same: pass keyword arguments matching the connector's params model.

```python
async def fmp_examples():
    connectors = build_connectors_from_env()

    # Search for companies
    search = await connectors["fmp_search"](query="Apple", limit=5)
    print(search.data[["symbol", "name", "exchangeShortName"]])

    # Historical prices
    prices = await connectors["fmp_prices"](
        symbol="AAPL",
        from_date="2024-01-01",
        to_date="2024-12-31",
    )
    print(prices.data.tail())

    # Annual income statements
    income = await connectors["fmp_income_statements"](
        symbol="MSFT",
        period="annual",
        limit=5,
    )
    print(income.data[["date", "revenue", "netIncome"]].head())

    # Screen for large-cap tech
    screener = await connectors["fmp_screener"](
        sector="Technology",
        market_cap_min=10_000_000_000,
        exchange="NASDAQ",
        limit=20,
    )
    print(screener.data.head())
```

> **Warning**: The `where_clause` parameter in `fmp_screener` uses `DataFrame.query()` internally. Do not pass untrusted user input as a `where_clause` value.

---

## Using the Catalog

The catalog lets you index discovered series and search them by text or semantic similarity.

### Basic setup with in-memory storage

```python
import asyncio
from parsimony import Catalog, SQLiteCatalogStore
from parsimony.connectors import build_connectors_from_env

async def catalog_example():
    connectors = build_connectors_from_env()
    catalog = Catalog(SQLiteCatalogStore(":memory:"))

    # Enumerate all series in a FRED release and index them
    result = await connectors["enumerate_fred_release"](release_id=10)
    index_summary = await catalog.index_result(result, embed=False)

    print(f"Indexed {index_summary.indexed} series, skipped {index_summary.skipped}")

    # Search the catalog
    matches = await catalog.search("unemployment rate", limit=5)
    for match in matches:
        print(f"  {match.namespace}/{match.code}: {match.title}")

asyncio.run(catalog_example())
```

### Using the `dry_run` option

```python
# Preview what would be indexed without writing to the store
summary = await catalog.index_result(result, embed=False, dry_run=True)
print(f"Would index {summary.indexed} entries, skip {summary.skipped}")
```

### Semantic search with embeddings

Semantic search requires the `[search]` extra and a LiteLLM-compatible embedding model.

```python
from parsimony import LiteLLMEmbeddingProvider, Catalog, SQLiteCatalogStore

async def semantic_search():
    provider = LiteLLMEmbeddingProvider(
        model="gemini/text-embedding-004",
        dimension=768,
    )
    catalog = Catalog(SQLiteCatalogStore(":memory:"), embeddings=provider)

    # Index some results (embeddings are computed during indexing)
    result = await connectors["enumerate_fred_release"](release_id=10)
    await catalog.index_result(result, embed=True)

    # Search using semantic similarity
    matches = await catalog.search("jobs market labor", limit=5)
    for match in matches:
        print(f"  {match.title}")
```

### Listing and retrieving catalog entries

```python
# List all namespaces
namespaces = await catalog.list_namespaces()

# Paginate through entries in a namespace
entries, total = await catalog.list_entries(namespace="fred", limit=50, offset=0)

# Retrieve a specific entry
entry = await catalog.get_entry(namespace="fred", code="UNRATE")
if entry:
    print(entry.title)
```

---

## Creating Custom Connectors

You can build your own connectors using the `@connector`, `@enumerator`, or `@loader` decorators. Custom connectors integrate seamlessly with `Connectors` bundles and the catalog.

### Minimal custom connector

```python
import pandas as pd
from pydantic import BaseModel
from parsimony import connector

class MyParams(BaseModel):
    symbol: str
    limit: int = 10

@connector(tags=["custom"])
async def my_data_source(params: MyParams) -> pd.DataFrame:
    """Fetch data from my internal API."""
    # Replace with real HTTP call
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=params.limit, freq="D"),
        "value": range(params.limit),
    })
```

### Custom connector with a declared schema

Use `OutputConfig` and `Column` to declare the semantic meaning of each column:

```python
from typing import Annotated
from pydantic import BaseModel
from parsimony import (
    connector, Namespace,
    OutputConfig, Column, ColumnRole,
)

class PriceParams(BaseModel):
    ticker: Annotated[str, Namespace("my_source")]

PRICE_OUTPUT = OutputConfig(columns=[
    Column(name="ticker", role=ColumnRole.KEY, namespace="my_source"),
    Column(name="name",   role=ColumnRole.TITLE),
    Column(name="close",  role=ColumnRole.DATA, dtype="numeric"),
    Column(name="volume", role=ColumnRole.DATA, dtype="numeric"),
])

@connector(output=PRICE_OUTPUT)
async def my_prices(params: PriceParams) -> pd.DataFrame:
    """Fetch daily closing prices from my source."""
    # Your implementation here
    return pd.DataFrame(...)
```

### Custom connector with dependency injection

Use keyword-only function parameters (after `*`) to declare dependencies like API keys:

```python
from pydantic import BaseModel
from parsimony import connector, Connectors

class SearchParams(BaseModel):
    query: str

@connector(tags=["custom"])
async def my_authenticated_source(params: SearchParams, *, api_key: str) -> pd.DataFrame:
    """Fetch from an authenticated API."""
    # api_key is injected via bind_deps; never stored in params or provenance
    ...

# Bind the API key before adding to a bundle
bound = my_authenticated_source.bind_deps(api_key="secret-key")

# Combine with the standard bundle
from parsimony.connectors import build_connectors_from_env
all_connectors = build_connectors_from_env() + Connectors([bound])
```

### Adding a result callback

Callbacks let you react to every result produced by a connector, for example to index into a catalog or emit a metric:

```python
from parsimony import Catalog, SQLiteCatalogStore, SemanticTableResult

catalog = Catalog(SQLiteCatalogStore(":memory:"))

async def auto_index(result):
    if isinstance(result, SemanticTableResult):
        await catalog.index_result(result, embed=False)

# Attach callback to a single connector
logged_connector = connectors["fred_fetch"].with_callback(auto_index)

# Attach callback to all connectors in a bundle
logged_bundle = connectors.with_callback(auto_index)
```

---

## Working with Results

### Accessing the DataFrame and provenance

```python
result = await connectors["fred_fetch"](series_id="GDP")

df = result.data           # pandas DataFrame
prov = result.provenance   # Provenance(source, params, fetched_at, ...)

print(prov.source)         # "fred"
print(prov.params)         # {"series_id": "GDP"}
```

### Promoting a Result to SemanticTableResult

If a connector returns a plain `Result` but you want to apply a schema:

```python
from parsimony import OutputConfig, Column, ColumnRole

my_schema = OutputConfig(columns=[
    Column(name="date",  role=ColumnRole.KEY, namespace="my_ns"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])

semantic_result = result.to_table(my_schema)

# Access typed column groups
print(semantic_result.data_columns)      # [Column(name="value", ...)]
print(semantic_result.metadata_columns)  # [Column(name=..., role=METADATA)]
```

### Serializing to Arrow and Parquet

Results can be saved and loaded round-trip using Apache Arrow or Parquet:

```python
# Save to Parquet
result.to_parquet("/tmp/gdp.parquet")

# Load from Parquet
from parsimony import Result
loaded = Result.from_parquet("/tmp/gdp.parquet")

# Arrow round-trip
table = result.to_arrow()
restored = Result.from_arrow(table)
```

---

## Filtering and Composing Connector Bundles

### Filter by name or tag

```python
connectors = build_connectors_from_env()

# Filter by tag (keyword argument)
equity = connectors.filter(tags=["equity"])
macro = connectors.filter(tags=["macro"])

# Filter by name substring
fred_only = connectors.filter(name="fred")
```

### Combine bundles

```python
from parsimony import Connectors

custom = Connectors([my_prices, my_authenticated_source.bind_deps(api_key="key")])
combined = build_connectors_from_env() + custom

# Use any connector by name
result = await combined["my_prices"](ticker="AAPL")
```

### Generate LLM tool descriptions

All connectors in a bundle can be serialized to a text description suitable for inclusion in an LLM system prompt:

```python
tool_descriptions = connectors.to_llm()
print(tool_descriptions[:500])
```

Each connector's description includes its name, docstring, tags, parameter names/types, and output columns. This output is consumed by agent frameworks that route LLM calls to connectors.

---

## Common Patterns

### Pattern 1: Run connectors in an async context

All connector calls are `async`. In a script, wrap them in `asyncio.run()`:

```python
import asyncio
from parsimony.connectors import build_connectors_from_env

async def main():
    connectors = build_connectors_from_env()
    result = await connectors["fred_fetch"](series_id="CPIAUCSL")
    print(result.data.tail())

asyncio.run(main())
```

In a Jupyter notebook, use `await` directly (notebooks run an event loop):

```python
connectors = build_connectors_from_env()
result = await connectors["fred_fetch"](series_id="CPIAUCSL")
result.data.tail()
```

### Pattern 2: Pass params as kwargs or a Pydantic model

Both forms are accepted:

```python
# Keyword arguments (validated internally by Pydantic)
result = await connectors["fred_fetch"](series_id="GDP")

# Pre-built Pydantic model
from parsimony.connectors.fred import FredFetchParams
result = await connectors["fred_fetch"](FredFetchParams(series_id="GDP"))
```

Note: raw `dict` is **not** accepted. Use keyword arguments or a typed model.

### Pattern 3: Bulk catalog indexing from an enumerator

```python
from parsimony import Catalog, SQLiteCatalogStore

catalog = Catalog(SQLiteCatalogStore(":memory:"))

# Enumerate and index
result = await connectors["enumerate_fred_release"](release_id=10)
summary = await catalog.index_result(result, embed=False)
print(f"Catalog now has {summary.indexed} entries")
```

### Pattern 4: Tool-only bundle for the MCP/search surface

If you only need the interactive agent tools (search, discovery, reference lookups), filter by tag:

```python
from parsimony.connectors import build_connectors_from_env

connectors = build_connectors_from_env()
tools = connectors.filter(tags=["tool"])
```

### Pattern 5: Handle missing optional connectors gracefully

When an optional env var is absent, the corresponding connector is excluded from the bundle. Check by name before calling:

```python
if "eodhd_fetch" in connectors:
    result = await connectors["eodhd_fetch"](symbol="AAPL.US")
```

---

## MCP Server (Coding Agent Integration)

parsimony includes an MCP server that exposes search and discovery connectors as native tools for coding agents (Claude Code, Cursor, Windsurf). The agent can search for data directly, then fetch and analyze it via code execution.

```bash
pip install -e ".[mcp]"
```

```json
{
  "mcpServers": {
    "parsimony": {
      "command": "python3",
      "args": ["-m", "parsimony.mcp"],
      "env": {
        "FRED_API_KEY": "your-key",
        "FMP_API_KEY": "your-key"
      }
    }
  }
}
```

See [docs/mcp-setup.md](mcp-setup.md) for full configuration, environment variables, and how to expose new connectors as MCP tools.
