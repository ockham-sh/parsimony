# User Guide

**Audience**: Python developers building data pipelines, notebooks, or agent
integrations against `parsimony-core`.

`parsimony-core` is a small kernel (connector primitives, discovery,
conformance, catalog). Every data source ships as a separate distribution
(`parsimony-fred`, `parsimony-sdmx`, `parsimony-fmp`, …) discovered through
the `parsimony.providers` entry-point group. The kernel has no in-tree
connectors.

---

## Table of contents

1. [Installation](#installation)
2. [Environment setup](#environment-setup)
3. [Quick start](#quick-start)
4. [Core concepts](#core-concepts)
5. [Working with connectors](#working-with-connectors)
6. [Using the catalog](#using-the-catalog)
7. [Creating custom connectors](#creating-custom-connectors)
8. [Working with results](#working-with-results)
9. [Filtering and composing bundles](#filtering-and-composing-bundles)
10. [Observing calls with callbacks](#observing-calls-with-callbacks)
11. [MCP server](#mcp-server)

---

## Installation

```bash
pip install parsimony-core
```

### Optional extras on the kernel

| Extra | Install command | What it enables |
|-------|----------------|-----------------|
| `standard` | `pip install "parsimony-core[standard]"` | Canonical `Catalog` — FAISS + BM25 + sentence-transformers + `hf://` loader |
| `litellm` | `pip install "parsimony-core[standard,litellm]"` | Hosted embeddings via the LiteLLM unified API (OpenAI, Gemini, Cohere, Voyage, Bedrock, …) |
| `s3` | `pip install "parsimony-core[standard,s3]"` | `s3://` URLs in `Catalog.from_url` / `Catalog.push` (planned) |
| `all` | `pip install "parsimony-core[all]"` | `standard + litellm + s3` |

### Connector plugins

Install connector plugins alongside the kernel. The kernel discovers every
installed plugin automatically via the `parsimony.providers` entry point.

```bash
pip install parsimony-fred          # FRED
pip install parsimony-sdmx          # SDMX (ECB, Eurostat, IMF, World Bank, BIS, OECD, ILO)
pip install parsimony-fmp           # Financial Modeling Prep
# ... and many more — see https://github.com/ockham-sh/parsimony-connectors
```

### MCP server

```bash
pip install parsimony-mcp
```

### Python version

`parsimony-core` requires **Python 3.11+**.

---

## Environment setup

Each connector plugin declares its own required env vars (see the plugin's
`ENV_VARS` mapping). Typical examples:

| Variable | Required by |
|---|---|
| `FRED_API_KEY` | `parsimony-fred` |
| `FMP_API_KEY` | `parsimony-fmp`, `parsimony-fmp-screener` |
| `EODHD_API_KEY` | `parsimony-eodhd` |
| `FINNHUB_API_KEY` | `parsimony-finnhub` |
| `TIINGO_API_KEY` | `parsimony-tiingo` |
| `COINGECKO_API_KEY` | `parsimony-coingecko` |
| `EIA_API_KEY` | `parsimony-eia` |
| `ALPHA_VANTAGE_API_KEY` | `parsimony-alpha-vantage` |
| `FINANCIAL_REPORTS_API_KEY` | `parsimony-financial-reports` |

SDMX, Polymarket, SEC Edgar, US Treasury, and most central-bank connectors
require no credentials.

`build_connectors_from_env()` reads `os.environ` and binds each plugin's
declared env vars. Plugins whose required env vars are absent are silently
skipped.

```bash
# .env
FRED_API_KEY=your-fred-api-key
FMP_API_KEY=your-fmp-api-key       # optional
```

---

## Quick start

```python
import asyncio
from parsimony.discovery import build_connectors_from_env

async def main():
    connectors = build_connectors_from_env()     # walks parsimony.providers
    result = await connectors["fred_fetch"](series_id="UNRATE")
    print(result.data.tail())
    print(result.provenance)

asyncio.run(main())
```

Or compose a specific plugin directly:

```python
from parsimony_fred import CONNECTORS as FRED

fred = FRED.bind_deps(api_key="your-key")
result = await fred["fred_fetch"](series_id="UNRATE")
```

---

## Core concepts

### Connectors

A `Connector` is an async function wrapped with metadata: a name,
description, Pydantic params model, and optional output schema. Call it
with keyword arguments (`await conn(series_id="GDP")`) or a typed Pydantic
model. Dependencies like API keys are injected via `bind_deps()` and never
appear in provenance or logs.

### Connectors (the collection)

`Connectors` is an immutable collection of `Connector` instances. Look up
by name with `connectors["fred_fetch"]`, compose with `+`, filter with
`.filter(tags=["macro"])`, and attach hooks with `.with_callback()`.

### Results

Every connector call returns a `Result` with `.data` (a pandas DataFrame)
and `.provenance` (source, parameters, timestamp). When a connector
declares an `OutputConfig`, the result additionally carries
`.output_schema` and exposes typed column groups (`entity_keys`,
`data_columns`, `metadata_columns`).

### Provenance

`Provenance` records where data came from: the connector name, the
user-facing parameters, and when it was fetched. API keys injected via
`bind_deps` are excluded. Provenance survives Arrow/Parquet round-trips.

---

## Working with connectors

### Search, fetch, enumerate

```python
# Search
search = await connectors["fred_search"](search_text="consumer price index")
print(search.data[["id", "title"]].head(5))

# Fetch with date range
obs = await connectors["fred_fetch"](
    series_id="UNRATE",
    observation_start="2020-01-01",
    observation_end="2024-12-31",
)
print(obs.data.tail(3))

# Enumerate a release for bulk indexing
enum = await connectors["enumerate_fred_release"](release_id=10)
print(f"Found {len(enum.data)} series")
```

### Querying SDMX providers

SDMX ships as `parsimony-sdmx`. Supported agencies: ECB, Eurostat (ESTAT),
IMF (IMF_DATA), World Bank (WB_WDI), BIS, OECD, ILO. No API key required.

```python
rates = await connectors["sdmx_fetch"](
    dataset_key="ECB-EXR",
    series_key="D.USD.EUR.SP00.A",
    start_period="2023-01-01",
    end_period="2024-12-31",
)
```

Dataset and series discovery goes through the catalog surface. The plugin
ships two HuggingFace FAISS bundle families:

- `sdmx_datasets` — one entry per `(agency, dataset_id)` across all agencies.
- `sdmx_series_{agency}_{dataset_id}` — one per-dataset bundle of series
  keys, selected by namespace.

See the [Quickstart](quickstart.md#step-3-explore-available-series) for the
two-hop discovery flow.

### Error handling

Every connector maps upstream failures to the typed hierarchy in
`parsimony.errors`:

```python
from parsimony import UnauthorizedError, RateLimitError, EmptyDataError

try:
    result = await connectors["fred_fetch"](series_id="NOT_A_SERIES")
except EmptyDataError as e:
    print(f"No data for {e.provider}")
except RateLimitError as e:
    print(f"Rate limited, retry after {e.retry_after}s; exhausted={e.quota_exhausted}")
except UnauthorizedError:
    print("Check your API key")
```

---

## Using the catalog

`parsimony.Catalog` (installed via `[standard]`) is a hybrid-search catalog:
Parquet rows + FAISS vectors + BM25 keywords + reciprocal rank fusion.

Three common flows:

1. **Load a published snapshot** via `Catalog.from_url("hf://...")` or
   `file:///...`.
2. **Build a catalog locally** from an `@enumerator` result, then save it
   or push to a URL.
3. **Publish via the CLI** — `parsimony publish --provider NAME --target
   'hf://org/catalog-{namespace}'` reads the plugin's `CATALOGS` export and
   builds one bundle per namespace.

### Loading a published snapshot

```python
import asyncio
from parsimony import Catalog

async def load_snb():
    catalog = await Catalog.from_url("hf://ockham/catalog-snb")
    matches = await catalog.search("policy rate", limit=5, namespaces=["snb"])
    for m in matches:
        print(f"  {m.namespace}/{m.code}: {m.title}  (sim={m.similarity:.3f})")

asyncio.run(load_snb())
```

The first `from_url` call downloads the three-file bundle (`meta.json`,
`entries.parquet`, `embeddings.faiss`) into the local Hugging Face cache;
subsequent calls hit the cache. The embedder recorded in `meta.json` is
reconstructed automatically — its `dim` and `normalize` flags must match
at query time or `ValueError` is raised.

### Building a catalog locally

```python
from parsimony import Catalog, LiteLLMEmbeddingProvider
from parsimony_fred import CONNECTORS as FRED

async def build_fred():
    fred = FRED.bind_deps(api_key="your-key")
    embedder = LiteLLMEmbeddingProvider(
        model="gemini/text-embedding-004",
        dimension=768,
    )
    catalog = Catalog("fred", embedder=embedder)

    result = await fred["enumerate_fred_release"](release_id=10)
    summary = await catalog.add_from_result(result)
    print(f"Indexed {summary.indexed} series, skipped {summary.skipped}")

    await catalog.push("file:///tmp/catalog-fred")
    # await catalog.push("hf://your-org/catalog-fred")
```

`Catalog.push` writes atomically (temp directory + rename), so a
partially-written snapshot is never visible at the destination.

### Dry-run and introspection

```python
summary = await catalog.add_from_result(result, dry_run=True)
print(f"Would index {summary.indexed} entries, skip {summary.skipped}")

namespaces = await catalog.list_namespaces()

entries, total = await catalog.list(
    namespace="fred", q="policy", limit=50, offset=0,
)

entry = await catalog.get(namespace="fred", code="UNRATE")
if entry is not None:
    print(entry.title)
```

### Custom catalog backends

`CatalogBackend` is a `typing.Protocol` with two methods: `add` and `search`.
Any class matching this shape works — no subclassing required. Useful for
Postgres + pgvector, Redis, OpenSearch, or in-memory fakes during testing.

```python
class MyBackend:
    name: str = "mine"

    async def add(self, entries):
        ...

    async def search(self, query, limit=10, *, namespaces=None):
        ...
```

---

## Creating custom connectors

You can build your own connectors using the `@connector`, `@enumerator`,
or `@loader` decorators. Custom connectors integrate seamlessly with
`Connectors` bundles and the catalog.

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
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=params.limit, freq="D"),
        "value": range(params.limit),
    })
```

### With a declared schema

Use `OutputConfig` and `Column` to declare the semantic meaning of each
column:

```python
from typing import Annotated
from pydantic import BaseModel
from parsimony import connector, OutputConfig, Column, ColumnRole

class PriceParams(BaseModel):
    ticker: Annotated[str, "ns:my_source"]

PRICE_OUTPUT = OutputConfig(columns=[
    Column(name="ticker", role=ColumnRole.KEY, namespace="my_source"),
    Column(name="name",   role=ColumnRole.TITLE),
    Column(name="close",  role=ColumnRole.DATA, dtype="numeric"),
    Column(name="volume", role=ColumnRole.DATA, dtype="numeric"),
])

@connector(output=PRICE_OUTPUT)
async def my_prices(params: PriceParams) -> pd.DataFrame:
    """Fetch daily closing prices from my source."""
    return pd.DataFrame(...)
```

The `Annotated[str, "ns:..."]` sentinel is the replacement for the old
`Namespace(...)` annotation class — same agent-cross-ref value, less
machinery.

### With dependency injection

Keyword-only parameters after `*` declare dependencies:

```python
@connector(tags=["custom"])
async def my_authenticated(params: MyParams, *, api_key: str) -> pd.DataFrame:
    """Fetch from an authenticated API."""
    # api_key is injected via bind_deps; never stored in provenance.
    ...

# Bind before use:
bound = my_authenticated.bind_deps(api_key="secret-key")
```

### Packaging as a plugin

To make your connectors discoverable via `build_connectors_from_env`, ship
them as a separate `parsimony-<name>` distribution. See
[`guide-new-plugin.md`](guide-new-plugin.md) for the public path and
[`building-a-private-connector.md`](building-a-private-connector.md) for
the internal/private path.

---

## Working with results

### Accessing the DataFrame and provenance

```python
result = await connectors["fred_fetch"](series_id="GDP")

df = result.data           # pandas DataFrame
prov = result.provenance   # Provenance(source, params, fetched_at, ...)

print(prov.source)         # "fred"
print(prov.params)         # {"series_id": "GDP"}
```

### Promoting a result to schema-aware

If a connector returns a `Result` without a schema but you want the
schema-aware accessors:

```python
from parsimony import OutputConfig, Column, ColumnRole

my_schema = OutputConfig(columns=[
    Column(name="date",  role=ColumnRole.KEY, namespace="my_ns"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
])

schemad = result.to_table(my_schema)

print(schemad.data_columns)      # [Column(name="value", ...)]
print(schemad.metadata_columns)  # [Column(name=..., role=METADATA)]
```

### Arrow and Parquet

```python
# Save
result.to_parquet("/tmp/gdp.parquet")

# Load
from parsimony import Result
loaded = Result.from_parquet("/tmp/gdp.parquet")

# Arrow round-trip
table = result.to_arrow()
restored = Result.from_arrow(table)
```

Schema and provenance are stored in the Arrow table metadata; round-trips
preserve the full shape.

---

## Filtering and composing bundles

### Filter by tag or name

```python
connectors = build_connectors_from_env()

equity = connectors.filter(tags=["equity"])
macro = connectors.filter(tags=["macro"])

fred_only = connectors.filter(name="fred")
```

### Combine bundles

```python
from parsimony import Connectors

custom = Connectors([my_prices, my_authenticated.bind_deps(api_key="key")])
combined = build_connectors_from_env() + custom

result = await combined["my_prices"](ticker="AAPL")
```

### Generate LLM tool descriptions

All connectors in a bundle can be serialized to a prompt-ready text block:

```python
print(connectors.to_llm()[:500])
```

Each connector's entry includes its name, docstring, tags, parameter
names/types, and output columns. This output is consumed by agent
frameworks that route LLM calls to connectors — the MCP server
(`parsimony-mcp`) is one such framework.

---

## Observing calls with callbacks

Attach a post-fetch hook to one connector or a whole bundle:

```python
import logging
logger = logging.getLogger("parsimony.monitor")

async def log_call(result):
    logger.info(
        "connector=%s rows=%d",
        result.provenance.source,
        len(result.data),
    )

monitored = connectors.with_callback(log_call)
```

Callbacks may be sync or async. Exceptions raised inside callbacks are
logged (via `parsimony.connector`), **not raised** — the caller's
`await connector(...)` always returns.

Typical uses: metrics emission, structured log lines, auto-indexing into a
catalog.

---

## MCP server

The MCP server lives in the separate `parsimony-mcp` distribution. It
exposes tool-tagged connectors (`tags=["tool", ...]`) from every installed
plugin to coding agents (Claude Code, Cursor, Windsurf) so the agent can
search for data directly, then fetch and analyze via code execution.

```bash
pip install parsimony-mcp
```

```json
{
  "mcpServers": {
    "parsimony": {
      "command": "parsimony-mcp",
      "env": {
        "FRED_API_KEY": "your-key",
        "FMP_API_KEY": "your-key"
      }
    }
  }
}
```

See [`mcp-setup.md`](mcp-setup.md) for full configuration.
