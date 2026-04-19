# parsimony

[![PyPI version](https://img.shields.io/pypi/v/parsimony)](https://pypi.org/project/parsimony/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/parsimony)](https://pypi.org/project/parsimony/)
[![CI](https://github.com/ockham-sh/parsimony/actions/workflows/test.yml/badge.svg)](https://github.com/ockham-sh/parsimony/actions)
[![Docs](https://img.shields.io/badge/docs-parsimony.dev-blue)](https://docs.parsimony.dev)

Typed, composable data connectors with searchable catalogs for Python.

## Why parsimony?

- **Unified interface** -- one async calling convention (`await connectors["name"](params)`) across FRED, SDMX, FMP, SEC Edgar, Polymarket, and more.
- **Typed parameters** -- every connector validates input through a Pydantic model with a JSON Schema for agent integration.
- **Provenance on every result** -- every `Result` carries its source, params, and fetch timestamp alongside the DataFrame.
- **Searchable catalog** -- index entities from any source into a `Catalog` with optional vector embeddings for semantic search.
- **MCP integration** -- expose connectors as [Model Context Protocol](https://modelcontextprotocol.io/) tools for AI agents.

## Install

The PyPI distribution name is `parsimony-core` (the bare `parsimony` name is squatted); the import path is unchanged, `from parsimony import ...`.

```bash
pip install parsimony-core              # core: connectors + BaseCatalog ABC
pip install 'parsimony-core[standard]'           # + standard Catalog (Parquet + FAISS + BM25 + sentence-transformers, hf:// loader)
pip install 'parsimony-core[standard,litellm]'   # + LiteLLMEmbeddingProvider for hosted-API embeddings (OpenAI, Gemini, ...)
pip install 'parsimony-core[s3]'        # + s3:// support
pip install 'parsimony-core[all]'       # standard + s3
```

Connector plugins live in their own packages and can be installed independently:

```bash
pip install parsimony-sdmx parsimony-edgar parsimony-financial-reports
pip install parsimony-starter      # meta: parsimony-core[standard] + the curated plugin set
```

## 30-Second Example (No API Key)

Fetch daily USD/EUR exchange rates from the ECB:

```python
import asyncio
from parsimony_sdmx import CONNECTORS as SDMX  # `pip install parsimony-sdmx`

async def main():
    result = await SDMX["sdmx"](
        dataset_key="ECB-EXR",
        series_key="D.USD.EUR.SP00.A",
        start_period="2024-01",
    )
    print(result.data.tail())
    print(result.provenance)

asyncio.run(main())
```

```
  series_key                       title  ... TIME_PERIOD    value
  D.USD.EUR.SP00.A  US dollar/Euro (EXR) ... 2024-12-27   1.0427
  D.USD.EUR.SP00.A  US dollar/Euro (EXR) ... 2024-12-30   1.0389
```

## With API Keys

FRED provides US macroeconomic data. Get a free key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html):

```python
from parsimony.connectors.fred import CONNECTORS as FRED

fred = FRED.bind_deps(api_key="your-key")

# Search
search = await fred["fred_search"](search_text="US unemployment rate")
print(search.data[["id", "title"]].head())

# Fetch
result = await fred["fred"](series_id="UNRATE", observation_start="2020-01-01")
print(result.data.tail())
print(result.provenance)
```

## Built-in Data Sources

| Source | API Key | Category |
|--------|---------|----------|
| **FRED** (Federal Reserve Economic Data) | Free | Macro |
| **SDMX** (ECB, Eurostat, IMF, World Bank, BIS) | None | Macro |
| **FMP** (Financial Modeling Prep) | Paid | Equities |
| **SEC Edgar** | None | Filings |
| **EODHD** (End of Day Historical Data) | Paid | Multi-asset |
| **Polymarket** | None | Prediction markets |
| **CoinGecko** | Free | Crypto |
| **Finnhub** | Free | News & events |
| **Tiingo** | Free | Equities |
| **Alpha Vantage** | Free | Equities |
| **EIA** (Energy Information Administration) | Free | Energy |
| **BLS** (Bureau of Labor Statistics) | Free | Employment |
| **US Treasury** | None | Bonds |
| **Central Banks** (SNB, RBA, Riksbank, BDE, BOJ, BOC, BDP, BDF, Destatis) | None | Macro |

## Features

**Three decorator primitives** -- all produce the same `Connector` runtime type:

- `@connector` -- typed fetch/search; `output=` is optional for schema-aware results.
- `@enumerator` -- catalog population (KEY + TITLE + METADATA, no DATA); requires `output=`.
- `@loader` -- observation persistence (KEY + DATA only); requires `output=` and a `DataStore`.

**Catalog** -- `BaseCatalog` is the ABC; the standard `parsimony.Catalog` (Parquet + FAISS + BM25 + RRF, `pip install 'parsimony-core[standard]'`) indexes entities by `(namespace, code)` from any connector result, persists as a three-file snapshot (`meta.json` + `entries.parquet` + `embeddings.faiss`), and loads from `file://`, `hf://`, and `s3://` (planned) URLs. Custom backends subclass `BaseCatalog` directly — there is no plugin axis for catalogs.

**Embedders** -- two implementations of the `EmbeddingProvider` contract ship with the catalog: `SentenceTransformerEmbedder` (local model, default `BAAI/bge-small-en-v1.5`) and `LiteLLMEmbeddingProvider` (hosted APIs — OpenAI, Gemini, Cohere, Voyage, Bedrock — via `parsimony-core[litellm]`). Subclass `EmbeddingProvider` for anything else and pass it as `Catalog("name", embedder=...)`.

**Provenance** -- every result tracks source, parameters, and fetch timestamp. Serialize to Arrow/Parquet for reproducible pipelines.

**Composable routing** -- combine connectors from multiple sources with `+`, bind dependencies once with `bind_deps()`, attach callbacks with `with_callback()`.

**MCP server** -- install [`parsimony-mcp`](https://github.com/ockham-sh/parsimony) and run `parsimony-mcp` to expose all configured connectors as MCP tools for Claude, GPT, and other AI agents.

## Documentation

Full docs at [docs.parsimony.dev](https://docs.parsimony.dev):

- [Quickstart](https://docs.parsimony.dev/quickstart/) -- zero to fetching data in five minutes
- [User Guide](https://docs.parsimony.dev/user-guide/) -- custom connectors, catalog, data stores
- [Architecture](https://docs.parsimony.dev/architecture/) -- design principles and internals
- [API Reference](https://docs.parsimony.dev/api-reference/) -- full class and function reference
- [Connector Guide](https://docs.parsimony.dev/connector-implementation-guide/) -- building new connectors

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding conventions, and the connector checklist.

## License

Apache 2.0
