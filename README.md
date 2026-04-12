# parsimony

[![PyPI version](https://img.shields.io/pypi/v/parsimony)](https://pypi.org/project/parsimony/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/parsimony)](https://pypi.org/project/parsimony/)
[![CI](https://github.com/ockham-sh/parsimony/actions/workflows/test.yml/badge.svg)](https://github.com/ockham-sh/parsimony/actions)

Typed, composable data connectors with searchable catalogs for Python.

## Why parsimony?

- **Unified interface** -- one async calling convention (`await connectors["name"](params)`) across FRED, SDMX, FMP, SEC Edgar, Polymarket, and more.
- **Typed parameters** -- every connector validates input through a Pydantic model with a JSON Schema for agent integration.
- **Provenance on every result** -- every `Result` carries its source, params, and fetch timestamp alongside the DataFrame.
- **Searchable catalog** -- index entities from any source into a `Catalog` with optional vector embeddings for semantic search.

## Install

```bash
pip install parsimony           # core + FRED
pip install parsimony[sdmx]     # + ECB, Eurostat, IMF, World Bank (no API key needed)
```

## 30-Second Example (No API Key)

Fetch daily USD/EUR exchange rates from the ECB:

```python
import asyncio
from parsimony.connectors.sdmx import CONNECTORS as SDMX

async def main():
    result = await SDMX["sdmx_fetch"](
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
result = await fred["fred_fetch"](series_id="UNRATE", observation_start="2020-01-01")
print(result.data.tail())
print(result.provenance)
```

## Built-in Data Sources

| Source | Connectors | API Key |
|--------|-----------|---------|
| **FRED** (Federal Reserve Economic Data) | `fred_search`, `fred_fetch` | Free ([register](https://fred.stlouisfed.org/docs/api/api_key.html)) |
| **SDMX** (ECB, Eurostat, IMF, World Bank, BIS) | `sdmx_fetch`, `sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys` | None |
| **FMP** (Financial Modeling Prep) | `fmp_stock_quote`, `fmp_income_statements`, `fmp_balance_sheet_statements`, `fmp_historical_prices`, `fmp_company_profile` | Paid |
| **SEC Edgar** | `sec_edgar_fetch` | None |
| **Polymarket** | `polymarket_clob_fetch`, `polymarket_gamma_fetch` | None |
| **EODHD** | `eodhd_fetch` | Paid |
| **Financial Reports** | `financial_reports_fetch` | Paid |

## Features

**Three decorator primitives** -- all produce the same `Connector` runtime type:

- `@connector` -- typed fetch/search; `output=` is optional for schema-aware results.
- `@enumerator` -- catalog population (KEY + TITLE + METADATA, no DATA); requires `output=`.
- `@loader` -- observation persistence (KEY + DATA only); requires `output=` and a `DataStore`.

**Catalog** -- `Catalog` indexes entities by `(namespace, code)` from any connector result. Supports text search out of the box and semantic search with optional `LiteLLMEmbeddingProvider`.

**Provenance** -- every result tracks source, parameters, and fetch timestamp. Serialize to Arrow/Parquet for reproducible pipelines.

**Composable routing** -- combine connectors from multiple sources with `+`, bind dependencies once with `bind_deps()`, attach callbacks with `with_callback()`.

## Documentation

- [Quickstart](docs/quickstart.md) -- zero to fetching data in five minutes
- [User Guide](docs/user-guide.md) -- custom connectors, catalog, data stores
- [Architecture](docs/architecture.md) -- design principles and internals
- [API Reference](docs/api-reference.md) -- full class and function reference
- [Connector Implementation Guide](docs/connector-implementation-guide.md) -- building new connectors

## License

Apache 2.0
