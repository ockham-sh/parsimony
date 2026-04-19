# parsimony

**Typed, composable data connectors with searchable catalogs for Python.**

Every financial data project starts the same way: write API wrappers, parse responses into DataFrames, track where data came from. `parsimony` replaces that boilerplate with a declarative `@connector` system that gives you Pydantic-validated parameters, standardized `Result` outputs with provenance tracking, composable routing across sources, and an optional vector-searchable catalog for entity discovery.

## Quick Start

```python
from parsimony_fred import CONNECTORS as FRED

client = FRED.bind_deps(api_key="your-fred-key")

result = await client["fred_fetch"](series_id="GDPC1", observation_start="2020-01-01")
print(result.data)        # pandas DataFrame
print(result.provenance)  # source="fred_fetch", params={...}
```

## Installation

Pick what you need. The kernel has no connectors of its own:

```bash
pip install parsimony-core                       # kernel only
pip install parsimony-core parsimony-fred        # + FRED
pip install parsimony-core parsimony-sdmx        # + SDMX (ECB, Eurostat, IMF, OECD, BIS, World Bank, ILO)
pip install 'parsimony-core[standard]'           # + canonical Catalog (FAISS + BM25 + sentence-transformers + hf://)
pip install 'parsimony-core[standard,litellm]'   # + hosted embeddings (OpenAI, Gemini, Cohere, Voyage, Bedrock)
pip install parsimony-mcp                        # MCP server (separate distribution)
```

!!! note "Distribution name vs import name"
    The PyPI distribution is **`parsimony-core`**; imports remain `from parsimony import ...`.
    The bare `parsimony` name on PyPI is currently unavailable — we plan to migrate the
    distribution name to `parsimony` once it becomes available. The import path will not change.

## Built-in Data Sources

| Source | Connectors | API Key |
|--------|-----------|---------|
| **FRED** (Federal Reserve Economic Data, via `parsimony-fred` plugin) | `fred_search`, `fred_fetch` | Free ([register](https://fred.stlouisfed.org/docs/api/api_key.html)) |
| **SDMX** (ECB, Eurostat, IMF, World Bank, via `parsimony-sdmx` plugin) | `sdmx_fetch` + `sdmx_datasets` / `sdmx_series_{agency}_{dataset_id}` catalog bundles | None |
| **FMP** (Financial Modeling Prep) | `fmp_stock_quote`, `fmp_income_statements`, `fmp_balance_sheet_statements`, `fmp_historical_prices`, `fmp_company_profile` | Paid |
| **SEC Edgar** | `sec_edgar_fetch` | None |
| **Polymarket** | `polymarket_clob_fetch`, `polymarket_gamma_fetch` | None |
| **EODHD** | `eodhd_fetch` | Paid |
| **Financial Reports** | `financial_reports_fetch` | Paid |
| **+ 15 more** | CoinGecko, Finnhub, Tiingo, Alpha Vantage, EIA, BLS, Treasury, central banks | Varies |

## Core Concepts

The framework centers on three **decorator primitives**, all producing the same runtime type (`Connector`):

- **`@connector`** — typed fetch/search operations with Pydantic-validated parameters
- **`@enumerator`** — catalog population: KEY + TITLE + METADATA, no DATA
- **`@loader`** — observation persistence: KEY + DATA only

Connectors compose into bundles via `Connectors`, bind dependencies once with `bind_deps()`, and dispatch by name. Every call returns a `Result` with the data plus provenance metadata.

## Next Steps

- [Quick Start](quickstart.md) — get up and running in 5 minutes
- [User Guide](user-guide.md) — detailed walkthrough of all features
- [Building Connectors](connector-implementation-guide.md) — full guide: provider research, implementation, patterns, and testing
- [API Reference](api-reference.md) — complete API documentation
- [Architecture](architecture.md) — system design and internals

## License

Apache 2.0
