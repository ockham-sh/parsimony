# ockham

**Typed, composable data connectors with searchable catalogs for Python.**

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

With optional extras:

```bash
pip install ockham[sdmx]        # SDMX providers (ECB, Eurostat, IMF, World Bank)
pip install ockham[embeddings]  # Vector-searchable catalog
pip install ockham[all]         # Everything
```

## Built-in Data Sources

| Source | Connectors | API Key |
|--------|-----------|---------|
| **FRED** (Federal Reserve Economic Data) | `fred_search`, `fred_fetch` | Free ([register](https://fred.stlouisfed.org/docs/api/api_key.html)) |
| **SDMX** (ECB, Eurostat, IMF, World Bank, ...) | `sdmx_fetch`, `sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys` | None |
| **FMP** (Financial Modeling Prep) | `fmp_stock_quote`, `fmp_income_statements`, `fmp_balance_sheet_statements`, `fmp_historical_prices`, `fmp_company_profile` | Paid |
| **SEC Edgar** | `sec_edgar_fetch` | None |
| **Polymarket** | `polymarket_clob_fetch`, `polymarket_gamma_fetch` | None |
| **EODHD** | `eodhd_fetch` | Paid |
| **IBKR** (Interactive Brokers) | `ibkr_fetch` | Gateway required |
| **Financial Reports** | `financial_reports_fetch` | Paid |

## Core Concepts

The framework centers on three **decorator primitives**, all producing the same runtime type (`Connector`):

- **`@connector`** — typed fetch/search operations with Pydantic-validated parameters
- **`@enumerator`** — catalog population: KEY + TITLE + METADATA, no DATA
- **`@loader`** — observation persistence: KEY + DATA only

Connectors compose into bundles via `Connectors`, bind dependencies once with `bind_deps()`, and dispatch by name. Every call returns a `Result` with the data plus provenance metadata.

## Next Steps

- [User Guide](user-guide.md) — detailed walkthrough of all features
- [Building Connectors](connector-implementation-guide.md) — create your own data connectors
- [API Reference](api-reference.md) — complete API documentation
- [Architecture](architecture.md) — system design and internals

## License

Apache 2.0
