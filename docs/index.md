# ockham

Typed, composable data connectors with searchable catalogs for Python.

## Install

```bash
pip install ockham
```

## I want to...

| Goal | Start here |
|------|-----------|
| Fetch FRED, SDMX, or FMP data in five minutes | [Quick Start](quickstart.md) |
| Understand connectors, catalogs, and loaders | [User Guide](user-guide.md) |
| See ready-made recipes and patterns | [Cookbook](cookbook.md) |
| Browse all built-in data sources | [Internal Data Sources](internal-connectors.md) |
| Build my own connector | [Connector Guide](connector-guide.md) |
| Look up a class or function signature | [API Reference](api-reference.md) |
| Understand the internals | [Architecture](architecture.md) |

## Built-in Data Sources

| Source | Connectors | Auth |
|--------|-----------|------|
| **FRED** | `fred_search`, `fred_fetch`, `enumerate_fred_release` | Free API key |
| **SDMX** (ECB, Eurostat, IMF, World Bank, BIS) | `sdmx_fetch`, `sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys` | None |
| **FMP** | 18 connectors (quotes, financials, screener, ...) | Paid API key |
| **SEC Edgar** | `sec_edgar_fetch` | None |
| **Polymarket** | `polymarket_clob_fetch`, `polymarket_gamma_fetch` | None |
| **EODHD** | `eodhd_fetch` | Paid API key |
| **IBKR** | `ibkr_fetch` | Local gateway |
| **Financial Reports** | `financial_reports_fetch` | Paid API key |

## Minimal example

```python
import asyncio
from ockham.connectors import build_connectors_from_env

async def main():
    connectors = build_connectors_from_env()
    result = await connectors["fred_fetch"]({"series_id": "GDP"})
    print(result.data.tail())

asyncio.run(main())
```

## License

Apache 2.0 -- see [GitHub](https://github.com/ockham-sh/ockham).
