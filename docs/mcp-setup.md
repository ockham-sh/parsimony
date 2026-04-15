# MCP Server Setup

parsimony includes an MCP (Model Context Protocol) server that exposes search and discovery connectors as native tools for coding agents like Claude Code, Cursor, and Windsurf.

## Installation

```bash
pip install -e ".[mcp]"
```

This adds the `mcp` and `tabulate` dependencies.

## Configuration

Add parsimony to your agent's MCP server configuration.

> **Important:** The `command` must point to the Python interpreter where parsimony is installed. Using bare `python3` will resolve to your system Python, which almost certainly does not have parsimony. Use the full path to your virtualenv's Python (e.g. `/path/to/your/venv/bin/python3`).

### Claude Code

Add to `~/.claude.json` under `mcpServers`:

```json
{
  "mcpServers": {
    "parsimony": {
      "type": "stdio",
      "command": "/path/to/your/venv/bin/python3",
      "args": ["-m", "parsimony.mcp"],
      "env": {
        "FRED_API_KEY": "your-key",
        "FMP_API_KEY": "your-key"
      }
    }
  }
}
```

### Cursor / Windsurf

Add to your MCP configuration file (typically `.cursor/mcp.json` or equivalent):

```json
{
  "mcpServers": {
    "parsimony": {
      "command": "/path/to/your/venv/bin/python3",
      "args": ["-m", "parsimony.mcp"],
      "env": {
        "FRED_API_KEY": "your-key",
        "FMP_API_KEY": "your-key"
      }
    }
  }
}
```

> **Tip:** To find the correct path, run `which python3` with your virtualenv activated, or use `<project-root>/.venv/bin/python3` if you installed with `uv`.

### Environment Variables

The server reads API keys from environment variables. You can set them in the `env` block above or in your shell environment. Only connectors with available keys will be loaded.

| Variable | Provider |
|----------|----------|
| `FRED_API_KEY` | FRED (US macro) |
| `FMP_API_KEY` | Financial Modeling Prep (equities) |
| `EIA_API_KEY` | EIA (US energy) |
| `BLS_API_KEY` | BLS (US labor) |
| `RIKSBANK_API_KEY` | Riksbank (Sweden) |
| `FINANCIAL_REPORTS_API_KEY` | Financial Reports (global filings) |

SDMX connectors (ECB, Eurostat, IMF, World Bank, BIS, OECD) and SEC Edgar require no API keys.

| Variable | Default | Purpose |
|----------|---------|---------|
| `PARSIMONY_CATALOG_TTL_DAYS` | `7` | Days before cached catalog files are re-downloaded |
| `PARSIMONY_CATALOG_REPO` | `ockham-sh/parsimony-catalogs` | GitHub repo for pre-built catalogs |
| `PARSIMONY_CATALOG_BRANCH` | `main` | Branch to download from |
| `PARSIMONY_CACHE_DIR` | `~/.cache/parsimony/catalogs` | Local cache directory |

## What Gets Exposed

The MCP server exposes connectors tagged `"tool"` — these are search and discovery connectors that return small, context-friendly results. Bulk data fetch connectors are not exposed as MCP tools; agents use them via code execution with `from parsimony import client`.

Currently exposed tools:

- **Catalog**: `catalog_search` — keyword search across all indexed providers (macro, central bank, SDMX agencies)
- **FRED**: `fred_search`
- **SDMX**: `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys`
- **FMP**: `fmp_search`, `fmp_taxonomy`, `fmp_company_profile`, `fmp_peers`, `fmp_index_constituents`, `fmp_market_movers`, `fmp_screener`
- **SEC Edgar**: `sec_edgar_find_company`, `sec_edgar_company_profile`, `sec_edgar_search_filings`
- **Financial Reports**: `fr_companies_search`, `fr_isic_browse`, `fr_isin_lookup`

> **Note:** `sdmx_list_datasets` was replaced by `catalog_search` — SDMX dataset discovery is now catalog-backed (offline, pre-indexed) instead of hitting slow live APIs.

## How It Works

The MCP server is a thin bridge between parsimony's `Connectors` collection and the MCP protocol:

1. On startup, `build_connectors_from_env()` loads all connectors with available API keys
2. A `Catalog` (SQLite-backed) is instantiated and `warm()` is called to pre-populate from GitHub pre-built catalogs
3. The `catalog_search` and `catalog_list_namespaces` connectors are bound to the catalog and added to the connector collection
4. The full connector collection is passed to `create_server()`, which registers tool-tagged connectors as native MCP tools and generates instructions from `connectors.to_llm()` for all client-callable (non-tool) connectors
5. Each tool's `name`, `description`, and `inputSchema` come directly from the connector metadata

## Catalog Cache

Pre-built catalog databases are downloaded from GitHub on first startup and cached locally:

```
~/.cache/parsimony/catalogs/{namespace}/catalog.db
```

Cached files auto-refresh every 7 days (configurable via `PARSIMONY_CATALOG_TTL_DAYS`). To force a refresh:

```bash
rm -rf ~/.cache/parsimony/catalogs/
# Then restart the MCP server
```

## Running Manually

```bash
# Test that the server starts
python -m parsimony.mcp
```

The server communicates over stdio — it won't produce visible output. Press Ctrl+C to stop.

## Adding New Tools

To expose a new connector as an MCP tool, add `"tool"` to its tags:

```python
@connector(tags=["macro", "tool"])
async def my_search(params: MySearchParams, *, api_key: str) -> Result:
    ...
```

The MCP server picks it up automatically on next restart.
