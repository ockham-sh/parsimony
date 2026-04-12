# MCP Server Setup

parsimony includes an MCP (Model Context Protocol) server that exposes search and discovery connectors as native tools for coding agents like Claude Code, Cursor, and Windsurf.

## Installation

```bash
pip install -e ".[mcp]"
```

This adds the `mcp` and `tabulate` dependencies.

## Configuration

Add parsimony to your agent's MCP server configuration.

### Claude Code

Add to `~/.claude.json` under `mcpServers`:

```json
{
  "mcpServers": {
    "parsimony": {
      "type": "stdio",
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

### Cursor / Windsurf

Add to your MCP configuration file (typically `.cursor/mcp.json` or equivalent):

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

## What Gets Exposed

The MCP server exposes connectors tagged `"tool"` — these are search and discovery connectors that return small, context-friendly results. Bulk data fetch connectors are not exposed as MCP tools; agents use them via code execution with `from parsimony import client`.

Currently exposed tools (~18):

- **FRED**: `fred_search`
- **SDMX**: `sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys`
- **FMP**: `fmp_search`, `fmp_taxonomy`, `fmp_company_profile`, `fmp_peers`, `fmp_index_constituents`, `fmp_market_movers`, `fmp_screener`
- **SEC Edgar**: `sec_edgar_find_company`, `sec_edgar_company_profile`, `sec_edgar_search_filings`
- **Financial Reports**: `fr_companies_search`, `fr_isic_browse`, `fr_isin_lookup`

## How It Works

The MCP server is a thin bridge between parsimony's `Connectors` collection and the MCP protocol:

1. On startup, `build_connectors_from_env()` loads all connectors with available API keys
2. Connectors tagged `"tool"` are filtered and registered as MCP tools
3. Each tool's `name`, `description`, and `inputSchema` come directly from the connector metadata
4. Server instructions (injected into the agent's system prompt) are generated from `connectors.to_llm(context="mcp")`

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
