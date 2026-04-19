# parsimony-starter

Curated meta-package for [parsimony](https://parsimony.dev). One install pulls
the typical batteries-included surface: the standard catalog stack, the most
common connectors, and the MCP adapter.

## Install

```bash
pip install parsimony-starter
```

## What you get

| Package | Purpose |
|---------|---------|
| `parsimony-core[standard]` | Core + standard `Catalog` (Parquet + FAISS + BM25 + sentence-transformers) and `hf://` loader |
| `parsimony-sdmx` | SDMX connectors (ECB, Eurostat, IMF, World Bank, …) |
| `parsimony-edgar` | SEC EDGAR connectors (15 endpoints) |
| `parsimony-financial-reports` | FinancialReports.eu connectors |
| `parsimony-mcp` | MCP server adapter |

## When to install individual packages instead

`parsimony-starter` pulls in `sentence-transformers` (and therefore `torch`).
If you only need the connector framework, install `parsimony-core` directly and
add just the plugins you use:

```bash
pip install parsimony-core parsimony-sdmx parsimony-edgar
pip install 'parsimony-core[standard]'   # add the standard Catalog later if needed
```
