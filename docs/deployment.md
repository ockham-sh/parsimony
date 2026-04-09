# ockham Integration Guide

**Version**: 0.1.0  
**Audience**: Developers integrating ockham into a Python project or CI environment

This guide covers everything needed to add ockham to a project: installation, dependency setup, environment variable configuration, testing the installation, and known limitations.

---

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Adding ockham to a Project](#adding-ockham-to-a-project)
3. [Required vs Optional Dependencies](#required-vs-optional-dependencies)
4. [Environment Variable Reference](#environment-variable-reference)
5. [Optional Extras](#optional-extras)
6. [Testing the Installation](#testing-the-installation)
7. [Running in a Monorepo](#running-in-a-monorepo)
8. [Known Limitations](#known-limitations)

---

## System Requirements

| Requirement | Constraint |
|-------------|-----------|
| Python | 3.11 or 3.12 only (`>=3.11,<3.13`) |
| Operating system | Any (Linux, macOS, Windows) |
| PyPI availability | **Not yet published** — install from source |

> Python 3.13 is not supported. The upper bound comes from a transitive dependency constraint.

---

## Adding ockham to a Project

### Install from source (recommended until PyPI release)

```bash
# From a local clone of the monorepo
pip install -e /path/to/packages/ockham

# With optional extras
pip install -e /path/to/packages/ockham[sdmx]
pip install -e /path/to/packages/ockham[embeddings]
pip install -e /path/to/packages/ockham[sec]
pip install -e /path/to/packages/ockham[financial-reports]
pip install -e /path/to/packages/ockham[all]
```

### Add to pyproject.toml

If your project uses pyproject.toml, add ockham as a path dependency:

```toml
[project]
dependencies = [
    "ockham @ file:///path/to/packages/ockham",
]
```

Or, if your project is in the same monorepo using a workspace setup:

```toml
[tool.uv.workspace]
members = ["packages/*"]

[project]
dependencies = [
    "ockham",
]
```

### Add to requirements.txt

```
# Install from local path
-e /path/to/packages/ockham

# Or install from a Git URL once the repository is public
# git+https://github.com/your-org/ockham.git#subdirectory=packages/ockham
```

---

## Required vs Optional Dependencies

### Core dependencies

These packages are installed automatically with ockham. All connectors require them.

| Package | Version constraint | Purpose |
|---------|-------------------|---------|
| `pydantic` | `>=2.11.1,<3` | Connector param validation; Pydantic v1 is not supported |
| `pydantic-core` | `>=2.33.0` | Directly imported for `CoreSchema` in connector base class |
| `pandas` | `>=2.3.0,<3` | All connector output DataFrames |
| `pyarrow` | `>=18.0.0` | Arrow/Parquet serialization; uses stable APIs (`pa.Table`, `pq.write_table`) |
| `httpx` | `>=0.28.1` | Async HTTP client for FRED, FMP, EODHD, IBKR, Polymarket |

### Dependency matrix by connector

| Connector(s) | Additional requirement | How to install |
|-------------|----------------------|----------------|
| `fred_*`, `enumerate_fred_release` | `FRED_API_KEY` env var | Set env var |
| `sdmx_*` | `sdmx1` package | `pip install ockham[sdmx]` |
| `fmp_*`, `fmp_screener` | `FMP_API_KEY` env var | Set env var |
| `sec_edgar_fetch` | `edgartools` package | `pip install ockham[sec]` |
| `eodhd_fetch` | `EODHD_API_KEY` env var | Set env var (optional) |
| `ibkr_fetch` | `IBKR_WEB_API_BASE_URL` env var + local IB gateway | Set env var (optional) |
| `polymarket_*` | None | Available immediately |
| `financial_reports_fetch` | `FINANCIAL_REPORTS_API_KEY` + SDK | Set env var + `pip install ockham[financial-reports]` |
| Semantic catalog search | `litellm` package | `pip install ockham[embeddings]` |

---

## Environment Variable Reference

All seven environment variables recognized by ockham are documented below. Variables marked **Required (for connector)** must be set for that connector to be included in the bundle returned by the factory functions. Variables marked **Optional** cause the connector to be silently excluded when absent.

| Variable | Status | Used By | Notes |
|----------|--------|---------|-------|
| `FRED_API_KEY` | Required (for FRED) | `fred_search`, `fred_fetch`, `enumerate_fred_release` | Register at [https://fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `FMP_API_KEY` | Required (for FMP) | All `fmp_*` connectors, `fmp_screener` | Register at [https://financialmodelingprep.com/developer/docs](https://financialmodelingprep.com/developer/docs) |
| `EODHD_API_KEY` | Optional | `eodhd_fetch` | Connector excluded from bundle if absent |
| `IBKR_WEB_API_BASE_URL` | Optional | `ibkr_fetch` | Local IB gateway URL (e.g. `http://localhost:5000`); connector excluded if absent |
| `FINANCIAL_REPORTS_API_KEY` | Optional | `financial_reports_fetch` | Connector excluded if absent; also requires SDK |
| `SEC_EDGAR_USER_AGENT` | Optional | `sec_edgar_fetch` | SEC request identification string (e.g. `"myapp myemail@example.com"`); falls back to `EDGAR_IDENTITY` |
| `EDGAR_IDENTITY` | Optional | `sec_edgar_fetch` | Alternative to `SEC_EDGAR_USER_AGENT`; `SEC_EDGAR_USER_AGENT` takes precedence if both are set |

### Recommended `.env` file

```bash
# Required for base bundle
FRED_API_KEY=your-fred-api-key-here
FMP_API_KEY=your-fmp-api-key-here

# Optional: EODHD historical price data
EODHD_API_KEY=your-eodhd-key-here

# Optional: Interactive Brokers (requires running IB Gateway on localhost)
IBKR_WEB_API_BASE_URL=http://localhost:5000

# Optional: Financial Reports API
FINANCIAL_REPORTS_API_KEY=your-financial-reports-key-here

# Optional: SEC Edgar request identification (recommended by SEC)
SEC_EDGAR_USER_AGENT="MyApp contact@mycompany.com"
```

Use `python-dotenv` or a similar tool to load these in development:

```bash
pip install python-dotenv
```

```python
from dotenv import load_dotenv
load_dotenv()

from ockham.connectors import build_connectors_from_env
connectors = build_connectors_from_env()
```

### Passing env vars programmatically

Both factory functions accept an explicit `env` dict, which is useful in testing or when credentials come from a secrets manager rather than the OS environment:

```python
import asyncio
from ockham.connectors import build_connectors_from_env

connectors = build_connectors_from_env(env={
    "FRED_API_KEY": "test-key",
    "FMP_API_KEY": "test-fmp-key",
})
```

---

## Optional Extras

### `[sdmx]` — SDMX provider support

```bash
pip install ockham[sdmx]
```

Installs: `sdmx1>=2.23.1,<3`

Enables the five SDMX connectors: `sdmx_fetch`, `sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys`. Without this extra, importing `ockham.connectors.sdmx` will raise `ImportError`.

### `[embeddings]` — Semantic catalog search

```bash
pip install ockham[embeddings]
```

Installs: `litellm>=1.59.0,<2`

Enables `LiteLLMEmbeddingProvider` and semantic search via `SeriesCatalog.search(..., semantic=True)`. Without this extra, `SeriesCatalog` works in text-search-only mode.

> **Known issue with litellm**: The `litellm` package declared in this extra is a fork. If you encounter installation failures related to the `litellm` dependency version, verify that the correct fork is available in your package registry. See [Known Limitations](#known-limitations) for details.

### `[sec]` — SEC Edgar filings

```bash
pip install ockham[sec]
```

Installs: `edgartools>=4.0.0`

Enables the `sec_edgar_fetch` connector for accessing SEC EDGAR filings. Without this extra, importing `sec_edgar_fetch` will raise `ImportError`.

### `[financial-reports]` — Financial Reports API

```bash
pip install ockham[financial-reports]
```

Installs: `financial-reports-generated-client>=0.1.0`

Enables the `financial_reports_fetch` connector. Without this extra, importing the connector will raise `ImportError`.

### `[all]` — Everything

```bash
pip install ockham[all]
```

Installs all optional extras: `sdmx`, `embeddings`, `sec`, `financial-reports`.

---

## Testing the Installation

After installing, run this script to verify that the core framework loads and at least one connector is available:

```python
import asyncio
import os
from ockham import Connector, Connectors, Result
from ockham.connectors import build_connectors_from_env

# Verify imports
print("ockham imported successfully")

# Verify factory function runs
connectors = build_connectors_from_env()
print(f"Connector bundle has {len(connectors)} connectors")

# List available connector names
for c in connectors:
    print(f"  {c.name}: {list(c.tags)}")
```

### Verify FRED connectivity

```python
import asyncio
from ockham.connectors import build_connectors_from_env

async def test_fred():
    connectors = build_connectors_from_env()
    if "fred_fetch" not in [c.name for c in connectors]:
        print("fred_fetch not available — check FRED_API_KEY")
        return

    result = await connectors["fred_fetch"]({
        "series_id": "GDP",
        "observation_start": "2023-01-01",
    })
    print(f"FRED test passed: {len(result.data)} rows fetched")
    print(result.data.tail(2))

asyncio.run(test_fred())
```

### Verify SDMX availability

```python
try:
    from ockham.connectors.sdmx import sdmx_fetch
    print("SDMX connectors available")
except ImportError as e:
    print(f"SDMX not available: {e}")
    print("Install with: pip install ockham[sdmx]")
```

### Verify catalog with in-memory store

```python
import asyncio
from ockham import SeriesCatalog, InMemoryCatalogStore

async def test_catalog():
    catalog = SeriesCatalog(InMemoryCatalogStore())
    namespaces = await catalog.list_namespaces()
    print(f"Catalog initialized, namespaces: {namespaces}")

asyncio.run(test_catalog())
```

---

## Running in a Monorepo

ockham lives in a monorepo alongside other packages. When working in the monorepo:

1. Use `uv` or `pip` with editable installs for all packages.
2. Ensure the Python environment resolves the correct local versions of shared packages.
3. The test suite can be run from the package directory:

```bash
cd packages/ockham
pytest tests/
```

Tests that require live credentials are skipped automatically when the relevant env vars are absent. The Supabase catalog test (`test_supabase_catalog_fts.py`) requires a running Supabase instance and will be skipped in CI without credentials.

---

## Known Limitations

### Not yet on PyPI

ockham v0.1.0 has not been published to PyPI. All installations must be done from source. Once published, the install command will be `pip install ockham`.

### Python version constraint

Python 3.13 is not supported. The constraint `python>=3.11,<3.13` is enforced in `pyproject.toml`. Attempting to install on Python 3.13 will fail at the pip resolution stage.

### litellm fork dependency

The `[embeddings]` extra depends on a specific `litellm` fork. If your environment uses the mainline `litellm` from PyPI and the fork is unavailable in your registry, installing the extra will fail. As a workaround, install `litellm` separately from PyPI and verify that `litellm.aembedding()` is available:

```bash
pip install litellm
```

Then install ockham without the `[embeddings]` extra and import `LiteLLMEmbeddingProvider` directly:

```python
from ockham.embeddings.litellm import LiteLLMEmbeddingProvider
```

### SEC Edgar and Financial Reports are optional extras

The `sec_edgar_fetch` and `financial_reports_fetch` connectors require optional extras (`[sec]` and `[financial-reports]` respectively). If the connector fails with `ImportError`, install the relevant extra.

### Supabase backend is external

The production `CatalogStore` and `DataStore` backends (Supabase) are not included in this package. Only in-memory implementations are bundled. To use ockham in production with persistent catalog storage, you must supply a `CatalogStore` implementation backed by your database of choice.

### IBKR connector uses SSL verification disabled

The `ibkr_fetch` connector sets `verify_ssl=False` because the Interactive Brokers Web API gateway runs on localhost with a self-signed certificate. This is expected behavior for the local IB gateway. Do not configure `IBKR_WEB_API_BASE_URL` to point to a remote host, as SSL verification would be skipped.

### FmpScreenerParams.where_clause is a raw pandas query string

The `where_clause` parameter of `fmp_screener` is passed directly to `DataFrame.query()`. If this library is used in a multi-tenant context where `where_clause` could be supplied by untrusted users, this represents a code injection risk. Only allow trusted internal callers to supply this parameter.

### Per-request httpx client creation

`HttpClient` creates a new `httpx.AsyncClient` for every HTTP request. This avoids event loop sharing errors but adds TCP connection overhead for high-volume sequential requests. If you are making large numbers of requests in a tight loop within a single event loop, consider batching requests or re-using connectors that support bulk params.
