# Frequently Asked Questions

## Installation

### What Python version do I need?

parsimony requires **Python 3.11+**.

### What are the optional extras?

SDMX support is included in the base install. The remaining optional extras are:

| Extra | Install command | What it enables |
|-------|----------------|-----------------|
| `search` | `pip install parsimony-core[search]` | Semantic catalog search via LiteLLM embeddings + sqlite-vec |
| `sec` | `pip install parsimony-core[sec]` | SEC Edgar connector via edgartools |
| `mcp` | `pip install parsimony-core[mcp]` | MCP server for AI agents |
| `all` | `pip install parsimony-core[all]` | Everything |

Install multiple extras at once:

```bash
pip install "parsimony-core[sec,search]"
```

### I get `ModuleNotFoundError: No module named 'edgartools'`

The SEC Edgar connector requires a separately-installed package:

```bash
pip install edgartools
```

---

## API Keys

### Where do I get a FRED API key?

Register for free at [https://fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html). Approval is instant.

### How do I configure API keys?

Set environment variables in your shell or a `.env` file:

```bash
# .env
FRED_API_KEY=your-fred-key
FMP_API_KEY=your-fmp-key
```

Then load them before running your script:

```bash
# Shell
export FRED_API_KEY="your-fred-key"
python my_script.py

# Or use a .env loader like python-dotenv
```

In code, the `build_connectors_from_env()` factory reads `os.environ` and injects keys automatically:

```python
from parsimony.connectors import build_connectors_from_env

connectors = build_connectors_from_env()
```

You can also bind keys manually on individual connectors:

```python
from parsimony.connectors.fred import fred_fetch

bound = fred_fetch.bind_deps(api_key="your-key")
result = await bound(series_id="GDP")
```

### Which connectors need API keys?

| Connector | Env variable | Required? |
|-----------|-------------|-----------|
| FRED | `FRED_API_KEY` | Yes (for FRED) |
| FMP / FMP Screener | `FMP_API_KEY` | Yes (for FMP) |
| EODHD | `EODHD_API_KEY` | Optional |
| Financial Reports | `FINANCIAL_REPORTS_API_KEY` | Optional |
| SDMX (ECB, Eurostat, etc.) | None | No key needed |
| Polymarket | None | No key needed |
| SEC Edgar | `SEC_EDGAR_USER_AGENT` | Optional (request ID) |

---

## Async Patterns

### How do I call connectors in a regular Python script?

Wrap your async code in `asyncio.run()`:

```python
import asyncio

async def main():
    result = await connectors["fred_fetch"](series_id="GDP")
    print(result.df.tail())

asyncio.run(main())
```

### How do I use parsimony in a Jupyter notebook?

Jupyter notebooks already run an event loop. Use `await` directly in a cell:

```python
result = await connectors["fred_fetch"](series_id="GDP")
result.df.tail()
```

### I get `SyntaxError: 'await' outside async function`

You are calling `await` in a synchronous context. Either:
- Wrap your code in `async def main()` and call `asyncio.run(main())`
- Or use a Jupyter notebook where top-level `await` works natively

### I get `RuntimeError: This event loop is already running`

This happens when calling `asyncio.run()` inside an environment that already has a running event loop (e.g., Jupyter). Use `await` directly instead of `asyncio.run()`. If you need to use parsimony from synchronous library code, consider `nest_asyncio`:

```python
import nest_asyncio
nest_asyncio.apply()
```

---

## Common Errors

### `TypeError: Connector 'fred_fetch' has unbound dependencies`

You called a connector that requires API keys without binding them first. Call `bind_deps()`:

```python
from parsimony.connectors.fred import fred_fetch

bound = fred_fetch.bind_deps(api_key="your-key")
result = await bound(series_id="GDP")
```

### Rate limit errors

FRED and FMP both have rate limits. FRED allows 120 requests per minute. FMP limits vary by plan. If you hit rate limits:

- Add delays between requests in batch operations
- Filter with `connectors.filter(tags=["tool"])` for a smaller surface when you only need interactive tool operations
- Check your FMP plan tier for API limits

### Empty results

If a connector raises `ValueError("Returned an empty DataFrame")`:

- Check your parameters (wrong `series_id`, date range with no data, etc.)
- For SDMX, verify the `dataset_key` format includes the agency prefix (e.g., `ECB-EXR`, not just `EXR`)
- For FRED search, try shorter or more general search terms

### `ImportError: cannot import name 'Catalog' from 'parsimony'`

The class is named `Catalog`, not `Catalog`:

```python
from parsimony import Catalog
```

---

## Security

### How does `bind_deps` protect API keys?

When you call `connector.bind_deps(api_key="secret")`, the key is injected as a keyword-only argument to the underlying function via `functools.partial`. It never appears in:

- The `Provenance.params` dict (which only records user-facing parameters)
- JSON schema output
- The `to_llm()` descriptions

This means API keys stay out of logs, LLM prompts, and serialized results.

### Is the FMP screener `where_clause` safe?

No. The `where_clause` parameter uses `DataFrame.query()` internally, which can execute arbitrary Python expressions. **Never pass untrusted user input** as a `where_clause` value. Only use it with trusted, developer-authored filter strings.

---

## Production

### How should I manage secrets in production?

Use your platform's secrets manager instead of environment variables in `.env` files:

- **AWS**: Secrets Manager or SSM Parameter Store
- **GCP**: Secret Manager
- **Azure**: Key Vault
- **Docker/K8s**: Docker secrets or Kubernetes Secrets

Pass retrieved secrets to `build_connectors_from_env(env={"FRED_API_KEY": secret_value})` using the `env` parameter to override `os.environ`.

### Is there connection pooling?

Each connector call creates a fresh HTTP client. For high-throughput batch operations, this is generally fine because Python's `httpx` and `requests` handle connection reuse at the TCP level. If you need explicit connection pooling, build connectors manually and inject a shared `httpx.AsyncClient` as a dependency.

### How do I monitor connector calls?

Use `with_callback` to attach monitoring hooks:

```python
import logging

logger = logging.getLogger("parsimony.monitor")

async def log_call(result):
    logger.info(
        "connector=%s rows=%d",
        result.provenance.source,
        len(result.df) if hasattr(result, "df") else 0,
    )

monitored = connectors.with_callback(log_call)
```

For structured observability, emit metrics or traces from the callback to your telemetry stack (Prometheus, OpenTelemetry, etc.).
