# Frequently asked questions

## Installation

### What Python version do I need?

`parsimony-core` requires **Python 3.11+**.

### What are the optional extras?

The kernel (`parsimony-core`) is tiny — it ships the connector primitives,
the `CatalogBackend` Protocol, and plugin discovery. The canonical catalog
and hosted-embeddings paths are extras:

| Extra | Install command | What it enables |
|---|---|---|
| `standard` | `pip install parsimony-core[standard]` | Canonical `Catalog` — Parquet + FAISS + BM25 + sentence-transformers + `hf://` loader |
| `litellm` | `pip install parsimony-core[standard,litellm]` | Hosted embeddings via the LiteLLM unified API |
| `s3` | `pip install parsimony-core[standard,s3]` | `s3://` URLs in `Catalog.from_url` / `Catalog.push` (planned) |
| `all` | `pip install parsimony-core[all]` | `standard + litellm + s3` |

Connectors ship as **separate distributions** — `parsimony-fred`,
`parsimony-sdmx`, `parsimony-fmp`, etc. — discovered through the
`parsimony.providers` entry point. Install whichever sources you need
alongside the kernel:

```bash
pip install parsimony-core parsimony-fred parsimony-sdmx
```

The MCP server lives in its own distribution:

```bash
pip install parsimony-mcp
```

---

## API keys

### Where do I get a FRED API key?

Register for free at
[https://fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html).
Approval is instant.

### How do I configure API keys?

Set environment variables in your shell or a `.env` file:

```bash
# .env
FRED_API_KEY=your-fred-key
FMP_API_KEY=your-fmp-key
```

Then load them before running your script:

```bash
export FRED_API_KEY="your-fred-key"
python my_script.py

# Or use python-dotenv
```

In code, `build_connectors_from_env()` reads `os.environ` and binds each
plugin's declared env vars automatically:

```python
from parsimony.discovery import build_connectors_from_env

connectors = build_connectors_from_env()
```

You can also bind keys manually on individual plugins:

```python
from parsimony_fred import CONNECTORS as FRED

bound = FRED.bind_deps(api_key="your-key")
result = await bound["fred_fetch"](series_id="GDP")
```

### Which connectors need API keys?

Every plugin declares its own `ENV_VARS`. Typical examples:

| Plugin | Env variable | Required? |
|---|---|---|
| `parsimony-fred` | `FRED_API_KEY` | Yes |
| `parsimony-fmp` / `parsimony-fmp-screener` | `FMP_API_KEY` | Yes |
| `parsimony-eodhd` | `EODHD_API_KEY` | Yes |
| `parsimony-finnhub` | `FINNHUB_API_KEY` | Yes |
| `parsimony-tiingo` | `TIINGO_API_KEY` | Yes |
| `parsimony-coingecko` | `COINGECKO_API_KEY` | Yes |
| `parsimony-eia` | `EIA_API_KEY` | Yes |
| `parsimony-financial-reports` | `FINANCIAL_REPORTS_API_KEY` | Yes |
| `parsimony-sdmx` (ECB, Eurostat, IMF, World Bank, BIS, OECD, ILO) | none | No |
| `parsimony-polymarket` | none | No |
| `parsimony-sec-edgar` | `SEC_EDGAR_USER_AGENT` | Optional (request identifier) |

---

## Async patterns

### How do I call connectors in a regular Python script?

Wrap your async code in `asyncio.run()`:

```python
import asyncio

async def main():
    result = await connectors["fred_fetch"](series_id="GDP")
    print(result.data.tail())

asyncio.run(main())
```

### How do I use parsimony in a Jupyter notebook?

Jupyter notebooks already run an event loop. Use `await` directly in a
cell:

```python
result = await connectors["fred_fetch"](series_id="GDP")
result.data.tail()
```

### I get `SyntaxError: 'await' outside async function`

You are calling `await` in a synchronous context. Either:

- Wrap in `async def main()` and call `asyncio.run(main())`, or
- Use a Jupyter notebook where top-level `await` works natively.

### I get `RuntimeError: This event loop is already running`

This happens when calling `asyncio.run()` inside an environment that
already has a running event loop (e.g. Jupyter). Use `await` directly
instead of `asyncio.run()`. If you need parsimony from synchronous
library code, consider `nest_asyncio`:

```python
import nest_asyncio
nest_asyncio.apply()
```

---

## Common errors

### `TypeError: Connector '...' has unbound dependencies`

You called a connector that requires API keys without binding them
first. Call `bind_deps()`:

```python
from parsimony_fred import CONNECTORS as FRED

bound = FRED.bind_deps(api_key="your-key")
result = await bound["fred_fetch"](series_id="GDP")
```

### Rate limit errors

Every upstream rate-limit signal maps to `RateLimitError`. The exception
carries `.retry_after: float` (seconds) and
`.quota_exhausted: bool` — the former is retryable, the latter is
terminal.

```python
from parsimony import RateLimitError

try:
    result = await connectors["fred_fetch"](series_id="GDP")
except RateLimitError as e:
    print(f"Rate limited; retry after {e.retry_after}s (exhausted={e.quota_exhausted})")
```

### `EmptyDataError`

Upstream returned `200 OK` but no rows — usually means your parameters
point at a non-existent resource (wrong `series_id`, date range with no
data). Catch `EmptyDataError` and handle it as a "not found" signal
rather than an error.

### `ImportError: cannot import name 'Catalog' from 'parsimony'`

Install the `[standard]` extra — the canonical `Catalog` class lives
there:

```bash
pip install 'parsimony-core[standard]'
```

The Protocol (`CatalogBackend`) is always available on the root
`parsimony` namespace; only the concrete `Catalog` class requires the
extra.

---

## Security

### How does `bind_deps` protect API keys?

When you call `connector.bind_deps(api_key="secret")`, the key is
injected as a keyword-only argument via `functools.partial`. It never
appears in:

- The `Provenance.params` dict (which only records user-facing
  parameters).
- The JSON schema output from `to_llm()`.
- Log records emitted by `HttpClient` (query-param values whose names
  match `api_key`, `token`, `password`, anything ending `_token` are
  redacted to `***REDACTED***`).

API keys stay out of logs, LLM prompts, and serialized results.

### Is the FMP screener `where_clause` safe?

No. `where_clause` uses `DataFrame.query()` internally, which evaluates
Python expressions. **Never pass untrusted user input** as a
`where_clause` value. Only use it with trusted, developer-authored
filter strings.

---

## Production

### How should I manage secrets in production?

Use your platform's secrets manager instead of environment variables in
`.env` files:

- **AWS**: Secrets Manager or SSM Parameter Store
- **GCP**: Secret Manager
- **Azure**: Key Vault
- **Docker/K8s**: Docker secrets or Kubernetes Secrets

Pass retrieved secrets to `build_connectors_from_env(env={...})` using
the `env` parameter to override `os.environ`:

```python
from parsimony.discovery import build_connectors_from_env

secrets = await fetch_from_vault(["FRED_API_KEY", "FMP_API_KEY"])
connectors = build_connectors_from_env(env=secrets)
```

### Is there connection pooling?

Each `HttpClient` call creates a fresh `httpx.AsyncClient`. For
high-throughput batch operations, use
`parsimony.transport.pooled_client` — an async context manager yielding
a connection-pooled `httpx.AsyncClient` for burst workloads.

### How do I monitor connector calls?

Use `with_callback` to attach a post-fetch observer:

```python
import logging

logger = logging.getLogger("parsimony.monitor")

async def log_call(result):
    logger.info(
        "connector=%s rows=%d",
        result.provenance.source,
        len(result.data),
    )

monitored = connectors.with_callback(log_call)
```

Callbacks may be sync or async. Exceptions are logged, not raised — the
caller's `await connector(...)` always returns. For structured
observability, emit metrics or traces from the callback to your
telemetry stack (Prometheus, OpenTelemetry, etc.).
