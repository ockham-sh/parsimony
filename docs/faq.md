# FAQ

## Installation

### Python version?

3.11+.

### What are the optional extras on `parsimony-core`?

| Extra | Adds |
|---|---|
| `standard` | `Catalog` (FAISS + BM25 + sentence-transformers + `hf://`) |
| `litellm` | Hosted embeddings via LiteLLM (OpenAI, Gemini, Cohere, Voyage, Bedrock) |
| `s3` | `s3://` URLs in `Catalog.from_url` / `push` (planned) |
| `all` | All of the above |

Connectors are separate distributions (`parsimony-fred`, `parsimony-sdmx`,
…); install whichever you need. The MCP server is its own distribution
(`parsimony-mcp`).

---

## API keys

### Where does a FRED key come from?

Free, instant approval at
[fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html).

### How do I configure keys?

Export env vars or use a `.env` file with `python-dotenv` /
`uv run --env-file`:

```bash
export FRED_API_KEY="your-key"
```

`Connectors.bind_env()` reads `os.environ` and binds each connector's
declared env vars. For tests or vault-sourced secrets, bind explicitly:

```python
bound = fred.bind(api_key="your-key")
```

### Which connectors need keys?

`Connectors.env_vars()` lists the union for everything installed:

```python
from parsimony import discover
print(sorted(discover.load_all().env_vars()))
```

Most paid sources (FRED, FMP, EODHD, Finnhub, Tiingo, CoinGecko, EIA) need
a key. SDMX agencies (ECB, Eurostat, IMF, World Bank, BIS, OECD, ILO),
Polymarket, US Treasury, and most central banks need none.

---

## Async patterns

### Script vs Jupyter?

Scripts: `asyncio.run(main())`. Jupyter cells: `await` directly — the
notebook runs an event loop already.

### `RuntimeError: This event loop is already running`

You called `asyncio.run()` inside a context that has its own loop (e.g.
Jupyter). Use `await` directly. If you must call from sync library code in
that context, `nest_asyncio.apply()` works but is a workaround.

---

## Common errors

### `UnauthorizedError: ... is not set`

The connector is in the collection but its env var wasn't resolved. Set
the env var and rebind, or use `bind(api_key=...)` explicitly. Inspect
unbound connectors via `connectors.unbound`.

### `TypeError: Connector '...' has unbound dependencies`

Non-env keyword-only deps weren't bound. Pass them via
`Connector.bind(**deps)` — see the
[private &amp; internal connector guide](building-a-private-connector.md) for
the DB-pool / HTTP-client pattern.

### `RateLimitError`

Carries `.retry_after: float` (seconds, retryable) and `.quota_exhausted:
bool` (terminal).

### `EmptyDataError`

Upstream returned `200 OK` with no rows — treat as "not found", not as a
failure. Usually a wrong identifier or empty date range.

### `ImportError: cannot import name 'Catalog' from 'parsimony'`

Install the `[standard]` extra. The `CatalogBackend` Protocol is always
available; the concrete `Catalog` class requires the extra.

---

## Security

### How does `bind` keep keys out of logs and provenance?

Keys injected via `bind` / `bind_env` enter as keyword-only args through
`functools.partial`. They never appear in `Provenance.params`, in
`to_llm()` output, or in `HttpClient` query-param logs (param values whose
names match `api_key`, `token`, `password`, or anything ending `_token`
are redacted).

### Is the FMP screener `where_clause` safe?

**No.** It uses `DataFrame.query()` internally, which evaluates Python
expressions. Never pass untrusted input as `where_clause`. Trusted,
developer-authored strings only.

### Production secrets management

Use your platform's secrets manager (AWS Secrets Manager / SSM, GCP Secret
Manager, Azure Key Vault, K8s Secrets). Layer over `os.environ` via
`bind_env(overrides=...)`:

```python
secrets = await fetch_from_vault(["FRED_API_KEY", "FMP_API_KEY"])
connectors = discover.load_all().bind_env(overrides=secrets)
```
