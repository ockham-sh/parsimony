# Guide

How to use parsimony in real code: install, compose connectors, handle
credentials, work with results, search the catalog. Anything beyond the
essentials lives in the [Reference](contract.md) section.

## Install

The kernel ships no connectors of its own. Install whichever you need
alongside `parsimony-core`:

```bash
pip install parsimony-core
pip install parsimony-fred parsimony-sdmx        # whichever sources you want
```

Optional kernel extras:

| Extra | Adds |
|---|---|
| `standard` | Canonical `Catalog` — FAISS + BM25 + sentence-transformers + `hf://` loader |
| `litellm` | Hosted embeddings via LiteLLM (OpenAI, Gemini, Cohere, Voyage, Bedrock) |
| `s3` | `s3://` URLs in `Catalog.from_url` / `Catalog.push` (planned) |
| `all` | All of the above |

```bash
pip install "parsimony-core[standard]"
```

## Core types

- **`Connector`** — an async function wrapped with metadata: name,
  description, Pydantic params model, optional `OutputConfig`, optional
  `env_map`. Call with keyword arguments or a typed model. Dependencies
  (API keys, pools) are injected via `bind` and never appear in
  provenance.
- **`Connectors`** — an immutable collection of connectors. Look up by
  name, merge, filter, attach hooks; surface credential state via
  `.unbound` / `.env_vars()`.
- **`Result`** — `(data: pd.DataFrame, provenance: Provenance)`. When the
  connector declares an `OutputConfig`, `result.output_schema` exposes
  typed column groups (`entity_keys`, `data_columns`, `metadata_columns`).
- **`Provenance`** — source, params, fetched_at. Survives Arrow / Parquet
  round-trip; never contains credentials.

## Calling a connector

Two ways to compose connectors. Use whichever fits — the resulting
`Connectors` collection behaves the same.

```python
# Specific plugin
from parsimony_fred import CONNECTORS as fred_connectors
fred = fred_connectors.bind_env()      # reads FRED_API_KEY

# Or every installed plugin in one call
from parsimony import discover
connectors = discover.load_all().bind_env()
```

Then dispatch by name:

```python
search = await connectors["fred_search"](search_text="consumer price index")
print(search.data[["id", "title"]].head())

obs = await connectors["fred_fetch"](
    series_id="UNRATE",
    observation_start="2020-01-01",
)
```

## Credentials

Each connector declares its env vars on the `@connector(env=...)`
decorator. `Connectors.bind_env()` reads `os.environ` and binds every
connector's declared deps. Connectors whose required env vars are missing
stay in the collection but raise `UnauthorizedError` on call:

```python
connectors = discover.load_all().bind_env()
print(connectors.unbound)      # connector names with missing env vars
print(connectors.env_vars())   # union of all env vars across connectors
```

Bind explicitly when not driven by env (tests, internal services):

```python
fred = fred_connectors.bind(api_key="explicit-key")
```

The kernel does not auto-load `.env` files. Use
[`python-dotenv`](https://github.com/theskumar/python-dotenv) or
`uv run --env-file` to populate `os.environ`. The standalone
`parsimony-mcp` distribution autoloads `.env` for the MCP boot path.

## Errors

Every connector maps upstream failures to the typed hierarchy in
`parsimony.errors`:

```python
from parsimony import UnauthorizedError, RateLimitError, EmptyDataError

try:
    result = await connectors["fred_fetch"](series_id="NOT_A_SERIES")
except EmptyDataError as e:
    print(f"No data for {e.provider}")
except RateLimitError as e:
    print(f"Rate limited; retry after {e.retry_after}s; exhausted={e.quota_exhausted}")
except UnauthorizedError:
    print("Check your API key")
```

`EmptyDataError` is a signal, not a failure — the upstream returned a
clean "no data for this input."

## Catalog

`parsimony.Catalog` is a hybrid-search catalog (Parquet + FAISS + BM25
with reciprocal rank fusion), available via the `[standard]` extra.

### Load a published snapshot

```python
from parsimony import Catalog

catalog = await Catalog.from_url("hf://ockham/catalog-snb")
for m in await catalog.search("policy rate", limit=5):
    print(f"  [{m.namespace}:{m.code}] {m.title}  (sim={m.similarity:.3f})")
```

The first `from_url` downloads `meta.json` + `entries.parquet` +
`embeddings.faiss` into the local HuggingFace cache; subsequent calls hit
the cache.

### Build a catalog locally

```python
from parsimony import Catalog
from parsimony_fred import CONNECTORS as fred_connectors

fred = fred_connectors.bind_env()
catalog = Catalog("fred")

result = await fred["enumerate_fred_release"](release_id=10)
await catalog.add_from_result(result)

await catalog.push("file:///tmp/catalog-fred")          # local
# await catalog.push("hf://your-org/catalog-fred")      # Hugging Face Hub
```

`Catalog.push` writes atomically (temp dir + rename), so a partial
snapshot is never visible at the destination.

### Custom backends

`CatalogBackend` is a `typing.Protocol` — two methods, `add` and `search`.
Any class matching the shape works (Postgres + pgvector, Redis,
OpenSearch, in-memory fakes). No subclassing required.

## Composing collections

`Connectors` is immutable. Combine, filter, swap entries:

```python
from parsimony import Connectors, discover

custom = Connectors([my_connector.bind(api_key="...")])
combined = Connectors.merge(discover.load_all().bind_env(), custom)

equity = combined.filter(tags=["equity"])
without_loaders = combined.filter(lambda c: "loader" not in c.tags)
```

`merge` raises `ValueError` on duplicate names; use `replace(name, conn)`
to swap an existing entry for a test double.

## Where to next

- **[Connectors](connectors/index.md)** — every available data source
- **[MCP Server](mcp-server/index.md)** — expose connectors as agent tools
- **[Building Plugins](connector-implementation-guide.md)** — write your own connector
- **[Recipes](cookbook.md)** — practical end-to-end examples
- **[Plugin Contract](contract.md)** — the authoritative spec
