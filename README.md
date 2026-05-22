# parsimony

Parsimony is a minimal connector framework for financial data and coding agents. A connector is an async Python callable that returns data; the kernel wraps successful calls in `Result` objects with provenance.

## Quickstart

```python
from parsimony import connector

@connector
async def hello(name: str) -> str:
    """Return a greeting."""
    return f"hello {name}"

result = await hello(name="world")
print(result.data)
```

## Credentials

Credentials are normal parameters. Bind operator-supplied values before exposing a connector to an agent:

```python
runtime_fetch = fred_fetch.bind(api_key="...")
result = await runtime_fetch(series_id="GDP")
```

The bound connector no longer exposes `api_key`, and provenance records only the call-time arguments.

Connector implementations decide how to handle provider-specific auth, including optional environment-variable fallback.

## Core Primitives

- `@connector`: wraps an async callable.
- `@enumerator`: connector for catalog enumeration output.
- `@loader`: connector for observation-loading output.
- `Connector.bind`: schema-aware partial application.
- `Connectors`: immutable collection with merge, bind, filter, replace, callbacks, and keyed lookup.

## Plugins

Plugins register under `parsimony.providers` and export `CONNECTORS: Connectors`.

```toml
[project.entry-points."parsimony.providers"]
yourname = "parsimony_yourname"
```

```python
from parsimony import Connectors, connector

@connector
async def yourname_fetch(symbol: str, api_key: str = ""):
    """Fetch private source data."""
    ...

CONNECTORS = Connectors([yourname_fetch])
```

The authoritative plugin contract is [`docs/contract.md`](docs/contract.md).

## Catalogs

Catalog snapshots are built directly from entries and catalog primitives:

```python
result = await enumerate_provider()
entries = (await enumerate_provider()).data  # list[CatalogEntry]

catalog = Catalog("provider_catalog")
catalog.set_entries(entries)
await catalog.build()
await catalog.save("hf://org/repo/provider_catalog")
loaded = await Catalog.load("hf://org/repo/provider_catalog")
```

## MCP

`parsimony-mcp` is a separate distribution. MCP/tool schemas are projections of connector signatures; Python connectors do not need JSON-compatible parameters unless they are exported as tools.

## License

Apache 2.0.
