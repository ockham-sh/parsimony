<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-brand-dark.png" />
  <img src="docs/assets/parsimony-brand-light.png" alt="parsimony" width="460" />
</picture>

</div>

Parsimony is a minimal connector framework for financial data and coding agents. A connector is an async Python callable that returns data; the kernel wraps successful calls in `Result` objects with provenance.

<p align="center">
  <img src="docs/assets/parsimony-hero.gif" alt="parsimony: decorate an async function with @connector, bind the operator's API key, and call it — the result comes back as a typed Result carrying both the data and full provenance (connector, source, call-time args, fetch time), with the bound api_key kept out of the record." width="900" />
</p>

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
- `Connectors`: immutable collection with `+`, bind, filter, search, callbacks, and keyed lookup.

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
from parsimony.catalog.source import entities_from_connector

entries = await entities_from_connector(enumerate_provider, ENUMERATE_OUTPUT)

catalog = Catalog("provider_catalog")
catalog.set_entities(entries)
await catalog.build()
await catalog.save("hf://org/repo/provider_catalog")
loaded = await Catalog.load("hf://org/repo/provider_catalog")
```

Tabular connectors return raw `pd.DataFrame` values. Use `ColumnRole.DATA` for observations and any field that can vary by row or time (for example a benchmark bond's rolling constituent ISIN). Use `ColumnRole.METADATA` only for entity descriptors that are constant per entity key when projecting catalog entries.

## MCP

`parsimony-mcp` is a separate distribution. MCP/tool schemas are projections of connector signatures; Python connectors do not need JSON-compatible parameters unless they are exported as tools.

## License

Apache 2.0.
