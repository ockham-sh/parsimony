# parsimony

[![PyPI version](https://img.shields.io/pypi/v/parsimony-core)](https://pypi.org/project/parsimony-core/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core)](https://pypi.org/project/parsimony-core/)
[![CI](https://github.com/ockham-sh/parsimony/actions/workflows/test.yml/badge.svg)](https://github.com/ockham-sh/parsimony/actions)
[![Docs](https://img.shields.io/badge/docs-parsimony.dev-blue)](https://docs.parsimony.dev)

Parsimony is a unified interface to public data, designed for agents. The same call shape works across every source, every result carries its own provenance, and the result shape keeps the agent's context light.

## Quickstart

```python
import asyncio
from parsimony_fred import CONNECTORS as fred

async def main():
    connectors = fred.bind_env()                              # FRED_API_KEY from os.environ
    result = await connectors["fred_fetch"](series_id="UNRATE")
    print(result.data.tail())
    print(result.provenance)                                  # source, params, fetched_at

asyncio.run(main())
```

That code shape is the same one you write for the European Central Bank, the OECD, SEC EDGAR, CoinGecko, or a private connector your team published yesterday. Install another `parsimony-*` package and the kernel finds it through entry-points; nothing else changes.

## Why parsimony

**The call shape is the same for every source.** `await connectors["fred_fetch"](series_id="UNRATE")` and `await connectors["sdmx_fetch"](agency="ECB", flow="ICP", key="M.U2.N.000000.4.ANR")` look identical. Parameters are Pydantic models; results are typed.

**Provenance is automatic.** Every `Result` carries its `source`, `params`, and `fetched_at`. Credentials are injected at the call boundary, so they never appear in provenance, logs, Parquet output, or the LLM-facing `to_llm()` projection.

**Made for agents that run Python in a code interpreter.** Connectors return data into Python variables in the agent's notebook or REPL. The optional `with_callback` hook prints whatever summary you want after every fetch, so the agent sees what happened without the full dataframe ever entering the LLM context.

```python
def summarise(result):
    print(f"{result.provenance.source}({result.provenance.params}) -> {len(result.data)} rows")

connectors = fred.bind_env().with_callback(summarise)
await connectors["fred_fetch"](series_id="UNRATE")
# UNRATE is now in a local variable. The model only saw the one-line summary.
```

**Partial credentials are not an error.** `bind_env()` resolves whatever env vars are set and leaves the rest unbound. Calling an unbound connector raises `UnauthorizedError`; inspect `connectors.unbound` to list what is missing.

**Catalogs ship as Hugging Face datasets.** `parsimony publish --provider sdmx --target 'hf://yourorg/catalog-{namespace}'` produces a hybrid FAISS + BM25 index over thousands of series. Search before you fetch:

```python
from parsimony import Catalog

cat = await Catalog.from_url("hf://parsimony-dev/sdmx_datasets")
hits = await cat.search("euro area unemployment", limit=10)
```

A fragment-deduplicating embedder fits 8000+ SDMX dataflows in roughly 3 GB of memory at publish time instead of 18.

**Private connectors are first-class.** A customer-internal plugin uses the same `parsimony.providers` entry-point contract as a public one; the kernel cannot tell them apart at discovery time. Your data spec does not have to ship to PyPI to be agent-addressable.

---

## Install

The kernel ships with no connectors of its own. Pick what you need:

```bash
pip install parsimony-core                          # kernel only (small footprint)
pip install parsimony-core parsimony-fred           # + FRED
pip install 'parsimony-core[standard]'              # + Catalog (FAISS + BM25 + sentence-transformers)
pip install parsimony-mcp                           # MCP server, separate distribution
```

Other install variants (ONNX runtime, LiteLLM embedders, CPU-only torch) are documented at [docs.parsimony.dev/install](https://docs.parsimony.dev/install/). The full list of officially-maintained connectors lives at [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).

> Imports are always `from parsimony import ...`. The bare `parsimony` PyPI name is squatted by an unrelated project, so the distribution ships as `parsimony-core`.

> CPU-only deployments: `[standard]` pulls `torch` from the default PyPI channel, which is the CUDA wheel and pulls roughly 4 GB of GPU libraries. On servers without a GPU, install the CPU wheel explicitly first to keep the image small:
>
> ```bash
> pip install torch --index-url https://download.pytorch.org/whl/cpu
> pip install 'parsimony-core[standard]'
> ```

## Core primitives

The kernel exposes three decorators and one runtime type:

- `@connector` declares a typed fetch or search function.
- `@enumerator` populates a catalog (KEY, TITLE, METADATA, no DATA).
- `@loader` persists observations into a `DataStore`.

Provenance on every result:

```python
result.provenance  # Provenance(source="fred", params={"series_id": "UNRATE"}, fetched_at=...)
```

## Repo boundaries

Parsimony ships across three repositories, each with a single job:

| Repo | PyPI | Role |
|---|---|---|
| [`parsimony`](https://github.com/ockham-sh/parsimony) | `parsimony-core` | The kernel. Primitives, discovery, catalog, publish CLI |
| [`parsimony-connectors`](https://github.com/ockham-sh/parsimony-connectors) | `parsimony-<name>` | First-party data source plugins |
| [`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp) | `parsimony-mcp` | MCP (Model Context Protocol) server |

The kernel knows nothing about specific connectors. Connectors depend on the kernel through the stable `parsimony.providers` entry-point contract. Consumers like the MCP server call `discover.load_all()` and pick up whatever is installed.

## The plugin contract

Every data source is a separate distribution implementing one contract:

```toml
# your-connector/pyproject.toml
[project]
name = "parsimony-yourname"
dependencies = ["parsimony-core>=0.4,<0.5", "pydantic"]

[project.urls]
Homepage = "https://your-provider.example"

[project.entry-points."parsimony.providers"]
yourname = "parsimony_yourname"
```

Your module exports `CONNECTORS` (required) and optional `CATALOGS` / `RESOLVE_CATALOG` (if the plugin publishes catalogs):

```python
# parsimony_yourname/__init__.py
from parsimony import Connectors, connector

@connector(env={"api_key": "YOUR_API_KEY"})
async def yourname_fetch(params, *, api_key: str): ...

CONNECTORS = Connectors([yourname_fetch])
```

Per-connector env vars live on the decorator (`env={...}`); homepage and version come from `pyproject.toml`. The full spec is in [`docs/contract.md`](docs/contract.md), which is the authoritative reference for plugin authors.

For a customer-private connector, see [`docs/building-a-private-connector.md`](docs/building-a-private-connector.md).

## Discovering plugins

```python
from parsimony import discover

for provider in discover.iter_providers():
    print(provider.dist_name, provider.version, provider.module_path)
```

Or from the command line:

```bash
parsimony list                  # what is installed plus declared catalogs
parsimony list --strict         # run conformance suite; non-zero exit on failure
parsimony list --strict --json  # machine-readable artefact for security review
```

The conformance suite (`parsimony.testing.assert_plugin_valid`) verifies that every connector exports a `CONNECTORS` collection, has non-empty descriptions, and that declared `env_map` keys map to real keyword-only dependencies on the function. It is the merge gate in the connectors monorepo.

## Publishing catalogs

`parsimony publish --provider NAME --target URL_TEMPLATE` builds one catalog per namespace declared on a plugin's `CATALOGS` export and pushes each to `URL_TEMPLATE.format(namespace=...)`.

```bash
parsimony publish --provider sdmx --target 'hf://myorg/catalog-{namespace}'
parsimony publish --provider sdmx --target 'file:///tmp/out/{namespace}'
```

The `file://` output is byte-identical to what `hf://` would write, so you can stage locally, sign, and upload from a separate machine. See [`docs/contract.md`](docs/contract.md) §6 for the publish contract.

## Cache

Heavy artefacts (HF snapshots, ONNX models, fragment embeddings, connector scratch) live in a shared user-home cache so a `parsimony-mcp` server and a REPL session reuse the same downloads. Override the root with `PARSIMONY_CACHE_DIR`. Inspect or clear with `parsimony cache info` and `parsimony cache clear`. Layout details: [docs.parsimony.dev/cache](https://docs.parsimony.dev/cache/).

## MCP server

The MCP server is a separate distribution, [`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp):

```bash
pip install parsimony-mcp
parsimony-mcp init
```

Connectors tagged `tool` (search, list, metadata) become MCP tools the agent can call. Bulk fetch goes through the agent's code interpreter via `discover.load_all().bind_env()`. See the `parsimony-mcp` README for the rationale.

## Documentation

Full docs at [docs.parsimony.dev](https://docs.parsimony.dev):

- [Quickstart](https://docs.parsimony.dev/quickstart/)
- [Plugin contract (authoritative)](docs/contract.md)
- [Building a new plugin](docs/guide-new-plugin.md)
- [Building a private connector](docs/building-a-private-connector.md)
- [Architecture](https://docs.parsimony.dev/architecture/)
- [API reference](https://docs.parsimony.dev/api-reference/)

## Contributing

- Kernel changes (this repo): see [`CONTRIBUTING.md`](CONTRIBUTING.md).
- New or updated connectors: contribute to [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors). The conformance suite is the merge gate.

The kernel does not accept provider-specific code. That is structurally enforced by [`tests/test_kernel_purity.py`](tests/test_kernel_purity.py).

## License

Apache 2.0.
