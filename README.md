# parsimony

[![PyPI version](https://img.shields.io/pypi/v/parsimony-core)](https://pypi.org/project/parsimony-core/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core)](https://pypi.org/project/parsimony-core/)
[![CI](https://github.com/ockham-sh/parsimony/actions/workflows/test.yml/badge.svg)](https://github.com/ockham-sh/parsimony/actions)
[![Docs](https://img.shields.io/badge/docs-parsimony.dev-blue)](https://docs.parsimony.dev)

Typed, composable data connectors for Python. A small kernel; every data source is a separate package discovered through the entry-point contract.

## Why parsimony

- **Light kernel.** `parsimony` ships as a small package (primitives, discovery, conformance, scaffolding). Data sources are separate PyPI distributions that plug in through a public contract.
- **One calling convention.** `await connectors["name"](params)` across every data source. Parameters are Pydantic models; results carry provenance.
- **Install what you need.** `pip install parsimony parsimony-fred parsimony-sdmx` — the kernel composes whatever is installed.
- **Private connectors as a first-class path.** Customer-private and vendor-published plugins use the same entry-point contract as official ones; the kernel cannot tell them apart.
- **MCP-ready.** Connectors are agent-addressable via the [Model Context Protocol](https://modelcontextprotocol.io/) — the server lives in the separate [`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp) distribution.

## Install

Pick what you need. The kernel has no connectors of its own:

```bash
pip install parsimony-core                       # kernel only (tiny footprint)
pip install parsimony-core parsimony-fred        # + FRED
pip install parsimony-core parsimony-sdmx        # + SDMX (ECB, Eurostat, IMF, OECD, BIS, World Bank, ILO)
pip install 'parsimony-core[standard]'           # + canonical Catalog (FAISS + BM25 + sentence-transformers, hf:// loader)
pip install 'parsimony-core[standard,litellm]'   # + LiteLLMEmbeddingProvider (OpenAI, Gemini, Cohere, Voyage, Bedrock)
pip install parsimony-mcp                        # MCP server (separate distribution)
```

> Imports are always `from parsimony import ...`; the bare `parsimony` PyPI name is squatted, so the distribution ships as `parsimony-core`.

Full list of officially-maintained connectors: [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).

## 30-Second Example

Fetch US unemployment rate from FRED:

```python
import asyncio
from parsimony_fred import CONNECTORS as FRED

async def main():
    fred = FRED.bind_deps(api_key="your-fred-key")
    result = await fred["fred_fetch"](series_id="UNRATE")
    print(result.data.tail())
    print(result.provenance)

asyncio.run(main())
```

Or compose everything configured in the environment:

```python
from parsimony import build_connectors_from_env

connectors = build_connectors_from_env()
result = await connectors["fred_fetch"](series_id="UNRATE")
```

`build_connectors_from_env()` walks every installed `parsimony.providers` entry point, binds dependencies from environment variables, and returns a single flat `Connectors` surface.

## Core Primitives

Three decorators, one runtime type:

- `@connector` — typed fetch/search; the bread and butter.
- `@enumerator` — catalog population (KEY + TITLE + METADATA, no DATA).
- `@loader` — observation persistence into a `DataStore`.

Provenance on every result:

```python
result.provenance  # Provenance(source="fred", params={"series_id": "UNRATE"}, fetched_at=...)
```

## The Plugin Contract

Every data source is a separate distribution implementing one contract:

```toml
# your-connector/pyproject.toml
[project]
name = "parsimony-yourname"
dependencies = ["parsimony-core>=0.1.0,<0.3", "pydantic"]
classifiers = ["Framework :: Parsimony :: Contract 1"]

[project.entry-points."parsimony.providers"]
yourname = "parsimony_yourname"
```

Your module exports `CONNECTORS` (required), plus optional `ENV_VARS` and `PROVIDER_METADATA`. The full spec is in [`docs/contract.md`](docs/contract.md) — it's the framework's load-bearing surface.

Building a private connector for your organisation? See [`docs/building-a-private-connector.md`](docs/building-a-private-connector.md) for the customer-private path.

## Discovering Plugins

```python
from parsimony.discovery import discovered_providers

for provider in discovered_providers():
    print(provider.distribution_name, provider.version, provider.connectors.names())
```

Or from the command line:

```bash
parsimony list-plugins                  # what's installed
parsimony conformance verify <package>  # validate against the contract
```

`parsimony conformance verify` is the release-gate tool for every connector package and the security-review artefact for regulated-finance customers.

## Security

- **Allow-list by default.** Officially-maintained plugins (`parsimony-<name>` from [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)) load without opt-in. Non-official plugins require `PARSIMONY_TRUST_PLUGINS=<name1>,<name2>` to load, with a structured log entry for every decision.
- **ABI gate.** The kernel reads each plugin's contract-version classifier before importing the module. Mismatches fail loudly rather than mid-fetch.
- **Single trust root.** Official plugins publish from one signed manifest (`OFFICIAL_PLUGINS.json`) under one publishing identity.

See [`SECURITY.md`](SECURITY.md) for disclosure.

## MCP Server

The MCP server is a separate distribution, [`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp):

```bash
pip install parsimony-mcp
parsimony-mcp
```

Every tool-tagged connector becomes an MCP tool.

## Documentation

Full docs at [docs.parsimony.dev](https://docs.parsimony.dev):

- [Quickstart](https://docs.parsimony.dev/quickstart/)
- [Plugin contract (authoritative)](docs/contract.md)
- [Building a private connector](docs/building-a-private-connector.md)
- [Architecture](https://docs.parsimony.dev/architecture/)
- [API Reference](https://docs.parsimony.dev/api-reference/)

## Contributing

- **Kernel changes** (this repo) — see [`CONTRIBUTING.md`](CONTRIBUTING.md).
- **New or updated connectors** — contribute to [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors). The conformance suite is the merge gate.

The kernel does not accept provider-specific code. That's structurally enforced (see [`tests/test_kernel_purity.py`](tests/test_kernel_purity.py)).

## License

Apache 2.0.
