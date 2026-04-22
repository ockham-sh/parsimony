# parsimony

[![PyPI version](https://img.shields.io/pypi/v/parsimony-core)](https://pypi.org/project/parsimony-core/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core)](https://pypi.org/project/parsimony-core/)
[![CI](https://github.com/ockham-sh/parsimony/actions/workflows/test.yml/badge.svg)](https://github.com/ockham-sh/parsimony/actions)
[![Docs](https://img.shields.io/badge/docs-parsimony.dev-blue)](https://docs.parsimony.dev)

Typed, composable data connectors for Python. A small kernel; every data
source is a separate package discovered through the entry-point contract.

## Why parsimony

- **Light kernel.** `parsimony-core` ships as a small package (primitives,
  discovery, conformance, catalog). Data sources are separate PyPI
  distributions that plug in through a public contract.
- **One calling convention.** `await connectors["name"](params)` across
  every data source. Parameters are Pydantic models; results carry
  provenance.
- **Install what you need.** `pip install parsimony-core parsimony-fred
  parsimony-sdmx` — the kernel composes whatever is installed.
- **Private connectors as a first-class path.** Customer-private and
  vendor-published plugins use the same entry-point contract as official
  ones; the kernel cannot tell them apart.
- **MCP-ready.** Connectors are agent-addressable via the
  [Model Context Protocol](https://modelcontextprotocol.io/) — the server
  lives in the separate [`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp)
  distribution.

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

> Imports are always `from parsimony import ...`; the bare `parsimony`
> PyPI name is squatted, so the distribution ships as `parsimony-core`.

Full list of officially-maintained connectors:
[ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).

## 30-second example

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
from parsimony.discovery import build_connectors_from_env

connectors = build_connectors_from_env()
result = await connectors["fred_fetch"](series_id="UNRATE")
```

`build_connectors_from_env()` walks every installed `parsimony.providers`
entry point, binds dependencies from environment variables, and returns a
single flat `Connectors` surface.

## Core primitives

Three decorators, one runtime type:

- `@connector` — typed fetch/search; the bread and butter.
- `@enumerator` — catalog population (KEY + TITLE + METADATA, no DATA).
- `@loader` — observation persistence into a `DataStore`.

Provenance on every result:

```python
result.provenance  # Provenance(source="fred", params={"series_id": "UNRATE"}, fetched_at=...)
```

## The plugin contract

Every data source is a separate distribution implementing one contract:

```toml
# your-connector/pyproject.toml
[project]
name = "parsimony-yourname"
dependencies = ["parsimony-core>=0.3,<0.5", "pydantic"]

[project.entry-points."parsimony.providers"]
yourname = "parsimony_yourname"
```

Your module exports `CONNECTORS` (required), plus optional `ENV_VARS`,
`PROVIDER_METADATA`, and `CATALOGS` / `RESOLVE_CATALOG` (if the plugin
publishes catalogs). The full spec is in [`docs/contract.md`](docs/contract.md)
— it's the framework's load-bearing surface.

Building a private connector for your organisation? See
[`docs/building-a-private-connector.md`](docs/building-a-private-connector.md)
for the customer-private path.

## Discovering plugins

```python
from parsimony.discovery import discovered_providers

for provider in discovered_providers():
    print(provider.distribution_name, provider.version, provider.connectors.names())
```

Or from the command line:

```bash
parsimony list                  # what's installed + declared catalogs
parsimony list --strict         # run conformance suite; exit non-zero on failure
parsimony list --strict --json  # machine-readable artefact for security review
```

## Publishing catalogs

`parsimony publish --provider NAME --target URL_TEMPLATE` builds one
catalog per namespace declared on a plugin's `CATALOGS` export and pushes
each to `URL_TEMPLATE.format(namespace=...)`. See
[`docs/contract.md`](docs/contract.md) §6 for the publish contract.

Three equivalent paths — the on-disk format is identical across schemes,
so you can stage locally and upload later without re-running the
enumerator.

**1. Direct to Hugging Face** — one step:

```bash
parsimony publish --provider sdmx --target 'hf://myorg/catalog-{namespace}'
```

**2. Local first, then re-publish via parsimony** — load and push:

```bash
parsimony publish --provider sdmx --target 'file:///tmp/out/{namespace}'

# inspect files, then later:
python -c "
import asyncio
from parsimony import Catalog

async def main():
    cat = await Catalog.from_url('file:///tmp/out/sdmx_datasets')
    await cat.push('hf://myorg/catalog-sdmx_datasets')

asyncio.run(main())
"
```

**3. Local first, then raw `huggingface-cli upload`** — bypass the
embedder reconstruction entirely:

```bash
parsimony publish --provider sdmx --target 'file:///tmp/out/{namespace}'
huggingface-cli upload myorg/catalog-sdmx_datasets \
                       /tmp/out/sdmx_datasets --repo-type dataset
```

Path 3 works because `parsimony`'s `hf://` push just writes the
three-file bundle (`meta.json`, `entries.parquet`, `embeddings.faiss`)
to a temp directory and uploads the folder — the `file://` output is
byte-identical.

When path 2 or 3 is preferable:

- Inspect the bundle before publishing (size, entry counts, embedder
  fingerprint).
- Re-publish without re-running the expensive enumerator.
- Enumerate on a build machine, push from a deploy machine.
- Publish the same bundle to multiple targets (HF + S3 mirror).

## Security

- **Credential redaction.** `parsimony.transport.HttpClient` redacts
  sensitive query-param values (`api_key`, `token`, `password`, anything
  ending in `_token`) in structured logs.
- **No provenance leak.** Keyword-only dependencies bound via
  `bind_deps()` are injected at the function-call boundary; they never
  appear in `Provenance.params`, Parquet/Arrow serializations, or
  `to_llm()` output.

See [`SECURITY.md`](SECURITY.md) for disclosure.

## MCP server

The MCP server is a separate distribution,
[`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp):

```bash
pip install parsimony-mcp
parsimony-mcp
```

Every tool-tagged connector becomes an MCP tool.

## Documentation

Full docs at [docs.parsimony.dev](https://docs.parsimony.dev):

- [Quickstart](https://docs.parsimony.dev/quickstart/)
- [Plugin contract (authoritative)](docs/contract.md)
- [Building a new plugin](docs/guide-new-plugin.md)
- [Building a private connector](docs/building-a-private-connector.md)
- [Architecture](https://docs.parsimony.dev/architecture/)
- [API reference](https://docs.parsimony.dev/api-reference/)

## Contributing

- **Kernel changes** (this repo) — see [`CONTRIBUTING.md`](CONTRIBUTING.md).
- **New or updated connectors** — contribute to
  [ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).
  The conformance suite is the merge gate.

The kernel does not accept provider-specific code. That's structurally
enforced (see [`tests/test_kernel_purity.py`](tests/test_kernel_purity.py)).

## License

Apache 2.0.
