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
pip install 'parsimony-core[standard-onnx]'      # + fast CPU-only embedder via ONNX Runtime + int8 quantization (2-3× faster, 4× smaller model)
pip install 'parsimony-core[standard,litellm]'   # + LiteLLMEmbeddingProvider (OpenAI, Gemini, Cohere, Voyage, Bedrock)
pip install parsimony-mcp                        # MCP server (separate distribution)
```

> **CPU-only deployments**: `[standard]` pulls `torch` from the default
> PyPI channel, which is a CUDA wheel (~4 GB of GPU libraries). On
> servers without a GPU — including most CI, Docker, and HF Spaces
> builds — install the CPU wheel explicitly first to keep the image
> small:
>
> ```bash
> pip install torch --index-url https://download.pytorch.org/whl/cpu
> pip install 'parsimony-core[standard]'
> ```

> Imports are always `from parsimony import ...`; the bare `parsimony`
> PyPI name is squatted, so the distribution ships as `parsimony-core`.

Full list of officially-maintained connectors:
[ockham-sh/parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors).

## 30-second example

Fetch US unemployment rate from FRED:

```python
import asyncio
from parsimony_fred import CONNECTORS as fred

async def main():
    connectors = fred.bind_env()                  # reads FRED_API_KEY from os.environ
    result = await connectors["fred_fetch"](series_id="UNRATE")
    print(result.data.tail())
    print(result.provenance)

asyncio.run(main())
```

Or compose everything installed in one call:

```python
from parsimony import discover

connectors = discover.load_all().bind_env()
result = await connectors["fred_fetch"](series_id="UNRATE")
```

`discover.load_all()` imports every installed `parsimony.providers` plugin
and merges their `CONNECTORS` exports. `.bind_env()` resolves each
connector's declared env vars from `os.environ`. Connectors whose required
env vars are missing stay in the collection but raise `UnauthorizedError`
on call — inspect via `connectors.unbound`.

## Core primitives

Three decorators, one runtime type:

- `@connector` — typed fetch/search; the bread and butter.
- `@enumerator` — catalog population (KEY + TITLE + METADATA, no DATA).
- `@loader` — observation persistence into a `DataStore`.

Provenance on every result:

```python
result.provenance  # Provenance(source="fred", params={"series_id": "UNRATE"}, fetched_at=...)
```

## Repo boundaries

Parsimony ships across three repos, each with a single job:

| Repo | PyPI | Role |
|---|---|---|
| [`parsimony`](https://github.com/ockham-sh/parsimony) | `parsimony-core` | The kernel — primitives, discovery, catalog, publish CLI |
| [`parsimony-connectors`](https://github.com/ockham-sh/parsimony-connectors) | `parsimony-<name>` | First-party data source plugins |
| [`parsimony-mcp`](https://github.com/ockham-sh/parsimony-mcp) | `parsimony-mcp` | MCP server |

Dependencies go one way. The kernel knows nothing about specific
connectors; connectors depend on the kernel through the stable
`parsimony.providers` entry-point contract; consumers like the MCP
server call `discover.load_all()` and pick up whatever the user has
installed. Private and customer-internal connectors follow the same
contract — at discovery time the kernel cannot tell first-party, third-
party, and private apart.

A few invariants keep the seams sharp:

- **The connectors monorepo is publishers-only.** Every
  `packages/*/` subdirectory ships a `parsimony.providers` entry point;
  CI fails any `pyproject.toml` without one. Consumers live elsewhere.
- **Plugin metadata lives on the decorator or `pyproject.toml`** —
  `@connector(env={...})` for env vars, `[project]` for homepage and
  version. No module-level metadata dicts.
- **Single discovery surface.** Everything the kernel knows about
  installed plugins flows through `parsimony.discover`. The public
  catalog of first-party connectors is regenerated at release time from
  `pyproject.toml` metadata, not hand-maintained.

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

Your module exports `CONNECTORS` (required) and optional `CATALOGS` /
`RESOLVE_CATALOG` (if the plugin publishes catalogs):

```python
# parsimony_yourname/__init__.py
from parsimony import Connectors, connector

@connector(env={"api_key": "YOUR_API_KEY"})
async def yourname_fetch(params, *, api_key: str): ...

CONNECTORS = Connectors([yourname_fetch])
```

Per-connector env vars live on the decorator (`env={...}`); homepage and
version come from `pyproject.toml`. The full spec is in
[`docs/contract.md`](docs/contract.md) — it's the framework's load-bearing
surface.

Building a private connector for your organisation? See
[`docs/building-a-private-connector.md`](docs/building-a-private-connector.md)
for the customer-private path.

## Discovering plugins

```python
from parsimony import discover

for provider in discover.iter_providers():
    print(provider.dist_name, provider.version, provider.module_path)
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

## Cache

Heavy artefacts (HF catalog snapshots, ONNX embedder models, fragment
embeddings, connector-owned scratch) live in a single user-home cache
shared across processes — so a `parsimony-mcp` server and a `terminal`
session running in parallel reuse the same downloads.

The cache root is `platformdirs.user_cache_dir("parsimony")` by default
(`~/.cache/parsimony` on Linux, `~/Library/Caches/parsimony` on macOS,
`%LOCALAPPDATA%/parsimony/Cache` on Windows). Set `PARSIMONY_CACHE_DIR`
to override.

Layout — each subdirectory holds one class of artefact:

| Subdir | Contents | Lifetime |
|---|---|---|
| `catalogs/` | HF snapshot downloads (read-side: search) | Until cleared |
| `models/` | ONNX models + tokenizers | Until cleared |
| `embeddings/<slug>/` | `FragmentEmbeddingCache` parquet, identity-keyed per embedder | Until cleared |
| `connectors/<provider>/` | Connector-owned scratch (e.g. dataflow listings) | Per-connector TTL |

Inspect or clear via the CLI:

```bash
parsimony cache path                              # print the resolved root
parsimony cache info                              # table of subdir, files, size, path
parsimony cache info --json                       # same, as JSON
parsimony cache clear --subdir embeddings --yes   # remove just one subdir
parsimony cache clear --yes                       # wipe everything under root
```

Connector authors who want to memoize plugin-specific data should write
into `parsimony.cache.connectors_dir("<provider>")` rather than a
repo-relative path — the cache is shared across publishes and survives
process recycles.

## Security

- **Credential redaction.** `parsimony.transport.HttpClient` redacts
  sensitive query-param values (`api_key`, `token`, `password`, anything
  ending in `_token`) in structured logs.
- **No provenance leak.** Keyword-only dependencies bound via
  `bind()` / `bind_env()` are injected at the function-call boundary; they
  never appear in `Provenance.params`, Parquet/Arrow serializations, or
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
