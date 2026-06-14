# Installation

Parsimony is published to PyPI as **`parsimony-core`** and imported as `parsimony`. The base
install is a small, dependency-light kernel: the [connector framework](../connectors/index.md),
the [typed errors](../connectors/errors.md), the data carriers, and the cache helpers. The heavy
[catalog](../catalog/index.md) runtime (FAISS vectors, sentence-transformers embedders,
Hugging Face snapshots) lives behind the `catalog` extra. Catalog-backed connector packages
declare `parsimony-core[catalog]` so a plain `pip install parsimony-<name>` pulls the full
discovery stack.

## Requirements

- **Python `>=3.11`** — tested on 3.11, 3.12, and 3.13.
- A PyPI-capable installer: `pip`, or [`uv`](https://docs.astral.sh/uv/) (used throughout the
  contributor flow below).

## Base install

```bash
pip install parsimony-core
```

The base distribution pulls a deliberately small set of runtime dependencies — the mandatory
kernel footprint for validation, data carriers, HTTP, and cache-directory resolution:

| Dependency | Role |
| --- | --- |
| `pydantic` (>=2.11.1, <3) | Schema validation for entities, output configs, and errors |
| `pandas` (>=2.3.0, <3) | The DataFrame/Series carried by a `TabularResult` |
| `pyarrow` (>=23.0.1) | Arrow / Parquet round-tripping of tabular results and snapshots |
| `httpx` (>=0.28.1) | The HTTP layer connector authors build on |
| `platformdirs` (>=4.0.0, <5) | Resolves the on-disk [cache](../caching.md) root |

That is everything `pip install parsimony-core` installs. It is enough to define and call fetch-only
connectors and work with results, provenance, and errors.

!!! note "No connectors ship in core"
    The core package is the framework plus the catalog API — **zero connectors**. Each data source
    is published as its own `parsimony-<name>` distribution (for example `parsimony-fred`) and
    discovered at runtime through the `parsimony.providers` entry-point group. Install the
    providers you need separately, then load them with
    [`parsimony.discover`](../plugins/discovery.md). See
    [Plugins and providers](../plugins/index.md).

## Optional extras

Add an extra in brackets to pull the dependencies a given feature needs. The base install never
pulls torch, FAISS, or any embedder backend — those are imported lazily on first use, so even
with an extra installed, `import parsimony` stays cheap (see
[Lazy heavy dependencies](#lazy-heavy-dependencies) below).

| Extra | `pip install` | Adds | Enables |
| --- | --- | --- | --- |
| `catalog` | `parsimony-core[catalog]` | `faiss-cpu`, `rank-bm25`, `sentence-transformers`, `huggingface_hub` | The canonical catalog runtime: BM25 keyword search, FAISS vector search, the default sentence-transformers embedder, and `hf://` snapshot load/save |
| `standard-onnx` | `parsimony-core[standard-onnx]` | everything in `catalog`, plus `optimum[onnxruntime]`, `onnxruntime` | The int8-quantized [`OnnxEmbedder`](../catalog/embedders.md) — a faster CPU embedding path; a superset of `catalog` |
| `litellm` | `parsimony-core[litellm]` | `litellm` | The hosted-API [`LiteLLMEmbeddingProvider`](../catalog/embedders.md) (OpenAI, Gemini, Cohere, Voyage, Bedrock) |
| `all` | `parsimony-core[all]` | `catalog`, `standard-onnx`, `litellm` | Everything above in one shot |

```bash
# The usual choice for searchable catalogs (also pulled by catalog-backed connectors):
pip install "parsimony-core[catalog]"

# Faster CPU embeddings (superset of catalog):
pip install "parsimony-core[standard-onnx]"

# Hosted embedding APIs:
pip install "parsimony-core[litellm]"

# Everything:
pip install "parsimony-core[all]"
```

!!! tip "Catalog-backed connectors declare `[catalog]`"
    Packages such as `parsimony-riksbank` and `parsimony-sdmx` depend on `parsimony-core[catalog]`,
    so `pip install parsimony-riksbank` already pulls the full hybrid-search stack. Install
    `parsimony-core[catalog]` directly only when you are building catalogs or using the catalog API
    without a catalog-backed connector.

### The `standard-onnx` superset

`standard-onnx` *includes* `catalog` — it depends on `parsimony-core[catalog]` and then adds
`optimum[onnxruntime]` and `onnxruntime`. Installing it therefore also gives you
sentence-transformers, FAISS, BM25, and Hugging Face Hub. Use it when you want the
[`OnnxEmbedder`](../catalog/embedders.md) fast path on x86 CPUs with AVX2/AVX-VNNI; you do not
need to list both extras.

## Lazy heavy dependencies

`import parsimony` is intentionally cheap. The catalog symbols — `Catalog`, `Entity`,
`BM25Index`, `VectorIndex`, `HybridIndex`, the ranking and store types — are
[lazy re-exports](../reference/api.md): they are resolved on first attribute access, not at
import time. Importing the package, or even naming a catalog class, does **not** pull torch,
FAISS, sentence-transformers, or litellm into memory. Those backends load only when a code path
actually needs them (for example `catalog.build()` on a `HybridIndex`).

If you call a catalog method without the `catalog` extra installed, the failure is an actionable
`ConnectorError` pointing at `pip install 'parsimony-core[catalog]'`.

## Installing connectors

Each connector is its own PyPI distribution:

```bash
pip install parsimony-core parsimony-fred parsimony-sdmx
```

`parsimony list` enumerates what is installed; `parsimony.discover.load_all()` composes their
`CONNECTORS` exports at runtime. See [Discovering installed providers](../plugins/discovery.md).

## Development install

Contributors typically work from a git checkout with the `dev` extra, which pulls pytest, ruff,
mypy, pip-audit, and the `catalog` + `litellm` surfaces the test suite exercises:

```bash
uv pip install -e ".[dev]"
make check
```

!!! note "The full test suite needs the `catalog` extra"
    Without `catalog` (specifically `faiss-cpu`), test collection aborts on
    `import faiss`. The `dev` extra includes `catalog`, so an editable `.[dev]` install runs the
    full suite.
