# Installation

Parsimony is published to PyPI as **`parsimony-core`** and imported as `parsimony`. The base
install is a small, dependency-light kernel: the [connector framework](../connectors/index.md),
the [typed errors](../connectors/errors.md), the data carriers, and the cache helpers. The heavy
[catalog](../catalog/index.md) runtime (FAISS vectors, sentence-transformers embedders,
Hugging Face snapshots) lives behind optional extras and never loads unless you ask for it.

## Requirements

- **Python `>=3.11`** — tested on 3.11, 3.12, and 3.13.
- A PyPI-capable installer: `pip`, or [`uv`](https://docs.astral.sh/uv/) (used throughout the
  contributor flow below).

## Base install

```bash
pip install parsimony-core
```

The base distribution pulls a deliberately small set of runtime dependencies — the mandatory
kernel footprint for validation, data carriers, async HTTP, and cache-directory resolution:

| Dependency | Role |
| --- | --- |
| `pydantic` (>=2.11.1, <3) | Schema validation for entities, output configs, and errors |
| `pandas` (>=2.3.0, <3) | The DataFrame/Series carried by a `TabularResult` |
| `pyarrow` (>=23.0.1) | Arrow / Parquet round-tripping of tabular results and snapshots |
| `httpx` (>=0.28.1) | The async HTTP layer connector authors build on |
| `platformdirs` (>=4.0.0, <5) | Resolves the on-disk [cache](../caching.md) root |

That is everything `pip install parsimony-core` installs. It is enough to define and call your
own connectors, build a keyword-only (BM25) catalog once the `standard` extra is present, and
work with results, provenance, and errors.

!!! note "No connectors ship in core"
    The core package is the framework plus the catalog — **zero connectors**. Each data source
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
| `standard` | `parsimony-core[standard]` | `faiss-cpu`, `rank-bm25`, `sentence-transformers`, `huggingface_hub` | The canonical catalog runtime: BM25 keyword search, FAISS vector search, the default sentence-transformers embedder, and `hf://` snapshot load/save |
| `standard-onnx` | `parsimony-core[standard-onnx]` | everything in `standard`, plus `optimum[onnxruntime]`, `onnxruntime` | The int8-quantized [`OnnxEmbedder`](../catalog/embedders.md) — a faster CPU embedding path; a superset of `standard` |
| `litellm` | `parsimony-core[litellm]` | `litellm` | The hosted-API [`LiteLLMEmbeddingProvider`](../catalog/embedders.md) (OpenAI, Gemini, Cohere, Voyage, Bedrock) |
| `s3` | `parsimony-core[s3]` | `s3fs` | Reserved for `s3://` catalog URLs — **not yet wired** (see the warning below) |
| `all` | `parsimony-core[all]` | `standard`, `standard-onnx`, `litellm`, `s3` | Everything above in one shot |

```bash
# The usual choice for searchable catalogs:
pip install "parsimony-core[standard]"

# Faster CPU embeddings (superset of standard):
pip install "parsimony-core[standard-onnx]"

# Hosted embedding APIs:
pip install "parsimony-core[litellm]"

# Everything:
pip install "parsimony-core[all]"
```

!!! tip "Most catalog work wants `standard`"
    The [catalog](../catalog/index.md) is designed around the `standard` stack — Parquet rows,
    a FAISS vector index, BM25 keywords, and the default sentence-transformers embedder. If you
    intend to do any vector or hybrid search, or to load a published `hf://` snapshot, install
    `parsimony-core[standard]`. A pure keyword catalog using only `BM25Index` also needs the
    `standard` extra (it brings `rank-bm25`).

!!! warning "`s3` is a reserved extra, not a working scheme"
    Installing `parsimony-core[s3]` adds `s3fs`, but it does **not** yet enable the `s3://`
    catalog URL scheme — the source handler is still a stub. Today,
    [`Catalog.save` / `Catalog.load`](../catalog/snapshots.md) understand only `file://` (or a
    bare local path) and `hf://`. Passing an `s3://` URL will not work until the handler lands.

### The `standard-onnx` superset

`standard-onnx` *includes* `standard` — it depends on `parsimony-core[standard]` and then adds
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
genuinely needs them — for example, FAISS loads the first time a `VectorIndex` builds or queries
vectors, and an embedder backend loads on its first encode.

```python
import sys
import parsimony

# The base kernel is imported; the heavy backends are not.
assert "torch" not in sys.modules
assert "faiss" not in sys.modules
assert "sentence_transformers" not in sys.modules

# Naming a catalog class is fine and does not pull FAISS:
print(parsimony.BM25Index.__name__)  # BM25Index
assert "faiss" not in sys.modules
```

The practical consequence: if you only define and call connectors, you never pay for the catalog
stack — neither in install size nor in import time. You add the `standard` extra when, and only
when, you build a searchable catalog.

!!! warning "An extra missing at runtime raises `ImportError`"
    If you exercise a path that needs an extra you did not install — building a `BM25Index` or
    `VectorIndex`, encoding with a `SentenceTransformerEmbedder`, or loading an `hf://` snapshot
    without `standard` — the lazy import fails with an `ImportError` (an unmet
    `ModuleNotFoundError` is one). For the BM25 and FAISS backends the message names the missing
    module (`rank_bm25`, `faiss`); the ONNX and litellm embedders name the extra directly (for
    example `parsimony-core[standard-onnx]`). Install the `standard` extra (or the named extra)
    and retry.

## Verify the install

```bash
python -c "import parsimony; print(parsimony.__version__)"
```

```text
0.7.0
```

The bundled console script confirms the CLI is on your `PATH` and reports which provider plugins
are installed (none, on a fresh `parsimony-core` install):

```bash
parsimony list
```

```text
No parsimony plugins discovered (0 plugins).
Install one to get started, e.g. `pip install parsimony-fred`.
```

See [Command-line interface](../cli.md) for the full `parsimony` command reference.

## Installing provider plugins

Connectors live in separate distributions named `parsimony-<name>`. Install the ones you need
the same way you install any package:

```bash
pip install parsimony-fred
```

Once installed, a plugin registers itself through the `parsimony.providers` entry-point group;
`parsimony list` will show it, and `parsimony.discover.load_all()` will pick it up at runtime.
See [Discovering installed providers](../plugins/discovery.md) and, if you want to ship your own,
[Authoring a provider plugin](../plugins/authoring.md).

## Contributor / editable install

To work on `parsimony-core` itself, install it editable with the `dev` extra. `dev` pulls the
test, lint, type-check, and audit tooling **and** the `standard` plus `litellm` extras, so the
full test suite can exercise the FAISS, BM25, sentence-transformers, and litellm paths:

```bash
uv pip install -e ".[dev]"
```

!!! note "The full test suite needs the `standard` extra"
    Without `standard` (specifically `faiss-cpu`), test collection aborts on
    `import faiss`. The `dev` extra includes `standard`, so an editable `.[dev]` install runs the
    whole suite. See [Development](../development.md) for the Make targets, quality gates, and CI
    layout.

## Configuring the cache directory

Parsimony keeps catalog snapshots, embedder model files, and connector scratch under a single
on-disk cache root. By default that is `platformdirs.user_cache_dir("parsimony")`
(`~/.cache/parsimony` on Linux, `~/Library/Caches/parsimony` on macOS,
`%LOCALAPPDATA%\parsimony\Cache` on Windows). Override it with the `PARSIMONY_CACHE_DIR`
environment variable (a leading `~` is expanded):

```bash
export PARSIMONY_CACHE_DIR=~/data/parsimony-cache
```

```bash
parsimony cache path
```

```text
/home/you/data/parsimony-cache
```

This is the only environment variable the base install reads at install time; for the complete
list of tunables see [Environment variables](../reference/environment.md) and
[Caching](../caching.md).

## See also

- [Quickstart](quickstart.md) — define a connector, call it, and build a tiny catalog.
- [Core concepts](concepts.md) — the mental model behind connectors, catalogs, and plugins.
- [Plugins and providers](../plugins/index.md) — how connectors are packaged and discovered.
- [Development](../development.md) — contributor setup, quality gates, and CI.
