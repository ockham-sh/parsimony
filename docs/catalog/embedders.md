# Embedders

An **embedder** turns text into dense vectors. The [`VectorIndex`](indexes.md) inside a
[Catalog](index.md) uses one to build and query its FAISS index, so semantic search depends
on whichever embedder you wire in. Parsimony defines a single structural contract,
`EmbeddingProvider`, and ships three implementations of it: a local PyTorch model, a local
quantized-ONNX model, and a hosted-API client. All of them live in `parsimony.embedder`.

```python
from parsimony.embedder import (
    EmbeddingProvider,
    EmbedderInfo,
    DEFAULT_MODEL,
    SentenceTransformerEmbedder,
    OnnxEmbedder,
    LiteLLMEmbeddingProvider,
)
```

!!! note "Not a top-level import"
    Embedder symbols are **not** re-exported from the top-level `parsimony` package — always
    import them from `parsimony.embedder`. Most of them also need an optional extra (see the
    table below), so the base `pip install parsimony-core` deliberately does not pull torch,
    onnxruntime, or litellm.

## The `EmbeddingProvider` protocol

`EmbeddingProvider` is a `@runtime_checkable` `Protocol`. It is a *structural* contract, not a
base class and not a plugin axis: any object that exposes the four members below satisfies it,
whether or not it inherits from anything. You instantiate one of the bundled implementations
(or write your own conforming class) and pass it to `VectorIndex(..., embedder=...)`.

| Member | Signature | Purpose |
|---|---|---|
| `dimension` | `property -> int` | The vector dimension the provider emits. |
| `embed_texts` | `(texts: list[str]) -> list[list[float]]` | Embeddings for corpus documents (indexing). |
| `embed_query` | `(query: str) -> list[float]` | A single embedding optimized for retrieval queries. |
| `info` | `() -> EmbedderInfo` | The persisted identity used in catalog metadata. |

`embed_texts` and `embed_query` are ordinary synchronous methods. `dimension` and `info()` are
synchronous as well.

Because the protocol is `runtime_checkable`, you can verify a custom object structurally with
`isinstance`. This example needs only `parsimony-core`:

```python
from parsimony.embedder import EmbeddingProvider, EmbedderInfo


class ZeroEmbedder:
    """A trivial, dependency-free embedder for tests and demos."""

    def __init__(self, *, dim: int = 8) -> None:
        self._dim = dim

    @property
    def dimension(self) -> int:
        return self._dim

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[0.0] * self._dim for _ in texts]

    def embed_query(self, query: str) -> list[float]:
        return [0.0] * self._dim

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="zero", dim=self._dim, normalize=True)


emb = ZeroEmbedder()
print(isinstance(emb, EmbeddingProvider))       # True — structural check
print(emb.embed_texts([]))                      # []
print(emb.embed_query("anything"))              # [0.0, 0.0, ... ] (8 zeros)
```

!!! note "Empty input is a no-op"
    Every bundled `embed_texts([])` returns `[]` immediately — no model load, no network call.
    The query/document methods are always **order-preserving**: `output[i]` corresponds to
    `input[i]`, even though the two local embedders internally sort by text length for padding
    efficiency before remapping the results back to your order.

## `EmbedderInfo` — the model identity

`EmbedderInfo` is a pydantic model that captures *which* model produced a catalog's vectors. It
is serialized into a [snapshot's](snapshots.md) `VectorIndex` metadata and validated back on
load.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `model` | `str` | — | Model identifier, e.g. `sentence-transformers/all-MiniLM-L6-v2`. |
| `dim` | `int` | — | Vector dimension the model produces. |
| `normalize` | `bool` | `True` | Whether vectors are L2-normalized at production time. |
| `package` | `str \| None` | `None` | Optional install hint surfaced in error messages; **not** used for resolution. |

The tuple `(model, dim, normalize)` is the **identity key**. The catalog layer keys vector reuse
on it and validates it on load — `package` is purely an advisory string and never participates in
equality. The bundled embedders set `package` to their install extra so a catalog loaded without
the right dependency can tell you what to install.

```python
from parsimony.embedder import EmbedderInfo

info = EmbedderInfo(model="my-model", dim=384)
print(info.normalize, info.package)   # True None
```

## `SentenceTransformerEmbedder` — local PyTorch (default)

The default, local embedder. It wraps a `sentence_transformers.SentenceTransformer` model and
runs on CPU or GPU. Requires the **`standard`** extra.

```python
SentenceTransformerEmbedder(
    *,
    model: str = DEFAULT_MODEL,   # "sentence-transformers/all-MiniLM-L6-v2"
    normalize: bool = True,
    device: str | None = None,
    batch_size: int = 64,
)
```

`DEFAULT_MODEL` is `sentence-transformers/all-MiniLM-L6-v2` — a 6-layer MiniLM that produces
384-dimensional vectors. All constructor arguments are **keyword-only**:
`SentenceTransformerEmbedder(model="...")` works; `SentenceTransformerEmbedder("...")` raises
`TypeError`.

Instantiation is cheap. The model loads lazily on the first attribute access that needs it — so
`.dimension`, `.info()`, or an `embed_*` call triggers the load, but bare construction does not.
`dimension` reads `model.get_sentence_embedding_dimension()` (raising `RuntimeError` if the model
reports no dimension). `embed_texts([])` returns `[]` without loading anything. `info()` reports
`package="parsimony-core[catalog]"`.

```python
from parsimony.embedder import SentenceTransformerEmbedder

emb = SentenceTransformerEmbedder()       # model=DEFAULT_MODEL, normalize=True, batch_size=64
print(emb.model_name)                     # sentence-transformers/all-MiniLM-L6-v2
print(emb.dimension)                      # 384 (loads the model on first access)

info = emb.info()
print(info.model, info.dim, info.normalize, info.package)
# sentence-transformers/all-MiniLM-L6-v2 384 True parsimony-core[catalog]

vectors = emb.embed_texts(["10 year euro area yield curve", "apple stock price"])
print(len(vectors), len(vectors[0]))      # 2 384
```

!!! warning "Needs the `catalog` extra"
    This example loads a real model. Install with `pip install 'parsimony-core[catalog]'` and
    expect a one-time model download. The empty-input and structural-protocol examples above run
    on `parsimony-core` alone.

## `OnnxEmbedder` — local quantized ONNX

The same default model run through ONNX Runtime, with optional int8 dynamic quantization. On x86
CPUs with AVX2 / AVX_VNNI the int8 path is 2-3× faster than the PyTorch embedder for the same
model, with a roughly 4× smaller on-disk footprint and no GPU dependencies. Requires the
**`standard-onnx`** extra.

```python
OnnxEmbedder(
    *,
    model: str = DEFAULT_MODEL,
    normalize: bool = True,
    quantize: bool = True,
    batch_size: int = 64,
    max_seq_length: int = 512,
    intra_op_threads: int | None = None,
    cache_dir: str | Path | None = None,
)
```

On first use it exports the HuggingFace encoder to ONNX (fp32), optionally int8-quantizes it,
caches both artifacts on disk, then runs inference through ONNX Runtime's `CPUExecutionProvider`.
It mean-pools the last hidden state weighted by the attention mask, then L2-normalizes when
`normalize=True`. Output vectors are bit-compatible with catalogs built by the PyTorch embedder,
so you can index with one and query with the other.

The disk cache lives under `parsimony.cache.models_dir() / <model_slug> / {fp32,int8}/`, which
resolves through `PARSIMONY_CACHE_DIR` (then `platformdirs.user_cache_dir("parsimony")`). Pass
`cache_dir=` to override that parent directory (mostly for tests); `cache_dir=` takes precedence
over `PARSIMONY_CACHE_DIR`. The fp32 export is reused as the source for quantization and is
skipped if already present. `intra_op_threads` sets ONNX Runtime's `intra_op_num_threads` when
not `None`, otherwise the runtime's default is used.

!!! warning "`dimension` and `info()` are not cheap here"
    Unlike the PyTorch embedder, `OnnxEmbedder.dimension` is **probed**, not declared: the first
    call runs the model once to measure the output width. Both `dimension` and `info()` therefore
    force the full export → (quantize) → load on a cold cache. Only bare construction is lazy.
    Exporting/quantizing additionally needs `optimum`; inference alone needs `onnxruntime` and
    `transformers`. A missing dependency raises an `ImportError` naming `parsimony-core[standard-onnx]`.

```python
import math
from pathlib import Path

from parsimony.embedder import EmbeddingProvider, OnnxEmbedder

emb = OnnxEmbedder(cache_dir=Path("/tmp/onnx-cache"), quantize=True)
# isinstance touches the `dimension` property, which on OnnxEmbedder forces the
# cold export/quantize/load (see the warning above) — it is not a free check here.
assert isinstance(emb, EmbeddingProvider)

vecs = emb.embed_texts(["5 year AAA spot rate", "apple stock price"])
assert all(len(v) == 384 for v in vecs)
for v in vecs:                                        # outputs are L2-normalized
    assert math.isclose(math.sqrt(sum(x * x for x in v)), 1.0, abs_tol=1e-3)

print(emb.info().package)                             # parsimony-core[standard-onnx]
```

!!! tip "Pointing the model cache elsewhere"
    With no `cache_dir=` override, `OnnxEmbedder` writes its ONNX and tokenizer files under
    `parsimony.cache.models_dir()`. Set `PARSIMONY_CACHE_DIR` before importing to relocate the
    whole cache root:

    ```python
    import os
    os.environ["PARSIMONY_CACHE_DIR"] = "/data/parsimony-cache"
    from parsimony import cache
    print(cache.models_dir())   # /data/parsimony-cache/models
    ```

    See [Caching](../caching.md) and [Environment variables](../reference/environment.md).

## `LiteLLMEmbeddingProvider` — hosted API

Hosted embeddings through the [litellm](https://github.com/BerriAI/litellm) unified API (OpenAI,
Gemini, Cohere, Voyage, Bedrock, and more). Requires the **`litellm`** extra.

```python
LiteLLMEmbeddingProvider(
    *,
    model: str,        # required
    dimension: int,    # required
    batch_size: int = 100,
)
```

`model` and `dimension` are **required** keyword arguments — there are no defaults. The class
does **not** introspect the remote endpoint for its dimension; you declare it. `dimension` and
`info()` are therefore free (no network call). Outputs are **always** L2-normalized via a
pure-Python helper, regardless of what the API returns, and `info().normalize` is always `True` —
so the vectors round-trip cleanly with the inner-product FAISS index.

`embed_texts` batches by `batch_size` and tags each call `task_type="RETRIEVAL_DOCUMENT"`;
`embed_query` issues a single call tagged `task_type="RETRIEVAL_QUERY"`. The provider first calls
`litellm.embedding(..., dimensions=...)` and retries with `output_dimensionality=...` on
`TypeError`, covering providers that name the parameter differently. Responses are strictly
validated: a missing `data` field, the wrong item count, a missing per-item `embedding`, or a
per-item dimension that differs from the declared `dimension` each raise `ValueError`. Any failure
from the underlying call is logged and re-raised as `RuntimeError`.

```python
from parsimony.embedder import LiteLLMEmbeddingProvider

# model and dimension are declared, not introspected.
emb = LiteLLMEmbeddingProvider(model="text-embedding-3-small", dimension=1536)
print(emb.dimension)         # 1536 (no remote call)
print(emb.info().normalize)  # True (always)

# Needs parsimony-core[litellm] plus provider creds in the environment (e.g. OPENAI_API_KEY).
qvec = emb.embed_query("euro area 10Y bond yield")
print(len(qvec))             # 1536, L2-normalized
```

!!! warning "Credentials live in the environment, not in parsimony"
    Provider keys (`OPENAI_API_KEY`, `GEMINI_API_KEY`, Cohere/Voyage/Bedrock variables, …) are
    read by litellm itself from the process environment — Parsimony only configures `model` and
    `dimension`. If you pass the wrong `dimension`, the first real call raises `ValueError`
    because the returned vectors will not match.

## Choosing an embedder

| Embedder | Extra | Default model | Dimension | When |
|---|---|---|---|---|
| `SentenceTransformerEmbedder` | `standard` | all-MiniLM-L6-v2 | 384 | Default, offline, CPU/GPU. |
| `OnnxEmbedder` | `standard-onnx` | all-MiniLM-L6-v2 | 384 (probed) | Offline, faster int8 CPU inference. |
| `LiteLLMEmbeddingProvider` | `litellm` | — (required) | declared | Hosted models, no local weights. |

The `standard-onnx` extra is a superset of `standard` (it depends on it), so installing it also
brings in FAISS, BM25, and sentence-transformers. See [Installation](../getting-started/installation.md)
for the full optional-extras matrix.

## Wiring an embedder into a catalog

A `VectorIndex` takes an embedder via `VectorIndex(embedder=...)`. When you leave it `None`, the
index lazily constructs a default `SentenceTransformerEmbedder` (from the stored model identity on
load, or the default MiniLM model otherwise). The same default is shared process-wide by the
adaptive index policy, so a catalog built without an explicit embedder still gets one. Building or
querying a `VectorIndex` requires the `catalog` extra (FAISS plus the embedder's runtime).

```python
from parsimony.catalog import Catalog, Entity, VectorIndex
from parsimony.embedder import OnnxEmbedder

emb = OnnxEmbedder(quantize=True)
entities = [
    Entity(namespace="test", code="YC_10Y", title="10 year euro area yield curve spot rate"),
    Entity(namespace="test", code="AAPL", title="Apple Inc. common stock close price"),
]
catalog = Catalog("test", indexes={"title": VectorIndex(embedder=emb)})
catalog.set_entities(entities)
catalog.build()
hits = catalog.search("euro area 10Y bond yield", limit=1)
print(hits[0].code)   # YC_10Y
```

!!! warning "Identity is validated on load"
    When you call `VectorIndex.load(..., embedder=...)` with an embedder, its
    `info()` identity must match the snapshot's stored `(model, dim, normalize)` tuple, or a
    `ValueError` is raised. Use the *same* embedder configuration for building and for loading.

## See also

- [Indexes](indexes.md) — the `VectorIndex` and `HybridIndex` that consume an embedder.
- [Building and searching](search.md) — how a catalog's search pipeline calls the embedder.
- [Snapshots and persistence](snapshots.md) — how `EmbedderInfo` is stored and validated on load.
- [Installation](../getting-started/installation.md) — the `standard`, `standard-onnx`, and `litellm` extras.
- [Caching](../caching.md) — where `OnnxEmbedder` writes its model cache.
