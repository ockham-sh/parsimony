# Environment variables

Parsimony's core library reads exactly **two** environment variables, both prefixed
`PARSIMONY_`. They tune where the on-disk cache lives and when the FAISS vector index
switches build strategies. Everything else — provider API keys, embedding-service
credentials — is read by the optional dependencies or the `parsimony-<name>` plugins, not
by `parsimony-core` itself.

This page is the one-stop reference for those tunables. For the systems they configure, see
[Caching](../caching.md), [Indexes](../catalog/indexes.md), and [Embedders](../catalog/embedders.md).

## Reference table

| Variable | Read by | Default | Effect |
|---|---|---|---|
| `PARSIMONY_CACHE_DIR` | `parsimony.cache` (transitively: catalogs, embedders) | `platformdirs.user_cache_dir("parsimony")` | Relocates the entire cache root. `~` is expanded. |
| `PARSIMONY_FAISS_IVF_THRESHOLD` | `parsimony.indexes` | `500000` | Row count at which `build_faiss` switches from HNSW to IVFFlat. Captured **at import time**. |

Both are optional. With neither set, Parsimony works out of the box: the cache root falls
back to the platform-correct user cache directory, and the FAISS threshold uses its built-in
default.

## `PARSIMONY_CACHE_DIR`

This relocates the root of Parsimony's on-disk cache. When unset, the root is resolved by
`platformdirs.user_cache_dir("parsimony")`, which is platform-correct:

| Platform | Default cache root |
|---|---|
| Linux | `~/.cache/parsimony` |
| macOS | `~/Library/Caches/parsimony` |
| Windows | `%LOCALAPPDATA%\parsimony\Cache` |

When set, the value is used verbatim with one transformation: a leading `~` is expanded via
`Path.expanduser()`. The path is otherwise taken as-is.

```python
import os

os.environ["PARSIMONY_CACHE_DIR"] = "~/my-parsimony-cache"

from parsimony import cache

print(cache.root())  # /home/<you>/my-parsimony-cache (created with 0o700 on POSIX)
```

The cache root holds four named subdirectories — `catalogs`, `models`, `connectors`, and
`staging` — each with a dedicated helper in `parsimony.cache`. Relocating the root moves all
four. The variable is consulted by every cache-path helper, and transitively by the systems
that use them:

- `parsimony.cache.models_dir()` — where `OnnxEmbedder` exports and caches ONNX/tokenizer
  artifacts (see [Embedders](../catalog/embedders.md)).
- `parsimony.cache.catalogs_dir()` — where Hugging Face catalog snapshots are downloaded
  (see [Snapshots and persistence](../catalog/snapshots.md)).
- `parsimony.cache.connectors_dir(provider)` — per-connector scratch space.

!!! note "Resolution is environment-first, then lazy"
    The override is read fresh every time the cache root is resolved — it is not captured at
    import time. Set or change it before the first cache access. The plain resolver does no
    I/O; the public helpers (`root()`, `models_dir()`, etc.) create the directory they return
    with safe permissions (`0o700` on POSIX).

!!! warning "World- and group-writable roots are rejected on POSIX"
    For security, the cache helpers refuse a root (or any existing ancestor) that is group- or
    world-writable **unless** the sticky bit is set — so `/tmp` (mode `0o1777`) is allowed but a
    plain `0o777` directory is not. Pointing `PARSIMONY_CACHE_DIR` at such a directory makes every
    helper raise `RuntimeError`. The fix is to pick a user-private directory or unset the
    variable. This guard is POSIX-only; on Windows the writable-bits check is skipped because
    `st_mode` does not reliably reflect ACLs.

!!! tip "Relative paths are not made absolute"
    The override is used verbatim apart from `~` expansion, so a relative value like
    `./cache` resolves against the current working directory at access time. Prefer an
    absolute path or a `~`-rooted one for predictability.

The `parsimony cache path` command prints the resolved root, and `parsimony cache info`
reports occupancy without creating anything. See [the CLI reference](../cli.md).

## `PARSIMONY_FAISS_IVF_THRESHOLD`

This sets the row count at which `parsimony.indexes.build_faiss` switches from an HNSW graph
index to an inverted-file (IVFFlat) index. FAISS index choice is adaptive on the number of
vectors `n`:

| Row count `n` | FAISS index | Why |
|---|---|---|
| `n < 4096` | `IndexFlatIP` | Exact, no build cost. |
| `4096 ≤ n < IVF_THRESHOLD` | `IndexHNSWFlat` | Highest recall, fits in RAM for medium catalogs. |
| `n ≥ IVF_THRESHOLD` | `IndexIVFFlat` | ~3× lower build-memory peak at scale. |

The threshold defaults to `500000`. HNSW's build memory is roughly 3–5× the raw embeddings,
which can OOM a host on very large catalogs; IVFFlat trades a few percent recall for the
headroom. The lower `4096` boundary (HNSW threshold) is a fixed constant, not configurable
via the environment.

```python
import os

os.environ["PARSIMONY_FAISS_IVF_THRESHOLD"] = "1000000"

# Import AFTER setting the variable — it is captured at module import.
from parsimony.indexes import IVF_THRESHOLD

print(IVF_THRESHOLD)  # 1000000
```

!!! warning "Captured at import time"
    `IVF_THRESHOLD` is read from the environment **once**, when `parsimony.indexes` is first
    imported. Setting or changing `PARSIMONY_FAISS_IVF_THRESHOLD` after that import has no
    effect on the already-bound module constant. Set it before importing Parsimony, or before
    anything else triggers the import (for example, building a `VectorIndex`).

!!! note "Only the FAISS build path uses it"
    This variable affects `build_faiss`, which backs `VectorIndex` (and the vector half of a
    `HybridIndex`). It is therefore only relevant when you build vector-backed catalog indexes,
    which require the `catalog` extra (`pip install "parsimony-core[catalog]"`). BM25-only
    catalogs never touch FAISS and so never consult this threshold. See
    [Indexes](../catalog/indexes.md).

## Provider and embedding-service credentials

Parsimony core does not read provider credentials. Two layers above it do, and you configure
them through their own environment variables:

- **`LiteLLMEmbeddingProvider`** routes through `litellm`, which reads its own provider
  variables from the environment — for example `OPENAI_API_KEY`, `GEMINI_API_KEY`,
  `COHERE_API_KEY`, `VOYAGE_API_KEY`, or the relevant Bedrock variables. Parsimony configures
  only the model id and dimension in code; the secret stays with `litellm`. This requires the
  `litellm` extra. See [Embedders](../catalog/embedders.md).
- **Connector plugins** (`parsimony-<name>` distributions) read whatever credentials their
  data source needs. By convention a connector takes its API key as a bound parameter — see
  [binding](../connectors/calling-binding-composing.md) — and a plugin may resolve a catalog
  snapshot URL from a per-connector environment variable of its own choosing (the variable
  name is configurable per connector, not a fixed `PARSIMONY_*` name).

!!! note "Core stays credential-free"
    Because no connectors ship in `parsimony-core`, the core package never needs a data-source
    secret. Keep provider keys in your deployment's secret store and let `litellm` or the
    plugin pick them up from the environment.

## See also

- [Caching](../caching.md) — the cache subsystem `PARSIMONY_CACHE_DIR` relocates.
- [Indexes](../catalog/indexes.md) — `build_faiss` and the FAISS index ladder `PARSIMONY_FAISS_IVF_THRESHOLD` controls.
- [Embedders](../catalog/embedders.md) — `OnnxEmbedder`'s model cache and `LiteLLMEmbeddingProvider`'s credentials.
- [Command-line interface](../cli.md) — `parsimony cache path` / `parsimony cache info` to inspect the resolved root.
