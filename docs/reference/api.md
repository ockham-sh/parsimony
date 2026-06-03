# Public API & import map

This page is the canonical map of what to import from where. Parsimony exposes a small,
curated **top-level** surface (`from parsimony import ...`) of 35 names, plus a larger set of
symbols that live only in submodules. When a name is not in the top-level list below, import it
from its submodule — the explicit path always works and is the convention this documentation
follows.

The distribution is `parsimony-core` `0.7.0`; the import name is `parsimony`; Python `>=3.11`.

```python
import parsimony

print(parsimony.__version__)  # "0.7.0"
print(len(parsimony.__all__))  # 35
```

## Top-level names

These 35 names are re-exported from the package root and make up `parsimony.__all__`. They are
grouped below by concern; the grouping is editorial — at runtime they are a flat namespace.

### Connectors

| Name | Kind | Import |
|---|---|---|
| `Connector` | class — frozen async callable + metadata | `from parsimony import Connector` |
| `Connectors` | class — immutable connector collection | `from parsimony import Connectors` |
| `connector` | decorator — turn an `async def` into a `Connector` | `from parsimony import connector` |
| `loader` | decorator — connector verb that feeds a data store | `from parsimony import loader` |
| `enumerator` | decorator — connector verb that feeds a catalog | `from parsimony import enumerator` |

See [The connector model](../connectors/index.md), [Defining connectors](../connectors/defining-connectors.md),
and [Loaders and enumerators](../connectors/loaders-and-enumerators.md).

### Results and output schemas

| Name | Kind | Import |
|---|---|---|
| `Result` | class — scalar/dict result envelope (`data` + `provenance`) | `from parsimony import Result` |
| `TabularResult` | class — DataFrame result envelope | `from parsimony import TabularResult` |
| `OutputConfig` | class — declarative output schema | `from parsimony import OutputConfig` |
| `Column` | class — one schema column | `from parsimony import Column` |
| `ColumnRole` | enum — `DATA` / `KEY` / `TITLE` / `METADATA` | `from parsimony import ColumnRole` |
| `Provenance` | class — framework-built fetch metadata | `from parsimony import Provenance` |

See [Results and output schemas](../connectors/results.md).

### Errors

| Name | Raised for | Import |
|---|---|---|
| `ConnectorError` | base for all operational connector errors (carries `provider`) | `from parsimony import ConnectorError` |
| `UnauthorizedError` | invalid/missing credentials (HTTP 401/403) | `from parsimony import UnauthorizedError` |
| `PaymentRequiredError` | plan does not permit the endpoint/params (HTTP 402) | `from parsimony import PaymentRequiredError` |
| `RateLimitError` | rate limit / quota (HTTP 429) | `from parsimony import RateLimitError` |
| `ProviderError` | 5xx, other 4xx, or timeout (carries `status_code`) | `from parsimony import ProviderError` |
| `EmptyDataError` | HTTP 200 but no rows | `from parsimony import EmptyDataError` |
| `ParseError` | HTTP 200 but unparseable (also a schema-application failure) | `from parsimony import ParseError` |
| `InvalidParameterError` | call-time parameter validation failure | `from parsimony import InvalidParameterError` |

See [Errors](../connectors/errors.md). Note `CatalogNotFoundError` is **not** top-level — see below.

### Catalog

| Name | Kind | Import |
|---|---|---|
| `Catalog` | class — searchable in-memory index over entities | `from parsimony import Catalog` |
| `Entity` | class — namespace + code + title + metadata record | `from parsimony import Entity` |
| `CatalogMatch` | class — one search hit | `from parsimony import CatalogMatch` |
| `CatalogIndex` | protocol — per-field index contract | `from parsimony import CatalogIndex` |
| `BM25Index` | class — lexical (BM25) field index | `from parsimony import BM25Index` |
| `VectorIndex` | class — dense-vector field index | `from parsimony import VectorIndex` |
| `HybridIndex` | class — BM25 + vector fusion field index | `from parsimony import HybridIndex` |

See [The Catalog](../catalog/index.md), [Indexes](../catalog/indexes.md), and
[Building and searching](../catalog/search.md).

!!! tip
    For catalog-heavy code the catalog names are also re-exported from `parsimony.catalog`, so
    `from parsimony.catalog import Catalog, Entity, CatalogMatch, BM25Index, ...` is the clearest
    convention. That submodule additionally exposes the symbols listed in the
    [submodule map](#submodule-only-names) below.

### Ranking and fusion

| Name | Kind | Import |
|---|---|---|
| `Ranker` | protocol — collapse per-index rankings into one | `from parsimony import Ranker` |
| `Ranking` | class — one ranked list of identities | `from parsimony import Ranking` |
| `RRF` | class — Reciprocal Rank Fusion ranker (`k=60`) | `from parsimony import RRF` |
| `ZScoreFusion` | class — Z-score score-fusion ranker (default Hybrid fusion) | `from parsimony import ZScoreFusion` |
| `MinMaxScoreFusion` | class — min-max score-fusion ranker | `from parsimony import MinMaxScoreFusion` |

See [Ranking and fusion](../catalog/ranking-and-fusion.md). The serializable ranker specs and the
helpers `concat` / `ranking_from_scores` live in `parsimony.ranking` only — see below.

### Stores

| Name | Kind | Import |
|---|---|---|
| `InMemoryDataStore` | class — process-local store for loader output | `from parsimony import InMemoryDataStore` |
| `LoadResult` | class — load statistics (total/loaded/skipped/errors) | `from parsimony import LoadResult` |

See [Data stores](../catalog/data-store.md).

### Modules

| Name | Kind | Import |
|---|---|---|
| `cache` | module — on-disk cache helpers and `TTLDiskCache` | `from parsimony import cache` |
| `discover` | module — plugin/provider discovery | `from parsimony import discover` |

See [Caching](../caching.md) and [Discovering installed providers](../plugins/discovery.md).

!!! note "These are the only top-level names"
    `from parsimony import <name>` works **only** for the 35 names above. Anything else raises
    `AttributeError`. Import every other symbol from its submodule, using the paths in the next
    section.

## Submodule-only names

The following symbols are public and supported, but they are **not** re-exported at the package
root. Import them from the module shown.

### `parsimony.connector`

| Name | Kind |
|---|---|
| `ResultCallback` | type alias — `Callable[[Result], Any]`; the `with_callback` observer contract |

```python
from parsimony.connector import ResultCallback
```

See [Calling, binding, and composing](../connectors/calling-binding-composing.md).

### `parsimony.errors`

| Name | Kind |
|---|---|
| `CatalogNotFoundError` | exception — a configured/lazy catalog bundle is missing or unreachable |

```python
from parsimony.errors import CatalogNotFoundError
```

All eight top-level error classes are also importable from `parsimony.errors`; only
`CatalogNotFoundError` is exclusive to it. See [Errors](../connectors/errors.md).

### `parsimony.catalog`

| Name | Kind |
|---|---|
| `DisMaxIndex` | class — DisMax index over several entity fields under one surface name |
| `StructuredQuery` | class — parsed structured query (`FIELD: v1, v2 && ...`) |
| `parse_query` | function — parse a query string into broad/structured form |
| `SearchDiagnostic` | class — `search()` diagnostics (carries `.mode`) |
| `UnknownIndexedFieldError` | exception — structured clause names an unindexed field |
| `BroadSearchUnavailableError` | exception — broad search with no default-field index |
| `BroadSearchConfigError` | exception — broad-search configuration error |
| `code_token` | function — normalize a string into a provider code token |
| `entity_key` | function — canonical `(namespace, code)` key |
| `normalize_namespace` | function — validate/normalize a namespace to snake_case |
| `field_text` | function — searchable text for an entity field |
| `field_values` | function — searchable value list for an entity field |

```python
from parsimony.catalog import (
    DisMaxIndex,
    StructuredQuery,
    parse_query,
    SearchDiagnostic,
    UnknownIndexedFieldError,
    BroadSearchUnavailableError,
    BroadSearchConfigError,
    code_token,
    entity_key,
    normalize_namespace,
    field_text,
    field_values,
)
```

!!! note
    `code_token`, `entity_key`, `normalize_namespace`, `field_text`, and `field_values` are
    defined in `parsimony.entity` and re-exported through `parsimony.catalog`; either import path
    resolves to the same object. See [Entities](../catalog/entities.md).

See [Building and searching](../catalog/search.md) and [Indexes](../catalog/indexes.md).

### `parsimony.catalog.policy`

| Name | Kind |
|---|---|
| `adaptive_field_index` | function — pick Hybrid below 1000 distinct values, else BM25 |
| `discovery_indexes` | function — a code/title/description index map for discovery |

```python
from parsimony.catalog.policy import adaptive_field_index, discovery_indexes
```

See [Indexes](../catalog/indexes.md).

### `parsimony.ranking`

The top-level `Ranker`, `Ranking`, `RRF`, `ZScoreFusion`, and `MinMaxScoreFusion` are also
importable here. These additional symbols live in `parsimony.ranking` only:

| Name | Kind |
|---|---|
| `RankedItem` / `RankedSetItem` | dataclass — one ranked identity / per-index ranked identity |
| `RankingSet` | dataclass — named collection of per-index rankings |
| `concat` | function — build a `RankingSet` from named `Ranking` objects |
| `ranking_from_scores` | function — build a `Ranking` from `(namespace, code, score)` rows |
| `RRFSpec` / `MinMaxScoreFusionSpec` / `ZScoreFusionSpec` | model — serializable ranker specs |
| `RankerSpec` | type alias — discriminated union of the three specs |
| `ranker_to_spec` / `ranker_from_spec` | function — serialize / rebuild a built-in ranker |

```python
from parsimony.ranking import concat, ranking_from_scores, RRFSpec, ranker_to_spec
```

See [Ranking and fusion](../catalog/ranking-and-fusion.md).

### `parsimony.embedder`

Used by `VectorIndex`. The embedder classes import lazily inside their methods, so the optional
extras (`standard`, `standard-onnx`, `litellm`) are needed only at use, not at import.

| Name | Kind |
|---|---|
| `EmbeddingProvider` | protocol — `dimension`, async `embed_texts`/`embed_query`, `info()` |
| `EmbedderInfo` | model — the `(model, dim, normalize)` identity key (Pydantic) |
| `SentenceTransformerEmbedder` | class — default embedder (`[standard]`) |
| `OnnxEmbedder` | class — int8-quantized ONNX embedder (`[standard-onnx]`) |
| `LiteLLMEmbeddingProvider` | class — hosted-API embedder (`[litellm]`) |
| `DEFAULT_MODEL` | constant — `"sentence-transformers/all-MiniLM-L6-v2"` |

```python
from parsimony.embedder import EmbeddingProvider, SentenceTransformerEmbedder, DEFAULT_MODEL
```

See [Embedders](../catalog/embedders.md).

### `parsimony.transport` and `parsimony.transport.helpers`

The async HTTP layer connector authors build on.

`parsimony.transport`:

| Name | Kind |
|---|---|
| `HttpClient` | class — async client with base URL, default headers/params, redacted logging |
| `HttpRetryPolicy` | dataclass — transient-retry policy |
| `DEFAULT_HTTP_RETRY_POLICY` | constant — the default `HttpRetryPolicy()` |
| `map_http_error` / `map_timeout_error` | function — map `httpx` errors to typed errors |
| `parse_retry_after` | function — extract retry-after seconds from a 429 response |
| `pooled_client` | async context manager — yields a connection-pooled `HttpClient` |
| `redact_url` / `redact_params_for_logging` / `redact_sensitive_text` | function — secret redaction |

`parsimony.transport.helpers`:

| Name | Kind |
|---|---|
| `fetch_json` | function — GET + `raise_for_status` + map + JSON (drops `None` params) |
| `make_http_client` | function — build an `HttpClient` |
| `make_api_key_client` | function — build an `HttpClient` that injects an API key (default query param `apikey`) |

```python
from parsimony.transport import HttpClient, HttpRetryPolicy, pooled_client
from parsimony.transport.helpers import fetch_json, make_api_key_client
```

See [HTTP transport](../connectors/http-transport.md).

### `parsimony.testing`

The conformance toolkit for plugin authors.

| Name | Kind |
|---|---|
| `assert_plugin_valid` | function — fail-fast; runs the five conformance checks on a module |
| `iter_check_names` | function — iterate the check names |
| `ConformanceError` | exception (subclasses `AssertionError`) — a failed conformance check |
| `ProviderTestSuite` | base class — auto-collected pytest suite for a provider module |

```python
from parsimony.testing import assert_plugin_valid, ProviderTestSuite, ConformanceError
```

See [Conformance testing](../plugins/conformance.md).

### `parsimony.indexes`

The low-level FAISS helpers used by `VectorIndex` / `HybridIndex` (the `standard` extra). `faiss`
imports lazily inside these functions, so importing the module itself does not require it.

| Name | Kind |
|---|---|
| `tokenize` | function — tokenize text for BM25 |
| `build_faiss` | function — build an adaptive Flat/HNSW/IVF FAISS index |
| `read_faiss` / `write_faiss` | function — load/save a FAISS index |

```python
from parsimony.indexes import tokenize, build_faiss, read_faiss, write_faiss
```

See [Indexes](../catalog/indexes.md) and [Environment variables](environment.md) for
`PARSIMONY_FAISS_IVF_THRESHOLD`.

## Lazy imports

`import parsimony` is intentionally cheap. The catalog, ranking, and store names
(`Catalog`, `Entity`, `CatalogMatch`, `BM25Index`, `VectorIndex`, `HybridIndex`, `CatalogIndex`,
`InMemoryDataStore`, `LoadResult`, `RRF`, `Ranker`, `Ranking`, `ZScoreFusion`,
`MinMaxScoreFusion`) are resolved lazily through the module's `__getattr__` on first access. Only
then is the underlying submodule — and any heavy dependency it pulls (FAISS, sentence-transformers,
huggingface-hub) — imported.

```python
import parsimony

# "Catalog" is not materialized in the module namespace until you touch it:
assert "Catalog" not in vars(parsimony)
Catalog = parsimony.Catalog  # triggers the lazy import of parsimony.catalog
```

The practical consequence: a connector-only program (define and call connectors, handle typed
errors) never pays for the catalog's optional dependencies. Those are pulled in on demand the
first time you reference a catalog symbol. See [Installation](../getting-started/installation.md)
for the optional-extras matrix.

## See also

- [Core concepts](../getting-started/concepts.md) — the mental model behind these names
- [Errors](../connectors/errors.md) — the typed exception taxonomy in full
- [The Catalog](../catalog/index.md) — the searchable index layer
- [Environment variables](environment.md) — `PARSIMONY_CACHE_DIR`, `PARSIMONY_FAISS_IVF_THRESHOLD`
