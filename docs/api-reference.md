# parsimony API Reference

**License**: Apache-2.0
**Python**: ≥ 3.11

Every symbol a plugin (or consumer) may import from `parsimony.*`. Stability
markings follow [`contract.md`](contract.md) §2: **stable** / **provisional**.
Anything not listed here is **private** and subject to change.

Connector inventories (FRED, SDMX, FMP, SEC Edgar, …) live with each plugin
package — see the [parsimony-connectors monorepo](https://github.com/ockham-sh/parsimony-connectors)
for the authoritative list. The kernel ships no provider-specific code.

---

## Table of contents

1. [Connector primitives](#connector-primitives)
2. [Result types](#result-types)
3. [Catalog](#catalog)
4. [Embedders](#embedders)
5. [Data store](#data-store)
6. [Discovery](#discovery)
7. [Publishing](#publishing)
8. [Testing](#testing)
9. [Transport](#transport)
10. [Errors](#errors)
11. [Utility helpers](#utility-helpers)
12. [CLI](#cli)
13. [Environment variables](#environment-variables)

---

## Connector primitives

```python
from parsimony import Connector, Connectors, connector, enumerator, loader, ResultCallback
```

### `Connector`

Frozen dataclass wrapping a single async data-fetching function.

**Stable.**

| Attribute | Type | Notes |
|---|---|---|
| `name` | `str` | Decorator-derived identifier. |
| `description` | `str` | First paragraph of docstring. |
| `tags` | `frozenset[str]` | Domain / `"tool"` markers. |
| `param_type` | `type[BaseModel]` | Pydantic params model. |
| `dep_names` | `frozenset[str]` | Required keyword-only deps (must be bound). |
| `optional_dep_names` | `frozenset[str]` | Optional keyword-only deps. |
| `output_config` | `OutputConfig \| None` | When set, result carries a schema. |

| Method | Signature | Description |
|---|---|---|
| `__call__` | `async (params_dict_or_model, **overrides) -> Result` | Validate params, inject bound deps, call fn, wrap in `Result`. |
| `bind_deps` | `(self, **deps) -> Connector` | Return new connector with named deps pre-bound. |
| `with_callback` | `(self, cb: ResultCallback) -> Connector` | Return new connector with an appended callback. |
| `to_llm` | `(self) -> str` | Serialize as a plain-text tool description. |
| `describe` | `(self) -> str` | Human-readable one-line summary. |

Mutation methods return **new** instances; the original is unchanged.

### `Connectors`

Immutable ordered collection of `Connector` instances.

**Stable.**

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(connectors: Iterable[Connector])` | Construct from any iterable. |
| `__add__` | `(self, other: Connectors) -> Connectors` | Combine two bundles. |
| `__getitem__` | `(self, name: str) -> Connector` | Look up by name; raises `KeyError`. |
| `__iter__` | `() -> Iterator[Connector]` | Ordered iteration. |
| `__contains__` | `(name: str) -> bool` | Name membership check. |
| `__len__` | `() -> int` | Number of connectors. |
| `filter` | `(self, *, tags=None, name=None) -> Connectors` | Filter by tag intersection or name substring. |
| `bind_deps` | `(self, **deps) -> Connectors` | Apply `bind_deps` to every connector. |
| `with_callback` | `(self, cb: ResultCallback) -> Connectors` | Apply `with_callback` to every connector. |
| `to_llm` | `(self) -> str` | Concatenate tool descriptions. |
| `describe` | `(self) -> str` | Multi-line summary. |
| `names` | `() -> list[str]` | Ordered list of connector names. |

### `connector` decorator

```python
connector(
    name: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    output: OutputConfig | None = None,
) -> Callable[[AsyncFunc], Connector]
```

**Stable.** Wraps an async function that takes a Pydantic model as its first
positional argument. When `output` is provided, the connector's return value
is coerced into a `Result` carrying that schema; otherwise the `Result`
carries no schema.

### `enumerator` decorator

```python
enumerator(output: OutputConfig) -> Callable[[AsyncFunc], Connector]
```

**Stable.** Like `connector`, but enforces a catalog-population shape on the
`OutputConfig`: exactly one KEY column, at least one TITLE column, no DATA
columns. Intended for functions that list entity identifiers for later
catalog ingestion.

### `loader` decorator

```python
loader(output: OutputConfig) -> Callable[[AsyncFunc], Connector]
```

**Stable.** Enforces observation-loading shape: at least one KEY column and
at least one DATA column.

### `ResultCallback`

```python
ResultCallback = Callable[[Result], Awaitable[None] | None]
```

**Provisional.** Post-fetch observer. Attached via `with_callback`; exceptions
are logged, not raised — the caller's `await connector(...)` always returns.

---

## Result types

```python
from parsimony import Column, ColumnRole, OutputConfig, Provenance, Result
```

### `ColumnRole`

```python
class ColumnRole(str, Enum):
    KEY      = "key"
    DATA     = "data"
    TITLE    = "title"
    METADATA = "metadata"
```

**Stable.**

### `Column`

```python
Column(
    name: str,
    role: ColumnRole,
    dtype: str = "auto",
    mapped_name: str | None = None,
    param_key: str | None = None,
    description: str | None = None,
    exclude_from_llm_view: bool = False,
    namespace: str | None = None,
)
```

**Stable.** Declares one column in a connector's output.

| Field | Purpose |
|---|---|
| `name` | Column name as it appears in the returned DataFrame. |
| `role` | KEY / TITLE / DATA / METADATA. |
| `dtype` | Coercion hint: `"auto"`, `"str"`, `"numeric"`, `"date"`, `"datetime"`, `"timestamp"`, `"bool"`, or a pandas dtype string. |
| `mapped_name` | Rename hint when the upstream response column differs. |
| `param_key` | Seed this column with a value from the connector's param model. |
| `namespace` | Catalog namespace for KEY columns. Optional — if omitted, the catalog `name` is used as the default at `add_from_result` time. |
| `exclude_from_llm_view` | Hide this column from agent-facing tool schemas. |

### `OutputConfig`

```python
OutputConfig(columns: list[Column])
```

**Stable.** Attached to a connector via the `output=` decorator argument.
Exposes:

| Method / property | Description |
|---|---|
| `.columns` | Declared columns. |
| `.validate_columns(df)` | Returns the list of declared columns not found in `df`; empty tuple means clean. |
| `.build_table_result(df, *, provenance, params=None)` | Produce a `Result` carrying this schema from a raw DataFrame. |

### `Provenance`

```python
Provenance(
    source: str,
    source_description: str | None = None,
    params: dict,
    fetched_at: datetime,
    title: str | None = None,
    properties: dict = {},
)
```

**Stable.** Immutable data-lineage record attached to every `Result`. The
`params` dict carries the serialized Pydantic params model; keyword-only
dependencies (API keys) are never included.

### `Result`

```python
Result(
    data: pd.DataFrame,
    provenance: Provenance,
    output_schema: OutputConfig | None = None,
)
```

**Stable.** Universal connector return type. The legacy
`SemanticTableResult` subclass was merged into `Result` in 0.3.0 — results
now carry the schema as an optional attribute rather than as a subclass.

**Class methods:**

| Method | Signature | Description |
|---|---|---|
| `from_dataframe` | `(df, provenance, *, output_schema=None) -> Result` | Wrap a raw DataFrame. |
| `from_arrow` | `(table: pa.Table) -> Result` | Deserialize from an Arrow table (schema + provenance are stored in Arrow metadata). |
| `from_parquet` | `(path: str \| Path) -> Result` | Deserialize from a Parquet file. |

**Instance methods and properties:**

| Member | Description |
|---|---|
| `.data` | The pandas DataFrame. |
| `.provenance` | The `Provenance` record. |
| `.output_schema` | The `OutputConfig`, if set. |
| `.entity_keys` | `pd.DataFrame` subset of KEY columns (empty if no schema). |
| `.data_columns` | `list[Column]` with `role == DATA`. |
| `.metadata_columns` | `list[Column]` with `role == METADATA`. |
| `.to_table(output_config)` | Return a new `Result` with the supplied schema. |
| `.to_arrow()` | Serialize to an Arrow table (schema + provenance in metadata). |
| `.to_parquet(path)` | Write to a Parquet file. |

---

## Catalog

```python
from parsimony import (
    Catalog,
    CatalogBackend,
    SeriesEntry,
    SeriesMatch,
    IndexResult,
    EmbedderInfo,
)
```

### `CatalogBackend` — Protocol

**Provisional.** Structural contract every catalog satisfies. Custom backends
(Postgres + pgvector, Redis, OpenSearch, in-memory mocks) are **any class
matching this shape** — no subclassing required.

```python
@runtime_checkable
class CatalogBackend(Protocol):
    name: str

    async def add(self, entries: list[SeriesEntry]) -> IndexResult: ...
    async def search(
        self,
        query: str,
        limit: int = 10,
        *,
        namespaces: list[str] | None = None,
    ) -> list[SeriesMatch]: ...
```

Two methods. The full `Catalog` class ships additional ergonomics
(`add_from_result`, `get`, `delete`, `list`, `save`, `push`, `from_url`) on
top of this base — they are **not** part of the Protocol, so custom
backends can omit them.

### `Catalog` — canonical implementation

```python
Catalog(name: str, *, embedder: EmbeddingProvider | None = None)
```

**Provisional.** Parquet rows + FAISS vectors + BM25 keywords + reciprocal
rank fusion. Requires `parsimony-core[standard]`. `name` is validated as
lowercase snake_case.

**Protocol methods** (also on every `CatalogBackend`):

| Method | Description |
|---|---|
| `add(entries)` | Upsert entries; embeds missing rows, rebuilds FAISS + BM25. Returns `IndexResult`. |
| `search(query, limit, *, namespaces=None)` | Hybrid RRF search over BM25 + FAISS. |

**Extras on the canonical `Catalog`:**

| Method | Description |
|---|---|
| `add_from_result(result, *, batch_size=100, extra_tags=None, dry_run=False, force=False)` | Lift rows from a `Result` into `SeriesEntry` values and `add` them. KEY columns with no `namespace=` fall back to this catalog's `name`. |
| `get(namespace, code)` | Retrieve one entry or `None`. |
| `delete(namespace, code)` | Remove one entry. |
| `exists(keys)` | Subset of `keys` that already exist. |
| `list(*, namespace=None, q=None, limit=50, offset=0)` | Paginated browse: `(entries, total)`. |
| `list_namespaces()` | Distinct namespaces, sorted. |
| `entries` *(property)* | In-memory list of all entries. |
| `embedder_info` *(property)* | `EmbedderInfo` or `None`. |
| `save(path, *, builder=None)` | Atomically write a three-file snapshot (`meta.json`, `entries.parquet`, `embeddings.faiss`). |
| `load(path, *, embedder=None)` *(classmethod)* | Load a snapshot from a local directory. |
| `from_url(url, *, embedder=None)` *(classmethod)* | Dispatch on scheme: `file://` / `hf://`. |
| `push(url)` | Publish to `file://` or `hf://`. |
| `close()` | Release resources. |

**On-disk snapshot layout** (one directory, three files):

| File | Contents |
|---|---|
| `meta.json` | Name, namespaces, entry count, embedder identity, build info. |
| `entries.parquet` | `namespace`, `code`, `title`, `description`, `tags_json`, `metadata_json`, `embedding`. |
| `embeddings.faiss` | FAISS index serialized via `faiss.write_index`. |

Writes are atomic via temp-directory rename. The embedder recorded in
`meta.json` must match (`dim`, `normalize`) the embedder supplied at
`load` / `from_url` time, or `ValueError` is raised.

### `SeriesEntry`

```python
SeriesEntry(
    namespace: str,
    code: str,
    title: str,
    tags: list[str] = [],
    description: str | None = None,
    metadata: dict = {},
    embedding: list[float] | None = None,
)
```

**Stable.** `embedding_text()` composes the string an embedder indexes:
title, then `", ".join("k: v")` over metadata, then `"tags: a, b"`, joined
with `" | "`. The representation is fixed — changing it would require
reindexing.

### `SeriesMatch`

```python
SeriesMatch(
    namespace: str,
    code: str,
    title: str,
    similarity: float,
    tags: list[str] = [],
    description: str | None = None,
    metadata: dict = {},
)
```

**Stable.** `similarity` is clamped to `[0, 1]`; higher is more relevant.

### `IndexResult`

```python
IndexResult(total: int = 0, indexed: int = 0, skipped: int = 0, errors: int = 0)
```

**Provisional.** Returned by `Catalog.add` / `Catalog.add_from_result`.

### `EmbedderInfo`

```python
EmbedderInfo(
    model: str,
    dim: int,
    normalize: bool = True,
    package: str | None = None,
)
```

**Provisional.** Persisted in `meta.json`. `package` is an install hint
surfaced in error messages when the recorded embedder can't be reconstructed.

---

## Embedders

```python
from parsimony import EmbeddingProvider, SentenceTransformerEmbedder, LiteLLMEmbeddingProvider
```

### `EmbeddingProvider` — Protocol

**Provisional.** Structural contract for embedders. Any class with these
methods satisfies it:

| Method | Signature |
|---|---|
| `dimension` | `@property -> int` |
| `embed_texts` | `async (texts: list[str]) -> list[list[float]]` |
| `embed_query` | `async (query: str) -> list[float]` |
| `info` | `() -> EmbedderInfo` |

Embedders are **not** a plugin axis — users pass whichever instance they need
to the catalog at construction.

### `SentenceTransformerEmbedder` — `[standard]` extra

```python
SentenceTransformerEmbedder(
    model: str = "BAAI/bge-small-en-v1.5",
    normalize: bool = True,
    device: str | None = None,
    batch_size: int = 64,
)
```

**Provisional.** Vectors are L2-normalized by default so the FAISS
inner-product index equals cosine similarity. Instantiation is cheap; the
model loads on first embed.

### `LiteLLMEmbeddingProvider` — `[litellm]` extra

```python
LiteLLMEmbeddingProvider(
    model: str,                # e.g. "gemini/text-embedding-004"
    dimension: int,
    normalize: bool = True,
    batch_size: int = 100,
)
```

**Provisional.** Wraps `litellm.aembedding()` for hosted embedding APIs
(OpenAI, Gemini, Cohere, Voyage, Bedrock, …). Identity (`model`,
`dimension`) is supplied at construction; this provider does not introspect
the remote endpoint.

---

## Data store

```python
from parsimony import InMemoryDataStore, LoadResult
```

### `InMemoryDataStore`

**Provisional.** Dict-backed `DataStore` for observation persistence used
with `@loader`. Suitable for development and testing.

| Method | Signature |
|---|---|
| `upsert` | `async (namespace, code, result) -> None` |
| `get` | `async (namespace, code) -> Result \| None` |
| `delete` | `async (namespace, code) -> None` |
| `exists` | `async (namespace, code) -> bool` |
| `load_result` | `async (entries, connector) -> LoadResult` |

### `LoadResult`

```python
LoadResult(total: int, loaded: int, skipped: int, errors: int)
```

**Provisional.** Summary from `DataStore.load_result`.

---

## Discovery

```python
from parsimony.discovery import (
    DiscoveredProvider,
    discovered_providers,
    iter_entry_points,
    load_provider,
    build_connectors_from_env,
    PluginError, PluginImportError, PluginContractError,
)
```

### `DiscoveredProvider`

**Stable.** Record returned from `discovered_providers` / `load_provider`.

| Field | Type |
|---|---|
| `name` | `str` — entry-point key. |
| `module_path` | `str` — dotted module path. |
| `connectors` | `Connectors` |
| `env_vars` | `dict[str, str]` |
| `provider_metadata` | `dict[str, Any]` |
| `distribution_name` | `str` |
| `version` | `str` |
| `module` | `ModuleType` |

### `discovered_providers() -> list[DiscoveredProvider]`

**Stable.** Walks the `parsimony.providers` entry-point group once, caches
results. Failures are logged and skipped — not raised.

### `iter_entry_points() -> Iterator[EntryPoint]`

**Stable.** Lower-level iteration over the entry-point group itself.

### `load_provider(entry_point) -> DiscoveredProvider`

**Stable.** Load one entry point and validate against §4. Raises
`PluginImportError` if the module fails to import, `PluginContractError` if
it violates the export contract.

### `build_connectors_from_env(*, env=None) -> Connectors`

**Stable.** Compose every discovered provider, binding `ENV_VARS` keys from
`os.environ` (or `env` if supplied). Providers whose required env vars are
absent are silently excluded.

---

## Publishing

```python
from parsimony.publish import publish, publish_provider, collect_catalogs, PublishReport
```

### `publish(module, *, target, only=None, dry_run=False) -> PublishReport`

**Stable.** Build one `Catalog` per declared namespace and push each to
`target.format(namespace=...)`. `only` restricts to specific namespaces
(uses `RESOLVE_CATALOG` if present to skip the `CATALOGS` walk).

### `publish_provider(name, *, target, only=None, dry_run=False) -> PublishReport`

**Stable.** Like `publish`, but looks up the provider module by the
entry-point key returned from `discovered_providers`.

### `collect_catalogs(module, *, only=None) -> list[tuple[str, Callable]]`

**Provisional.** Iterate a plugin module's declared catalogs without
publishing them. Useful for dry-run verification.

### `PublishReport`

**Provisional.**

| Field | Type |
|---|---|
| `provider` | `str` |
| `target_template` | `str` |
| `published` | `list[str]` — namespaces successfully published. |
| `skipped` | `list[str]` — namespaces that had no rows. |
| `failed` | `list[str]` — namespaces that raised. |

---

## Testing

```python
from parsimony.testing import assert_plugin_valid, ConformanceError, ProviderTestSuite, iter_check_names
```

### `assert_plugin_valid(module, *, skip: Iterable[str] = ()) -> None`

**Stable.** Run the conformance suite against a plugin module. Raises
`ConformanceError` on any failure. See [`contract.md`](contract.md) §7 for
the exact checks.

### `ConformanceError`

**Stable.** `AssertionError` subclass carrying:

| Attribute | Description |
|---|---|
| `check` | Check name (e.g. `"check_connectors_exported"`). |
| `reason` | Human-readable failure reason. |
| `module_path` | Dotted module path. |
| `next_action` | Optional suggested fix. |
| `to_report_dict()` | Dict suitable for JSON serialization. |

### `ProviderTestSuite`

**Stable.** Pytest-native base class. Subclass it and set `module` to
the plugin module; the four `test_*` methods are discovered by pytest.

```python
import parsimony_myplugin
from parsimony.testing import ProviderTestSuite

class TestMyPlugin(ProviderTestSuite):
    module = parsimony_myplugin
```

### `iter_check_names() -> Iterable[str]`

**Provisional.** Enumerate conformance-check identifiers.

---

## Transport

```python
from parsimony.transport import (
    HttpClient,
    pooled_client,
    map_http_error,
    map_timeout_error,
    redact_url,
    parse_retry_after,
)
```

### `HttpClient`

**Stable.** Thin wrapper around `httpx.AsyncClient` with credential-redacted
structured logging. A new `httpx.AsyncClient` is created per request to
avoid event-loop sharing issues across `asyncio.run()` calls.

Redacted query-param names: `api_key`, `apikey`, `token`, `access_token`,
`refresh_token`, `id_token`, `client_secret`, `secret`, `password`,
`authorization`, and any name ending in `_token`.

### `pooled_client(base_url, *, default_params=None)`

**Provisional.** Async context manager yielding a connection-pooled
`httpx.AsyncClient` for burst workloads. Use when making many requests in a
tight loop; `HttpClient` is better for one-shot calls.

### `map_http_error(exc, *, provider, op_name=None) -> ConnectorError`

**Provisional.** Translate `httpx.HTTPStatusError` to a typed
`parsimony.errors` exception:

- `401` / `403` → `UnauthorizedError`
- `402` → `PaymentRequiredError`
- `429` → `RateLimitError`
- anything else → `ProviderError`

### `map_timeout_error(exc, *, provider, op_name=None) -> ProviderError`

**Provisional.** Translate `httpx.TimeoutException` to a `ProviderError`
carrying the provider identity.

### `redact_url(url) -> str`

**Provisional.** Strip sensitive query-param values from a URL (same
redacted-name list as `HttpClient`).

### `parse_retry_after(response, *, default=60.0) -> float`

**Provisional.** Extract retry-after seconds from a 429 response (honoring
`Retry-After` and `X-Ratelimit-Reset` epoch fallback).

---

## Errors

```python
from parsimony import (
    ConnectorError,
    EmptyDataError,
    ParseError,
    ProviderError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
```

All **stable**. Hierarchy:

```
ConnectorError(provider: str)
├── UnauthorizedError      — 401/403 credentials rejected
├── PaymentRequiredError   — 402 plan restriction
├── RateLimitError         — 429 rate-limit (carries retry_after: float, quota_exhausted: bool)
├── ProviderError          — 5xx / unexpected status
├── EmptyDataError         — 200 but no rows
└── ParseError             — 200 but unparseable
```

Every error carries `.provider: str` so callers can identify the source
without parsing message strings. `RateLimitError.retry_after` is the wait
in seconds; `RateLimitError.quota_exhausted=True` indicates a terminal
condition (do not retry).

These are operational errors. Programmer errors (bad types, invalid
parameters) remain `TypeError` / `ValueError` / Pydantic `ValidationError`.

---

## Utility helpers

```python
from parsimony import (
    catalog_key,
    code_token,
    normalize_code,
    normalize_entity_code,
    parse_catalog_url,
    series_match_from_entry,
)
```

| Symbol | Stability | Signature | Description |
|---|---|---|---|
| `catalog_key` | stable | `(namespace, code) -> tuple[str, str]` | Canonical in-memory key; normalizes both sides. |
| `code_token` | stable | `(value) -> str` | Provider-side: slugify any string into a valid `code` (lowercase, underscores, leading-digit guard). |
| `normalize_code` | stable | `(value) -> str` | Validate lowercase snake_case namespace; raises `ValueError` otherwise. |
| `normalize_entity_code` | stable | `(value) -> str` | Validate non-empty trimmed entity-code string. |
| `parse_catalog_url` | stable | `(url) -> tuple[str, str, str]` | Lift `scheme://root[/sub]` into `(scheme, root, sub)`. |
| `series_match_from_entry` | provisional | `(entry, *, similarity) -> SeriesMatch` | Build a match record from a stored entry. |

---

## CLI

The `parsimony` console script exposes two verbs.

### `parsimony list`

Enumerate installed plugins and their declared catalogs.

```bash
parsimony list                  # human-readable table
parsimony list --json           # machine-readable JSON
parsimony list --strict         # run conformance suite; exit non-zero on failure
parsimony list --strict --json  # JSON + strict — the security-review artefact
```

### `parsimony publish`

Build one `Catalog` per declared namespace and push each to the URL template.

```bash
parsimony publish --provider NAME --target 'file:///out/{namespace}'
parsimony publish --provider NAME --target 'hf://org/catalog-{namespace}'
parsimony publish --provider NAME --target '...' --only ns1 --only ns2
parsimony publish --provider NAME --target '...' --dry-run
```

`{namespace}` in `--target` is substituted per catalog before the push.
`--only NS` can be repeated; when the plugin exports `RESOLVE_CATALOG`, the
publisher tries the resolver first and skips the `CATALOGS` walk if every
requested namespace resolves.

Supported target schemes:

| Scheme | Destination | Extra required |
|---|---|---|
| `file://<path>` | Local filesystem | — |
| `hf://<org>/<repo>` | Hugging Face dataset repo | `[standard]` |

`s3://` is planned; not yet shipping.

---

## Environment variables

Variables read by the kernel itself (plugin-specific credentials are listed
in each plugin's own docs).

| Variable | Purpose |
|---|---|
| `PARSIMONY_EMBED_CONCURRENCY` | Cap on concurrent embedder calls (default: CPU count). |
| `PARSIMONY_MAX_LOADED_BUNDLES` | LRU cap on `Catalog.from_url` cache (default: 16). |
| `PARSIMONY_CATALOG_PIN` | Pin a specific revision when loading `hf://` bundles. |
| `PARSIMONY_LOG_LEVEL` | Log level for the `parsimony.*` logger family. |
