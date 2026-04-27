# Architecture

**Audience**: kernel contributors and plugin authors who want the full mental
model.

This document describes the internal design of `parsimony-core`: the flat
module layout, the connector pattern and its three decorator variants, the
`CatalogBackend` Protocol and its canonical `Catalog` implementation, the
HTTP transport layer, the typed error hierarchy, and the plugin discovery /
publish contract.

For the authoritative import surface, see [`api-reference.md`](api-reference.md).
For the plugin contract that external packages implement, see
[`contract.md`](contract.md).

---

## Design principles

Three principles shape every decision:

**1. The kernel ships no data.** No connector is in-tree. Every data source is
a separate PyPI distribution (`parsimony-<name>`) discovered through the
`parsimony.providers` entry-point group. `tests/test_kernel_purity.py`
enforces this structurally — the kernel source tree contains no
provider-specific code.

**2. Structural typing over nominal inheritance.** `CatalogBackend` and
`EmbeddingProvider` are `typing.Protocol` classes, not ABCs. Custom
backends — Postgres + pgvector, Redis, OpenSearch, in-memory fakes — are
any class matching the shape; no subclassing required.

**3. Flat module layout.** The kernel is 13 top-level files in a single
`parsimony/` package. No subpackages. A reader can see the Protocol and
its reference implementation in one scroll.

---

## Module layout

```
parsimony/
├── __init__.py         Public surface (lazy-loaded via PEP 562 __getattr__).
├── connector.py        Connector + Connectors + @connector / @enumerator / @loader.
├── result.py           Result + Provenance + OutputConfig + Column + ColumnRole.
├── catalog.py          CatalogBackend Protocol + Catalog + SeriesEntry/Match/IndexResult + parse_catalog_url.
├── embedder.py         EmbeddingProvider Protocol + SentenceTransformerEmbedder + LiteLLMEmbeddingProvider + EmbedderInfo.
├── indexes.py          FAISS + BM25 + RRF pure functions (used by Catalog; not public).
├── publish.py          publish(module, ...) orchestrator — reads CATALOGS / RESOLVE_CATALOG.
├── discover.py         Provider + iter_providers + load + load_all (~70 LOC).
├── stores.py           InMemoryDataStore + LoadResult (observation persistence for @loader).
├── errors.py           ConnectorError hierarchy.
├── transport.py        HttpClient + pooled_client + map_http_error + redact_url + parse_retry_after.
├── testing.py          assert_plugin_valid (3 checks) + ProviderTestSuite (4 test methods).
├── cli.py              Two verbs: `parsimony list` and `parsimony publish`.
└── py.typed
```

Heavy catalog dependencies — `faiss-cpu`, `sentence-transformers`,
`huggingface_hub` — live under the `[standard]` extra and load lazily. A
bare `import parsimony` without the extra succeeds and costs <100 ms; only
first access to `Catalog` / embedders pays the import cost.

The two most widely imported modules are `result.py` and `catalog.py`. The
dependency graph is a DAG — no circular imports.

---

## The connector pattern

The central abstraction is the `Connector` frozen dataclass defined in
`connector.py`:

```python
@dataclass(frozen=True)
class Connector:
    name: str
    description: str
    tags: tuple[str, ...]
    param_type: type[BaseModel]
    dep_names: frozenset[str]
    optional_dep_names: frozenset[str]
    fn: Callable                    # wrapped async function (partial after bind/bind_env)
    output_config: OutputConfig | None
    env_map: Mapping[str, str]      # decorator-declared env-var backings; frozen
    bound: bool                     # False on bind_env clones with missing required env vars
    _callbacks: tuple[ResultCallback, ...]
```

### Decoration flow

When an author writes:

```python
@connector(output=MY_OUTPUT, tags=["macro", "tool"])
async def my_fetch(params: MyParams, *, api_key: str) -> pd.DataFrame:
    """First line becomes the connector description."""
    ...
```

the decorator inspects the function signature to extract:

- `param_type` — first positional parameter's Pydantic model annotation.
- `dep_names` — required keyword-only parameters after `*`.
- `optional_dep_names` — keyword-only parameters with defaults.
- `output_config` — the `output=` argument.
- `description` — first paragraph of the docstring.

The decorator replaces the function with a `Connector` instance. Calling the
connector with kwargs or a model instance triggers Pydantic validation, calls
the wrapped function with bound deps, and wraps the return value in a
`Result`.

### Decorator variants

Three decorators, all producing the same `Connector` runtime type but
enforcing different column-role contracts on `OutputConfig`:

| Decorator | KEY required | TITLE required | DATA allowed | METADATA allowed | Intended use |
|---|:-:|:-:|:-:|:-:|---|
| `@connector` | no | no | yes | yes | Search / profile / fetch. |
| `@enumerator(output)` | yes | yes | **no** | yes | Catalog population: list series IDs for `Catalog.add_from_result`. |
| `@loader(output)` | yes | no | yes | yes | Observation loading: time-series persistence via `DataStore.load_result`. |

The `output` argument is mandatory for `@enumerator` / `@loader`, optional
for `@connector`. The schema enforcement exists so catalog indexing and
data-store loading are reliable — an `@enumerator` result can always be
passed to `Catalog.add_from_result` because it is guaranteed to carry
identifiable KEY + TITLE columns.

### Immutability

All mutation returns a **new** `Connector`; the original is untouched:

```python
bound = my_fetch.bind(api_key="secret")              # new Connector
logged = bound.with_callback(my_observer)            # new Connector
assert my_fetch.dep_names == frozenset({"api_key"})  # unchanged
assert bound.dep_names == frozenset()                # deps consumed
```

`Connectors` follows the same pattern: `.merge()`, `.filter()`,
`.bind()`, `.bind_env()`, `.replace()`, `.with_callback()` all return new
instances.

---

## Dependency injection

Keyword-only parameters after `*` declare runtime dependencies. The
decorator's `env={...}` keyword maps each one to the env var that should
back it:

```
@connector(env={"api_key": "FRED_API_KEY"})
async def connector_fn(params, *, api_key: str): ...
     ↓
Connector(
    dep_names=frozenset({"api_key"}),
    env_map={"api_key": "FRED_API_KEY"},
    fn=connector_fn,
    bound=True,
)
     ↓ Connectors([connector_fn]).bind_env()  # reads os.environ["FRED_API_KEY"]
Connector(
    dep_names=frozenset(),
    fn=partial(connector_fn, api_key=...),
    bound=True,
)
     ↓ connector(series_id="GDP")
1. Reject if bound=False → raise UnauthorizedError naming missing env var
2. Pydantic validates kwargs → MyParams(series_id="GDP")
3. fn(params, **bound_deps) called
4. Return value wrapped in Result
5. Callbacks fired with the Result
```

`Connectors.bind_env(overrides=None)` walks each connector's `env_map`
and resolves values from `os.environ` (optionally layered with
`overrides`). Connectors whose required env vars are missing stay in the
collection but are marked `bound=False` — calling one raises
`UnauthorizedError` immediately, naming the missing variable. Inspect via
`Connectors.unbound`.

For non-env dependencies (DB pools, HTTP clients), use `Connectors.bind`
directly. Dependencies bound via either path become part of the function
partial. They are never stored in `Provenance.params` — API keys do not
appear in lineage records, logs, or serialized results.

---

## Result pipeline

```
Raw API response (JSON/CSV/XML)
    ↓ connector implementation
pd.DataFrame
    ↓ Connector.__call__()
Result(data=df, provenance=Provenance(...), output_schema=OutputConfig | None)
    ↓ observer callbacks fired (exceptions logged, not raised)
Result returned to caller
```

`Result` is a single class. Results carry an optional
`output_schema: OutputConfig | None`; the schema-aware accessors
(`entity_keys`, `data_columns`, `metadata_columns`) return empty sequences
when the schema is absent.

Both Arrow and Parquet serialization are supported. The schema and
provenance are stored in the Arrow table metadata, so a round-trip via
`to_arrow` / `from_arrow` (or `to_parquet` / `from_parquet`) preserves the
full shape.

---

## Catalog subsystem

### Protocol + reference implementation

The catalog is a two-layer design:

1. **`CatalogBackend`** — a `typing.Protocol` in `catalog.py`. Two methods:
   `add(entries)` and `search(query, limit, *, namespaces=None)`.
2. **`Catalog`** — the canonical implementation. Parquet rows + FAISS
   vectors + BM25 keywords + Reciprocal Rank Fusion, with `file://` and
   `hf://` load/push support. Ships under `parsimony-core[standard]`.

Custom backends match the Protocol; they are **not** required to subclass
anything. A Postgres + pgvector backend is just a class with the right two
methods:

```python
class PgVectorCatalog:
    name: str

    async def add(self, entries: list[SeriesEntry]) -> IndexResult: ...
    async def search(self, query, limit=10, *, namespaces=None) -> list[SeriesMatch]: ...
```

### Snapshot layout

`Catalog.save(path)` writes three files into one directory, atomically via
temp-directory rename:

```
<dir>/
├── meta.json           name, namespaces, entry count, embedder identity, build info
├── entries.parquet     namespace, code, title, description, tags_json, metadata_json, embedding
└── embeddings.faiss    faiss.write_index over L2-normalized vectors
```

`Catalog.from_url(url, *, embedder=None)` dispatches on scheme:

- `file://` — read the directory in place.
- `hf://<org>/<repo>` — `snapshot_download` into the HF cache, then load.
  Subsequent calls hit the cache.

The embedder identity recorded in `meta.json` (`dim`, `normalize`) must
match the embedder supplied at load — mismatches raise `ValueError`. When
`embedder` is omitted, a `SentenceTransformerEmbedder` is reconstructed
from the recorded `model` string.

### Search

`Catalog.search(query, limit, *, namespaces=None)` runs a hybrid search:

1. Pull candidates from each retriever over the full corpus:
   - BM25 over whitespace-lowercased `entry.keyword_text()` (title + description + metadata + tags — cheap linear cost, so keyword-rich).
   - FAISS inner-product over the embedded query vector; entries are indexed via `entry.semantic_text()` (title + description only — O(N²) attention makes it expensive to pollute with identifiers).
2. Fuse the two ranked lists with reciprocal rank fusion (`rrf_fuse`, k=60).
3. Filter by `namespaces` (if given) and truncate to `limit`.

Candidate pulls happen **before** the namespace filter so each retriever
scores the full corpus and the RRF rankings stay meaningful.

### Ingest

`Catalog.add_from_result(result)` lifts rows from a `Result` carrying an
`OutputConfig` into `SeriesEntry` values:

1. Extract the KEY column value per row. If the KEY column declares a
   `namespace=`, use it; otherwise default to the catalog's own `name`.
2. Extract the TITLE column value.
3. Collect METADATA columns into the `metadata` dict.
4. Call `Catalog.add(entries)` which dedupes via `exists` (unless
   `force=True`), embeds missing rows in 256-batch chunks, updates the
   in-memory dict, rebuilds FAISS + BM25.

Retryable transport failures inside `add` are caught and counted in
`IndexResult.errors`; programmer errors propagate.

---

## Plugin discovery

`parsimony.discover` is the entire discovery surface — three functions
plus one frozen dataclass. No cache, no singleton, no import-time side
effects. Consumers cache at their own level if they need to.

```
parsimony.providers entry point(s)
    ↓ discover.iter_providers()
    ↓ for each ep: yield Provider(name, module_path, dist_name, version)
Provider(...)                                    ← metadata only; nothing imported yet
    ↓ p.load()                                   ← imports the module on demand
    ├─ importlib.import_module(p.module_path)
    └─ validate CONNECTORS is a Connectors instance
Connectors                                       ← from one provider

discover.load_all()
    ↓ for each Provider: try p.load(); on import failure log+skip
    ↓ Connectors.merge(*loaded)                  ← duplicate-name → ValueError
Connectors                                       ← single flat collection
    ↓ .bind_env(overrides=None)
Connectors                                       ← env-bound clones; some may be bound=False
```

Failure modes:

- Two distributions register the same provider name → `iter_providers`
  raises `RuntimeError` naming both distributions.
- Plugin module missing or wrong type for `CONNECTORS` → `Provider.load()`
  raises `TypeError`.
- Strict `discover.load("name")` for an absent provider → `LookupError`
  with the available names listed.
- Plugin import error inside `discover.load_all()` → logged at WARNING
  via the `parsimony.discover` logger and skipped, so a single broken
  plugin cannot take down the whole load.

Provider metadata (homepage, version, distribution name) is read from
`importlib.metadata` on demand — no per-module dictionaries duplicate the
PEP 621 data.

---

## Catalog publishing

The `parsimony publish` CLI reads one of two shapes from a plugin module:

```python
# Static:
CATALOGS: list[tuple[str, Callable[[], Awaitable[Result]]]] = [
    ("fred", fred_enumerate),
]

# Or async generator:
async def CATALOGS() -> AsyncIterator[tuple[str, Callable[[], Awaitable[Result]]]]:
    async for agency, flow in _discover_live():
        ns = f"sdmx_series_{agency.lower()}_{flow.id.lower()}"
        yield ns, partial(enumerate_series, agency=agency, dataset_id=flow.id)
```

Optional reverse-lookup:

```python
def RESOLVE_CATALOG(namespace: str) -> Callable | None:
    # Build the enumerator for `namespace` without walking CATALOGS.
    ...
```

The publisher pipeline:

```
parsimony publish --provider NAME --target 'hf://org/catalog-{namespace}'
    ↓ discover.iter_providers() → find NAME → p.load() → CATALOGS / RESOLVE_CATALOG
    ↓ if --only and RESOLVE_CATALOG:
    │     for ns in only: resolve(ns) or fall back to CATALOGS walk
    │ else:
    │     walk CATALOGS, optionally filtered by --only
    ↓ for each (namespace, fn):
    │     bound = fn.bind_env() if isinstance(fn, Connector) else fn
    │     result = await bound()
    │     catalog = Catalog(name=namespace, embedder=default)
    │     await catalog.add_from_result(result)
    │     await catalog.push(target.format(namespace=namespace))
    ↓
PublishReport(published=[...], skipped=[...], failed=[...])
```

There is no resume logic, no manifest, no content hashing. Publishes are
idempotent at the target level (the atomic `save` → `push` pattern
guarantees a partial write is never visible). If a publish fails, re-run
it.

---

## HTTP transport

`parsimony.transport` ships three utilities for plugin authors:

### `HttpClient`

Thin wrapper around `httpx.AsyncClient`. Creates a new client per request
to avoid event-loop sharing issues across `asyncio.run()` calls. Automatic
structured-logging redaction of sensitive query parameters (names
`api_key`, `token`, `password`, and anything ending in `_token`).

If a new connector introduces a credential query-param name not in the
default list, add it to `_SENSITIVE_QUERY_PARAM_NAMES` in `transport.py`.

### `pooled_client`

Context manager yielding a connection-pooled `httpx.AsyncClient` for burst
workloads (batch enumerators, screeners). Use `HttpClient` for one-shots.

### Error mapping

`map_http_error(exc, *, provider, op_name)` translates
`httpx.HTTPStatusError` into the typed `parsimony.errors` hierarchy:

- `401` / `403` → `UnauthorizedError`
- `402` → `PaymentRequiredError`
- `429` → `RateLimitError` (reads `Retry-After` via `parse_retry_after`)
- anything else → `ProviderError`

`map_timeout_error` does the same for `httpx.TimeoutException` →
`ProviderError`. `redact_url` strips sensitive query-param values before
embedding a URL in an exception message.

Every kernel-aware plugin funnels upstream exceptions through these
helpers so the agent-facing error surface is consistent.

---

## Error hierarchy

```
ConnectorError(provider: str)
├── UnauthorizedError      (401/403 — bad credentials)
├── PaymentRequiredError   (402 — plan restriction)
├── RateLimitError         (429 — burst or quota)
│   ├── retry_after: float      — wait seconds
│   └── quota_exhausted: bool   — terminal, do not retry
├── ProviderError          (5xx / unexpected status / timeouts)
├── EmptyDataError         (200 but no rows — idiomatic "not found")
└── ParseError             (200 but unparseable)
```

Every error carries `.provider: str` so callers can identify the source
without parsing message strings. The MCP server (separate `parsimony-mcp`
distribution) maps these to MCP error responses.

Operational errors only. Programmer errors (bad types, invalid parameters)
remain `TypeError` / `ValueError` / Pydantic `ValidationError`.

---

## Observability

Every connector call optionally fires registered `ResultCallback` hooks
after the result is produced. Callbacks may be sync or async; exceptions
are logged (via the `parsimony.connector` logger), **not raised** — the
caller's `await connector(...)` always returns.

Typical usage:

```python
import logging
logger = logging.getLogger("parsimony.monitor")

async def log_call(result: Result) -> None:
    logger.info("connector=%s rows=%d", result.provenance.source, len(result.data))

monitored = connectors.with_callback(log_call)
```

The hook attaches to a copy of the bundle; the original is unchanged.

---

## Conformance

`parsimony.testing` ships the conformance suite that every plugin must
pass. Three checks:

1. `check_connectors_exported` — module exports `CONNECTORS`, a non-empty
   `Connectors` instance.
2. `check_descriptions_non_empty` — every connector has a non-empty
   description (no silently empty LLM tool schemas).
3. `check_env_map_matches_deps` — for every connector, each key in its
   `env_map` (decorator-declared via `@connector(env={...})`) names a real
   keyword-only dependency on that connector.

The suite runs as a merge gate in `parsimony-connectors` CI, as a release
gate per connector, and as a security-review artefact via
`parsimony list --strict`.

---

## CLI

Two verbs, wired as the `parsimony` console script:

```
parsimony list [--json] [--strict]
parsimony publish --provider NAME --target URL_TEMPLATE [--only NS]... [--dry-run]
```

`--strict` folds the conformance suite into `list` — exits non-zero on any
failure. `--only` can be repeated; the publisher tries `RESOLVE_CATALOG`
first for each namespace and falls back to the `CATALOGS` walk only if
any requested namespace doesn't resolve.

---

## Key design decisions

### Why Protocol over ABC for `CatalogBackend` and `EmbeddingProvider`?

ABCs require nominal subclassing. A user wiring up a Postgres backend
would have to import a base class just to inherit from it; a test fake
would do the same. Protocols carry the same structural contract without
the inheritance tax. A plugin author copying a reference implementation
can freely rename, reshape, or delete the base — the kernel only checks
that the final object has the right methods.

### Why is `Result` a single class with optional `output_schema`?

One result type instead of a parent/subclass split. Schema-aware
accessors (`entity_keys`, `data_columns`) check
`result.output_schema is not None` and return empty sequences when the
schema is absent — the type system stays flat without sacrificing the
schema-aware operations.

### Why per-call namespaces, not a templating mini-language?

Plain Python already solves per-call namespace assembly without needing
an annotation class or a `{placeholder}` template grammar that plugin
authors must learn:

```python
ns = f"sdmx_series_{agency.lower()}_{dataset_id.lower()}"
return OutputConfig(columns=[Column(..., role=KEY, namespace=ns), ...])
```

No decorator magic, no reverse-regex resolution.

### Why is `Column.namespace` optional on KEY columns?

When a catalog is built from a single enumerator (the common case — FRED,
Treasury, BLS), repeating the catalog name in every `Column(role=KEY,
namespace="fred")` adds noise. Making it optional and defaulting to the
catalog's own `name` at `add_from_result` time keeps the explicit path
for multi-namespace catalogs (SDMX) while cutting boilerplate for the
common case.

### Why do we create a new `httpx.AsyncClient` per request?

A shared `AsyncClient` across multiple `asyncio.run()` calls (each of
which creates a new event loop) raises `RuntimeError: Event loop is
closed`. The per-request client avoids this at the cost of TCP connection
setup. For burst workloads where TCP overhead matters, `pooled_client`
provides a managed shared client.

### Why separate MCP tools from client connectors?

`parsimony` draws a hard line between two access paths to the same data
sources:

| | MCP Tools (search/discovery) | Client Connectors (fetch/load) |
|---|---|---|
| **Tagged** | `"tool"` | no `"tool"` tag |
| **Caller** | Agent via MCP protocol | Agent-written Python code |
| **Result size** | Small — fits in a context window | Large — full datasets, thousands of rows |
| **Purpose** | Figure out *what* to fetch | Fetch the actual data |

The core problem is context-window economics. When an agent calls an MCP
tool, the result is injected into its context. A 10,000-row DataFrame
returned as an MCP tool response would crowd out reasoning. The agent
doesn't need all that data in context — it needs it in a variable it can
manipulate with code.

The workflow this enables: **discover → fetch → analyze**. The agent
calls `fred_search` as an MCP tool (small metadata result), then writes
and executes Python that loads connectors explicitly:

```python
from parsimony_fred import CONNECTORS as fred
result = await fred.bind_env()["fred_fetch"](series_id="UNRATE")
```

The full DataFrame lands in the code-execution environment, where the
agent can operate on it programmatically.

The `"tool"` tag is the only mechanism. The MCP server
(`parsimony-mcp` — separate distribution) filters with
`connectors.filter(tags=["tool"])` at startup.
