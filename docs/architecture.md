# Architecture

Parsimony is intentionally small. The kernel has three jobs:

1. wrap async Python callables as connectors;
2. normalize successful outputs into `Result` objects with provenance;
3. provide catalog/store utilities around those results.

## Connector Flow

```mermaid
flowchart LR
    Fn[Async callable] --> Decorator[@connector]
    Decorator --> Conn[Connector]
    Conn -->|"bind(api_key=...)"| BoundConn[Bound Connector]
    BoundConn -->|"await connector(series_id=...)"| ResultObj[Result]
```

A connector's function signature is its parameter surface. `bind` returns a new connector with selected parameters fixed. The returned connector exposes only the remaining parameters.

## Provenance

The framework builds provenance after a connector succeeds. It records:

- connector name;
- connector description;
- fetch timestamp;
- call-time parameters after binding;
- source-specific properties added through `Result.with_properties`.

Bound values are omitted. Secret-shaped call-time keys are redacted.

## Tool Projection

JSON Schema is not required for Python execution. MCP and other tool hosts call `Connector.to_json_schema()` when they need a tool schema. Unsupported public parameter types fail at that boundary.

## Auth

Auth is provider-specific. Connectors may read environment variables, use SDK clients, accept OAuth token providers, or expose explicit parameters. The kernel does not own env lookup. Operators can create configured variants with `bind`.

## Collections

`Connectors` is immutable and composable. It supports merge, bind, replace, filter, callbacks, and keyed lookup.

## Catalogs and Indexing

The catalog design is centered on three main principles:

1. **One Index Per Field**: Every field has exactly one index at the Catalog level.
2. **Within-Field Fusion**: If a field has multiple search representations (such as both a BM25 keyword index and a Vector semantic index), they are wrapped into a single `HybridIndex` which manages within-field fusion (using `ZScoreFusion` by default). This decouples fusion policy from the Catalog itself.
3. **Structured vs. Broad Search**: Broad queries search against a configurable `default_field`, while structured queries (using `FIELD: value`) parse and execute across multiple per-field indexes, composing their rankings via deterministic intersection (AND) and union (OR) arithmetic.

## Caching & Persistence

- **Hugging Face disk cache**: Remote catalogs loaded from Hugging Face (`hf://`) are downloaded using `huggingface_hub.snapshot_download` and cached locally under `$PARSIMONY_CACHE_DIR/catalogs`. Subsequent loads read from the local disk cache directly.
- **In-memory reuse**: `Catalog.load()` always returns a freshly loaded instance. Connectors that serve many searches in one process hold a module-level singleton or small LRU themselves.
