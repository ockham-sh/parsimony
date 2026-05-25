# The Parsimony Plugin Contract

**Status:** Contract v3 (kernel >= 0.6).

This document is the load-bearing surface for connector plugins. Everything not listed here is private.

## Philosophy

Parsimony keeps connector authorship as close to normal Python as possible:

- A connector is an async callable plus metadata.
- Function parameters are connector parameters.
- Binding fixes parameter values and returns a new connector with a smaller public surface.
- Tool schemas are projections. Python connectors may use non-JSON-compatible types until a caller asks to expose them as tools.
- Auth and environment lookup belong in connector implementation unless a future repeated pattern proves a helper is necessary.

## Entry Points

Plugins register exactly one provider module per entry under `parsimony.providers`:

```toml
[project.entry-points."parsimony.providers"]
foo = "parsimony_foo"
```

The provider key must be lowercase snake case. The module must export `CONNECTORS`.

## Module Exports

### Required

`CONNECTORS: Connectors`

An immutable `parsimony.Connectors` collection of decorated connectors. It must be non-empty and connector names must be unique.

Every catalog-producing `Result` must declare `namespace=...` on its `KEY` column. `Catalog.name` is the snapshot artifact name; it is not a fallback row namespace.

## Connector Contract

```python
from parsimony import connector

@connector
async def fred_fetch(series_id: str, api_key: str = "") -> pd.DataFrame:
    """Fetch observations for a FRED series."""
    ...
```

Rules:

- Connector functions must be async.
- The docstring or `description=` is required.
- The callable signature defines the connector parameters.
- Pydantic models, dataclasses, primitives, and arbitrary Python objects are all ordinary parameter annotations.
- Public connector parameters must be **flat**: every user-facing parameter appears as a top-level function parameter. Do not expose a bundled ``params: SomeParams`` argument on connectors, enumerators, or loaders. Use Pydantic models internally for validation when helpful.
- `Connector.bind(**kwargs)` returns a new connector with those parameters fixed.
- Bound values are not exposed in `exposed_signature` and are not recorded as call-time provenance parameters.
- Tabular connectors with `output=` must return a `pd.DataFrame` (or `pd.Series`); the framework applies the schema via `build_table_result`. Returning `Result`, `TabularResult`, or `output.build_table_result(df)` from the connector body is a `TypeError`.
- Provider facts belong in the returned payload. For tabular connectors, put series descriptors and row-varying attributes in DataFrame columns. Do not attach provider metadata through ``provenance.properties`` or ``(data, {...})`` tuple returns.

### Metadata vs data

- ``ColumnRole.DATA``: observations and any field that can vary by row, date, vintage, instrument roll, maturity, or constituent within the result.
- ``ColumnRole.METADATA``: entity-level descriptors used when projecting catalog ``Entity`` rows. A metadata column must be constant for each entity key in the result (it may differ across keys in a multi-entity frame).
- Example: a benchmark bond series whose constituent ISIN changes over time should store ``isin`` as ``DATA``, not ``METADATA``. If the entity key is the bond itself and ISIN is stable for that key, ``isin`` may be ``METADATA``.
- Entity projection rejects metadata columns that take more than one distinct non-null value within a single entity key.

Auth example:

```python
@connector
async def fred_fetch(series_id: str, api_key: str = "") -> pd.DataFrame:
    """Fetch observations for a FRED series."""
    key = api_key or os.environ.get("FRED_API_KEY", "")
    if not key:
        raise UnauthorizedError("fred", env_var="FRED_API_KEY")
    ...

runtime_fetch = fred_fetch.bind(api_key="...")
```

The framework does not inspect or bind environment variables. This keeps auth provider-specific and makes `bind` the single mechanism for producing configured connector variants.

## Optional Provider Convenience Helpers

The required plugin contract is only `CONNECTORS`. Providers may additionally expose a
side-effect-light helper such as `load(...)` or `configure(...)` for operator
ergonomics:

```python
def load(*, api_key: str) -> Connectors:
    """Return CONNECTORS with credentials bound for runtime use."""
    return CONNECTORS.bind(api_key=api_key)
```

Rules for optional helpers:

- They must not be required by the kernel or conformance suite.
- They must not perform network I/O, download catalogs, enumerate providers, or build
  indexes at import time.
- They may bind call-time parameters, set provider-local runtime defaults, or return
  a preconfigured `Connectors` view.
- Catalog warming belongs in search-time load-or-build paths (configured URL → lazy
  disk cache → provider build callable), not in module import or `load(...)`.

Direct imports remain the preferred style in application code:

```python
from parsimony_fred import CONNECTORS as FRED

runtime = FRED.bind(api_key=os.environ["FRED_API_KEY"])
```

Dynamic hosts (CLI, agent runtimes) may use `parsimony.discover.load(...)`.

## Public Kernel Surface

Plugins should import from `parsimony`:

- `Connector`
- `Connectors`
- `connector`
- `enumerator`
- `loader`
- `Result`, `Provenance`, `OutputConfig`, `Column`, `ColumnRole`
- catalog, store, transport, and error symbols documented in the API reference

## Search Semantics

The catalog framework supports two query paths:

1. **Structured Search (preferred for agents):** Triggered when a query starts with an indexed field followed by a colon (regex `^\s*\w+\s*:`). Syntax:
   - `FIELD: value`: Searches for `value` in the index of `FIELD`.
   - `FIELD: val1, val2`: Within-field OR composition (uses union of candidates and `max(scores)`).
   - `FIELD: val && FIELD2: val`: Across-fields AND composition (uses intersection of candidates and `sum(scores)`).
2. **Broad Search:** Any plain-text query that does not trigger structured query detection. Executed against the `default_field` of the `Catalog` (which defaults to `"title"` when a title index exists).

If a structured query names a field that is not indexed, search fails fast with `UnknownIndexedFieldError` listing the indexed fields. There is no fallback to broad search for typoed field names.

When `Catalog(..., indexes=None)`, the framework builds BM25 indexes at `build()` time for `code`, `title`, and every metadata key present on the entries. Explicit `indexes=[...]` means the caller owns the full index policy — no extra indexes are added silently.

## Conformance

`parsimony.testing.assert_plugin_valid(module)` checks:

1. module exports non-empty `CONNECTORS: Connectors`;
2. every connector has a non-empty description (20–800 characters);
3. enumerators declare mandatory `output=` and annotate `pd.DataFrame` return types;
4. public connector parameters are flat (no bundled `params: BaseModel` signatures).

Connectors with auth-bearing parameters must declare them via `secrets=(...)` on `@connector` / `@enumerator` / `@loader`; declared names are stripped from `Provenance.params` at fetch time.

The conformance suite does not enforce runtime auth/env fallback behavior — connector packages own that.
