# Data stores

A data store is the "load" half of Parsimony's fetch/load split. After a
[loader](../connectors/loaders-and-enumerators.md) returns a
[`Result`](../connectors/results.md), a data store extracts the `DATA`
columns and persists one DataFrame per entity, keyed by a canonical
`(namespace, code)` tuple. Where the [catalog](index.md) is the *discovery* layer
over [entities](entities.md), the data store is the *observation* layer that holds
the values those entities point at.

Parsimony ships one concrete implementation, `InMemoryDataStore`, importable from
the top level or from `parsimony.stores`:

```python
from parsimony import InMemoryDataStore, LoadResult
# equivalently:
from parsimony.stores import InMemoryDataStore, LoadResult
```

!!! note "There is no `DataStore` Protocol yet"
    The module deliberately names the single backend `InMemoryDataStore` rather
    than exposing an abstract `DataStore` type. A structural `Protocol` will be
    extracted from the public method set only when a second backend (SQLite,
    Parquet, …) lands. Until then, type your code against `InMemoryDataStore`
    directly — there is no `DataStore` symbol to import.

## The CRUD surface

`InMemoryDataStore` wraps a process-local dict mapping each canonical
`(namespace, code)` key to a pandas DataFrame.

| Method | Signature | Behavior |
| --- | --- | --- |
| `upsert` | `upsert(namespace, code, df) -> None` | Insert or replace one entity's observations. Stores `df.copy()`. |
| `get` | `get(namespace, code) -> pd.DataFrame \| None` | Return a copy of the stored frame, or `None` if the key is absent. |
| `delete` | `delete(namespace, code) -> None` | Idempotently remove an entity. No error if the key is absent. |
| `exists` | `exists(keys) -> set[tuple[str, str]]` | Given a list of `(namespace, code)` pairs, return the canonicalized subset that is present. |
| `load_result` | `load_result(table, *, force=False) -> LoadResult` | Extract `DATA` columns from a `Result` and persist each entity. |

Every method routes its key through `entity_key(namespace, code)`, which
normalizes the namespace to lowercase snake_case (`^[a-z][a-z0-9_]*$`) and strips
the code. An invalid namespace or empty code therefore raises `ValueError` at call
time — keys never silently miss. See [Entities](entities.md) for the
normalization rules.

```python
import pandas as pd

from parsimony import InMemoryDataStore

store = InMemoryDataStore()
df = pd.DataFrame({"value": [1, 2]})

store.upsert("fred", "gdp", df)
assert store.exists([("fred", "gdp")]) == {("fred", "gdp")}

# get() returns a defensive copy: mutating it does not touch the store.
got = store.get("fred", "gdp")
assert got is not None
got.loc[0, "value"] = 999
again = store.get("fred", "gdp")
assert again is not None and again["value"].iloc[0] == 1

store.delete("fred", "gdp")
assert store.get("fred", "gdp") is None
```

!!! tip "Copy semantics"
    `upsert` stores `df.copy()` and `get` returns `stored.copy()`. Callers can
    neither mutate stored state through a returned frame nor leak a later mutation
    of the input frame into the store. Each entity's stored frame is independent.

`exists` returns *canonicalized* keys, not the raw pairs you passed in. If your
inputs were not already normalized (for example a code with trailing whitespace),
comparing the result against your raw inputs can mismatch — compare against the
normalized form instead.

## Loading a result

The bridge from connector output to storage is `load_result`. It delegates identity
and grouping entirely to [`Result.entities`](../connectors/results.md#entity-projection)
— the same lazy tuple-keyed projection covered on the results page — so there is no
second grouping pass here. Each `EntityResult.data` (already narrowed to that entity's
`DATA` columns) is upserted under its `(namespace, code)` key. `TITLE`, `METADATA`, and
the `KEY` column itself are consumed by the projection and never land in the stored
frame.

```python
import pandas as pd

from parsimony import InMemoryDataStore
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result

SCHEMA = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

store = InMemoryDataStore()
table = Result(
    data=pd.DataFrame(
        {
            "series_id": ["GDP", "GDP", "CPI"],
            "value": [1.0, 2.0, 3.0],
        }
    ),
    provenance=Provenance(source="fred", source_description="FRED"),
    output_spec=SCHEMA,
)

stats = store.load_result(table)
assert (stats.total, stats.loaded, stats.skipped, stats.errors) == (2, 2, 0, 0)

gdp = store.get("fred", "GDP")
assert gdp is not None
assert list(gdp.columns) == ["value"]  # KEY consumed
assert len(gdp) == 2  # two rows for GDP collapsed into one frame
```

The two `GDP` rows collapse into a single two-row frame under code `GDP`, while
`CPI` becomes its own one-row entity — so `total` counts distinct entities, not
input rows. The namespace is taken from the `KEY` column's `namespace=...`, not
passed to `load_result`; a `KEY` column is the only place a `Column` may set
`namespace`.

### Statistics: `LoadResult`

`load_result` returns a `LoadResult`, a small pydantic model tallying the run.
All fields default to `0`.

| Field | Meaning |
| --- | --- |
| `total` | Distinct entities extracted from the table. |
| `loaded` | Entities actually upserted. |
| `skipped` | Entities already present and not forced (only with `force=False`). |
| `errors` | Entities whose upsert raised a caught exception. |

If the table yields no rows (an empty DataFrame, or no entities after grouping),
`load_result` returns early with `total == 0` and nothing is written.

### `force` and existing keys

By default (`force=False`) `load_result` calls `exists` once for all extracted
keys, skips any entity whose canonical key is already present, and upserts only
the rest. With `force=True` it treats nothing as pre-existing and upserts every
extracted entity unconditionally.

```python
import pandas as pd

from parsimony import InMemoryDataStore
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result

SCHEMA = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

store = InMemoryDataStore()
store.upsert("fred", "GDP", pd.DataFrame({"value": [0.0]}))

table = Result(
    data=pd.DataFrame({"series_id": ["GDP"], "value": [9.0]}),
    provenance=Provenance(source="fred", source_description="FRED"),
    output_spec=SCHEMA,
)

first = store.load_result(table, force=False)
assert (first.loaded, first.skipped) == (0, 1)  # GDP already present

second = store.load_result(table, force=True)
assert (second.loaded, second.skipped) == (1, 0)  # overwritten

gdp = store.get("fred", "GDP")
assert gdp is not None and gdp["value"].iloc[0] == 9.0
```

!!! tip "Idempotent backfills"
    `force=False` makes `load_result` a cheap incremental backfill: re-running the
    same load only writes entities you have not stored yet, and `skipped` tells you
    how many were already present. Use `force=True` only when you intend to
    overwrite existing observations with fresh values.

## End to end: loader output into a store

A [loader](../connectors/loaders-and-enumerators.md) already returns a
`Result` carrying its `output` schema, so wiring its output into a store is
a two-line call. The loader's schema satisfies the extraction contract by
construction — exactly one namespaced `KEY` column and at least one `DATA` column,
and no `TITLE`/`METADATA` columns.

```python
import pandas as pd

from parsimony import InMemoryDataStore
from parsimony.connector import loader
from parsimony.result import Column, ColumnRole, OutputSpec

LOAD_SCHEMA = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


@loader(output=LOAD_SCHEMA)
def gdp_observations(series_id: str = "GDP") -> pd.DataFrame:
    """Return observation values for a FRED series."""
    return pd.DataFrame({"series_id": [series_id, series_id], "value": [1.0, 2.0]})


store = InMemoryDataStore()
result = gdp_observations(series_id="GDP")  # a Result
stats = store.load_result(result)
assert stats.loaded == 1

df = store.get("fred", "GDP")
assert df is not None and list(df.columns) == ["value"] and len(df) == 2
```

## Extraction errors vs. per-entity errors

`load_result` distinguishes two failure modes.

**Extraction-time validation** runs before any entity is written and *raises* —
it is never counted in `errors`. It is exactly the [entity projection
contract](../connectors/results.md#requirements-and-errors) that `Result.entities`
enforces: the table's schema must be well-formed.

| Condition | Raises |
| --- | --- |
| `output_spec` is `None` | `ValueError` |
| `data` is not a DataFrame | `TypeError` |
| Not exactly one `KEY` column | `ValueError` |
| `KEY` column has no `namespace` | `ValueError` |
| A declared column (`KEY`, `TITLE`, `METADATA`, `DATA`) is absent from the data | `ValueError` |

```python
import pandas as pd

from parsimony import InMemoryDataStore
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result

store = InMemoryDataStore()
table = Result(
    data=pd.DataFrame({"series_id": ["GDP"], "value": [1.0]}),
    provenance=Provenance(source="x", source_description="x"),
    output_spec=OutputSpec(
        columns=[
            Column(name="series_id", role=ColumnRole.KEY),  # no namespace
            Column(name="value", role=ColumnRole.DATA),
        ]
    ),
)
try:
    store.load_result(table)
except ValueError as exc:
    assert "namespace" in str(exc)
```

**Per-entity upsert failures** are different. Once extraction succeeds,
`load_result` upserts entities one by one; if an individual upsert raises
`OSError`, `RuntimeError`, `ValueError`, or `TypeError`, the failure is logged at
`WARNING` on the `parsimony.stores` logger, counted in `errors`, and the run
continues with the remaining entities. A single bad entity does not abort a load.

!!! warning "Process-local and not concurrency-safe"
    `InMemoryDataStore` keeps all state in an in-memory dict; it is lost when the
    process exits and is not shared across processes. There is no lock, so
    concurrent `upsert` calls (including those inside two overlapping
    `load_result` runs) targeting the same key race on a plain dict. Serialize
    access yourself if multiple threads share one store.

## See also

- [Loaders and enumerators](../connectors/loaders-and-enumerators.md) — the loader verb that produces the `Result` a store consumes.
- [Results and output schemas](../connectors/results.md) — `Result`, `OutputSpec`, `Column`, `ColumnRole`, and the `entities` projection `load_result` delegates to.
- [Entities](entities.md) — namespace/code normalization and the `entity_key` canonical key.
- [The Catalog](index.md) — the discovery layer; an enumerator's output feeds a catalog, a loader's feeds a data store.
