# Results and output schemas

A connector returns **raw data** — a DataFrame, Series, scalar, or dict. The framework wraps that return value in a result envelope (`Result` or `TabularResult`) carrying framework-built [`Provenance`](#provenance), and — when the connector declares an [`OutputConfig`](#outputconfig) — applies a declarative column schema. This page covers the data carriers and the schema system that shapes tabular output.

These types live in `parsimony.result` and are all re-exported at the top level, so either import path works:

```python
from parsimony import Result, TabularResult, OutputConfig, Column, ColumnRole, Provenance
# equivalently, the explicit submodule path:
from parsimony.result import Result, TabularResult, OutputConfig, Column, ColumnRole, Provenance
```

!!! note "You rarely construct these directly"
    The framework builds `Result` / `TabularResult` and `Provenance` for you when a [connector](defining-connectors.md) returns. A connector that returns a `Result`, `TabularResult`, or a `(data, properties)` tuple raises `TypeError` — provider facts belong in DataFrame columns, not in the result envelope. You *do* construct `OutputConfig` and `Column` to declare a connector's `output=` schema, and you may build a `TabularResult` by hand for tests or for the catalog / data-store flows.

## Result

`Result` is the opaque base envelope: any payload plus provenance. It is the carrier the framework uses when a connector returns a scalar or a `dict`.

| Field | Type | Default |
|---|---|---|
| `data` | `Any` | required |
| `provenance` | `Provenance` | `Provenance(source="", source_description="")` |

The model allows arbitrary types (`arbitrary_types_allowed`), so `data` is not deep-validated. Two members are worth knowing:

- `text` (property) — returns `data` unchanged if it is already a `str`, otherwise `str(data)`.
- `_with_properties(**properties)` — internal helper for serialization/tests; merges keyword arguments into `provenance.properties` on a **new** `Result`. Connector code should not call this.

## TabularResult

`TabularResult` subclasses `Result`, narrows `data` to a `pandas.DataFrame`, and adds an optional `output_schema`.

| Field | Type | Default |
|---|---|---|
| `data` | `pd.DataFrame` | required |
| `output_schema` | `OutputConfig \| None` | `None` |

### Schema-derived views

When an `output_schema` is present, these read-only properties project it by [role](#columnrole):

| Property | Returns |
|---|---|
| `df` | the underlying `pd.DataFrame` (alias for `data`) |
| `columns` | `list[Column]` from the schema, or `[]` when there is no schema |
| `data_columns` | columns whose role is `DATA` |
| `metadata_columns` | columns whose role is `METADATA` |
| `entity_keys` | a DataFrame of the `KEY` column(s), addressed by `mapped_name or name` |

`entity_keys` raises `ValueError` if a declared key column is absent from the data, and returns an empty DataFrame when the schema declares no `KEY` column.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig, TabularResult

df = pd.DataFrame({"sym": ["A", "B"], "title": ["Alpha", "Beta"], "v": [1, 2]})
schema = OutputConfig(columns=[
    Column(name="sym", role=ColumnRole.KEY, namespace="demo"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="v", role=ColumnRole.DATA),
])
result = TabularResult(data=df, output_schema=schema)

print(list(result.entity_keys.columns))           # ['sym']
print([c.name for c in result.data_columns])       # ['v']
print([c.name for c in result.metadata_columns])   # []
```

### Constructors and re-application

- `TabularResult.from_dataframe(df)` — wraps a DataFrame (or a Series, coerced first) with **no schema**. Raises `ValueError("Returned an empty DataFrame.")` on empty input.
- `to_table(output)` — re-applies a new `OutputConfig` to the existing data with `merge_unmapped_as_data=True`, preserving the current provenance. Unmapped columns become `DATA`.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, TabularResult

raw = TabularResult(
    data=pd.DataFrame({"k": ["a"], "title": ["T"], "obs": [1.0]}),
    provenance=Provenance(source="s", source_description="demo source"),
)
shaped = raw.to_table(OutputConfig(columns=[
    Column(name="k", role=ColumnRole.KEY, namespace="demo"),
    Column(name="title", role=ColumnRole.TITLE),
]))
roles = {c.name: c.role for c in shaped.output_schema.columns}
print(roles["obs"])              # data  (ColumnRole is a StrEnum; unmapped → DATA)
print(shaped.provenance.source)  # "s"  (provenance preserved)
```

### Arrow and Parquet serialization

`TabularResult` round-trips through Arrow and Parquet with provenance and schema embedded in the table metadata (under the binary key `b"parsimony.result"`):

| Method | Behavior |
|---|---|
| `to_arrow()` | `pa.Table` with `provenance.safe_dump()` and the column dumps embedded as metadata |
| `from_arrow(table)` | classmethod; reverses `to_arrow`; tolerates a vanilla table with no such metadata by returning a schemaless result |
| `to_parquet(path)` | writes the Arrow table to Parquet |
| `from_parquet(path)` | classmethod; reads Parquet written by `to_parquet` |

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, TabularResult

result = TabularResult(
    data=pd.DataFrame({"code": ["UNRATE"], "title": ["Unemployment"]}),
    provenance=Provenance(source="fred", source_description="FRED", params={"q": "unemployment"}),
    output_schema=OutputConfig(columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
    ]),
)

table = result.to_arrow()
restored = TabularResult.from_arrow(table)
print([c.name for c in restored.output_schema.columns])  # ['code', 'title']
print(restored.output_schema.columns[0].namespace)        # 'fred'
print(restored.provenance.params)                          # {'q': 'unemployment'}
```

## The `to_llm()` view

`to_llm()` renders a compact, **schema-in-context** view of a result for an LLM prompt — type and shape, not the full payload. It is the framework-owned counterpart to dumping `result.data`: the size it adds to context is O(schema) for tables and O(structure) for opaque payloads, not O(rows) or O(bytes). Every `Result` has it; `TabularResult` overrides it with a governed, schema-aware rendering.

`to_llm()` is the data layer's single convention for "the governed string an LLM may see of this object" — the same method name carries the connector card (`Connector.to_llm()`), the bundle listing (`Connectors.to_llm()`), and this result view. (A runtime such as `parsimony-agents` has its own, separate `to_llm(mode) -> blocks` convention for *assembling* a message; it delegates the *content* of a governed object back to these methods.)

The signature is shared across both types, so a caller holding any `Result` can call it blindly:

```python
to_llm(*, max_rows: int = 10, max_chars: int = 2000) -> str
```

`TabularResult` honors `max_rows`; the opaque `Result` honors `max_chars`. Each ignores the other's knob.

### TabularResult preview

Renders a shape line, a per-column schema block (**dtype + [role](#columnrole) + namespace**), and a small head/tail CSV sample. Columns flagged [`exclude_from_llm_view`](#column) are dropped from **both** the schema block and the sample.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig, TabularResult

df = pd.DataFrame({"date": pd.to_datetime(["2020-01-01", "2020-01-02"]), "value": [1.0, 2.0]})
result = TabularResult(data=df, output_schema=OutputConfig(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
    Column(name="value", role=ColumnRole.DATA),
]))
print(result.to_llm())
# TabularResult: 2 rows × 2 columns
# Columns:
# - date: datetime64[ns] (KEY ns:fred_series)
# - value: float64 (DATA)
# Sample (2 rows):
# date,value
# 2020-01-01,1.0
# 2020-01-02,2.0
```

With no `output_schema` the schema lines carry dtype only (no role annotation). For frames longer than `max_rows` the sample shows a head and a tail separated by a `...` line, and wide cell values are truncated.

### Result preview (opaque payloads)

For non-tabular `data` (dict/JSON, list, str, scalar, bytes, pydantic model) the base method emits a depth-limited structural summary — one level of expansion, with nested values collapsed to a `type[shape]` token:

```python
from parsimony.result import Result

print(Result(data={"name": "Alice", "items": [1, 2, 3], "meta": {"a": 1}}).to_llm())
# Result (dict): 3 keys
# - name: str
# - items: list[3]
# - meta: dict[1 keys]

print(Result(data=4.25).to_llm())   # Result (float): 4.25
```

!!! note "One owner for governed rendering"
    `Column.llm_annotation()` is the single source of truth for how a column's role and namespace
    are rendered into **any** LLM-facing view — the connector card's `Returns:` line,
    `to_llm`, and downstream consumers (for example the agent's fetch log). Downstream
    layers **call it** rather than re-deriving role/namespace formatting, so the governed
    vocabulary never drifts across the stack. A runtime may still add its own *presentation*
    around the data (pagination, charts, caching handles); what it must not do is re-implement the
    governed schema rendering.

## OutputConfig

`OutputConfig` is the declarative schema you attach to a connector via `output=`. It is an ordered `list[Column]` that maps a raw DataFrame into a schema-applied `TabularResult`.

```python
class OutputConfig(BaseModel):
    columns: list[Column]
```

### Role validation (at construction)

An after-validator enforces three rules when you build an `OutputConfig`; violations raise `ValueError` (surfaced as pydantic `ValidationError`):

- **at most one** `KEY` column
- **at most one** `TITLE` column
- **at least one** column with role `DATA`, `KEY`, or `TITLE`

```python
from parsimony.result import Column, ColumnRole, OutputConfig

# raises: "Output config must have at most one KEY column"
OutputConfig(columns=[
    Column(name="a", role=ColumnRole.KEY),
    Column(name="b", role=ColumnRole.KEY),
])
```

### build_table_result

```python
build_table_result(df: pd.DataFrame | pd.Series, *, merge_unmapped_as_data: bool = True) -> TabularResult
```

This is the core transform. It walks the declared columns **in order** and, for each, matches a DataFrame column by `Column.name` (or claims all remaining columns for the `"*"` wildcard), copies the series, [coerces its dtype](#dtype-coercion), and renames it. The result:

- A column is **consumed at most once**, so an explicit name always wins over a later `"*"` wildcard.
- When `mapped_name` is set, the output column is renamed to that literal string; otherwise the source name is kept.
- When `merge_unmapped_as_data=True` (the default), every still-unconsumed DataFrame column is appended as a fresh `DATA` `Column(dtype="auto")`.
- The returned `TabularResult` carries a **resolved** `OutputConfig` whose `Column.name`s are the final output names (post-rename).

It raises `TypeError` on a non-DataFrame/Series input, and `ValueError` if the frame is empty with no columns. If **no** declared column matches anything it raises `ValueError("Column config matched no input columns.")` — but first it logs a `WARNING` (logger `parsimony.result`) naming the absent columns, so a caller that swallows the exception still sees the diagnostic.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig

raw = pd.DataFrame({"d": ["2020-01-01", "2021-06-15"], "v": ["1", "2.5"], "meta": ["x", "y"]})
cfg = OutputConfig(columns=[
    Column(name="d", dtype="datetime", role=ColumnRole.DATA),
    Column(name="v", dtype="numeric", role=ColumnRole.DATA, mapped_name="value"),
    Column(name="meta", role=ColumnRole.METADATA),
])
result = cfg.build_table_result(raw)

print(list(result.df.columns))                # ['d', 'value', 'meta']  ('v' renamed)
print(str(result.df["value"].dtype))          # 'float64'  (numeric coercion)
print([c.name for c in result.metadata_columns])  # ['meta']
```

The `"*"` wildcard pulls in every column not already claimed by an explicit name:

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig

cfg = OutputConfig(columns=[Column(name="*", dtype="numeric", role=ColumnRole.DATA)])
result = cfg.build_table_result(pd.DataFrame({"a": [1], "b": [2]}))
print(set(result.df.columns))  # {'a', 'b'}
```

!!! tip "Connectors do this for you"
    When a connector declares `output=OutputConfig(...)` and returns a DataFrame, the framework calls `build_table_result` automatically. For an [enumerator](loaders-and-enumerators.md) it instead calls it with `merge_unmapped_as_data=False`, so unmapped columns are dropped rather than folded in as data.

### validate_columns

```python
validate_columns(df: pd.DataFrame) -> list[str]
```

Returns the sorted declared (non-wildcard) column names that are **absent** from `df`. Use it to check a frame against a schema before applying it.

```python
import pandas as pd
from parsimony.result import Column, OutputConfig

cfg = OutputConfig(columns=[Column(name="x"), Column(name="y")])
print(cfg.validate_columns(pd.DataFrame({"x": [1]})))  # ['y']
```

### build_entities

```python
build_entities(df: pd.DataFrame) -> list[Entity]
```

Bridges a schema and a DataFrame into a list of [`Entity`](../catalog/entities.md) records for the [Catalog](../catalog/index.md). It requires **exactly one** `KEY` column carrying a `namespace` (else `ValueError`), an optional single `TITLE`, and any number of `METADATA` columns. A `METADATA` column named `"*"` is a wildcard matching every DataFrame column not already claimed by `KEY`, `TITLE`, or an explicit `METADATA` entry. When the key column's `namespace` is the sentinel `"__row__"`, the per-row namespace is read from an `entity_namespace` column instead.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputConfig

cfg = OutputConfig(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="*", role=ColumnRole.METADATA),
])
entities = cfg.build_entities(pd.DataFrame({
    "code": ["unrate"], "title": ["Unemployment"], "freq": ["monthly"],
}))
print(entities[0].namespace, entities[0].code)  # fred unrate
print(entities[0].metadata)                       # {'freq': 'monthly'}
```

## Column

`Column` declares one column in an `OutputConfig`.

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | `str` | required | matched against DataFrame columns; `"*"` is the wildcard |
| `dtype` | `str` | `"auto"` | drives [coercion](#dtype-coercion) |
| `role` | `ColumnRole` | `DATA` | accepts the JSON alias `kind` |
| `mapped_name` | `str \| None` | `None` | output column rename (literal string) |
| `description` | `str \| None` | `None` | free annotation |
| `exclude_from_llm_view` | `bool` | `False` | forbidden on `DATA` and `TITLE` columns |
| `namespace` | `str \| None` | `None` | allowed **only** on `KEY` or `METADATA` columns |

Two after-validators apply (each raises `ValueError`, surfaced as `ValidationError`):

- `exclude_from_llm_view=True` is rejected on `DATA` and `TITLE` columns.
- `namespace` is rejected on any role other than `KEY` or `METADATA`, and must be non-empty when set.

The `role` field accepts the legacy alias `kind` on input, which is convenient when validating from serialized data:

```python
from parsimony.result import Column, ColumnRole

col = Column.model_validate({"name": "freq", "kind": "metadata"})
print(col.role)  # ColumnRole.METADATA
```

## ColumnRole

`ColumnRole` is a string enum naming a column's semantic role:

| Member | Value | Meaning |
|---|---|---|
| `ColumnRole.DATA` | `"data"` | an observation / measurement column |
| `ColumnRole.KEY` | `"key"` | the entity identifier (its `code`); carries a `namespace` for catalog flows |
| `ColumnRole.TITLE` | `"title"` | a human-readable label |
| `ColumnRole.METADATA` | `"metadata"` | descriptive attributes (frequency, units, …) |

These roles drive entity extraction ([Entities](../catalog/entities.md)) and loader output validation ([Data stores](../catalog/data-store.md)).

## dtype coercion

`Column.dtype` is a string that controls how `build_table_result` converts each matched series. The default `"auto"` passes the series through untouched.

| `dtype` | Conversion |
|---|---|
| `"auto"` | passthrough — no conversion |
| `"datetime"` | `pd.to_datetime(series)` |
| `"timestamp"` | unix epoch → datetime (see heuristic below); already-datetime series pass through |
| `"date"` | `pd.to_datetime(series).dt.normalize()` (time component zeroed) |
| `"numeric"` | `pd.to_numeric(series, errors="coerce")` |
| `"bool"` | `series.astype(bool)` |
| any other string | `series.astype(dtype)` — any valid pandas/numpy dtype (`"int64"`, `"string"`, …); incompatible input raises a descriptive `ValueError` |

The `"timestamp"` heuristic divides values greater than `1e11` by `1000` (treating them as milliseconds) before interpreting them as unix **seconds**. It is a magic threshold, not a declared unit, so `1577836800` (seconds) and `1577836800000` (milliseconds) both resolve to `2020-01-01`.

!!! warning "All-NaN after coercion raises"
    If a `"timestamp"` or `"numeric"` column was not entirely missing before coercion but becomes entirely `NaT` / `NaN` after it, `build_table_result` raises `ValueError` rather than emitting a column of nothing — this surfaces bad input (e.g. non-numeric strings) early instead of silently producing empty data.

## Provenance

`Provenance` records where and how tabular data was obtained. It is a **framework-only** type: connectors never import or build it. The framework constructs it as part of wrapping a connector's return value, and it strips any declared `secrets` from the recorded params.

| Field | Type | Default |
|---|---|---|
| `source` | `str` | required |
| `source_description` | `str` | required |
| `params` | `dict[str, Any]` | `{}` |
| `fetched_at` | `datetime \| None` | `None` |
| `properties` | `dict[str, Any]` | `{}` |

The model is strict (`extra="forbid"`): validating a dict with any key outside the five fields raises `ValidationError`, as does omitting `source` or `source_description`. The `properties` dict is reserved for framework/serialization use, not connector-authored provider metadata.

`safe_dump()` produces a wire-safe JSON projection. When the serialized `params` or `properties` blob exceeds the internal budget (2000 bytes), that field is **replaced** — not prefixed — with a structured marker:

```python
from parsimony.result import Provenance

prov = Provenance(source="fred", source_description="FRED", params={"big": "x" * 3000})
dumped = prov.safe_dump()
print(dumped["params"])  # {'truncated': True, 'byte_length': ..., 'field': 'params'}
```

!!! warning "Truncation replaces the value"
    The oversize field is replaced wholesale rather than prefixed, deliberately, so the head of an unredacted secret cannot leak into the projection. The original value is not present in `safe_dump()` output. The 2000-byte budget is fixed and not configurable.

## See also

- [Defining connectors](defining-connectors.md) — how `output=` schemas are declared and how raw return values are wrapped
- [Loaders and enumerators](loaders-and-enumerators.md) — the stricter `OutputConfig` shapes the two verbs require
- [Errors](errors.md) — a schema `ValueError` during wrapping becomes a typed `ParseError`
- [Entities](../catalog/entities.md) — what `build_entities` produces and how DataFrames become catalog records
