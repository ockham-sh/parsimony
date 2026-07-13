# Results and output specs

A connector returns **raw data** — a DataFrame, Series, scalar, or dict. The framework wraps that return value in a single result envelope, `Result`, carrying framework-built [`Provenance`](#provenance), and — when the connector declares an [`OutputSpec`](#outputspec) — attaches that spec as a declaration of how the columns may be interpreted. The spec never changes the data: `Result.data` is exactly what the connector returned. This page covers the unified result carrier, the annotation system, and the entity projection it unlocks.

These types live in `parsimony.result` and are all re-exported at the top level, so either import path works:

```python
from parsimony import Result, OutputSpec, Column, ColumnRole, Provenance
# equivalently, the explicit submodule path:
from parsimony.result import Result, OutputSpec, Column, ColumnRole, Provenance
```

!!! note "You rarely construct these directly"
    The framework builds `Result` and `Provenance` for you when a [connector](defining-connectors.md) returns. A connector that returns a `Result` or a `(data, properties)` tuple raises `TypeError` — provider facts belong in DataFrame columns, not in the result envelope. You *do* construct `OutputSpec` and `Column` to declare a connector's `output=` spec, and you may build a `Result` by hand for tests or for the catalog / data-store flows.

## Result

`Result` is the one envelope for every payload: any `data` plus provenance, optionally tabular. There is no separate type for DataFrames — a result is *tabular* exactly when its `data` is a `pandas.DataFrame`, and the tabular-only accessors (`frame`, `output_spec`, the role-derived views, Arrow/Parquet serialization) apply only then.

| Field | Type | Default |
|---|---|---|
| `data` | `Any` | required |
| `provenance` | `Provenance` | `Provenance(source="", source_description="")` |
| `output_spec` | `OutputSpec \| None` | `None` |

The model allows arbitrary types (`arbitrary_types_allowed`), so `data` is not deep-validated. `data` is the canonical payload accessor — a DataFrame for tabular fetches, but it may equally be a Series, scalar, `dict`, `str`, or `bytes`. The framework never renames, coerces, or validates it: what the connector returned is what you read. The members worth knowing:

| Member | Kind | Returns |
|---|---|---|
| `data` | field | the raw payload, whatever its type |
| `is_tabular` | property | `True` when `data` is a `pandas.DataFrame` |
| `frame` | property | the DataFrame payload; raises `TypeError` if not tabular |
| `df` | property | a live alias of `frame` (the tabular convenience accessor) |
| `text` | property | `data` unchanged if already a `str`, otherwise `str(data)` |
| `to_entities()` | method | the [entity projection](#entity-projection-resultto_entities) of a role-annotated tabular result — a `list[Entity]` |
| `to_llm()` | method | a governed, length-bounded text preview — the right thing to print for an agent |

Use `is_tabular` to branch on payload shape — never `isinstance(result, ...)`:

```python
import pandas as pd
from parsimony.result import Result

tabular = Result(data=pd.DataFrame({"v": [1, 2]}))
scalar = Result(data=4.25)

print(tabular.is_tabular)   # True
print(scalar.is_tabular)    # False
print(tabular.frame.shape)  # (2, 1)  — frame works on tabular payloads
print(scalar.text)          # 4.25    — stringified opaque payload
```

`_with_properties(**properties)` is an internal helper for serialization/tests; it merges keyword arguments into `provenance.properties` on a **new** `Result`. Connector code should not call this.

### Spec-derived views (tabular)

When an `output_spec` is present, these read-only properties expose its declared columns by [role](#columnrole). They read the *declaration*, not the data — a declared column may or may not be present in the returned frame:

| Property | Returns |
|---|---|
| `frame` | the underlying `pd.DataFrame` (raises `TypeError` if not tabular) |
| `df` | a live alias of `frame` |
| `columns` | `list[Column]` from the spec, or `[]` when there is no spec |
| `data_columns` | columns whose role is `DATA` |
| `metadata_columns` | columns whose role is `METADATA` |

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame({"sym": ["A", "B"], "title": ["Alpha", "Beta"], "v": [1, 2]})
spec = OutputSpec(columns=[
    Column(name="sym", role=ColumnRole.KEY, namespace="demo"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="v", role=ColumnRole.DATA),
])
result = Result(data=df, output_spec=spec)

print([c.name for c in result.data_columns])       # ['v']
print([c.name for c in result.metadata_columns])   # []
```

### Entity projection: `result.to_entities()`

A tabular result whose spec declares a namespaced `KEY` column can be projected into catalog **entities**: rows sharing a key become one [`Entity`](../catalog/entities.md). `result.to_entities()` returns that projection as a `list[Entity]` — a plain method call (nothing is cached) that leaves `result.data` untouched.

```python
import pandas as pd
from parsimony import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame({
    "code": ["unrate", "unrate", "gdpc1"],
    "title": ["Unemployment", "Unemployment", "Real GDP"],
    "freq": ["monthly", "monthly", "quarterly"],
    "value": [3.5, 3.6, 21000.0],
})
result = Result(data=df, output_spec=OutputSpec(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="freq", role=ColumnRole.METADATA),
    Column(name="value", role=ColumnRole.DATA),
]))

entities = result.to_entities()                # list[Entity], first-appearance order
print(len(entities))                           # 2
print(entities[0].namespace, entities[0].code) # fred unrate
print(entities[0].title)                       # 'Unemployment'
print(entities[0].metadata)                    # {'freq': 'monthly'}
```

The mapping is role-driven: the `KEY` column's `namespace=` plus each key value form the entity identity; the `TITLE` column supplies `title` (an all-null TITLE falls back to the code; an empty-string title fails `Entity` validation loudly); `METADATA` columns become `metadata`, with null values dropped per entity and repeated equal values collapsed to one. The list feeds a catalog directly:

```python
from parsimony import Catalog

catalog = Catalog("fred")
catalog.set_entities(result.to_entities())
catalog.build()
```

**Validation happens at projection time, never during connector execution.** Declaring a column that the connector did not return is not an error when the connector runs; `result.to_entities()` is where the role invariants are checked, and it raises when they do not hold:

| Condition | Raises |
|---|---|
| the result is not tabular | `TypeError` |
| `output_spec` is `None` | `ValueError` |
| not exactly one `KEY` column, or the `KEY` column has no `namespace=` | `ValueError` |
| a declared column (KEY, TITLE, explicit METADATA, or non-wildcard DATA) is absent from the data | `ValueError` naming the missing and available columns |
| the `KEY` column contains nulls | `ValueError` |
| a TITLE or METADATA column holds two distinct non-null values within one entity's rows | `ValueError` ("values vary within the entity key") |
| two keys normalize to the same code | `ValueError` ("Duplicate entity") |
| a TITLE value is the empty string | `ValueError` ("title must be non-empty") |

A METADATA column named `"*"` is a wildcard claiming every returned column not taken by another declaration. An empty frame — including a bare `pd.DataFrame()` with no columns — projects to `[]`.

For a bare DataFrame you hold outside a `Result`, `parsimony.catalog.source.entities_from_raw(raw, output_spec)` runs the same projection and returns the `list[Entity]` directly.

### Constructors

`Result.from_dataframe(df)` wraps a DataFrame (or a Series, coerced first) with **no spec**. It raises `ValueError("Returned an empty DataFrame.")` on empty input.

### Arrow and Parquet serialization

A tabular `Result` round-trips through Arrow and Parquet with provenance and the output spec embedded in the table metadata (under the binary key `b"parsimony.result"`):

| Method | Behavior |
|---|---|
| `to_arrow()` | `pa.Table` with `provenance.safe_dump()` and the column dumps embedded as metadata |
| `from_arrow(table)` | classmethod; reverses `to_arrow`; tolerates a vanilla table with no such metadata by returning a spec-less result |
| `to_parquet(path)` | writes the Arrow table to Parquet |
| `from_parquet(path)` | classmethod; reads Parquet written by `to_parquet` |

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result

result = Result(
    data=pd.DataFrame({"code": ["UNRATE"], "title": ["Unemployment"]}),
    provenance=Provenance(source="fred", source_description="FRED", params={"q": "unemployment"}),
    output_spec=OutputSpec(columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
    ]),
)

table = result.to_arrow()
restored = Result.from_arrow(table)
print([c.name for c in restored.output_spec.columns])  # ['code', 'title']
print(restored.output_spec.columns[0].namespace)        # 'fred'
print(restored.provenance.params)                        # {'q': 'unemployment'}
```

## The `to_llm()` view

`to_llm()` renders a compact, **schema-in-context** view of a result for an LLM prompt — type and shape, not the full payload. It is the framework-owned counterpart to dumping `result.data`: the size it adds to context is O(schema) for tables and O(structure) for opaque payloads, not O(rows) or O(bytes). A single `Result.to_llm()` covers both cases, branching internally on `is_tabular`.

`to_llm()` is the data layer's single convention for "the governed string an LLM may see of this object" — the same method name carries the connector card (`Connector.to_llm()`), the bundle listing (`Connectors.to_llm()`), and this result view. (A runtime such as `parsimony-agents` has its own, separate `to_llm(mode) -> blocks` convention for *assembling* a message; it delegates the *content* of a governed object back to these methods.)

The signature is uniform, so a caller holding any `Result` can call it blindly:

```python
to_llm(*, max_rows: int = 10, max_chars: int = 2000) -> str
```

A tabular result honors `max_rows`; an opaque one honors `max_chars`. Each ignores the other's knob.

### Tabular preview

Renders a shape line, a per-column schema block (**dtype + [role](#columnrole) + namespace**), and the first `max_rows` rows as CSV — never a head/tail sample masquerading as the whole. Columns flagged [`exclude_from_llm_view`](#column) are dropped from **both** the schema block and the rows.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame({"date": pd.to_datetime(["2020-01-01", "2020-01-02"]), "value": [1.0, 2.0]})
result = Result(data=df, output_spec=OutputSpec(columns=[
    Column(name="date", role=ColumnRole.KEY, namespace="fred_series"),
    Column(name="value", role=ColumnRole.DATA),
]))
print(result.to_llm())
# Result (table): 2 rows × 2 columns
# Columns:
# - date: datetime64[ns] (KEY ns:fred_series)
# - value: float64 (DATA)
# Rows (2):
# date,value
# 2020-01-01 00:00:00,1.0
# 2020-01-02 00:00:00,2.0
```

With no `output_spec` the schema lines carry dtype only (no role annotation). For a frame longer than `max_rows` the header counts stay honest (the *real* row total) and the row label becomes `Rows (showing N of M):`, showing only the first `N` rows; wide cell values are truncated.

### Opaque preview

For non-tabular `data` (dict/JSON, list, str, scalar, bytes, pydantic model) `to_llm()` emits a depth-limited structural summary — one level of expansion, with nested values collapsed to a `type[shape]` token:

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

## OutputSpec

`OutputSpec` is the declaration you attach to a connector via `output=`. It is an ordered `list[Column]` stating how consumers may interpret the columns the connector returns — roles, namespaces, descriptions, LLM visibility.

```python
class OutputSpec(BaseModel):
    columns: list[Column]
```

It is **pure annotation, never transformation**. The framework does not rename columns, coerce dtypes, or validate the returned frame against the spec — the connector body parses, renames, and coerces before returning, and the spec only describes the result. Declaring a column the connector did not return is not an execution error either; role-driven operations — the [`result.to_entities()`](#entity-projection-resultto_entities) projection, [data-store loading](../catalog/data-store.md) — validate presence and role invariants themselves when invoked.

### Role validation (at construction)

Only the declaration itself is validated. An after-validator enforces three rules when you build an `OutputSpec`; violations raise `ValueError` (surfaced as pydantic `ValidationError`):

- **at most one** `KEY` column
- **at most one** `TITLE` column
- **at least one** column with role `DATA`, `KEY`, or `TITLE`

```python
from parsimony.result import Column, ColumnRole, OutputSpec

# raises: "Output spec must have at most one KEY column"
OutputSpec(columns=[
    Column(name="a", role=ColumnRole.KEY),
    Column(name="b", role=ColumnRole.KEY),
])
```

A `METADATA` column named `"*"` is a wildcard matching every returned column not claimed by another declaration — it matters only to role-driven consumers such as the entity projection.

## Column

`Column` declares one column in an `OutputSpec`. It is pure annotation: it names a column the connector is expected to return and states how consumers may interpret it. Declaring a column never renames, coerces, or validates the data the connector actually returned.

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | `str` | required | the column name as returned by the connector; `"*"` is the METADATA wildcard |
| `role` | `ColumnRole` | `DATA` | accepts the JSON alias `kind` |
| `description` | `str \| None` | `None` | free annotation |
| `exclude_from_llm_view` | `bool` | `False` | forbidden on `DATA` and `TITLE` columns |
| `namespace` | `str \| None` | `None` | allowed **only** on `KEY` or `METADATA` columns |

The model is strict (`extra="forbid"`): passing any other keyword raises a pydantic `ValidationError`. In particular there is no `dtype` and no `mapped_name` — connectors parse, rename, and coerce inside the connector body and return already-shaped data.

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

These roles drive the [entity projection](#entity-projection-resultto_entities) and loader output validation ([Data stores](../catalog/data-store.md)).

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

- [Defining connectors](defining-connectors.md) — how `output=` specs are declared and how raw return values are wrapped
- [Loaders and enumerators](loaders-and-enumerators.md) — the stricter `OutputSpec` shapes the two verbs require
- [Errors](errors.md) — the typed exception taxonomy connectors raise
- [Entities](../catalog/entities.md) — what the entity projection produces and how DataFrames become catalog records
