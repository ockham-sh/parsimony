# Results and output schemas

A connector returns **raw data** — a DataFrame, Series, scalar, or dict. The framework wraps that return value in a single result envelope, `Result`, carrying framework-built [`Provenance`](#provenance), and — when the connector declares an [`OutputSpec`](#outputspec) — attaches it unchanged as passive metadata. This page covers the unified result carrier, the declarative schema system, and the entity projection built on top of it.

These types live in `parsimony.result` and are all re-exported at the top level, so either import path works:

```python
from parsimony import Result, OutputSpec, Column, ColumnRole, Provenance, EntityResult
# equivalently, the explicit submodule path:
from parsimony.result import Result, OutputSpec, Column, ColumnRole, Provenance, EntityResult
```

!!! note "You rarely construct these directly"
    The framework builds `Result` and `Provenance` for you when a [connector](defining-connectors.md) returns. A connector that returns a `Result` or a `(data, properties)` tuple raises `TypeError` — provider facts belong in DataFrame columns, not in the result envelope. You *do* construct `OutputSpec` and `Column` to declare a connector's `output=` schema, and you may build a `Result` by hand for tests or for the catalog / data-store flows.

## Result

`Result` is the one envelope for every payload: any `data` plus provenance, optionally tabular. There is no separate type for DataFrames — a result is *tabular* exactly when its `data` is a `pandas.DataFrame`, and the tabular-only accessors (`frame`, `columns`, `entities`, Arrow/Parquet serialization) apply only then.

| Field | Type | Default |
|---|---|---|
| `data` | `Any` | required |
| `provenance` | `Provenance` | `Provenance(source="", source_description="")` |
| `output_spec` | `OutputSpec \| None` | `None` |

The model allows arbitrary types (`arbitrary_types_allowed`), so `data` is not deep-validated. The framework **never copies, coerces, renames, or reorders** whatever a connector returned — `data` is exactly that object. `data` is the canonical payload accessor: a DataFrame for tabular fetches, but it may equally be a Series, scalar, `dict`, `str`, or `bytes`. The members worth knowing:

| Member | Kind | Returns |
|---|---|---|
| `data` | field | the raw payload, whatever its type |
| `is_tabular` | property | `True` when `data` is a `pandas.DataFrame` |
| `frame` | property | the DataFrame payload; raises `TypeError` if not tabular |
| `text` | property | `data` unchanged if already a `str`, otherwise `str(data)` |
| `columns` | property | `output_spec.columns`, or `[]` when there is no schema |
| `entities` | property | lazy tuple-keyed entity projection — see [Entity projection](#entity-projection) |
| `to_entities()` | method | `list[Entity]` — lossy catalog conversion built on `entities` |
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

## OutputSpec

`OutputSpec` is the declarative schema you attach to a connector via `output=`. It is an ordered `list[Column]` naming each column's semantic [role](#columnrole) — nothing more.

```python
class OutputSpec(BaseModel):
    columns: list[Column]
```

`OutputSpec` never sees data. It has **no methods that accept a DataFrame** — no coercion, no renaming, no matching side effects, no result construction. The framework attaches it to `Result.output_spec` unchanged; it becomes operational only when a caller asks for an [entity projection](#entity-projection) (`Result.entities`) or a governed presentation (`Result.to_llm()`) — both interpret the declaration against `Result.data` without modifying it.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame({"sym": ["A", "B"], "title": ["Alpha", "Beta"], "v": [1, 2]})
schema = OutputSpec(columns=[
    Column(name="sym", role=ColumnRole.KEY, namespace="demo"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="v", role=ColumnRole.DATA),
])
result = Result(data=df, output_spec=schema)

print([c.name for c in result.columns])  # ['sym', 'title', 'v']
```

### Declaration validation (at construction)

An after-validator enforces these rules when you build an `OutputSpec`; violations raise `ValueError` (surfaced as pydantic `ValidationError`):

- declared column **names must be unique** (the `"*"` wildcard is exempt from this check — there can only be one anyway)
- **at most one** `KEY` column
- **at most one** `TITLE` column
- **at most one** `"*"` wildcard column, and it must have role `DATA` or `METADATA` (a wildcard cannot identify an entity)

```python
from parsimony.result import Column, ColumnRole, OutputSpec

# raises: "OutputSpec must have at most one KEY column, found 2: ['a', 'b']"
OutputSpec(columns=[
    Column(name="a", role=ColumnRole.KEY),
    Column(name="b", role=ColumnRole.KEY),
])
```

Unlike the legacy `OutputConfig`, a `KEY` column may **omit** `namespace` at declaration time — useful when the namespace is resolved dynamically per call, or when the schema is shared with a connector that never projects entities. `namespace` is only required when an [entity projection](#entity-projection) is actually requested; see below.

### The `"*"` wildcard

`"*"` is the sole dynamic rule in a declaration: a wildcard column assigns its role (`DATA` or `METADATA`) to every column *actually present in the data* that isn't named explicitly elsewhere in the declaration. It is resolved only inside [`Result.entities`](#entity-projection) — never eagerly, and never against anything but the frame in hand.

```python
from parsimony.result import Column, ColumnRole, OutputSpec

# Every undeclared column folds in as METADATA when entities are projected.
schema = OutputSpec(columns=[
    Column(name="series_key", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="value", role=ColumnRole.DATA),
    Column(name="*", role=ColumnRole.METADATA),
])
```

## Entity projection

`Result.entities` is a lazy, read-only, tuple-keyed view (`Mapping[tuple[str, str], EntityResult]`) grouping a tabular result's rows into one [`EntityResult`](#entityresult) per `(namespace, code)`. It is the single place `OutputSpec` roles are interpreted against real data — the same algorithm backs `Result.to_entities()`, catalog construction, and data-store loading.

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame({
    "code": ["unrate", "unrate", "cpi"],
    "title": ["Unemployment", "Unemployment", "CPI"],
    "freq": ["monthly", "monthly", "monthly"],
    "date": ["2024-01", "2024-02", "2024-01"],
    "value": [3.7, 3.9, 310.3],
})
schema = OutputSpec(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="freq", role=ColumnRole.METADATA),
    Column(name="date", role=ColumnRole.DATA),
    Column(name="value", role=ColumnRole.DATA),
])
result = Result(data=df, output_spec=schema)

entities = result.entities                 # bind once; the property is uncached
unrate = entities["fred", "unrate"]
print(unrate.title)                        # 'Unemployment'
print(unrate.metadata)                     # {'freq': 'monthly'}
print(list(unrate.data.columns))           # ['date', 'value']
print(len(unrate.data))                    # 2 rows for this entity
```

### Requirements and errors

`Result.entities` requires:

- a tabular `data` (`TypeError` otherwise)
- an `output_spec` declaring **exactly one** `KEY` column (`ValueError` otherwise)
- that `KEY` column must declare a non-empty `namespace` (`ValueError`: `"KEY column ... must declare namespace=... for entity projection"`) — this is checked here, at projection time, not at `OutputSpec` construction
- every declared non-wildcard column (`KEY`, `TITLE`, `METADATA`, `DATA`) must actually be present in the frame (`ValueError` listing the missing names)
- no duplicate DataFrame column labels among the columns the projection reads (`ValueError`)
- no null values in the `KEY` column (`ValueError`)
- **consistent** `TITLE`/`METADATA` values within one entity's rows — a `TITLE` or `METADATA` column may vary row-to-row *only* if it resolves to the same non-null value throughout that entity's group; otherwise `ValueError`

Rows are grouped by the normalized `(namespace, code)` pair (see [`normalize_namespace`/`normalize_entity_code`](../catalog/entities.md)), in first-appearance order. Each `EntityResult.data` contains exactly that entity's `DATA`-role rows and columns — `KEY`, `TITLE`, and `METADATA` columns are consumed into the entity's identity, not duplicated into `data`.

!!! warning "Intentionally uncached"
    `Result.data` is a mutable raw object, so caching the projection would risk a stale view after in-place mutation. `Result.entities` recomputes on every access — bind it to a local once for repeated lookups (`entities = result.entities`) rather than re-accessing the property in a loop.

### Per-row namespace

When a connector's entity namespace is not static (e.g. a search endpoint that spans several catalogs in one call), declare the `KEY` column's `namespace` as the sentinel `"__row__"` and add a `METADATA` column literally named `entity_namespace` carrying the per-row value:

```python
from parsimony.result import Column, ColumnRole, OutputSpec

schema = OutputSpec(columns=[
    Column(name="code", role=ColumnRole.KEY, namespace="__row__"),
    Column(name="entity_namespace", role=ColumnRole.METADATA),
    Column(name="title", role=ColumnRole.TITLE),
])
```

### EntityResult

`EntityResult` is a frozen dataclass — one entity's slice of a tabular `Result`: identity plus `DATA` rows.

| Field / property | Type | Notes |
|---|---|---|
| `entity` | `Entity` | the resolved identity (see [Entities](../catalog/entities.md)) |
| `data` | `pd.DataFrame` | only this entity's `DATA`-role columns, in original row order and index |
| `provenance` | `Provenance` | the **same** call-level object as the parent `Result` — shared, not copied |
| `namespace` | `str` | delegates to `entity.namespace` |
| `code` | `str` | delegates to `entity.code` |
| `title` | `str` | delegates to `entity.title` |
| `metadata` | `Mapping[str, Any]` | delegates to `entity.metadata` |

### to_entities()

```python
to_entities() -> list[Entity]
```

A lossy catalog conversion: identity, title, and metadata only — no `DATA` rows. This is what catalog-build code calls to turn a connector's `Result` into the records a [`Catalog`](../catalog/index.md) indexes.

```python
entities = result.to_entities()
print(entities[0].namespace, entities[0].code)  # fred unrate
```

## Column

`Column` declares one column's semantics in an `OutputSpec`. It is purely declarative — it never inspects, transforms, or renames the connector's returned data.

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | `str` | required | matched against DataFrame columns by exact label; `"*"` is the wildcard |
| `role` | `ColumnRole` | `DATA` | |
| `description` | `str \| None` | `None` | free annotation |
| `namespace` | `str \| None` | `None` | allowed **only** on `KEY` columns; required for entity projection, not for declaration |
| `exclude_from_llm_view` | `bool` | `False` | forbidden on `DATA` and `TITLE` columns |

An after-validator applies (raises `ValueError`, surfaced as `ValidationError`):

- `exclude_from_llm_view=True` is rejected on `DATA` and `TITLE` columns
- `namespace` is rejected on any role other than `KEY`, and must be non-empty when set

```python
from parsimony.result import Column, ColumnRole

col = Column(name="freq", role=ColumnRole.METADATA)
print(col.role)  # ColumnRole.METADATA
```

`llm_annotation()` renders the governed `(ROLE)` / `(ROLE ns:<namespace>)` token used by every LLM-facing schema view (connector cards, `to_llm()`, downstream fetch logs) — the single source of truth for that formatting.

## ColumnRole

`ColumnRole` is a string enum naming a column's semantic role:

| Member | Value | Meaning |
|---|---|---|
| `ColumnRole.DATA` | `"data"` | an observation / measurement column |
| `ColumnRole.KEY` | `"key"` | the entity identifier (its `code`); carries a `namespace` for entity projection |
| `ColumnRole.TITLE` | `"title"` | a human-readable label |
| `ColumnRole.METADATA` | `"metadata"` | descriptive attributes (frequency, units, …) |

These roles drive [entity projection](#entity-projection) and are what a [data store](../catalog/data-store.md)'s `@loader` output validates against.

!!! tip "No dtype coercion, ever"
    `OutputSpec`/`Column` declare semantics only — they never coerce a column's dtype, rename it, or otherwise touch the returned DataFrame. If a connector needs `date` parsed to `datetime64` or a numeric field coerced from a provider's string encoding, it does that explicitly in the connector body (e.g. `df["date"] = pd.to_datetime(df["date"])`) before returning. This keeps `OutputSpec` a pure, inspectable declaration and keeps all data transformation visible in one place: the connector's own code.

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

## Arrow and Parquet serialization

A tabular `Result` round-trips through Arrow and Parquet with provenance and schema embedded in the table metadata (under the binary key `b"parsimony.result"`):

| Method | Behavior |
|---|---|
| `to_arrow()` | `pa.Table` with `provenance.safe_dump()` and the column dumps embedded as metadata |
| `from_arrow(table)` | classmethod; reverses `to_arrow`; tolerates a vanilla table with no such metadata by returning a schemaless result |
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
print(restored.provenance.params)                       # {'q': 'unemployment'}
```

!!! note "Legacy fields are ignored on read"
    Retired fields on older Arrow/Parquet payloads written before this schema simplified — `dtype`, `mapped_name`, and the `kind` role alias — are ignored rather than rejected by `from_arrow`, so old files remain readable. New writes never emit them.

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

- [Defining connectors](defining-connectors.md) — how `output=` schemas are declared and attached to a `Result`
- [Loaders and enumerators](loaders-and-enumerators.md) — the stricter `OutputSpec` shapes the two verbs require
- [Errors](errors.md) — the typed `ConnectorError` taxonomy a connector raises itself; entity-projection failures stay plain `ValueError`
- [Entities](../catalog/entities.md) — what `to_entities()` produces and how DataFrames become catalog records
