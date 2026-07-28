# Entities

An `Entity` is the unit a [catalog](index.md) indexes and returns: a normalized,
discoverable identity made of a `namespace`, a `code`, a `title`, and an open
`metadata` dictionary. This page covers the `Entity` model and its sibling
`CatalogMatch`, the normalization helpers that enforce identity rules, the
field-extraction helpers that decide what indexes actually read, and the two ways
you turn a DataFrame into entities.

## The `Entity` model

`Entity` is a Pydantic v2 model with exactly four fields. It is a top-level export,
but for catalog-heavy code the clearest convention is to import it from
`parsimony.catalog`.

```python
from parsimony.catalog import Entity

e = Entity(
    namespace="fred",
    code="UNRATE",
    title="Unemployment Rate",
    metadata={"frequency": "M", "tags": ["labor", "rates"]},
)
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `namespace` | `str` | yes | Lowercase snake_case identity scope. Normalized on construction. |
| `code` | `str` | yes | The entity's identifier within the namespace. Trimmed; otherwise preserved verbatim. |
| `title` | `str` | yes | Human-readable label. Trimmed; must be non-empty. |
| `metadata` | `dict[str, Any]` | no | Open key/value space; defaults to `{}`. |

The model is configured with `extra="forbid"`. Any keyword that is not one of the
four declared fields raises a `pydantic.ValidationError` at construction. There is
no `tags`, `description`, or `frequency` field on the model: `metadata` is the only
place for anything beyond identity.

```python
from pydantic import ValidationError
from parsimony.catalog import Entity

# tags / description are NOT model fields
try:
    Entity(namespace="fred", code="X", title="T", description="old")
except ValidationError:
    pass  # extra='forbid'

# put them in metadata instead
e = Entity(namespace="fred", code="X", title="T", metadata={"description": "ok", "tags": ["a"]})
assert "tags" not in Entity.model_fields
```

!!! warning "`extra='forbid'` is strict"
    Migrating older records that carried top-level `tags` or `description` keys will
    raise `ValidationError`. Move those keys into `metadata` before constructing the
    entity. Parsimony does not silently drop unknown fields.

### Field validators

Three `field_validator`s run when an `Entity` is constructed. They delegate to the
standalone normalization helpers described below, so the same rules apply whether you
build an `Entity` directly or normalize a value by hand.

- `namespace` is passed through `normalize_namespace`: trimmed, then required to match
  `^[a-z][a-z0-9_]*$` — lowercase letters, digits, and underscores, never starting
  with a digit, never empty.
- `code` is passed through `normalize_entity_code`: trimmed and required to be
  non-empty. It is deliberately permissive otherwise, so connector-native identifiers
  survive unchanged.
- `title` is trimmed and required to be non-empty (`ValueError: title must be
  non-empty`).

## `CatalogMatch`

`CatalogMatch` is the resolved search result returned by `Catalog.search`. It mirrors
the three string fields of `Entity` — with the same three validators — keeps the open
`metadata` dict, and adds the ranking evidence: `score` plus optional `search_detail`. There
is no third, more authoritative channel.

| Field | Type | Notes |
|---|---|---|
| `namespace` | `str` | Re-normalized via `normalize_namespace`. |
| `code` | `str` | Re-normalized via `normalize_entity_code`. |
| `title` | `str` | Trimmed, non-empty. |
| `score` | `float \| None` | Ranked relevance in `(0, 1]` relative to this query's best hit; `None` when the read was filter-only (nothing ranked). Never comparable across queries or catalogs. |
| `search_detail` | `SearchDetail \| None` | Defaults to `None`. Typed ranking evidence; `None` for a filter-only match. |
| `metadata` | `dict[str, Any]` | Defaults to `{}`. Like `Entity`, `extra="forbid"`. |

Ranked rows order by `(score desc, namespace, code)`. See
[Ranking and fusion](ranking-and-fusion.md) for what `score` and `search_detail` mean in
practice, how they're computed, and where ranking that isn't relevance belongs.

You rarely build a `CatalogMatch` yourself — the catalog does it during ranking — but
the adapter is public. `catalog_match_from_entity` lives in `parsimony.catalog.models`
(not re-exported at the catalog top level), and `score`/`search_detail` are both
keyword-only.

```python
from parsimony.catalog import Entity, CatalogMatch
from parsimony.catalog.models import catalog_match_from_entity

e = Entity(namespace="fred", code="UNRATE", title="Unemployment", metadata={"freq": "M"})
m = catalog_match_from_entity(e, score=0.87)
assert isinstance(m, CatalogMatch)
assert (m.namespace, m.code, m.title, m.score) == ("fred", "UNRATE", "Unemployment", 0.87)
assert m.search_detail is None  # helper does not invent evidence
assert m.metadata is not e.metadata  # shallow copy via dict(entity.metadata)
```

!!! note "Shallow copy"
    The adapter copies metadata with `dict(entity.metadata)`. Mutating the match's
    top-level metadata dict does not touch the entity's, but nested mutable values
    (a list or dict inside `metadata`) are shared between the two.

## Identity normalization helpers

These functions are the building blocks behind the validators. Import them from
`parsimony.catalog` or `parsimony.entity` — both expose them. The full set of
helpers is in `parsimony.entity`.

### `normalize_namespace(value) -> str`

Trims, then enforces `^[a-z][a-z0-9_]*$`. Raises `ValueError("Value must be
non-empty")` on a blank string and `ValueError("Value must be lowercase snake_case
(letters, numbers, underscores)")` on a pattern mismatch.

```python
from parsimony.entity import normalize_namespace

assert normalize_namespace("fred") == "fred"
# normalize_namespace("Bad Code")  -> ValueError (not snake_case)
# normalize_namespace("1bad")      -> ValueError (starts with a digit)
```

### `normalize_entity_code(value) -> str`

Trims and requires non-empty (`ValueError("code must be non-empty")` when blank).
Intentionally loose otherwise so provider-native identifiers — uppercase, dots,
mixed punctuation — pass through unchanged.

```python
from parsimony.entity import normalize_entity_code

assert normalize_entity_code("GDPC1") == "GDPC1"
assert normalize_entity_code("  B.U.Y.10Y ") == "B.U.Y.10Y"
```

!!! warning "namespace and code are not interchangeable"
    `code` preserves uppercase and dots; `namespace` rejects them. The two helpers
    also emit different empty-string messages (`Value must be non-empty` versus
    `code must be non-empty`) — do not assume a uniform error string.

### `code_token(value) -> str`

Turns an arbitrary string into a safe, snake_case code token: lowercases, maps
`-`/space/`.` (and any other non-`[a-z0-9_]` character) to `_`, collapses repeated
underscores, strips edge underscores. Returns `"unknown"` if nothing survives, and
prefixes `v_` when the result would start with a digit. Use it in a provider when you
must synthesize a code from a free-form label.

```python
from parsimony.catalog import code_token

assert code_token("Real GDP (2017 $)") == "real_gdp_2017"
assert code_token("10Y") == "v_10y"
assert code_token("---") == "unknown"
```

### `entity_key(namespace, code) -> tuple[str, str]`

The canonical in-memory key for a `(namespace, code)` pair, used internally by the
catalog's lookup table. It returns `(normalize_namespace(namespace),
normalize_entity_code(code))`, so it applies both rules at once.

```python
from parsimony.catalog import entity_key

assert entity_key("fred", "  UNRATE ") == ("fred", "UNRATE")
```

## What an index reads: field extraction

Indexes do not read raw Python objects off an entity — they read **scalar** text.
Three helpers in `parsimony.entity` define that contract. `field_values` and
`field_text` are also re-exported from `parsimony.catalog`; `field_value` and
`require_scalar_text` are on `parsimony.entity`.

| Helper | Returns | Use |
|---|---|---|
| `field_value(entity, field)` | the raw value (`Any`) or `None` | low-level single-field accessor |
| `require_scalar_text(value, *, field=...)` | trimmed text or `None`; rejects nested types | the searchable/filterable boundary |
| `field_values(entity, field)` | `[]` or a one-element `[text]` list | what an index builds on |
| `field_text(entity, field)` | the same text (or `""`) | a single searchable string |

All three resolve `field` the same way: `namespace`, `code`, and `title` are
first-class; any other name is a `metadata.get(field)` lookup (so a missing key
yields `None`).

**Searchable and filterable columns are scalar.** Nested metadata (`list` / `dict` /
`set`) remains legal on `Entity.metadata` for display and storage, but indexing or
filtering such a field raises `ValueError` / `InvalidParameterError` with the field
name and observed type. Operators that need search over nested data must expose an
intentional derived scalar (for example `tags_text="energy prices"`) or normalize the
relation into another catalog.

`require_scalar_text` accepts:

| Value type | Result |
|---|---|
| `None` (missing key) | `None` → `field_values` returns `[]` |
| `str` | trimmed text, or `None` if blank |
| `bool` | `"true"` / `"false"` |
| `int` / finite `float` | `str(value)` |
| `list` / `tuple` / `set` / `dict` | rejected |
| non-finite float | rejected |

Bools are scalar so they remain **filterable** (`filter={"active": True}`) and can be
named in an explicit `indexes=` map. The [default index policy](search.md#the-default-index-policy)
does **not** auto-create a BM25 index for bool metadata — a flag is a facet, not a
ranking surface.

```python
from parsimony.catalog import Entity, field_values, field_text

e = Entity(
    namespace="fred",
    code="UNRATE",
    title="Unemployment Rate",
    metadata={"frequency": "M", "tags": ["labor", "rates"]},  # tags: display-only
)

assert field_values(e, "title") == ["Unemployment Rate"]
assert field_values(e, "frequency") == ["M"]
assert field_values(e, "missing") == []
# field_values(e, "tags")  # raises ValueError: must be scalar
```

## Turning a DataFrame into entities

Connectors return raw DataFrames, not entities. The path from a tabular `Result` to
entities is `Result.entities` (see
[results and output schemas](../connectors/results.md#entity-projection)), which projects
the result's `output_spec` against `result.raw` and returns just the identity side of
each entity — namespace, code, title, metadata — dropping the `DATA` rows. It is a lazy
`Mapping[EntityRef, Entity]`; call `.values()` for a plain iterable.

!!! tip "Just want to search a frame you already hold?"
    `Result.entities` builds a *curated* catalog — explicit column roles, key grouping,
    metadata-consistency checks, ready to persist. If instead you have a DataFrame in
    hand and only want to find rows in it, reach for
    [`auto_catalog(df)`](search.md#ad-hoc-runtime-catalogs): every row becomes an
    entity, every column becomes searchable, and you get back an already-built catalog.
    It is a convenience over the runtime path, not the way catalogs are built.

### `Result.entities`

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame(
    {
        "code": ["UNRATE"],
        "title": ["Unemployment Rate"],
        "frequency": ["M"],
        "description": ["Civilian unemployment rate"],
    }
)
schema = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="description", role=ColumnRole.METADATA),
    ]
)
result = Result(raw=df, output_spec=schema)
entities = list(result.entities.values())
assert entities[0].namespace == "fred"
assert entities[0].metadata == {"frequency": "M", "description": "Civilian unemployment rate"}
```

Rows are grouped by the normalized `(namespace, code)` pair, so repeated keys collapse
into one entity. Requirements:

- Exactly one `KEY` column, and that column must declare a `namespace=` — otherwise
  `ValueError: KEY column ... must declare namespace=... for entity projection`. Note
  that `namespace` may be omitted when the `OutputSpec` is *declared* — it is only
  required at projection time, i.e. when `entities` is actually accessed.
- At most one `TITLE` column (optional). When absent, the code is used as the title.
- `METADATA` columns are optional. A metadata column named `"*"` is a wildcard that
  claims every DataFrame column not already taken by the `KEY`, `TITLE`, `DATA`, or
  another explicit `METADATA` entry.
- A `KEY` namespace of `"__row__"` switches on per-row namespaces, read from a
  `METADATA` column named `entity_namespace`.

The wildcard form is convenient when you want every remaining column as metadata:

```python
import pandas as pd
from parsimony.result import Column, ColumnRole, OutputSpec, Result

df = pd.DataFrame({"code": ["A"], "name": ["Alpha"], "sector": ["Tech"], "region": ["US"]})
schema = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="demo"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="*", role=ColumnRole.METADATA),
    ]
)
result = Result(raw=df, output_spec=schema)
entities = list(result.entities.values())
assert entities[0].metadata == {"sector": "Tech", "region": "US"}
```

!!! warning "Metadata must be constant within an entity key"
    Projection groups rows by the key and requires each `TITLE`/`METADATA` column to
    hold a single, consistent non-null value per group. A column whose value differs
    across rows that share a key (for example an `isin` that changes between two rows
    of the same `code`) raises a `ValueError` — that column is observation `DATA`, not
    identity metadata, or your key is too coarse.

Once you have entities, hand them to a catalog with `set_entities(result.entities.values())`,
then build and search. See [building and searching](search.md).

## See also

- [The Catalog](index.md) — the lifecycle that consumes entities and returns matches.
- [Building and searching](search.md) — `set_entities`, `build`, and the query methods that return `CatalogMatch` results.
- [Results and output schemas](../connectors/results.md) — `OutputSpec`, `Column`, `ColumnRole`, and `Result.entities`/`Result.data`, the projection that produces entities and observations from a tabular `Result`.
- [Loaders and enumerators](../connectors/loaders-and-enumerators.md) — enumerators emit the DataFrames that become catalog entities.
