# Connector Implementation Walkthrough

How to build a public parsimony connector from scratch — provider research,
scaffold, schema design, error mapping, testing, catalog integration.

For the **authoritative** plugin contract, see [`contract.md`](contract.md).
For private / internal connectors (Postgres, Snowflake, S3), see
[`building-a-private-connector.md`](building-a-private-connector.md). To contribute
to the official monorepo, start with
[ockham-sh/parsimony-connectors CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md).

---

## Plugin shape

Every parsimony connector ships as its own PyPI distribution
(`parsimony-<name>`) registered via the `parsimony.providers` entry point.
The kernel has no in-tree connectors.

```text
parsimony-<name>/
├── parsimony_<name>/
│   ├── __init__.py         CONNECTORS (+ optional CATALOGS / RESOLVE_CATALOG)
│   ├── connectors.py       @connector / @enumerator / @loader functions
│   └── py.typed
├── tests/
│   ├── test_conformance.py          assert_plugin_valid — release-blocking
│   └── test_<name>_connectors.py    happy path + error mapping (respx mocks)
└── pyproject.toml          entry-point registration + [project.urls] Homepage
```

`tags=["tool"]` opts a connector into the MCP tool surface — agents invoke
it interactively, so its result must fit in a context window. Decide which
connectors are tools before writing them (Phase 2).

---

## Phase 0 — Provider research

**Core principle: docs lie. Test live before trusting anything.** Spend
30–60 minutes exploring the API before writing code.

### 1. Authenticate first

```bash
export MY_SOURCE_API_KEY="your-key-here"
```

Verify with one authenticated request before anything else.

### 2. Live exploration (the critical step)

```python
import httpx
r = httpx.get("https://api.example.com/v1/series", params={"id": "CPI"})
print(r.status_code, r.headers.get("content-type"))
print(r.json())
```

Verify each endpoint:

- Does it respond? Is the response shape what docs claim?
- What pagination — offset/limit, cursor, `Link` headers?
- What rate-limit signals — `X-RateLimit-Remaining`, 429 bodies?
- What do nulls actually look like — `null`, `"NaN"`, `"."`, empty string,
  missing keys?

Discover undocumented endpoints via the provider's data-explorer browser
dev tools (filter Network → XHR/Fetch). For SDMX-shaped providers, try
`{base_url}/sdmx/v2.1/dataflow/all/all/latest` even if the docs don't
mention SDMX — if it returns XML, hand the agency to `parsimony-sdmx`.

### 3. Classify search capability

| Tier | Capability | Catalog strategy |
|---|---|---|
| 1 | Native search returns good results | `@connector(tags=["tool"])` direct |
| 2 | Structured list / SDMX DSD | `@enumerator` → `Catalog.add_from_result` |
| 3 | Browsable site, no API search | Scrape or curate, then index |
| 4 | Bulk files only | Parse files into `@enumerator` output |

### 4. Write down what you found

Especially what differs from the docs. Future-you and reviewers need it.

---

## Phase 1 — Scaffold

```bash
uvx cookiecutter gh:ockham-sh/parsimony-plugin-template
```

The scaffold writes a working pyproject + entry point + conformance test.
The two pieces you'll touch by hand:

```toml
# pyproject.toml — the load-bearing entries
[project.urls]
Homepage = "https://your-provider.example"

[project.entry-points."parsimony.providers"]
<your-name> = "parsimony_<your_name>"
```

```python
# parsimony_<your_name>/__init__.py
from parsimony import Connectors
from parsimony_<your_name>.connectors import <your_name>_search, <your_name>_fetch

CONNECTORS = Connectors([<your_name>_search, <your_name>_fetch])
```

`CONNECTORS` is required. Provider metadata (homepage, version, description)
lives in `pyproject.toml` and is read on demand via `importlib.metadata` —
there is no module-level `ENV_VARS`, `PROVIDER_METADATA`, or `__version__`.

---

## Phase 2 — Tags

Tags control (1) MCP exposure and (2) domain category.

```python
@connector(tags=["macro", "tool"])    # MCP tool + macro category
@connector(tags=["macro"])            # fetch-only, not exposed to MCP
@enumerator(tags=["macro", "us"])     # US macro enumerator
```

**Rule of thumb:** if the agent calls it *interactively* to discover or
search data, add `"tool"`. If the agent calls it *programmatically* after
catalog discovery, omit `"tool"`.

---

## Phase 3 — Params models

One Pydantic model per connector. Field descriptions appear verbatim in the
agent's system prompt — write them as if for an LLM, not a human.

```python
from typing import Annotated
from pydantic import BaseModel, Field, field_validator

class MySourceFetchParams(BaseModel):
    """Parameters for fetching a time series from MySource."""

    series_id: Annotated[str, "ns:my_source"] = Field(
        ..., description="Series identifier (e.g. CPI.TOTAL)"
    )
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_id must be non-empty")
        return v
```

`Annotated[str, "ns:my_source"]` is the kernel sentinel naming the catalog
namespace this parameter draws from.

### Aliasing reserved keywords

When the API uses `from`, `type`, `in`, etc. as parameter names:

```python
from pydantic import ConfigDict

class MyParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_date: str | None = Field(
        default=None,
        alias="from",
        description="Start date ISO 8601. Use as from_date='2024-01-15'",
    )
```

**Critical:** the description must include `Use as from_date=`. Without it,
agents try `from="2024-01-15"` and hit a `SyntaxError`.

Share param models across connectors only when parameters are *truly
identical* (e.g. FMP's three statement endpoints all take `(symbol, period,
limit)`). When parameters differ even slightly, separate models.

---

## Phase 4 — OutputConfig

`OutputConfig` declares the semantic role of each column. Four roles:

| Role | Purpose | Constraint |
|---|---|---|
| `KEY` | Entity identifier (series_id, ticker) | Exactly one. |
| `TITLE` | Human-readable name | Exactly one when KEY is present. |
| `DATA` | Observation values | The actual data columns. |
| `METADATA` | Supplementary context | Optional. |

```python
from parsimony import OutputConfig, Column, ColumnRole

FETCH_OUTPUT = OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY,
           param_key="series_id", namespace="my_source"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])
```

Column options worth knowing:

- `dtype` — coercion: `"auto"`, `"datetime"`, `"date"`, `"numeric"`,
  `"timestamp"`, `"bool"`, `"str"`, or a pandas dtype string
- `param_key` — fill the column from a named param field (every row gets
  the same param value)
- `mapped_name` — rename an upstream column to a canonical name
- `namespace` — catalog namespace for KEY columns; defaults to the
  catalog's own name when omitted
- `exclude_from_llm_view` — hide METADATA from agent-facing tool schemas

Columns in the DataFrame not declared in `OutputConfig` automatically
become DATA columns.

### dtype reference

| dtype | Pipeline | Expected input | Failure |
|---|---|---|---|
| `"auto"` | pandas infers | Any | No validation |
| `"timestamp"` | `to_numeric` → scale ms→s if >1e11 → `to_datetime(unit="s")` | Unix epoch (s or ms) | `ParseError` if all NaT |
| `"date"` | `to_datetime(...).dt.normalize()` | ISO 8601 date | Raises on unparseable |
| `"datetime"` | `to_datetime(...)` | ISO 8601 datetime or epoch | Raises on unparseable |
| `"numeric"` | `to_numeric(errors="coerce")` | Numeric string or number | `ParseError` if all NaN |

**`"timestamp"` vs `"date"` is the most common trap.** `"timestamp"` is for
unix epoch ints (`1704067200`). `"date"` is for ISO strings
(`"2024-01-01"`). Mixing them produces all-NaT and raises.

**Missing-data sentinels.** APIs return `"."`, `"None"` (string), `"-"`, or
empty strings. `"numeric"` handles these via coercion; `"date"` /
`"datetime"` crash. Replace sentinels with `None` in the row-building loop
for date columns.

### Multi-namespace providers

When identifier spaces are disjoint (equity ticker `AAPL` vs crypto pair
`btcusd` are meaningless to each other's endpoints), use separate
namespaces — `my_source_equity`, `my_source_crypto`, `my_source_fx`. The
`Annotated[str, "ns:..."]` sentinel on parameters then prevents an agent
from passing a crypto pair to an equity endpoint.

When a single connector serves multiple resource types with different
schemas, pick the `OutputConfig` from a `_OUTPUT_MAP[params.resource]`
dict at runtime.

**Validate column match in tests.** Column names must match what
`pd.DataFrame(response.json())` actually produces — not docs:

```python
assert not MY_OUTPUT.validate_columns(sample_df), MY_OUTPUT.validate_columns(sample_df)
```

---

## Phase 5 — HTTP client

Use `parsimony.transport.HttpClient`. It wraps `httpx.AsyncClient` and
redacts sensitive query-param values (`api_key`, `token`, `password`,
anything ending `_token`) in structured logs.

```python
from parsimony.transport import HttpClient

_BASE_URL = "https://api.my-source.example.com/v1"

def _make_http(api_key: str) -> HttpClient:
    return HttpClient(_BASE_URL, default_params={"api_key": api_key}, timeout=30.0)
```

If your credential param name isn't in the default redaction list, file a
kernel issue.

---

## Phase 6 — Connectors

```python
import pandas as pd
from parsimony import connector, enumerator
from parsimony.transport import HttpClient, map_http_error

@connector(
    output=FETCH_OUTPUT,
    env={"api_key": "MY_SOURCE_API_KEY"},
    tags=["my_source", "tool"],
)
async def my_source_fetch(
    params: MySourceFetchParams,
    *,
    api_key: str,
) -> pd.DataFrame:
    """Fetch time series observations by series_id from MySource."""
    async with HttpClient(_BASE_URL, default_params={"api_key": api_key}) as http:
        try:
            response = await http.get(f"/series/{params.series_id}/observations")
        except httpx.HTTPStatusError as exc:
            raise map_http_error(exc, provider="my_source", op_name="fetch") from exc
    return pd.DataFrame(response.json().get("observations", []))
```

`env={"api_key": "MY_SOURCE_API_KEY"}` tells the kernel which env var backs
the `api_key` keyword-only dep. Multi-credential providers list one entry
per dep.

You return a DataFrame; the decorator wraps it in a `Result` with
provenance generated from the params model and `OutputConfig`.

### Error mapping

Funnel upstream HTTP failures through `map_http_error`:

- `401` / `403` → `UnauthorizedError`
- `402` → `PaymentRequiredError`
- `429` → `RateLimitError` (carries `retry_after: float`)
- anything else → `ProviderError`

For timeouts: `from parsimony.transport import map_timeout_error`.

Empty results are a signal, not an error shape. Raise `EmptyDataError`
when upstream clearly returned "no data for this input" — don't let a
zero-row DataFrame propagate silently.

### Pagination

Pick one of offset/limit, cursor, or `Link` header — whichever the
provider supports. Offset/limit shape:

```python
rows, offset = [], 0
while True:
    response = await http.get("/series", params={"limit": 100, "offset": offset})
    batch = response.json().get("series", [])
    if not batch:
        break
    rows.extend(batch)
    offset += 100
```

Cursor pagination follows `payload.get("next_cursor")`; `Link` pagination
follows the `link` response header. For burst-heavy enumerators, use
`parsimony.transport.pooled_client` to share a TCP pool.

---

## Phase 7 — Test

The full happy-path / error-mapping shape is specified in
[testing-template.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/testing-template.md)
in the connectors monorepo. Two files per package:

```python
# tests/test_conformance.py — release-blocking
import parsimony_my_source
from parsimony.testing import assert_plugin_valid

def test_plugin_conforms() -> None:
    assert_plugin_valid(parsimony_my_source)
```

```python
# tests/test_my_source_connectors.py — happy path + error mapping
@respx.mock
@pytest.mark.asyncio
async def test_fetch_happy_path():
    respx.get("https://api.my-source.example.com/v1/series/CPI/observations").mock(
        return_value=httpx.Response(200, json={"observations": [
            {"series_id": "CPI", "date": "2024-01-01", "value": 100.0},
        ]})
    )
    bound = CONNECTORS.bind(api_key="test-key")
    result = await bound["my_source_fetch"](series_id="CPI")
    assert result.provenance.source == "my_source"
    assert len(result.data) == 1

@respx.mock
@pytest.mark.asyncio
async def test_fetch_401_maps_to_unauthorized():
    respx.get("https://api.my-source.example.com/v1/series/X/observations").mock(
        return_value=httpx.Response(401, json={"error": "bad key"})
    )
    bound = CONNECTORS.bind(api_key="live-looking-key")
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound["my_source_fetch"](series_id="X")
    assert "live-looking-key" not in str(exc_info.value)   # no key leak
```

Required tests for any connector with an `api_key` / `token`: 401 →
`UnauthorizedError` and 429 → `RateLimitError`, both with key-leak
assertions.

```bash
pytest tests/ -v && ruff check . && mypy parsimony_<your_name>/
```

All four (pytest, ruff, mypy, conformance) must pass to cut a release.

---

## Phase 8 — Catalog integration (optional)

If your plugin publishes catalog bundles, export `CATALOGS`:

```python
from parsimony_my_source.connectors import my_source_enumerate

CATALOGS = [("my_source", my_source_enumerate)]
```

Then `parsimony publish --provider my_source --target 'hf://org/catalog-{namespace}'`
runs the enumerator, ingests into a fresh `Catalog`, and pushes.

For dynamic namespace sets (e.g. SDMX fans across live agencies),
`CATALOGS` may be an `async def` generator yielding `(namespace, callable)`
tuples. For large dynamic sets, also export `RESOLVE_CATALOG(namespace)`
so `--only NS` can build a single catalog without walking the generator.
Full spec in [`contract.md`](contract.md) §6.

---

## Publish

1. Configure [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/)
   for your repo (one-time).
2. Copy a release workflow from an existing plugin
   (e.g. `parsimony-fred/.github/workflows/release.yml`).
3. Tag + push: `git tag v0.1.0 && git push --tags`. GitHub Actions
   publishes via OIDC — no tokens in secrets.
4. Verify in a fresh venv: `pip install parsimony-core parsimony-<yourname> && parsimony list`.

---

## v0.1.0 checklist

- [ ] `CONNECTORS` exported (and `CATALOGS` / `RESOLVE_CATALOG` if applicable)
- [ ] `@connector(env={...})` covers every keyword-only dep
- [ ] `[project.urls] Homepage` set; entry point registered
- [ ] `parsimony.testing.assert_plugin_valid(module)` passes
- [ ] Tool-tagged connectors have ≥40-char descriptions
- [ ] Tests cover happy path + 401 + 429 (key-leak asserted)
- [ ] `parsimony list --strict`, `ruff check`, `mypy` all green
- [ ] `README.md` covers install, setup, example
- [ ] `LICENSE` (Apache-2.0 for official plugins)

---

## Per-provider vs protocol-grouped plugin

Default is **per-provider** (`parsimony-<provider>`) for bespoke APIs.
Use **protocol-grouped** (`parsimony-<protocol>`) only when multiple
providers share a wire protocol, >60% of the implementation, dependency
tree, and maintenance cadence. Examples: `parsimony-sdmx`, `parsimony-pxweb`.
