# Connector Implementation Guide

How to build a parsimony connector from scratch. Covers provider research,
the plugin skeleton, schema design, error mapping, testing, and catalog
integration.

For the **authoritative** plugin contract, see [`contract.md`](contract.md).
For private / internal connectors (Postgres, Snowflake, S3), see
[`building-a-private-connector.md`](building-a-private-connector.md). To contribute a public
connector to the official monorepo, start with
[ockham-sh/parsimony-connectors CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md).

---

## Overview

Every parsimony connector ships as its own PyPI distribution —
`parsimony-<name>` — registered via the `parsimony.providers` entry-point
group. The kernel has no in-tree connectors; the package layout below
matches every plugin, from the minimal `parsimony-treasury` (no
credentials, flat REST) up to `parsimony-sdmx` (multi-agency SDMX with
catalog publishing).

```text
parsimony-<name>/
├── parsimony_<name>/
│   ├── __init__.py         CONNECTORS (+ optional CATALOGS / RESOLVE_CATALOG)
│   ├── connectors.py       @connector / @enumerator / @loader functions
│   └── py.typed
├── tests/
│   ├── test_conformance.py          assert_plugin_valid — release-blocking
│   └── test_<name>_connectors.py    happy path + error mapping (respx mocks)
├── .github/workflows/
│   ├── ci.yml                       lint + type + test + conformance
│   └── release.yml                  OIDC PyPI publish on release
├── pyproject.toml                   entry-point registration + [project.urls] Homepage
├── README.md
├── CHANGELOG.md
└── LICENSE
```

> **Tags and MCP exposure.** Every `@connector` and `@enumerator` takes a
> `tags=` argument. Tagging a connector with `"tool"` opts it into the MCP
> server's tool surface — that means the agent invokes it interactively,
> and its result must fit in a context window. Read
> [Phase 2 — Tags and MCP exposure](#phase-2-tags-and-mcp-exposure)
> before writing your first decorator.

---

## Phase 0 — Provider research

> **Core principle: docs lie. Test everything live before trusting it.**

Spend 30–60 minutes researching the provider before writing any connector
code. Skipping live exploration saves hours now and loses days later when
documented claims turn out to be wrong.

### 1. Documentation scan (15 min max)

- [ ] Find the official API documentation
- [ ] Identify the claimed protocol: REST, SDMX, GraphQL, bulk download
- [ ] Note the base URL and API version
- [ ] Check for an OpenAPI/Swagger spec
- [ ] Note what docs claim about: auth, rate limits, search, response formats

Do **not** trust any of the above. Every claim gets verified in step 2.

### 2. Authentication setup

A human has to do this before hitting any endpoint. For commercial APIs
this step is **mandatory** — skip it and every test below is invalid.

```bash
export MY_SOURCE_API_KEY="your-key-here"
```

Verify credentials load correctly with one authenticated request before
proceeding.

### 3. Live API exploration (30–45 min — the critical step)

Open a terminal. Use `curl` or `httpx` in a REPL:

```bash
curl -s "https://api.example.com/v1/series?id=CPI" | python -m json.tool
```

```python
import httpx, json
r = httpx.get("https://api.example.com/v1/series", params={"id": "CPI"})
print(r.status_code, r.headers.get("content-type"))
print(json.dumps(r.json(), indent=2)[:2000])
```

**Verify each documented endpoint:**

- [ ] Does the endpoint exist and respond?
- [ ] Does the response structure match the docs? (Field names, nesting,
      types — all frequently differ.)
- [ ] What pagination method? (offset/limit, cursor, `Link` headers.)
- [ ] What rate-limit signals? (`X-RateLimit-Remaining`, 429 bodies.)
- [ ] What nulls look like in actual responses (`null`, `"NaN"`, `"."`,
      empty string, or just missing keys)?

**Discover undocumented endpoints:**

- **Browser dev tools.** Open the provider's data-explorer UI, filter
  Network by XHR/Fetch, and watch what calls the frontend makes. This
  often reveals search, catalog, and filter endpoints that aren't in the
  public docs.
- **Common URL patterns.** `/search`, `/query`, `/series`, `/datasets`,
  `/metadata`, `/v2/`, `/sdmx/v2.1/dataflow/all/all/latest`.
- **SDMX discovery.** Even if the provider doesn't mention SDMX, try
  `{base_url}/sdmx/v2.1/dataflow/all/all/latest`. If it responds with
  XML, you've found an SDMX endpoint — hand the agency off to the
  `parsimony-sdmx` plugin (add the agency to its `ALL_AGENCIES` set if
  not already supported).

### 4. Search-capability tier

Classify the provider **from live testing**, not from docs:

| Tier | Capability | Catalog strategy |
|---|---|---|
| **1** | Native search endpoint that returns good results | Use directly as `@connector(tags=["tool"])` |
| **2** | Structured list endpoint (paginated, or SDMX DSD/JSON schema) | `@enumerator` → `Catalog.add_from_result` → `catalog.search` |
| **3** | Website browsable, no API search | Scrape or curate the catalog, then index |
| **4** | Bulk files only | Parse files into `@enumerator` output |

### 5. Document findings

Before writing code, write down what you learned — what differs from the
docs, what rate limits you actually observed, which endpoints are dead,
which shape the response takes. Future-you (and reviewers) will need it.

---

## Phase 1 — Scaffold the plugin

Use the [parsimony plugin template](https://github.com/ockham-sh/parsimony-plugin-template):

```bash
uvx cookiecutter gh:ockham-sh/parsimony-plugin-template
```

Answer the prompts (`provider_name`, `description`, author info). The
scaffold produces the structure shown at the top of this doc.

### pyproject.toml

```toml
[project]
name = "parsimony-<your-name>"
version = "0.1.0"
license = "Apache-2.0"
requires-python = ">=3.11"
dependencies = [
    "parsimony-core>=0.4,<0.5",
    "pydantic>=2.11,<3",
    "pandas>=2.3,<3",
    "httpx>=0.27,<1",
]

[project.urls]
Homepage = "https://your-provider.example"

[project.optional-dependencies]
dev = [
    "pytest>=9.0",
    "pytest-asyncio>=1.3",
    "respx>=0.22",
    "ruff>=0.15",
    "mypy>=1.10",
]

[project.entry-points."parsimony.providers"]
<your-name> = "parsimony_<your_name>"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["parsimony_<your_name>"]
```

The entry-point registration is what makes your plugin discoverable by
the kernel. Exactly one entry per provider module. `[project.urls] Homepage`
is what the kernel surfaces as `Provider.homepage`.

### Minimum `parsimony_<your_name>/__init__.py`

```python
from parsimony import Connectors
from parsimony_<your_name>.connectors import <your_name>_search, <your_name>_fetch

CONNECTORS = Connectors([<your_name>_search, <your_name>_fetch])
```

- `CONNECTORS` — **required**. The immutable `Connectors` collection
  containing every decorated function in this plugin.
- Per-connector env vars live on the `@connector(env={...})` decorator
  (see Phase 6). The consumer resolves them via `Connectors.bind_env()`.
- Provider metadata (homepage, version, description) lives in
  `pyproject.toml` (`[project.urls] Homepage`, `[project] description`).
  The kernel reads it on demand via `importlib.metadata`. There is no
  module-level `ENV_VARS`, `PROVIDER_METADATA`, or `__version__`.

See [Phase 8 — Catalog integration](#phase-8-catalog-integration) below
if your plugin ships catalog bundles.

---

## Phase 2 — Tags and MCP exposure

Tags control two things: (1) whether the connector is exposed as an
interactive MCP tool, and (2) which domain category it belongs to for
filtering and catalog organization.

```python
@connector(tags=["macro", "tool"])    # MCP tool + macro category
@connector(tags=["macro"])            # fetch-only, not an MCP tool
@connector(tags=["equity", "tool"])   # equity MCP tool
@enumerator(tags=["macro", "us"])     # US macro enumerator
```

- `"tool"` — marks connectors exposed as interactive MCP tools (search,
  discovery, screener). Fetch connectors typically omit `"tool"` because
  the agent invokes them programmatically after catalog discovery.
- Domain tags (`"macro"`, `"equity"`, `"us"`, `"global"`) support
  filtering and catalog organization.

**Rule of thumb:** if the agent calls it *interactively* to discover or
search data, add `"tool"`. If the agent calls it *programmatically*
(after finding what it needs via the catalog), omit `"tool"`.

---

## Phase 3 — Write the params models

One Pydantic model per connector function. The framework reads the JSON
Schema from the type annotation and uses it for LLM tool descriptions
via `to_llm()`.

```python
from typing import Annotated
from pydantic import BaseModel, Field, field_validator

class MySourceFetchParams(BaseModel):
    """Parameters for fetching a time series from MySource."""

    series_id: Annotated[str, "ns:my_source"] = Field(
        ..., description="Series identifier (e.g. CPI.TOTAL)"
    )
    start_date: str | None = Field(
        default=None, description="Start date (YYYY-MM-DD)"
    )
    end_date: str | None = Field(
        default=None, description="End date (YYYY-MM-DD)"
    )

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_id must be non-empty")
        return v
```

Conventions:

- `Field(...)` for required; `Field(default=...)` for optional.
- `description=` on **every** field — these appear verbatim in the
  agent's system prompt.
- `Annotated[str, "ns:my_source"]` — sentinel string telling the kernel
  this parameter is the entity code in the `my_source` catalog namespace.
  Replaces the older `Namespace("my_source")` annotation class.
- `@field_validator` for input sanitization.

### Aliasing reserved Python keywords

Some APIs use Python keywords as parameter names (`from`, `type`, `in`,
`class`). Use `alias=` with `populate_by_name=True` to give the field a
legal Python name while serializing with the API's name:

```python
from pydantic import ConfigDict

class MyParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_date: str | None = Field(
        default=None,
        alias="from",
        description="Start date ISO 8601. Use as from_date='2024-01-15'",
    )
    to_date: str | None = Field(
        default=None,
        alias="to",
        description="End date ISO 8601. Use as to_date='2024-12-31'",
    )
```

The `alias=` controls how the field serializes to the API query string;
the Python-facing name (`from_date`) is what callers and LLM agents use.
**Critically**, the `description=` must include `Use as from_date=`
— without it, agents attempt `from="2024-01-15"` and hit a `SyntaxError`.

### Sharing models across connectors

Share a param model across connectors only when the parameters are
**truly identical**. For example, FMP's `income_statement`,
`balance_sheet`, and `cash_flow_statement` all accept the same
`(symbol, period, limit)` — one model is correct. If the parameters
differ even slightly, use separate models.

---

## Phase 4 — Design the `OutputConfig`

`OutputConfig` declares the semantic meaning of each column. Four roles:

| Role | Purpose | Constraint |
|---|---|---|
| `KEY` | Entity identifier (series_id, ticker) | Exactly one. Namespace optional — defaults to catalog name. |
| `TITLE` | Human-readable name | Exactly one when KEY is present. |
| `DATA` | Observation values (date, value, price) | The actual data columns. |
| `METADATA` | Supplementary context (frequency, units) | Optional. |

```python
from parsimony import OutputConfig, Column, ColumnRole

ENUMERATE_OUTPUT = OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="my_source"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="category", role=ColumnRole.METADATA),
    Column(name="frequency", role=ColumnRole.METADATA),
])

FETCH_OUTPUT = OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY,
           param_key="series_id", namespace="my_source"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    Column(name="value", dtype="numeric", role=ColumnRole.DATA),
])
```

Column options:

- `dtype` — coercion hint: `"auto"`, `"datetime"`, `"date"`, `"numeric"`,
  `"timestamp"`, `"bool"`, `"str"`, or a pandas dtype string.
- `param_key` — seed this column with the value from a named param field
  (e.g. `param_key="series_id"` fills every row with the param value).
- `mapped_name` — rename the upstream column to match a canonical name.
- `exclude_from_llm_view` — hide the column from agent-facing tool
  schemas (METADATA only).
- `namespace` — catalog namespace for KEY columns. **Optional.** When
  omitted, `Catalog.add_from_result` uses the catalog's own `name` as
  the default.

Columns in the DataFrame not declared in `OutputConfig` automatically
become DATA columns.

### dtype reference

| dtype | Coercion pipeline | Expected input | Failure mode |
|---|---|---|---|
| `"auto"` | pandas infers | Any | No validation |
| `"timestamp"` | `pd.to_numeric(errors="coerce")` → scale ms→s if >1e11 → `pd.to_datetime(unit="s")` | Unix epoch seconds or milliseconds | `ParseError` if all values NaT |
| `"date"` | `pd.to_datetime(series).dt.normalize()` | ISO 8601 date string or epoch | Raises on unparseable |
| `"datetime"` | `pd.to_datetime(series)` | ISO 8601 datetime or epoch | Raises on unparseable |
| `"numeric"` | `pd.to_numeric(errors="coerce")` | Numeric string or number | `ParseError` if all NaN |
| `"bool"` | `.astype(bool)` | Truthy/falsy | `ParseError` on astype failure |
| `"str"` | `.astype(str)` | Any | Never fails |
| custom (e.g. `"category"`) | `.astype(dtype)` fallback | Must be a valid pandas dtype | `ParseError` on astype failure |

**`"timestamp"` vs `"date"` — the most common coercion trap:**

- Use `"timestamp"` when the API returns **unix epoch values** (integers
  like `1704067200` or `1704067200000`).
- Use `"date"` when the API returns **ISO 8601 date strings** (like
  `"2024-01-01"`).

Mixing them up produces all-NaT values and raises `ParseError`. The
error message names the failing column.

**Missing-data sentinels.** APIs use `"."`, `"None"` (string), `"-"`, or
empty strings. The `"numeric"` dtype handles these via
`pd.to_numeric(errors="coerce")`; `"date"` and `"datetime"` **crash**.
Replace sentinels with `None` in the row-building loop for date/datetime
columns.

### Multi-namespace providers

Some providers serve multiple asset classes under one API. When identifier
spaces are disjoint (equity ticker `AAPL` is meaningless in the crypto
endpoint, crypto pair `btcusd` is meaningless in equity), use separate
namespaces:

```python
# Equities
Column(name="ticker", role=ColumnRole.KEY, namespace="my_source_equity")
# Crypto
Column(name="ticker", role=ColumnRole.KEY, namespace="my_source_crypto")
# Forex
Column(name="ticker", role=ColumnRole.KEY, namespace="my_source_fx")
```

`Annotated[str, "ns:my_source_crypto"]` on parameter models restricts
which connectors accept which identifiers — preventing an agent from
passing a crypto pair to an equity endpoint.

### OutputConfig pitfalls

**Match actual response columns, not docs.** Column names in
`OutputConfig` must match what `pd.DataFrame(response.json())` actually
produces, not what documentation claims. Make a real API call and
inspect `df.columns` before defining the config.

**Missing columns log a warning.** `OutputConfig.build_table_result()`
matches declared columns against the DataFrame. A typo logs a WARNING
listing unmatched config columns and the available DataFrame columns; it
doesn't crash. Watch logs during development.

You can also assert column match in tests:

```python
assert not MY_OUTPUT.validate_columns(sample_df), (
    f"Unmatched: {MY_OUTPUT.validate_columns(sample_df)}"
)
```

### Per-resource OutputConfig mapping

When a single connector serves multiple resource types with different
schemas, use a mapping:

```python
_OUTPUT_MAP = {
    "filing_types": FILING_TYPES_OUTPUT,
    "countries": COUNTRIES_OUTPUT,
    "languages": GENERIC_OUTPUT,
}

output = _OUTPUT_MAP.get(params.resource, GENERIC_OUTPUT)
return output.build_table_result(df, provenance=..., params=params.model_dump())
```

---

## Phase 5 — HTTP client

Use `parsimony.transport.HttpClient`. It wraps `httpx.AsyncClient` and
redacts sensitive query-param values in structured logs (`api_key`,
`token`, `password`, anything ending `_token`, etc.).

```python
from parsimony.transport import HttpClient

_BASE_URL = "https://api.my-source.example.com/v1"

def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        _BASE_URL,
        default_params={"api_key": api_key},
        timeout=30.0,
    )
```

If your credential query-param name isn't in the default redaction list,
file an issue against the kernel to add it.

---

## Phase 6 — Write the connectors

```python
import pandas as pd
from parsimony import connector, enumerator, Result, Provenance
from parsimony.transport import HttpClient, map_http_error


@enumerator(
    output=ENUMERATE_OUTPUT,
    env={"api_key": "MY_SOURCE_API_KEY"},
    tags=["my_source"],
)
async def enumerate_my_source(
    params: MySourceEnumerateParams,
    *,
    api_key: str,
) -> pd.DataFrame:
    """Enumerate every series in the MySource catalog."""
    async with HttpClient(_BASE_URL, default_params={"api_key": api_key}) as http:
        try:
            response = await http.get("/series")
        except httpx.HTTPStatusError as exc:
            raise map_http_error(exc, provider="my_source", op_name="enumerate") from exc
    data = response.json()
    return pd.DataFrame(data.get("series", []))


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

The `env={"api_key": "MY_SOURCE_API_KEY"}` argument tells the kernel
which environment variable backs the `api_key` keyword-only dep. Both
decorators accept the same `env=` kwarg; multi-credential providers
(username + password, etc.) use one entry per dep.

The decorator wraps the returned DataFrame in a `Result` with the
provenance generated from the params model and the declared
`OutputConfig`. You return a DataFrame; the framework handles the rest.

### Error mapping

Every connector should funnel upstream HTTP failures through
`map_http_error`. The mapping:

- `401` / `403` → `UnauthorizedError`
- `402` → `PaymentRequiredError`
- `429` → `RateLimitError` (carries `retry_after: float`)
- anything else → `ProviderError`

For timeouts: `from parsimony.transport import map_timeout_error` and
wrap `httpx.TimeoutException` similarly.

Empty results are a signal, not an error shape. Raise `EmptyDataError`
when the upstream clearly returned "no data for this input" (an empty
list, `status: "no_data"`, etc.) rather than letting a zero-row DataFrame
propagate silently.

### Pagination

Document what the provider supports; choose one pattern:

**Offset/limit:**

```python
rows = []
offset = 0
while True:
    response = await http.get("/series", params={"limit": 100, "offset": offset})
    batch = response.json().get("series", [])
    if not batch:
        break
    rows.extend(batch)
    offset += 100
```

**Cursor:**

```python
rows = []
cursor = None
while True:
    params = {"limit": 100}
    if cursor:
        params["cursor"] = cursor
    response = await http.get("/series", params=params)
    payload = response.json()
    rows.extend(payload["data"])
    cursor = payload.get("next_cursor")
    if not cursor:
        break
```

**Link header:**

```python
url = "/series?limit=100"
while url:
    response = await http.get(url)
    rows.extend(response.json())
    url = _next_link(response.headers.get("link"))
```

For burst-heavy enumerators (fan-out enrichment, screener joins), use
`parsimony.transport.pooled_client` instead of `HttpClient` to share a
TCP connection pool.

---

## Phase 7 — Test

### Conformance (release-blocking)

```python
# tests/test_conformance.py
import parsimony_my_source
from parsimony.testing import assert_plugin_valid

def test_plugin_conforms() -> None:
    assert_plugin_valid(parsimony_my_source)
```

Or pytest-class style:

```python
from parsimony.testing import ProviderTestSuite
import parsimony_my_source

class TestMySource(ProviderTestSuite):
    module = parsimony_my_source
```

### Happy path + error mapping

Use `respx` to mock HTTP responses:

```python
import httpx
import pytest
import respx
from parsimony import UnauthorizedError, RateLimitError
from parsimony_my_source import CONNECTORS

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
    # Ensure the key doesn't leak into the exception message:
    assert "live-looking-key" not in str(exc_info.value)
```

**Required error tests** for any connector with an `api_key` / `token`
dep:

- 401 → `UnauthorizedError`, with an assertion that the key doesn't
  appear in the exception message.
- 429 → `RateLimitError`, same key-leak assertion.

### Run locally

```bash
pip install -e .[dev]
pytest tests/ -v
ruff check .
mypy parsimony_<your_name>/
```

All four must pass before you cut a release.

---

## Phase 8 — Catalog integration

If your plugin publishes catalog bundles (reusable Hugging Face FAISS
snapshots for agents to load), export `CATALOGS` on the module.

### Static `CATALOGS` (simple case)

When the namespace set is known at import time:

```python
from parsimony_my_source.connectors import my_source_enumerate

CATALOGS = [("my_source", my_source_enumerate)]
```

`parsimony publish --provider my_source --target 'hf://org/catalog-{namespace}'`
runs `my_source_enumerate`, ingests the result into a fresh `Catalog`,
and pushes it to `hf://org/catalog-my_source`.

### Dynamic `CATALOGS` (async generator)

When namespaces are discovered at build time (e.g. SDMX fans out across
live agencies / dataflows):

```python
from functools import partial
from typing import AsyncIterator, Awaitable, Callable

async def CATALOGS() -> AsyncIterator[tuple[str, Callable[[], Awaitable]]]:
    yield "my_source_datasets", enumerate_datasets
    async for family in _fetch_families():
        ns = f"my_source_family_{family.code.lower()}"
        yield ns, partial(enumerate_family, family_code=family.code)
```

### `RESOLVE_CATALOG` (optional reverse lookup)

For large dynamic `CATALOGS` generators, plugins can supply a reverse
lookup so `--only NS` can build a single catalog without walking the
generator:

```python
def RESOLVE_CATALOG(namespace: str) -> Callable | None:
    if namespace == "my_source_datasets":
        return enumerate_datasets
    prefix = "my_source_family_"
    if namespace.startswith(prefix):
        family_code = namespace.removeprefix(prefix).upper()
        return partial(enumerate_family, family_code=family_code)
    return None
```

When the user runs `parsimony publish --only my_source_family_gdp`, the
publisher calls `RESOLVE_CATALOG("my_source_family_gdp")` first and
skips the `CATALOGS` walk entirely if the resolver returns a callable.

See [`contract.md`](contract.md) §6 for the full spec.

---

## Publishing the plugin

1. Configure [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/)
   for your GitHub repo — one-time setup.
2. Copy the release workflow from an existing plugin
   (e.g. `parsimony-fred/.github/workflows/release.yml`) into yours.
3. Tag a release:
   ```bash
   git tag v0.1.0
   git push --tags
   ```
   GitHub Actions publishes to PyPI via OIDC trusted publishing — no
   tokens in GitHub secrets.
4. Verify discovery in a fresh venv:
   ```bash
   pip install parsimony-core parsimony-<yourname>
   parsimony list
   ```
   Your plugin should appear. `parsimony list --strict` runs the
   conformance suite and exits non-zero on any failure — the bar every
   release must clear.

---

## Checklist before cutting `v0.1.0`

- [ ] `parsimony_<your_name>` module exports `CONNECTORS`; optionally
      `CATALOGS`, `RESOLVE_CATALOG`.
- [ ] Per-connector `@connector(env={...})` declarations cover every
      required keyword-only dep.
- [ ] `[project.urls] Homepage` set in `pyproject.toml`.
- [ ] Entry point registered in `pyproject.toml` under
      `parsimony.providers`.
- [ ] `parsimony.testing.assert_plugin_valid(module)` passes.
- [ ] Tool-tagged connectors have ≥40-char descriptions.
- [ ] Unit tests cover happy path + at least one error path (401, 429, empty).
- [ ] `parsimony list --strict` exits 0.
- [ ] `ruff check` + `mypy` green.
- [ ] `README.md` documents install, setup, example usage.
- [ ] `LICENSE` file present (Apache-2.0 for official plugins).
- [ ] CI workflows green on main.

---

## When to create a per-provider vs protocol-grouped plugin

- **Per-provider (`parsimony-<provider>`)** when the API is bespoke.
  **Default.**
- **Protocol-grouped (`parsimony-<protocol>`)** only when multiple
  providers share a wire protocol, >60% of implementation, dependency
  tree, and maintenance cadence. Examples: `parsimony-sdmx`,
  `parsimony-pxweb`.
