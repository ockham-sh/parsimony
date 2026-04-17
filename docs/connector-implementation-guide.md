# Connector Implementation Guide

How to build a parsimony connector from scratch. Covers provider research,
the structural skeleton, catalog integration, registration, and practical
patterns from real implementations. For wrapping internal data sources
(Postgres, Snowflake, S3) instead of public APIs, see
[internal-connectors.md](internal-connectors.md).

> **Key concept — Tags and MCP exposure:** Every `@connector` and `@enumerator`
> takes a `tags=` argument that controls whether it appears as an MCP tool and
> how it is categorized. Read [Tags and MCP Exposure](#tags-and-mcp-exposure)
> before writing your first decorator.

---

## Quick Start: Prompt Template

This guide is designed to be read by the agent, not memorized by you. Copy the template below, fill in the blanks, and paste it as your prompt:

```
Implement a {provider_name} connector in @parsimony/parsimony/connectors/ .

Read @parsimony/docs/connector-implementation-guide.md carefully — it is your bible for structure, patterns, and registration.

Provider details:
- Auth: {env var(s) e.g. X_API_KEY, or X_USER/X_PASSWORD, or "no auth"}
- Docs: {https://provider.com/docs/}
- Plan tier: {free / basic / pro}
- Connectors I want: {e.g. "search + company profile + historical prices"}

The docs are large — delegate reading them to dedicated agents that report back with exactly what you need (endpoint URL, method, params, response
shape, quirks, rate limits, tier restrictions). Don't bloat your own context.
```

Adapt freely — drop lines that don't apply, add specifics you already know
(e.g. "responses come as CSV, not JSON" or "pagination uses cursor tokens").

---

## Phase 0: Provider Research

> **Core principle: docs lie. Test everything live before trusting it.**

Before writing any connector code, spend 30-60 minutes researching the provider.
The live exploration step is the bulk of the time. Do NOT skip it -- it saves
hours of rework when docs turn out to be wrong.

### 1. Documentation Scan (15 min max)

- [ ] Find official API documentation
- [ ] Identify claimed protocol: REST, SDMX, GraphQL, bulk download, scrape-only
- [ ] Note base URL and API version
- [ ] Check for an OpenAPI/Swagger spec
- [ ] Note what docs claim about: auth, rate limits, search, response formats

Do NOT trust any of the above. Every claim gets verified in step 2.

**Where to find documentation:**
- The provider's website. Start there -- look for "API", "Developers", "Data
  access", "Bulk download", or "Open data" sections. Many central banks and
  statistical agencies bury their API docs under "Statistics" or "Publications".
- Official API docs page (often under `/developers`, `/api`, `/data/api`)
- OpenAPI/Swagger spec (try `{base_url}/swagger.json`, `{base_url}/openapi.json`,
  `{base_url}/v2/api-docs`). If a spec exists, it's usually more accurate than the
  prose docs and you can import it into Postman or generate a client.
- GitHub repos -- some providers publish SDKs that reveal undocumented endpoints
  in their source code
- For central banks and statistical agencies: check if they mention SDMX
  compliance. Many publish SDMX endpoints they barely document.

### 2. Authentication Setup (before any live testing)

> **A human has to do this before hitting any endpoint.** Without credentials you can't test
> commercial APIs at all, and you'll get misleading results from public APIs
> that silently degrade unauthenticated requests.

- [ ] Obtain credentials (register, generate key, etc.)
- [ ] Set the environment variable locally:
  ```bash
  export MY_SOURCE_API_KEY="your-key-here"
  ```
- [ ] Verify credentials load correctly by running one authenticated request
  before proceeding to full exploration.

For commercial providers this step is **mandatory** — skip it and every test result below is invalid.


### 3. Live API Exploration (30-45 min -- the critical step)

This is where you actually learn how the API works. Documentation is a starting
point, not the truth. Open a terminal and start hitting endpoints.

**Start with `curl` or a Python REPL:**

```bash
# Test the base URL -- is it alive?
curl -s "https://api.example.com/v1" | python -m json.tool

# Test a documented endpoint -- does the response match the docs?
curl -s "https://api.example.com/v1/series?id=CPI" | python -m json.tool

# Test without auth -- does it actually require a key?
curl -s "https://api.example.com/v1/series" -w "\n%{http_code}\n"
```

Or use `httpx` in a Python REPL for more control:

```python
import httpx, json
r = httpx.get("https://api.example.com/v1/series", params={"id": "CPI"})
print(r.status_code, r.headers.get("content-type"))
print(json.dumps(r.json(), indent=2)[:2000])  # first 2K chars
```

**Discover undocumented endpoints.** Many providers have more than their docs
show. Techniques:

- **Browser dev tools.** Open the provider's website, open Network tab (filter
  by XHR/Fetch), and navigate their data explorer or search UI. Watch what API
  calls the frontend makes. This often reveals search endpoints, catalog
  endpoints, and filtering parameters that aren't in the public docs.
- **Common URL patterns.** Try these against the base URL -- you'd be surprised
  how often they work:
  - `/search`, `/query`, `/find` -- search endpoints
  - `/series`, `/datasets`, `/catalog`, `/list`, `/dataflow` -- catalog endpoints
  - `/metadata`, `/structure`, `/schema` -- structural metadata
  - `/v2/`, `/v3/` -- newer API versions that may not be documented yet
- **SDMX discovery.** Even if the provider doesn't mention SDMX, try:
  `{base_url}/sdmx/v2.1/dataflow/all/all/latest`. If it responds with XML,
  you've found an SDMX endpoint and can use the existing `sdmx.py` infrastructure.
- **robots.txt and sitemap.** `{base_url}/robots.txt` sometimes reveals API paths.

**Verify documented endpoints actually work:**

- [ ] Hit every documented endpoint. Many are dead, moved, or behave differently
  than described.
- [ ] Compare response structure to docs. Field names, nesting, types -- all
  frequently differ. The response you get back is what your connector must handle.
- [ ] Check response headers for undocumented info: rate limit counters
  (`X-RateLimit-Remaining`), pagination links (`Link`), API version (`X-API-Version`).

**Test search and filtering:**

- [ ] Does the search endpoint exist and return useful results?
- [ ] What filtering parameters are supported? Try passing parameters from the
  docs and see which actually filter. Many APIs document filters they don't
  implement, or implement filters they don't document.
- [ ] How does search relevance work? Full-text? Prefix match? Exact only?
- [ ] **Verify every enum value live.** Docs often list values like `"stocks"`
  but the API requires `"common_stock"`. A 422 Unprocessable Entity response is
  the signal — inspect the error body for the valid values list, then test each
  one. Use these verified values in your `Literal[...]` type annotations.

**Test catalog/bulk access:**

- [ ] Can you get a complete list of available series/datasets? How many entities?
- [ ] Pagination: what method? (`offset`/`limit`, `page`/`per_page`, cursor-based,
  `Link` header). What's the max page size? What happens past the last page?
- [ ] How long does a full catalog download take? This determines whether your
  `@enumerator` is practical.

**Test data fetching:**

- [ ] **Fetch real data** for at least 3 different series. Validate:
  - Date formats (ISO 8601? Unix timestamps? Mixed?)
  - Value types (string numbers? actual floats? `"."` for missing?)
  - Null handling (`null`, `"NaN"`, `"."`, empty string, missing key?)
  - Timezone behavior (UTC? local? naive?)
- [ ] These details directly determine your `OutputConfig` dtypes and DataFrame
  cleanup code. Getting them wrong means broken data at runtime.

**Test auth and rate limits:**

- [ ] Try requests with and without auth. Some APIs work fine without keys but
  with lower rate limits. Some require keys but don't document it.
- [ ] Check if different endpoints have different rate limits (common for search
  vs fetch).
- [ ] For **public/open data** providers: rate limits are often undocumented or
  inconsistently enforced — send a small burst (5-10 requests) to observe actual
  throttling behavior.
- [ ] For **commercial providers**: documented rate limits are reliable — trust
  them and do NOT waste API calls testing limits empirically. Just record what
  the docs say.

### 4. Authentication (verify after setup)

- [ ] Method: API key, OAuth, none, guest credentials
- [ ] How to obtain: instant registration, approval queue, paid only
- [ ] Where to pass: query param, header, POST body, cookie
- [ ] **Verified live**: does auth actually change behavior (different data, higher limits, etc.)?

### 5. Search Capability Assessment

Classify into a tier **based on live testing, not docs**:

| Tier | Capability | Catalog Strategy |
|------|-----------|-----------------|
| **1** | Native search endpoint -- tested, returns good results | Use directly as a `@connector(tags=["tool"])` |
| **2** | Structured metadata available (list endpoint, SDMX DSD, JSON schema) | Build `@enumerator` → Catalog index → `catalog.search()` |
| **3** | Website browsable, no API search -- confirmed no hidden API | Scrape or manually curate catalog, then index |
| **4** | Bulk files only -- confirmed no API alternative | Parse files into `@enumerator` output |

See [Building a Search Catalog](#building-a-search-catalog-tier-2-4) for the
full indexing workflow for Tier 2-4 providers.

### 6. Data Coverage

- [ ] Topics: GDP, inflation, labor, trade, monetary, financial, etc.
- [ ] Geography: EU, single country, global
- [ ] Time depth: how far back? Update frequency?
- [ ] Granularity: datasets only, or individual series?

### 7. Technical Constraints (verified, not documented)

- [ ] Rate limits (tested for public APIs; from docs for commercial)
- [ ] Actual response format (fetched and inspected)
- [ ] Pagination method (tested with real data)
- [ ] Maximum response size (tested)
- [ ] Quirks discovered during testing

### 8. Existing Libraries

- [ ] Check PyPI for Python client libraries
- [ ] Check if `sdmx1` supports this agency (if SDMX)
- [ ] Evaluate: use library vs build from scratch

A connector doesn't need a raw HTTP API. If a good Python client library
exists, wrap it with `@connector` the same way -- the library becomes
the transport layer instead of `HttpClient`. See
[internal-connectors.md](internal-connectors.md) for the pattern of wrapping
sync SDKs with `asyncio.to_thread()` and injecting clients via `bind_deps`.

### 9. Document Findings

Add a section to `PROVIDERS.md` (or create it) using this template:

```markdown
### {Provider Name}

**{Full name}** -- {Country/Region}

- **Protocol**: {REST API / SDMX / Python SDK / etc.}
- **Base URL**: `{url}` -- verified {alive/moved/dead} on {date}
- **Auth**: {method} -- verified: {works without auth / key required}
- **Search tier**: {1-4} (based on live testing)
- **Coverage**: {topics}
- **Rate limits**: {X req/sec} — {source: tested empirically / from official docs (commercial)}
- **Docs**: [{link text}]({url}) -- accuracy: {good / partially wrong / mostly wrong}
- **Existing libraries**: {PyPI packages, quality}

#### Surprises & Deviations from Docs
{What was different from what docs claimed -- most valuable section}
```

---

## Anatomy of a Connector Module

Every connector module in `parsimony/connectors/` follows the same structure:

```
parsimony/connectors/my_source.py
├── ENV_VARS dict           # maps dependency names → env var names (only if credentials needed)
├── Pydantic params models  # one per connector function
├── OutputConfig schemas    # semantic column declarations
├── _make_http() helper     # HttpClient factory (if needed)
├── @connector functions    # async fetch/search operations
├── @enumerator functions   # async catalog population
└── CONNECTORS              # all connectors (fetch + enumerator + search)
```

Reference implementations:

| File | Pattern |
|------|---------|
| `fred.py` | REST API with search + fetch + paginated enumerator |
| `treasury.py` | No-auth public data |
| `bls.py` | Optional API key, POST-based auth |
| `fmp.py` | HTTP status → typed exception mapping |
| `fmp_screener.py` | Concurrent enrichment with asyncio.Semaphore |
| `sdmx.py` | Multi-step discovery (list → DSD → codelist → keys → fetch) |
| `sec_edgar.py` | Third-party SDK integration, duck-typed DataFrame coercion |
| `alpha_vantage.py` | 200-with-errors, unified Literal-param connectors, mixed JSON/CSV, response key normalization |

Aim for < 400 lines per connector module. If a provider has 15+ connectors,
500-600 lines is fine for a single-provider module. Don't split into multiple
files unless you have genuinely separate concerns (e.g., separate SDK clients).

---

## Tags and MCP Exposure

Tags control categorization and visibility. Every `@connector` and `@enumerator`
decorator accepts a `tags=` list that determines two things: (1) whether the
function is exposed as an interactive MCP tool, and (2) which domain category
it belongs to for filtering and catalog organization.

```python
@connector(tags=["macro", "tool"])    # MCP tool + macro category
@connector(tags=["macro"])            # fetch-only, not an MCP search tool
@connector(tags=["equity", "tool"])   # equity MCP tool
@enumerator(tags=["macro", "us"])     # US macro enumerator
```

- `"tool"` -- marks connectors exposed as interactive MCP tools (search, discovery,
  screener). Fetch connectors typically omit `"tool"` because the agent invokes
  them programmatically after catalog discovery.
- Domain tags (`"macro"`, `"equity"`, `"us"`, `"global"`) are for filtering and
  organization. Choose the most specific applicable domain.

**Rule of thumb:** if the agent needs to *call it interactively* to discover or
search data, add `"tool"`. If the agent calls it *programmatically* after finding
what it needs via the catalog, omit `"tool"`.

---

## Step 1: Create the Module

```python
"""My Data Source: fetch + catalog enumeration.

API docs: https://api.my-source.example.com/docs
Authentication: API key required.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from parsimony.connector import (
    Connectors,
    EmptyDataError,
    Namespace,
    connector,
    enumerator,
)
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient
```

### ENV_VARS

Modules with credentials export an `ENV_VARS` dict mapping dependency parameter
names to environment variable names. The factory in `__init__.py` reads this to
wire credentials at startup via `_bind_optional_deps` or `_bind_required_deps`.

```python
# Requires an API key:
ENV_VARS: dict[str, str] = {"api_key": "MY_SOURCE_API_KEY"}

# Multiple credentials:
ENV_VARS: dict[str, str] = {"username": "MY_USER", "password": "MY_PASS"}
```

Public APIs with no credentials do **not** need `ENV_VARS` — the factory adds
them directly with `result + my_source.CONNECTORS`.

---

## Step 2: Pydantic Params Models

One model per connector function. The framework infers the JSON Schema from the
type annotation and uses it for LLM tool descriptions via `to_llm()`.

```python
class MySourceFetchParams(BaseModel):
    """Parameters for fetching time series from MySource."""

    series_id: Annotated[str, Namespace("my_source")] = Field(
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


class MySourceEnumerateParams(BaseModel):
    """No parameters needed -- enumerates the full catalog."""

    pass
```

Conventions:
- `Field(...)` for required, `Field(default=...)` for optional.
- `description=` on every field -- these appear verbatim in the agent's system prompt.
- `Annotated[str, Namespace("my_source")]` on the primary key to link it to the catalog.
- `@field_validator` for input sanitization (strip whitespace, validate formats).

**Aliasing reserved Python keywords.** Some APIs use Python keywords as
parameter names (`from`, `type`, `in`, `class`). Use `alias=` with
`populate_by_name=True` to give the field a legal Python name while serialising
with the API's name. Critically, the `description=` must spell out the Python
name explicitly, because an LLM agent will otherwise guess the alias, which is a
syntax error:

```python
class MyParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_date: str | None = Field(
        default=None,
        alias="from",
        description="Start date ISO 8601 e.g. 2024-01-15. Use as from_date='2024-01-15'",
    )
    to_date: str | None = Field(
        default=None,
        alias="to",
        description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'",
    )
```

The `alias=` controls how the field serialises to the API query string; the
Python-facing name (e.g. `from_date`) is what callers and agents use. Without
the `Use as from_date=` hint in the description, agents will attempt
`from="2024-01-15"` and get a `SyntaxError`.

**When to share param models:** Share a model across connectors only when the
parameters are truly identical. For example, `income_statement`, `balance_sheet`,
and `cashflow_statement` all accept the same `symbol, period, limit` fields --
one `FinancialStatementParams` model is correct. If the parameters differ even
slightly, use separate models.

---

## Step 3: OutputConfig Schemas

`OutputConfig` declares the semantic meaning of each column. Four roles:

| Role | Purpose | Constraint |
|------|---------|------------|
| `KEY` | Entity identifier (series_id, ticker) | Exactly one. Must set `namespace=`. |
| `TITLE` | Human-readable name | Exactly one when KEY is present. |
| `DATA` | Observation values (date, value, price) | The actual data columns. |
| `METADATA` | Supplementary info (frequency, units) | Optional context columns. |

```python
ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="my_source"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY,
               param_key="series_id", namespace="my_source"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)
```

Column options:
- `dtype` -- coercion: `"auto"`, `"datetime"`, `"date"`, `"numeric"`, `"timestamp"`, `"bool"`.
- `param_key` -- inject a param value as a constant column (e.g. fill `series_id` from params).
- `mapped_name` -- rename the column in output.
- `exclude_from_llm_view` -- hide from agent descriptions (METADATA only, not DATA/TITLE).
- `namespace` -- catalog namespace (KEY columns only).

Columns in the DataFrame not declared in OutputConfig automatically become DATA columns.

### Multi-Namespace Providers

Some providers serve multiple asset classes (equities, crypto, forex) under a
single API. When the identifier spaces are disjoint — an equity ticker like
`AAPL` is meaningless in the crypto endpoint, and a crypto pair like `btcusd`
is meaningless in the equity endpoint — use **separate namespaces** per asset
class:

```python
# Equities
Column(name="ticker", role=ColumnRole.KEY, namespace="my_source_equity")
# Crypto
Column(name="ticker", role=ColumnRole.KEY, namespace="my_source_crypto")
# Forex
Column(name="ticker", role=ColumnRole.KEY, namespace="my_source_fx")
```

Each namespace indexes independently in the catalog. `Annotated[str, Namespace("my_source_crypto")]`
on parameter models restricts which connectors accept which identifiers, preventing
an agent from passing a crypto pair to an equity endpoint.

### OutputConfig Pitfalls

**Match actual response columns, not API docs.** Column names in OutputConfig must
match what `pd.DataFrame(response.json())` actually produces, not what the
documentation says the response *should* contain. Make a real API call and inspect
`df.columns` before defining the config. Common mismatches:

- A list endpoint returns `country_code` (flat string), but the detail endpoint
  returns `country.name` (nested object that `json_normalize` flattens differently).
- Optional nested objects: `sector.name` only exists when the response includes a
  `sector` object. If it's `null`, the column won't exist at all.

**Missing columns trigger a warning.** `build_table_result()` matches config
columns against the DataFrame. A wrong column name doesn't crash -- but it logs
a `WARNING` listing the unmatched config columns and the available DataFrame
columns. Watch your logs during development to catch typos early.

You can also validate column names explicitly in tests:

```python
assert not MY_OUTPUT.validate_columns(sample_df), (
    f"Unmatched: {MY_OUTPUT.validate_columns(sample_df)}"
)
```

**Per-resource OutputConfig mapping.** When a single connector serves multiple
resource types with different schemas, use a mapping dict:

```python
_OUTPUT_MAP = {
    "filing_types": FILING_TYPES_OUTPUT,
    "countries": COUNTRIES_OUTPUT,
    "languages": GENERIC_OUTPUT,
}

output = _OUTPUT_MAP.get(params.resource, GENERIC_OUTPUT)
return output.build_table_result(df, provenance=...)
```

### OutputConfig dtype reference

The `dtype` field on a `Column` controls how raw API values are coerced before
the column is stored in the result DataFrame. Choose `dtype` based on what the
API actually returns, not what you would prefer to store.

| dtype | Coercion pipeline | Expected input | Failure mode |
|-------|------------------|----------------|--------------|
| `"auto"` | No coercion — pandas infers dtype | Any | No validation |
| `"timestamp"` | `pd.to_numeric(errors="coerce")` → scale ms→s if value >1e11 → `pd.to_datetime(unit="s")` | Unix epoch seconds or milliseconds (int or float) | `ParseError` if all values are NaT |
| `"date"` | `pd.to_datetime(series).dt.normalize()` | ISO 8601 date string or unix epoch | Raises on unparseable strings |
| `"datetime"` | `pd.to_datetime(series)` | ISO 8601 datetime string or unix timestamp | Raises on unparseable strings |
| `"numeric"` | `pd.to_numeric(errors="coerce")` | Numeric string or number | `ParseError` if all values are NaN |
| `"bool"` | `.astype(bool)` | Truthy/falsy values | `ParseError` if `astype` raises |
| `"str"` | `.astype(str)` | Any | Never fails |
| custom (e.g. `"category"`) | `series.astype(column.dtype)` via pandas fallback | Must be a valid pandas dtype string | `ParseError` if `astype` raises |

**`"timestamp"` vs `"date"` — the most common coercion trap:**

- Use `"timestamp"` when the API returns **unix epoch values** (integers like `1704067200` or `1704067200000`).
- Use `"date"` when the API returns **ISO 8601 date strings** (like `"2024-01-01"`).

Mixing them up produces all-NaT values and raises `ParseError`. The error message
will tell you which column failed and what input format is expected — but it will
never include raw sample values from the response.

**Partial NaN/NaT is expected.** The `ParseError` guard only triggers on *total*
coercion failure (100% null result from a non-null input). If some values fail to
coerce (e.g. a `"numeric"` column where 5% of rows are `"N/A"`), coercion silently
converts those to `NaN` — which is normal pandas behaviour for `errors="coerce"`.

**Missing data sentinels.** APIs use `"."`, `"None"` (string), `"-"`, or empty
strings for missing values. The `"numeric"` dtype handles these automatically
via `pd.to_numeric(errors="coerce")`. However, `"date"` and `"datetime"` dtypes
**will crash** on these sentinels. Replace them with `None` in the row-building
loop for any column that uses date coercion.

---

## Step 4: HTTP Client

Extract a `_make_http()` factory. `HttpClient` handles logging with automatic
credential redaction (any param named `api_key`, `token`, etc. is replaced
with `***REDACTED***` in logs).

```python
_BASE_URL = "https://api.my-source.example.com/v1"


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        _BASE_URL,
        query_params={"api_key": api_key, "format": "json"},
    )
```

For header-based auth:
```python
def _make_http(api_key: str) -> HttpClient:
    return HttpClient(_BASE_URL, headers={"Authorization": f"Bearer {api_key}"})
```

---

## Step 5: Implement the Fetch Connector

```python
@connector(output=FETCH_OUTPUT, tags=["macro"])
async def my_source_fetch(params: MySourceFetchParams, *, api_key: str) -> Result:
    """Fetch time series observations from MySource by series_id.

    Returns date + value with series metadata.
    """
    http = _make_http(api_key)

    req_params: dict[str, Any] = {"series_id": params.series_id}
    if params.start_date:
        req_params["start_date"] = params.start_date
    if params.end_date:
        req_params["end_date"] = params.end_date

    response = await http.request("GET", "/observations", params=req_params)
    response.raise_for_status()
    body = response.json()

    observations = body.get("observations", [])
    if not observations:
        raise EmptyDataError(
            provider="my_source",
            message=f"No data for series: {params.series_id}",
        )

    df = pd.DataFrame(observations)
    df["series_id"] = params.series_id
    df["title"] = body.get("series_name", params.series_id)

    return Result.from_dataframe(
        df,
        Provenance(
            source="my_source",
            params={"series_id": params.series_id},
        ),
    )
```

Rules:
- **Must be `async`.**
- **Docstring is required** -- it becomes the LLM-facing description via `to_llm()`.
- Dependencies (API keys) are keyword-only params after `*`, bound at startup via `bind_deps()`.
- Use typed exceptions (see below), not generic `ValueError`.
- The function can return a `pd.DataFrame`, a `Result`, or `Result.from_dataframe()`.
  When `output=` is set on the decorator and the return is a DataFrame, the framework
  wraps it as a `SemanticTableResult` automatically.

### Flattening Nested Responses

Some APIs nest data arrays inside parent objects instead of returning a flat
list. For example, a crypto endpoint might return:

```json
[
  {"ticker": "btcusd", "priceData": [{"date": "2024-01-01", "close": 44208}, ...]},
  {"ticker": "ethusd", "priceData": [{"date": "2024-01-01", "close": 2281}, ...]}
]
```

You can't pass this directly to `pd.DataFrame()`. Loop through the outer list,
extract parent-level fields, and merge them into each nested row:

```python
all_rows: list[dict] = []
for entry in data:
    for p in entry.get("priceData", []):
        all_rows.append({
            "ticker": entry.get("ticker", ""),       # parent field → KEY
            "exchange": entry.get("exchange", ""),    # parent field → METADATA
            "date": p.get("date"),                    # nested field → DATA
            "close": p.get("close"),                  # nested field → DATA
        })
df = pd.DataFrame(all_rows)
```

Parent-level fields typically become KEY or METADATA columns (the identifier and
context that applies to every nested row), while nested fields become DATA
columns (the actual observations). Drop parent fields that are redundant with
other columns already in the row — only carry fields that add information.

### Docstrings: Tier Annotations and Access Notes

The docstring is injected verbatim into the agent's system prompt via `to_llm()`.
For providers with tiered pricing, **prefix the docstring with the minimum tier
in brackets** using the provider's own tier names. This tells the agent which
plan is required before it attempts the call, preventing wasted tool invocations
that return `PaymentRequiredError`.

```python
@connector(output=QUOTE_OUTPUT, tags=["equity", "tool"])
async def fmp_quotes(params: FmpSymbolsParams, *, api_key: str) -> Result:
    """[Starter+] Fetch real-time quotes for one or more symbols.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """

@connector(output=ESTIMATES_OUTPUT, tags=["equity"])
async def fmp_analyst_estimates(params: FmpAnalystEstimatesParams, *, api_key: str) -> Result:
    """[Professional+] Fetch forward analyst consensus estimates: revenue,
    EBITDA, net income, EPS low/avg/high plus analyst coverage counts.
    """
```

After the tier prefix, include notes about what changes across tiers
(e.g. "Demo: 3 symbols. Starter+: all symbols") so the agent understands
degraded behavior on lower tiers rather than just failing.

No need to list return columns in the docstring -- the `OutputConfig` column
names are appended automatically by `to_llm()`. Focus the docstring on what
the connector does, tier requirements, and workflow chaining hints.

### Docstrings: Workflow Chaining

When a provider has multiple connectors that form a workflow, tell the user
(or agent) exactly which connector to call next and how to pass identifiers:

```python
@connector(output=COMPANIES_OUTPUT, tags=["equity", "tool"])
async def my_source_search(params: SearchParams, *, api_key: str) -> Result:
    """Search companies by name, country, or industry.

    Use the company ID with my_source_filings(company=id) to find filings,
    my_source_profile(id=id) for the full profile.
    Use my_source_industries to discover valid industry codes for the sector filter.
    """
```

Without chaining hints, the user must guess the workflow. Generic docstrings
like `"""Search companies."""` provide no guidance on what to do with the results.

Reference/lookup connectors are often missed unless you explain when to use them:

```python
"""List reference data: filing types, categories, countries.
Use filing type codes in my_source_filings(types=...).
Use country codes in my_source_search(countries=...)."""
```

### Result Types

The `result_type` parameter on `@connector` is a free-form string hint that gets
appended to the `to_llm()` output. It tells callers what `.data` contains so
they don't blindly call `.head()` on a string or iterate over a dict. The
default is `"dataframe"`.

```python
@connector(tags=["my_source"], result_type="text")
async def my_source_document(params: DocumentParams, *, api_key: str) -> Result:
    """Retrieve document as markdown text."""
    return Result(data=markdown_text, provenance=...)

@connector(tags=["my_source"], result_type="dict")
async def my_source_metadata(params: MetadataParams, *, api_key: str) -> Result:
    """Fetch metadata as a structured dict."""
    return Result(data={"key": "value", ...}, provenance=...)
```

Use whatever string describes the actual return type. Common values: `"dataframe"`
(default), `"text"`, `"dict"`, `"list"`.

---

## Step 6: Implement the Enumerator

Enumerators populate the searchable catalog. They return a DataFrame of entities
(KEY + TITLE + optional METADATA), no observation DATA.

```python
@enumerator(output=ENUMERATE_OUTPUT, tags=["macro"])
async def enumerate_my_source(params: MySourceEnumerateParams) -> pd.DataFrame:
    """Enumerate all available series from MySource for catalog indexing."""
    import httpx

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(f"{_BASE_URL}/series")
        resp.raise_for_status()
        data = resp.json().get("series", [])

    rows = [
        {
            "series_id": s["id"],
            "title": s["name"],
            "category": s.get("category", ""),
            "frequency": s.get("frequency", ""),
        }
        for s in data
        if s.get("id")
    ]

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_id", "title", "category", "frequency"]
    )
```

Enumerator rules:
- Return an empty DataFrame with the correct columns when no data is found. Never raise.
- For large catalogs, paginate and add `await asyncio.sleep()` between pages.
- `httpx.AsyncClient` directly is fine when the enumerator needs no credential management.
- The `@enumerator` decorator validates the OutputConfig at import time: exactly one KEY
  with `namespace=`, exactly one TITLE, no DATA columns.

### Static File Enumerators

Some providers publish their catalog as a downloadable file (CSV, ZIP, Excel)
from a CDN rather than a paginated REST endpoint. The enumerator downloads and
parses the file in memory:

```python
@enumerator(output=ENUMERATE_OUTPUT, tags=["equities"])
async def enumerate_my_source(params: MySourceEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate tickers from MySource's static catalog file."""
    import csv
    import io
    import zipfile

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get("https://media.my-source.example.com/catalog.zip")
        resp.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    text = zf.read(zf.namelist()[0]).decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    rows = [
        {"series_id": row["id"], "title": row.get("name", "")}
        for row in reader
        if row.get("id")
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["series_id", "title"])
```

Key differences from API-based enumerators:
- **Longer timeouts** (60-120s) — file downloads are slow.
- **`follow_redirects=True`** — CDNs often redirect.
- **No rate-limit handling** — static files are served from CDNs, not rate-limited APIs.
- **In-memory parsing** — use `io.BytesIO` / `io.StringIO` + stdlib `zipfile`, `csv`,
  or `pandas.read_csv()`. Avoid writing temp files.
- **Refresh at most once per day** — these are static snapshots, not live data.

---

## Step 7: Export the Bundle

```python
CONNECTORS = Connectors([my_source_search, my_source_fetch, enumerate_my_source])
```

Every connector module must export:

| Name | Contents | Used by |
|------|----------|---------|
| `ENV_VARS` | `dict[str, str]` mapping dep names → env vars | Provider registry (only for modules with credentials) |
| `CONNECTORS` | All connectors (search + fetch + enumerator) | Factory, MCP server, catalog builder, integration tests |

Tag search/discovery connectors with `tags=["tool"]` so downstream consumers
(MCP server, terminal agent) can filter them from the full set.

---

## Step 8: Register in the Provider Registry

Add one entry to the `PROVIDERS` tuple in `parsimony/connectors/__init__.py`:

```python
PROVIDERS: tuple[ProviderSpec, ...] = (
    # ...existing entries...
    ProviderSpec("parsimony.connectors.my_source"),            # optional (skipped if key absent)
    ProviderSpec("parsimony.connectors.my_source", required=True),  # required (raises if key absent)
    # ...
)
```

That's it. The factory reads `CONNECTORS` and `ENV_VARS` from your module
automatically. No other wiring needed.

---

## Typed Exception Hierarchy

Always use the framework's typed exceptions. They carry structured fields so callers
handle errors programmatically, and the agent receives clear messages instead of
raw tracebacks.

```python
from parsimony.connector import (
    ConnectorError,       # base -- don't raise directly
    EmptyDataError,       # HTTP 200 but no results
    UnauthorizedError,    # HTTP 401/403 -- bad credentials
    PaymentRequiredError, # HTTP 402 -- plan too low
    RateLimitError,       # HTTP 429 -- burst or quota
    ProviderError,        # HTTP 5xx or unexpected status
    ParseError,           # HTTP 200 but malformed response
)
```

| Exception | When | Retry? |
|-----------|------|--------|
| `EmptyDataError(provider="x", message="...")` | Valid query, zero results | No |
| `UnauthorizedError("x")` | HTTP 401 or 403 | No -- fix credentials |
| `PaymentRequiredError("x")` | HTTP 402 or tier-gated | No -- upgrade plan |
| `RateLimitError("x", retry_after=60.0)` | HTTP 429 burst | Yes after `retry_after` |
| `RateLimitError("x", retry_after=0, quota_exhausted=True)` | Quota hit | No |
| `ProviderError("x", status_code=500)` | Server error | Maybe (transient) |
| `ParseError("x")` | Malformed response body | No (likely API change) |

### HTTP status mapping pattern (from `fmp.py`):

```python
response = await http.request("GET", path, params=params)
if response.status_code == 401:
    raise UnauthorizedError("my_source")
if response.status_code == 402:
    raise PaymentRequiredError("my_source")
if response.status_code == 429:
    retry_after = float(response.headers.get("Retry-After", "60"))
    raise RateLimitError("my_source", retry_after=retry_after)
response.raise_for_status()  # catches remaining 4xx/5xx as httpx errors
```

For simpler sources, `response.raise_for_status()` + `EmptyDataError` is sufficient.

### 401 repurposed for plan-gating (CoinGecko pattern)

Some providers return HTTP 401 for **both** bad credentials **and** plan-tier
restrictions -- the same status code, distinguished only by an `error_code` in
the response body. CoinGecko is the canonical example: it never returns 402.

The correct approach is body inspection inside the 401 handler. Raise
`PaymentRequiredError` for known plan-gate error codes; fall through to
`UnauthorizedError` for all others:

```python
case 401:
    try:
        body = e.response.json()
        # Some providers use {"status": {...}}, others {"error": {"status": {...}}}
        status = body.get("status") or body.get("error", {}).get("status", {})
        code = status.get("error_code", 0)
        msg = status.get("error_message", "")
        if code in (10005, 10006, 10012):  # CoinGecko Pro-only / plan-gated codes
            raise PaymentRequiredError(
                provider="coingecko",
                message=f"CoinGecko plan restriction (error_code={code}): {msg}",
            ) from e
    except (ValueError, AttributeError):
        pass
    raise UnauthorizedError(provider="coingecko", message="Invalid or missing API key") from e
```

Key points:
- Wrap the body parse in `try/except` -- if the body is not JSON or the structure
  is unexpected, fall through to `UnauthorizedError` rather than crashing.
- The error body schema may vary across endpoints of the same provider. CoinGecko
  uses `{"status": {...}}` for some errors and `{"error": {"status": {...}}}` for
  others. Use `body.get("status") or body.get("error", {}).get("status", {})` to
  handle both without nested conditionals.
- Document the plan-gated connectors with `[Pro+]` tier prefixes so the agent
  skips them when it knows the configured key is on a lower plan.

### Rate Limits: Burst vs Quota

Not every provider has aggressive rate limits, but when they do, distinguish
between **burst limits** (short-term, retryable) and **quota limits** (billing-
period exhaustion, don't retry). Use `RateLimitError` with `quota_exhausted=True`
for the latter so callers know not to retry.

For providers where burst 429s are common, consider adding a retry wrapper in
the connector module. The key insight: burst retries should be silent (the caller
never sees the error), while quota exhaustion should surface immediately with a
clear message.

### Errors embedded in HTTP 200

Some providers always return HTTP 200 and embed errors in the response body
as dedicated keys or a different response shape. Check for error keys
**before** parsing data, and put the checks in a shared `_fetch()` helper:

```python
body = response.json()
if "Error Message" in body:
    raise EmptyDataError(provider="x", message=body["Error Message"])
if "Note" in body:
    raise RateLimitError(provider="x", retry_after=60.0, message=body["Note"])
# Now safe to parse the actual data
```

A single body key may signal different error types (rate limit vs premium
gate) — inspect the message string to distinguish. Keep
`response.raise_for_status()` for network-level errors, but don't rely on
it for API-level ones. See `alpha_vantage.py:_av_fetch()`.

### Unified connectors with Literal params

When many API endpoints share the **same response schema and parameters**,
they're not really separate operations — the endpoint name is just another
parameter selecting from a family of same-shaped series. Treat it as one
connector with a `Literal[...]` param:

```python
_INDICATORS = ("REAL_GDP", "CPI", "INFLATION", "UNEMPLOYMENT")

class EconParams(BaseModel):
    indicator: Literal[_INDICATORS] = Field(...)  # type: ignore[valid-type]

@connector(output=ECON_OUTPUT, tags=["macro"])
async def my_source_econ(params: EconParams, *, api_key: str) -> Result: ...
```

This is distinct from the generic path anti-pattern (see Anti-Pattern section)
because the values are a closed, validated set — conceptually no different
from `series_id`. The test: if every value produces the same column schema
and accepts the same parameters, it's a parameter, not a separate operation.
When schemas or params diverge, use separate connectors.

See `alpha_vantage.py:alpha_vantage_econ()` and
`alpha_vantage.py:alpha_vantage_technical()`.

### Mixed JSON/CSV endpoints

Some providers return CSV for certain endpoints (calendars, bulk listings)
while the rest is JSON. Build a separate `_fetch_csv()` helper. Check raw
text for embedded errors before passing to `pd.read_csv()` — error responses
may still arrive as JSON or get garbled into CSV columns.

```python
text = response.text
if text.startswith("{") or text.startswith("Information"):
    raise RateLimitError(...)
df = pd.read_csv(io.StringIO(text))
```

See `alpha_vantage.py:_av_fetch_csv()`.

### Provider overlap

When a new provider covers data available through an existing connector,
implement only what's genuinely additive (e.g. real-time pricing not
available elsewhere). Skip redundant endpoints and document the decision
in the module docstring so future maintainers don't re-add them.

---

## Building a Search Catalog (Tier 2-4)

Most data providers don't have a native search endpoint. The agent discovers
series through parsimony's `Catalog` -- a SQLite-backed index populated by
`@enumerator` connectors and searchable via FTS5 (keyword) or optional
vector embeddings (semantic).

The flow:

```
@enumerator runs against live API
        ↓
Returns DataFrame with KEY + TITLE + METADATA columns
        ↓
Catalog.index_result() extracts SeriesEntry rows
        ↓
SQLite FTS5 index enables catalog.search("US unemployment")
        ↓
Agent gets series_id → calls @connector to fetch data
```

### Tier 2: Provider has a list/catalog endpoint

The provider has an endpoint that returns all available series (or datasets)
with metadata. This is the most common case for central banks and statistical
agencies.

**What makes a good Tier 2 source:**
- A `/series`, `/datasets`, `/catalog`, or `/list` endpoint that returns IDs + titles
- SDMX agencies (use `sdmx_list_datasets` + `sdmx_series_keys` -- already built)
- REST APIs with a bulk metadata endpoint

**Implementation:** Write an `@enumerator` that calls the list endpoint and
returns a DataFrame with the standard schema:

```python
@enumerator(output=ENUMERATE_OUTPUT, tags=["macro"])
async def enumerate_my_source(params: MySourceEnumerateParams) -> pd.DataFrame:
    """Enumerate all series from MySource for catalog indexing."""
    http = _make_http(api_key)

    # Many APIs paginate their catalog -- fetch all pages
    all_series: list[dict] = []
    page = 1
    while True:
        resp = await http.request("GET", "/series", params={"page": page, "per_page": 100})
        resp.raise_for_status()
        batch = resp.json().get("series", [])
        if not batch:
            break
        all_series.extend(batch)
        if len(batch) < 100:
            break
        page += 1
        await asyncio.sleep(0.5)  # respect rate limits

    rows = [
        {
            "series_id": s["id"],
            "title": s["name"],
            "category": s.get("category", ""),
            "frequency": s.get("frequency", ""),
        }
        for s in all_series
        if s.get("id")
    ]

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_id", "title", "category", "frequency"]
    )
```

**Enriching with metadata:** The more metadata in the TITLE and METADATA columns,
the better catalog search works. If the list endpoint returns minimal info but
a detail endpoint returns richer metadata (units, description, topic), consider
enriching during enumeration (with semaphore-bounded concurrent requests):

```python
async def _enrich(http: HttpClient, sem: asyncio.Semaphore, series_id: str) -> dict:
    async with sem:
        resp = await http.request("GET", f"/series/{series_id}")
        if resp.status_code == 200:
            detail = resp.json()
            return {"series_id": series_id, "title": detail.get("long_name", series_id),
                    "category": detail.get("topic", ""), "frequency": detail.get("freq", "")}
    return {"series_id": series_id, "title": series_id, "category": "", "frequency": ""}
```

### Tier 3: No API search, but browsable structure

The provider has a website with a navigable hierarchy but no API for listing
series. Common with smaller central banks and national statistics offices.

**Options (in order of preference):**

1. **Check for a hidden API.** Open browser dev tools, navigate the site, and
   watch the XHR/Fetch requests. Many "no API" sites actually have a JSON API
   backing their frontend -- just undocumented.

2. **SDMX discovery.** Some agencies support SDMX but don't advertise it.
   Try `https://{host}/sdmx/v2.1/dataflow/all/all/latest` -- if it responds,
   you can use the existing `sdmx.py` infrastructure instead of building a
   new connector.

3. **Scrape the catalog.** Write an `@enumerator` that fetches the HTML
   catalog pages, parses series IDs and titles. Use `httpx` + a lightweight
   parser. This is a last resort but works. Rate-limit aggressively.

4. **Curate manually.** For small providers (< 500 series), maintain a static
   list as a Python dict or JSON file in the module. The `@enumerator` returns
   it as a DataFrame. Update periodically. It should be the last option.

### Tier 4: Bulk files only

The provider distributes data as downloadable files (CSV, Excel, SDMX-ML,
ZIP archives) with no API.

**Implementation:** The `@enumerator` downloads the file index or metadata
file and parses it. The `@connector` (fetch) downloads and parses the data file.

```python
@enumerator(output=ENUMERATE_OUTPUT, tags=["macro"])
async def enumerate_my_source(params: MySourceEnumerateParams) -> pd.DataFrame:
    """Enumerate series from MySource bulk catalog file."""
    import httpx

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.get("https://my-source.example.com/catalog.csv")
        resp.raise_for_status()

    # Parse CSV catalog
    import csv
    import io
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = [
        {"series_id": r["code"], "title": r["description"],
         "category": r.get("topic", ""), "frequency": r.get("periodicity", "")}
        for r in reader if r.get("code")
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["series_id", "title", "category", "frequency"]
    )
```

### Registering the Enumerator in the Catalog Builder

After implementing your `@enumerator`, register it in
`parsimony/catalog/builder.py` → `_collect_enumerators()`:

```python
# My Source (requires API key)
my_key = env.get("MY_SOURCE_API_KEY")
if my_key:
    try:
        from parsimony.connectors.my_source import MySourceEnumerateParams, enumerate_my_source
        conn = enumerate_my_source.bind_deps(api_key=my_key)
        enumerators.append(("my_source", conn, MySourceEnumerateParams()))
    except ImportError:
        pass

# My Source (no auth)
try:
    from parsimony.connectors.my_source import MySourceEnumerateParams, enumerate_my_source
    enumerators.append(("my_source", enumerate_my_source, MySourceEnumerateParams()))
except ImportError:
    pass
```

Then build the catalog bundle locally:

```bash
python -m parsimony.stores.hf_bundle.builder build <namespace> ./build/<namespace>
# produces ./build/<namespace>/{entries.parquet, index.faiss, manifest.json}
```

### How the Agent Uses the Catalog

At runtime, the agent doesn't call your `@enumerator` directly. Instead:

1. The catalog is pre-built (or lazily populated on first search).
2. The agent calls `catalog.search("US unemployment rate")` -- this hits the
   SQLite FTS5 index.
3. The search returns `SeriesMatch` objects with `namespace`, `code`, and `title`.
4. The agent calls your `@connector` (fetch) with the matched `series_id`.

This is why every connector module needs **both** an `@enumerator` (to populate
the catalog) and a `@connector` (to fetch data by ID). The enumerator creates
the bridge between human-language queries and provider-specific identifiers.

---

## Data Source Types

Not every connector wraps a REST API. parsimony connectors can wrap any
async-callable data source.

### REST API (most common)

Use `HttpClient` from `parsimony.transport.http`. See Steps 4-5 above.

### Python Client Library / SDK

If a good Python library already exists for the provider, wrap it instead of
reimplementing the HTTP layer. The library becomes your transport.

```python
@connector(output=FETCH_OUTPUT, tags=["macro"])
async def my_source_fetch(params: MySourceFetchParams, *, client: MySourceClient) -> Result:
    """Fetch data from MySource via the official Python client."""
    # Sync SDK: wrap in asyncio.to_thread()
    data = await asyncio.to_thread(client.get_series, params.series_id)
    df = pd.DataFrame(data)
    return Result.from_dataframe(df, Provenance(source="my_source", params={...}))
```

Bind the client at startup:

```python
ENV_VARS: dict[str, str] = {"api_key": "MY_SOURCE_API_KEY"}

# In your factory or startup code:
from my_sdk import MySourceClient
client = MySourceClient(api_key=os.environ["MY_SOURCE_API_KEY"])
CONNECTORS.bind_deps(client=client)
```

**Sync SDKs:** Many SDKs are synchronous. Wrap blocking calls with
`asyncio.to_thread()`. For complex multi-step sync operations, extract them
into a single sync function and wrap the whole thing in one `to_thread()` call
to minimize thread-switching overhead.

**SDK `to_context()` methods:** Some modern SDKs include AI-optimized methods
(`to_context()`, `to_llm()`). Check before building manual attribute-extraction
pipelines. If available, expose as a `result_type="text"` connector -- it's
maintained by the SDK authors and evolves with their data model.

See [internal-connectors.md](internal-connectors.md) for complete patterns
(Postgres via `asyncpg`, Snowflake via `snowflake-connector-python`, S3 via
`boto3`). The same `@connector` / `@enumerator` / `bind_deps` pattern works
identically whether the transport is HTTP, a database driver, or an SDK.

### SDMX Agency

If the provider supports SDMX, you probably don't need a new module at all.
Check if the agency is already supported by `sdmx1`:

```python
import sdmx
print(sdmx.list_sources())
```

If it is, the existing `parsimony/connectors/sdmx.py` handles it. If not,
you may need to add the agency to `sdmx1` or implement SDMX XML parsing directly.

### Bulk Files (CSV, Excel, ZIP)

For providers that only distribute files, use `httpx` to download and `pandas`
to parse. See the Tier 4 catalog pattern above. The `@connector` (fetch) follows
the same pattern but returns observation data instead of catalog entries.

---

## Common Patterns

### Paginated APIs

```python
all_data: list[dict] = []
offset = 0
page_size = 1000

while True:
    response = await http.request("GET", "/endpoint", params={
        "limit": page_size, "offset": offset,
    })
    response.raise_for_status()
    batch = response.json().get("results", [])
    if not batch:
        break
    all_data.extend(batch)
    if len(batch) < page_size:
        break
    offset += page_size
```

See `fred.py:_enumerate_release_series()` for the canonical example.

### Concurrent Requests with Semaphore

```python
import asyncio

_SEMAPHORE_LIMIT = 10

async def _fetch_many(http: HttpClient, ids: list[str]) -> list[dict]:
    sem = asyncio.Semaphore(_SEMAPHORE_LIMIT)

    async def _one(id: str) -> dict | None:
        async with sem:
            resp = await http.request("GET", f"/item/{id}")
            return resp.json() if resp.status_code == 200 else None

    results = await asyncio.gather(*[_one(id) for id in ids])
    return [r for r in results if r is not None]
```

See `fmp_screener.py` for the canonical example.

### Authentication Variants

**Query param** (most common): `HttpClient(URL, query_params={"api_key": key})`

**Header**: `HttpClient(URL, headers={"Authorization": f"Bearer {key}"})`

**Custom header** (CoinGecko): `HttpClient(URL, headers={"x-cg-demo-api-key": key})`

**POST body** (BLS): `await http.request("POST", "/endpoint", json={"registrationkey": key})`

**Username/password** (Destatis): `ENV_VARS = {"username": "...", "password": "..."}`

**No auth** (Treasury, SNB): `ENV_VARS = {}`, no keyword-only deps on the function.

### Enumerator with Cascading Fallback

For unreliable enumeration endpoints, implement multiple strategies:

```python
@enumerator(output=ENUMERATE_OUTPUT)
async def enumerate_source(params: EnumerateParams) -> pd.DataFrame:
    rows: list[dict] = []

    try:
        rows = await _primary_api(client)
    except Exception:
        logger.warning("Primary enumeration failed")

    if len(rows) < 100:
        try:
            fallback = await _bulk_fallback(client)
            seen = {r["id"] for r in rows}
            rows.extend(r for r in fallback if r["id"] not in seen)
        except Exception:
            logger.warning("Bulk fallback failed")

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[...])
```

### Discovery → Inspect → Fetch

Design connectors in layers that let callers narrow down before committing to
expensive operations:

1. **Search/List** -- cheap, returns identifiers + minimal metadata
2. **Sections/TOC** -- medium, returns what's available inside a resource
3. **Fetch item** -- targeted, returns only the specific content needed

Example flow:
```
search_filings(query="AI risk") → accession numbers
filing_sections(accession_number=...) → list of items
filing_item(accession_number=..., item="1A") → Risk Factors text only
```

This saves massive bandwidth vs fetching a 500-page document to find one section.

When a resource contains multiple heterogeneous tables (different schemas),
don't try to return them all in one DataFrame. Instead: list tables (summary
with index, caption, size) → fetch individual table by index.

### Batch Operations

If the provider supports batch retrieval, expose it. Calling a connector in a
loop (N sequential API calls) is slow. A single connector that accepts a list
of identifiers and returns a combined result is much more efficient.

Only add batch params if the provider actually supports it server-side.
Client-side batching (looping internally) doesn't save latency and adds complexity.

---

## DataFrame Sanitization

API responses often produce DataFrames with issues that crash downstream
serialization (Arrow conversion, JSON export). Handle these in the connector:

**MultiIndex columns.** Some SDKs produce `pd.MultiIndex` columns. Flatten them:

```python
if isinstance(df.columns, pd.MultiIndex):
    df.columns = [" | ".join(str(c) for c in col if str(c).strip()) for col in df.columns]
```

**Duplicate column names.** Financial tables often repeat year headers. Deduplicate:

```python
seen: dict[str, int] = {}
new_cols: list[str] = []
for col in df.columns:
    col_str = str(col)
    if col_str in seen:
        seen[col_str] += 1
        new_cols.append(f"{col_str}_{seen[col_str]}")
    else:
        seen[col_str] = 1
        new_cols.append(col_str)
if new_cols != list(df.columns):
    df.columns = new_cols
```

**Mixed-type columns.** Columns containing both strings (`"$1,234"`) and floats
(`NaN`) cause `ArrowTypeError`. Coerce to uniform type:

```python
for col in df.columns:
    if df[col].dtype == object:
        types = set(type(v).__name__ for v in df[col].dropna())
        if len(types) > 1:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else "")
```

**Unhashable cells (nested dicts/lists).** `pd.json_normalize()` with `max_level`
leaves nested objects as dict/list cells. Use `json.dumps()`, not `str()`:

```python
import json
for col in df.columns:
    sample = df[col].dropna()
    if len(sample) > 0 and isinstance(sample.iloc[0], (list, tuple, dict)):
        df[col] = df[col].apply(
            lambda x: json.dumps(x, default=str)
            if isinstance(x, (list, tuple, dict)) else x
        )
```

**Timezone-aware datetimes (Pydantic TzInfo).** Pydantic v2 uses
`pydantic_core.TzInfo` for timezone-aware datetimes. Pandas can't serialize
these. Use `model_dump(mode="json")` to get ISO strings before creating DataFrames:

```python
raw = resp.model_dump(mode="json")  # datetimes -> ISO strings
df = pd.json_normalize(raw["results"])
```

---

## Testing

### Smoke test

```bash
python -c "
import asyncio
from parsimony.connectors.my_source import CONNECTORS

c = CONNECTORS.bind_deps(api_key='YOUR_KEY')
result = asyncio.run(c['my_source_fetch'](series_id='TEST_SERIES'))
print(result.data.head())
print(result.provenance)
"
```

### Integration tests

Guard with `pytest.mark.skipif` so CI doesn't require API keys:

```python
import os
import asyncio
import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("MY_SOURCE_API_KEY"),
    reason="MY_SOURCE_API_KEY not set",
)


class TestMySourceIntegration:
    def test_fetch_real_series(self) -> None:
        from parsimony.connectors.my_source import CONNECTORS
        c = CONNECTORS.bind_deps(api_key=os.environ["MY_SOURCE_API_KEY"])
        result = asyncio.run(c["my_source_fetch"](series_id="KNOWN_SERIES"))
        assert len(result.data) > 0
```

### Verify `to_llm()` output

After implementing, inspect what the agent will see:

```python
from parsimony.connectors.my_source import CONNECTORS
print(CONNECTORS.to_llm())
```

Check that descriptions include workflow chaining hints, parameter descriptions are
specific, and column names appear in the output.

---

## Anti-Pattern: The Generic Path Connector

**Do not** build a single connector with `path: str` and `extra="allow"` that
dispatches to arbitrary API endpoints. This is the single biggest design mistake.

```python
# BAD -- agent sees one opaque blob, can't discover operations or validate params
class GenericFetchParams(BaseModel):
    model_config = ConfigDict(extra="allow")
    method: Literal["GET", "POST"] = "GET"
    path: str = Field(..., description="API path, may contain {placeholders}")

@connector(tags=["my_source"])
async def my_source_fetch(params: GenericFetchParams, *, api_key: str) -> Result:
    """My Source REST API (path, method, query params)."""
```

This hides parameter schemas from the agent -- `to_llm()` shows one opaque function
with a generic `path` parameter. The agent has no way to discover what operations
exist, what parameters each accepts, or what columns come back.

**Instead,** create typed connectors per operation:

```python
# GOOD -- agent discovers each operation with typed, documented params
class MySourceQuoteParams(BaseModel):
    symbol: Annotated[str, Namespace("my_source")] = Field(
        ..., description="Ticker symbol (e.g. AAPL)"
    )

@connector(output=QUOTE_OUTPUT, tags=["equity", "tool"])
async def my_source_quote(params: MySourceQuoteParams, *, api_key: str) -> Result:
    """[Starter+] Fetch real-time quote for a symbol."""

class MySourceHistoricalParams(BaseModel):
    symbol: Annotated[str, Namespace("my_source")] = Field(...)
    start_date: str | None = Field(default=None, description="Start date YYYY-MM-DD")
    end_date: str | None = Field(default=None, description="End date YYYY-MM-DD")

@connector(output=HISTORICAL_OUTPUT, tags=["equity"])
async def my_source_historical(params: MySourceHistoricalParams, *, api_key: str) -> Result:
    """[Starter+] Fetch daily OHLCV price history for a symbol."""
```

**Why this matters for agents:** The `to_llm()` output lists each connector as a
separate tool with its own typed parameters. A generic path connector gives the
agent one tool that can "do anything" but provides no guidance on what to do.
Typed connectors give the agent a menu of discoverable operations, each with
validated parameters and a clear description. See `fmp.py` for the canonical
example of typed connectors per operation.

---

## Publishing HF Catalog Bundles

After building a catalog with your new `@enumerator`, publish it as a
HuggingFace Hub bundle under the `parsimony-dev` organization so other users
get instant search without running your enumerator themselves.

### How it works

The `Catalog` class lazily downloads pre-built bundles from HuggingFace Hub.
When a user searches for a namespace that isn't already loaded, the store
(`HFBundleCatalogStore`) asks `HfApi.repo_info` for the current revision,
downloads `entries.parquet` + `index.faiss` + `manifest.json` via
`snapshot_download`, verifies SHA-256 integrity against the manifest, and
loads the bundle for FAISS vector search. If no bundle is published, the
catalog falls back to running the `@enumerator` live (slower).

Each namespace lives in its own HF dataset repo:
`parsimony-dev/<namespace>`.

### Publishing your bundle

Prerequisites:

- Write access to the `parsimony-dev` HF org.
- A write-scoped HF token exported as `HF_TOKEN` (or `HUGGING_FACE_HUB_TOKEN`).
- The pinned embedding-model revision exported as `PARSIMONY_EMBED_REVISION`
  (full 40-char commit SHA of the `sentence-transformers/all-MiniLM-L6-v2`
  weights).

The publish CLI has three safety rails:

| Flag | Purpose |
|---|---|
| `--dry-run` | Build locally, print the manifest JSON, skip upload entirely |
| `--yes` | Required for an actual upload (prevents stray CLI invocations) |
| `--allow-shrink` | Permit publishing when the fresh `entry_count` is <50% of the currently-published bundle (otherwise refused) |
| `--keep-dir <path>` | Copy the built bundle into a caller-owned directory before the tempdir is cleaned up |

1. **Build locally and inspect:**
   ```bash
   python -m parsimony.stores.hf_bundle.builder build <namespace> ./build/<namespace>
   ls ./build/<namespace>
   # entries.parquet  index.faiss  manifest.json
   ```

2. **Verify the bundle loads:**
   ```python
   from parsimony.embeddings.sentence_transformers import SentenceTransformersEmbeddingProvider
   from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

   provider = SentenceTransformersEmbeddingProvider(
       model_id="sentence-transformers/all-MiniLM-L6-v2",
       revision="<pinned-sha>",
       expected_dim=384,
   )
   store = HFBundleCatalogStore(embeddings=provider, cache_dir="./build")
   # Use try_load_remote with cache-only semantics by setting PARSIMONY_CATALOG_PIN
   # to a bundle revision you've placed in ./build/<namespace>/<revision>/.
   ```

3. **Dry-run publish to preview the manifest:**
   ```bash
   python -m parsimony.stores.hf_bundle.builder publish <namespace> --dry-run
   ```
   The full manifest JSON is written to stdout; no upload happens.

4. **Publish:**
   ```bash
   python -m parsimony.stores.hf_bundle.builder publish <namespace> --yes
   ```
   Runs the builder, writes the three files, fetches the currently-published
   `entry_count` for a shrink guard, and calls
   `HfApi.upload_folder(..., allow_patterns=[...], ...)` to commit the
   bundle to `parsimony-dev/<namespace>`. The publish report (commit SHA,
   entry counts, shrink ratio, status) is written to stdout as JSON.

### Bundle versioning

Cache freshness is keyed on the HF commit SHA. To pin to a specific
revision for reproducibility (CI, regression tests, customer repro):

```bash
export PARSIMONY_CATALOG_PIN=<40-char-commit-sha>
```

When `PARSIMONY_CATALOG_PIN` is set, the store skips the HEAD check and
serves only that revision; if the pin isn't in the local cache and HF is
unreachable, `BundleIntegrityError` is raised (fail-closed — pinning is a
security/reproducibility contract).

### Cache layout

The client cache lives at `$PARSIMONY_CACHE_DIR` (defaults to the
platformdirs user cache) with layout:

```
<cache_base>/<namespace>/<commit_sha>/
    manifest.json
    entries.parquet
    index.faiss
```

One revision per namespace is kept — a fresh download cleans up sibling
SHA directories. The commit SHA is encoded in the directory name; there
is no sidecar metadata file.

---

## Checklist

### Required

- [ ] Module file in `parsimony/connectors/my_source.py`
- [ ] `ENV_VARS: dict[str, str]` exported at module level (only if the provider requires credentials)
- [ ] Pydantic params model(s) with `Field(description=...)` on every field
- [ ] `async` connector function(s) with descriptive docstring
- [ ] `OutputConfig` with semantic column roles (KEY + namespace, TITLE, DATA, METADATA)
- [ ] Typed exceptions (`EmptyDataError`, `UnauthorizedError`, etc.) -- not `ValueError`
- [ ] `CONNECTORS` bundle exported (all connectors: search + fetch + enumerator)
- [ ] `ProviderSpec` entry added to `PROVIDERS` in `parsimony/connectors/__init__.py`
- [ ] At least one `@enumerator` for catalog population
- [ ] **Typed connectors per operation** -- not a generic path-based dispatcher (see Anti-Pattern section)
- [ ] **Tier annotations** in docstrings for paid providers (`[Starter+]`, `[Professional+]`, etc.)
- [ ] Tags: domain tags + `"tool"` for MCP-exposed search/discovery connectors

### Recommended

- [ ] Integration test guarded with `pytest.mark.skipif`
- [ ] `@field_validator` for input sanitization
- [ ] Pagination for large result sets
- [ ] `to_llm()` output reviewed for clarity
- [ ] **HF catalog bundle published** to `parsimony-dev/<namespace>` (see Publishing HF Catalog Bundles)

### Before submitting

```bash
pytest tests/ -v
ruff check parsimony/connectors/my_source.py
mypy parsimony/connectors/my_source.py
```

---

## Quick Reference: Imports

```python
# Decorators, collection, and exceptions
from parsimony.connector import (
    connector, enumerator, loader, Connectors, Namespace,
    ConnectorError, EmptyDataError, UnauthorizedError,
    PaymentRequiredError, RateLimitError, ProviderError, ParseError,
)

# Result types and schema
from parsimony.result import (
    Result, SemanticTableResult, OutputConfig, Column, ColumnRole, Provenance,
)

# HTTP client (auto-redacts credentials in logs)
from parsimony.transport.http import HttpClient
```

## Further Reading

- [internal-connectors.md](internal-connectors.md) -- wrapping Postgres, Snowflake, S3,
  and other internal data sources with `@connector` / `bind_deps`.
- [architecture.md](architecture.md) -- system design and internals.
- [api-reference.md](api-reference.md) -- complete API documentation.
