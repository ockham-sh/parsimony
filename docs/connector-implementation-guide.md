# Connector Implementation Guide

Lessons learned from implementing the SEC EDGAR and Financial Reports connectors.
This guide covers patterns, pitfalls, and decisions that apply to any new data provider.

## Architecture: One Connector Per Operation

**Never** use a single connector with an `endpoint: str` parameter and `extra="allow"`.
This hides parameter schemas from the agent — `to_llm()` shows one opaque blob instead
of discoverable operations with typed parameters.

**Always** create separate connectors for each distinct operation. Each gets its own
Pydantic `BaseModel` with explicit, typed, documented fields. The agent discovers
available operations and their parameters automatically via `to_llm()`.

```python
# BAD: agent sees nothing useful
class FetchParams(BaseModel):
    model_config = ConfigDict(extra="allow")
    endpoint: str = Field(..., description="company | filings | profile")

# GOOD: agent sees exactly what each operation accepts
class CompanySearchParams(BaseModel):
    identifier: str = Field(..., description="Company name, ticker, or CIK")

class FilingsParams(BaseModel):
    identifier: str | None = Field(default=None, description="...")
    form: str | None = Field(default=None, description="e.g. '10-K', '8-K'")
    limit: int = Field(default=20, ge=1, le=100)
```

### When to share param models

Share a param model across connectors only when the parameters are truly identical.
Example: `income_statement`, `balance_sheet`, and `cashflow_statement` all accept
`identifier, periods, annual, view` — one `FinancialStatementParams` model is correct.

## Result Types: Tell the Agent What `.data` Contains

The framework defaults to `.data` being a DataFrame. When a connector returns text
(markdown, context summaries), use `result_type="text"` on the `@connector` decorator:

```python
@connector(tags=["my_source"], result_type="text")
async def my_source_document(params: DocumentParams) -> Result:
    """Retrieve document as markdown text."""
    ...
    return Result(data=markdown_text, provenance=...)
```

This adds `-> result.data is text (not a DataFrame).` to the `to_llm()` output,
preventing the agent from writing `.iloc[0]` or `.head()` on a string.

### When to use text vs DataFrame

- **DataFrame**: structured, tabular data the agent will filter/aggregate/chart
- **Text**: documents, metadata summaries, section content the agent will read/reason about

If the data is a single row with 30+ columns from a flattened API response, consider
whether `to_context()` (if the SDK provides it) or a curated summary would serve the
agent better than a wide DataFrame it can't easily display.

## DataFrame Sanitization

SDK responses often produce DataFrames with issues that crash downstream serialization
(the agent's display layer, Arrow conversion, JSON export). Handle these in the connector:

### MultiIndex columns

Some SDKs produce DataFrames with `pd.MultiIndex` columns (e.g., multi-level table headers).
Downstream `df.to_json(orient="table")` does not support MultiIndex.

```python
if isinstance(df.columns, pd.MultiIndex):
    df.columns = [" | ".join(str(c) for c in col if str(c).strip()) for col in df.columns]
```

### Duplicate column names

Financial tables often have years as column headers repeated (amount column + symbol column).
This causes `KeyError` or `ValueError` in pandas operations.

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

### Mixed-type columns

Columns containing both strings (`"$1,234"`) and floats (`NaN`) cause `ArrowTypeError`
when the agent's display layer converts to Arrow format. Coerce to uniform type:

```python
for col in df.columns:
    if df[col].dtype == object:
        sample = df[col].dropna()
        if len(sample) > 0:
            types = set(type(v).__name__ for v in sample)
            if len(types) > 1:
                df[col] = df[col].apply(lambda x: str(x) if x is not None else "")
```

### Unhashable cells (nested dicts/lists)

`pd.json_normalize()` with `max_level` leaves nested objects as dict/list cells.
Pandas operations on these fail with `TypeError: unhashable type`.

**Use `json.dumps()`, not string concatenation.** Comma-joining lists or calling `str()`
on dicts produces opaque strings the agent can't parse back. JSON strings preserve
structure and can be re-parsed downstream:

```python
import json

for col in df.columns:
    sample = df[col].dropna()
    if len(sample) == 0:
        continue
    first = sample.iloc[0]
    if isinstance(first, (list, tuple, dict)):
        df[col] = df[col].apply(
            lambda x: json.dumps(x, default=str)
            if isinstance(x, (list, tuple, dict)) else x
        )
```

### Timezone-aware datetimes (Pydantic TzInfo)

Pydantic v2 models use `pydantic_core.TzInfo` for timezone-aware datetimes. Pandas
can't serialize these (`'TzInfo' object has no attribute 'zone'`). Use
`model_dump(mode="json")` to serialize datetimes to ISO strings before creating DataFrames:

```python
raw = resp.model_dump(mode="json")  # datetimes -> ISO strings
df = pd.json_normalize(raw["results"])
```

## Rate Limit Handling

APIs with rate limits need two distinct strategies: **burst limits** (short-term speed
limits, retry after a few seconds) and **quota limits** (monthly caps, don't retry).

### Distinguish burst from quota

Parse the `retry_after` value from the 429 response body. If it's short (< 60s), retry
with exponential backoff. If it's long (hours/days), raise immediately with a clear
message — retrying wastes time and burns agent tool calls.

```python
_MAX_BURST_RETRIES = 3

async def _with_retry(coro_factory, api_key: str):
    """Execute an async SDK call with retry on burst 429 errors."""
    for attempt in range(_MAX_BURST_RETRIES + 1):
        try:
            async with _sdk_client(api_key) as client:
                return await coro_factory(client)
        except Exception as exc:
            if getattr(exc, "status", None) != 429:
                raise
            retry_after = _parse_retry_after(exc)  # extract from body
            if retry_after > 60:  # quota exhaustion
                raise ValueError(
                    "API quota exhausted for the current billing period. "
                    "Upgrade your plan or wait for the next billing cycle."
                ) from exc
            if attempt < _MAX_BURST_RETRIES:
                wait = max(retry_after, 1.0) * (2 ** attempt)
                await asyncio.sleep(wait)
            else:
                raise ValueError(
                    f"Rate limit exceeded after {_MAX_BURST_RETRIES} retries."
                ) from exc
```

### Why this matters for agents

Without retry handling, the agent sees a raw `ApiException(429)` with a wall of HTTP
headers. It doesn't know whether to retry or give up, so it often loops — trying the
same call repeatedly, burning tool calls and time. With clear error messages:

- **Burst**: the connector retries silently, the agent never sees the error
- **Quota**: the agent gets `"quota exhausted"` and immediately falls back to other sources

### Apply to all connectors uniformly

Wrap every SDK call through the retry helper. Use a lambda/closure pattern so complex
connectors (with conditional logic) can still benefit:

```python
# Simple connector
resp = await _with_retry(lambda c: c.companies.list(**kwargs), api_key)

# Complex connector with conditional dispatch
async def _call(client):
    api = getattr(client, "isic", None) or ISICApi(client.api_client)
    return await {"sections": api.sections_list, "divisions": api.divisions_list}[level](**kwargs)

resp = await _with_retry(_call, api_key)
```

## SDK Workarounds

### Broken SDK modules

If an SDK has a syntax error or broken import in a module you don't use, pre-load a
stub in `sys.modules` before importing:

```python
import sys, types

stub_key = "broken_sdk.api.broken_module"
if stub_key not in sys.modules:
    stub = types.ModuleType(stub_key)
    class _Stub:
        def __init__(self, *a, **kw): pass
    stub.BrokenClass = _Stub
    sys.modules[stub_key] = stub

from broken_sdk import Client  # now imports cleanly
```

This is clean, requires no disk writes, and is harmless when the SDK is fixed.

### Sync SDKs in async connectors

Many SDKs are synchronous. Wrap blocking calls with `asyncio.to_thread()`:

```python
@connector(tags=["my_source"])
async def my_source_fetch(params: FetchParams) -> Result:
    entity = await asyncio.to_thread(_resolve_entity, params.identifier)
    data = await asyncio.to_thread(entity.get_data)
    ...
```

For complex multi-step sync operations, extract them into a single sync function
and wrap the whole thing in one `to_thread()` call to minimize thread-switching overhead.

## OutputConfig: Match Actual Response Columns, Not API Docs

OutputConfig column names must match what `json_normalize()` actually produces, not what
the API documentation says the response *should* contain. APIs often return different
columns for summary/list views vs full/detail views.

### Verify columns empirically

Make a real API call (or inspect `_to_dataframe(resp).columns`) before defining the
OutputConfig. Common mismatches:

- **Summary vs full view**: a list endpoint returns `country_code` (flat string), but
  the detail endpoint returns `country_iso.name` (nested object that gets normalized).
  Your search OutputConfig must use `country_code`, not `country_iso.name`.
- **Optional nested objects**: `sector.name` only exists when the API response includes
  a `sector` object. If it's `null`, `json_normalize` won't create the column at all.
- **Renamed fields**: the SDK may rename fields during `model_dump()`.

### Missing columns are silently skipped

`build_table_result()` matches config columns against the DataFrame. If a config column
doesn't exist in the DataFrame, it's silently skipped — no error. This means a wrong
column name doesn't crash, it just produces incomplete output. The agent still gets data,
but without the semantic roles you intended.

### Per-resource OutputConfig mapping

When a single connector dispatches to different resources (e.g., a reference data connector
that serves filing types, countries, languages), each resource has different columns.
Use a mapping dict to select the right OutputConfig at runtime:

```python
_OUTPUT_MAP = {
    "filing_types": FILING_TYPES_OUTPUT,   # id, code, name, description
    "countries": COUNTRIES_OUTPUT,          # alpha_2, name
    "languages": GENERIC_OUTPUT,           # id, name
}

# In the connector:
output = _OUTPUT_MAP.get(params.resource, GENERIC_OUTPUT)
return output.build_table_result(df, provenance=...)
```

## Docstrings as Workflow Guidance

Connector docstrings are injected verbatim into the agent's system prompt via `to_llm()`.
They're not just documentation — they're **instructions** that guide the agent through
multi-step workflows.

### Chain connectors explicitly

Tell the agent exactly which connector to call next and how to pass identifiers between them:

```python
@connector(output=COMPANIES_SEARCH_OUTPUT, tags=["my_source"])
async def my_source_companies_search(params: CompaniesSearchParams, *, api_key: str) -> Result:
    """Search companies by name, country, or industry.

    Returns company profiles with ID, name, and country.
    Use the company ID with my_source_filings_search(company=id) to find filings,
    my_source_company_retrieve(id=id) for the full profile, or
    my_source_next_report(id=id) for report predictions.
    Use my_source_industries to discover valid industry codes for the sector filter.
    """
```

### Anti-pattern: generic docstrings

Without chaining hints, the agent must guess the workflow. It often picks the wrong
connector, passes wrong parameters, or skips steps entirely:

```python
# BAD: agent doesn't know what to do with the results
"""Search companies on the platform."""

# GOOD: agent knows the exact next steps
"""Search companies by name, country, or industry.
Use the company ID with my_source_filings_search(company=id) to find filings."""
```

### Reference data connectors need extra guidance

Reference/lookup connectors are often missed by the agent unless you tell it when to
use them. Be specific about which filter parameters accept which reference codes:

```python
"""List reference data: filing types, categories, languages, countries, or sources.
Use filing type codes in my_source_filings_search(types=...).
Use country codes in my_source_filings_search(countries=...) or my_source_companies_search(countries=...)."""
```

## Connector Design Patterns

### Discovery -> Inspect -> Fetch

Design connectors in layers that let the agent narrow down before committing to
expensive operations:

1. **Search/List** — cheap, returns identifiers + minimal metadata
2. **Sections/TOC** — medium, returns what's available inside a resource
3. **Fetch item** — targeted, returns only the specific content needed

Example flow:
```
search_filings(query="AI risk") -> accession numbers
filing_sections(accession_number=...) -> list of items
filing_item(accession_number=..., item="1A") -> Risk Factors text only
```

This saves massive tokens vs fetching a 500-page document to find one section.

### Table listing + individual fetch

When a resource contains multiple heterogeneous tables (different schemas, column counts),
don't try to return them all in one DataFrame. Instead:

1. **List tables** — returns summary DataFrame (index, caption, type, size)
2. **Fetch table** — returns individual table by index as a clean DataFrame

The agent uses the listing to pick the right table, then fetches only that one.

### Batch operations

If the SDK supports batch retrieval, expose it. The agent calling a connector in a
loop (N sequential API calls) is slow and token-heavy. A single connector that accepts
a list of identifiers and returns a combined result is much more efficient.

However, only add batch params if the SDK actually supports it server-side. Client-side
batching (looping internally) doesn't save latency and adds complexity.

## Provenance

The `Connector.__call__` method automatically stamps `provenance.source` with the
connector name and `provenance.source_description` with the connector's docstring.
You don't need to manually set these — just construct your Provenance with `params`:

```python
return Result.from_dataframe(
    df,
    Provenance(source="my_source", params=params.model_dump()),
)
```

The framework will override `source` with the connector name (e.g., `my_source_filings`)
at call time. The UI uses this to show which specific connector produced each result.

If you need to pass additional metadata through provenance (e.g., API response headers,
pagination info), use the `properties` dict:

```python
Provenance(
    source="my_source",
    params=params.model_dump(),
    properties={"page": 1, "total_pages": 5},
)
```

## SDK Feature Discovery: Use `to_context()` When Available

Modern data SDKs increasingly include AI-optimized methods (`to_context()`, `to_llm()`,
`to_agent_tools()`). Before building a manual attribute-extraction pipeline:

1. Check if the SDK objects implement a `to_context()` or similar method
2. If they do, use it — it's maintained by the SDK authors, handles all object types,
   and produces structured output designed for LLM consumption
3. Expose it as a `result_type="text"` connector

This avoids the trap of maintaining a hard-coded attribute list that falls behind
the SDK's evolving data model.

## SDK Sub-APIs Not on the High-Level Client

Generated SDK clients often expose a convenience wrapper (e.g., `client.filings`,
`client.companies`) that covers the main endpoints, but leave specialized APIs
(ISINs, ISIC classifications, chat, watchlist) accessible only through low-level
API classes.

Check what the high-level client exposes vs what exists in the SDK's `api/` directory.
For sub-APIs not on the wrapper, instantiate the low-level class directly:

```python
async def _call(client):
    # Try high-level client first, fall back to low-level API
    if hasattr(client, "isic"):
        return await client.isic.sections_list(**kwargs)
    from my_sdk import ISICClassificationsApi
    return await ISICClassificationsApi(client.api_client).sections_list(**kwargs)
```

This is forward-compatible: if the SDK later adds the sub-API to the high-level client,
your code uses it automatically.

## Testing with the Agent

Run `uv run python scripts/agent_eval.py "your query"` to test connectors end-to-end
with the actual agent. This catches issues that unit tests miss:

### Common failures discovered only through agent testing

- **Wrong attribute names**: the SDK's actual API vs what you assumed from docs
- **DataFrame serialization errors**: MultiIndex, duplicates, mixed types, Arrow conversion
- **Empty results**: `row_count > 0` in metadata but `to_dataframe()` returns empty
- **Agent confusion**: the agent misuses connectors when `to_llm()` descriptions are
  unclear or when result types aren't communicated
- **OutputConfig column mismatch**: columns in the config that don't exist in the actual
  API response (silently skipped, produces incomplete output with no error)
- **Rate limit loops**: without retry handling, the agent sees raw 429 errors and loops
  trying the same call, burning 10-20+ tool calls before giving up

### Test workflows, not just individual connectors

The agent chains connectors together. Test the full workflow:
- Search -> list -> fetch -> analyze
- Error recovery: what happens when the first result is empty?
- Fallback behavior: does the agent find alternative connectors when one fails?

### Test degradation scenarios

- **Rate limits**: what does the agent see when the API throttles? A clear message like
  "quota exhausted" lets the agent fall back to alternatives. A raw exception causes loops.
- **Partial API access**: tiered APIs may return 403 for premium endpoints. The agent
  should get a clear message, not an opaque stack trace.
- **Empty search results**: raise `ValueError` with the search params so the agent can
  adjust its query rather than assuming the connector is broken.

### Verify `to_llm()` output

After implementing connectors, call `CONNECTORS.to_llm()` and read the output. This is
exactly what the agent sees. Check that:
- Each connector's description includes workflow chaining hints
- `Returns:` lists the OutputConfig columns (so the agent knows what to expect)
- Parameter descriptions are specific enough to use without guessing
- `result_type="text"` connectors are clearly marked

## Exports Checklist

```python
# Every connector module should export:
CONNECTORS = Connectors([
    my_source_search,
    my_source_fetch,
    my_source_detail,
    ...
])

# If some connectors need API keys and others don't:
FETCH_CONNECTORS = Connectors([...])  # subset that needs bind_deps
```

In `__init__.py`, use `bind_deps(api_key=key)` for connectors requiring API keys,
and add key-free connectors directly:

```python
result = result + MY_SOURCE_FETCH.bind_deps(api_key=key)  # needs key
result = result + MY_OTHER_SOURCE  # no key needed
```

## File Size

Aim for < 400 lines per connector module. If a provider has 15+ connectors, the file
will be ~500-600 lines — that's fine for a single-provider module. Don't split into
multiple files unless you have genuinely separate concerns (e.g., separate SDK clients).
