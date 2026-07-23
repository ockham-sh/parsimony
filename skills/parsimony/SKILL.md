---
name: parsimony
description: Use when discovering or fetching macro, financial, economic, or official-statistics data (FRED, ECB, Eurostat, BLS, central banks, market data, SEC filings) from Python via the parsimony library, or when wrapping a not-yet-covered data source as a reusable connector, instead of hand-rolling one-off requests scripts against provider APIs.
---

# parsimony

`parsimony` is the kernel of a connector ecosystem for financial and economic data. Each data
provider (FRED, ECB/Eurostat via SDMX, BLS, central banks, market data, …) is a separately
installed `parsimony-<name>` package exposing **connectors**: plain Python functions you call to
fetch or search. A call returns a typed `Result` carrying the payload plus **provenance** (which
connector, the call params, the fetch time); operational failures come back as a small typed
error taxonomy built for agent loops, not raw HTTP exceptions. A portable hybrid-search
**catalog** lets you discover which series exist when you don't know the code.

Reach for a parsimony connector instead of hand-rolling `requests` against a provider API: the
connector already encodes that provider's API shape, auth, pagination (including silent row
caps), and parsing, and hands back a typed result. That is the point of the library.

## Install

```bash
pip install parsimony-core                 # the kernel: discovery, results, errors
pip install 'parsimony-core[catalog]'      # + hybrid search (needed for catalog search)
pip install parsimony-fred parsimony-sdmx  # one distribution per connector — install what you need
```

## The core idiom

```python
from parsimony import discover

connectors = discover.load_all()                 # every installed parsimony-* connector, as one bundle
result = connectors["fred_fetch"](series_id="UNRATE")

result.raw                   # the payload — a pandas DataFrame for a series fetch
result.provenance.source     # 'fred_fetch' — every result records where it came from
result.to_llm()              # a bounded, schema-aware text preview to drop into context
```

- `result.raw` is the payload (a `DataFrame` for a tabular fetch; some connectors return other
  shapes). `result.provenance` carries `source`, `params`, and `fetched_at`. Prefer
  `result.to_llm()` for a governed, length-bounded preview rather than dumping the raw frame.
- `discover.load_all()` is forgiving (logs and skips broken or uninstalled plugins) — unless
  *every* installed provider fails to load, which raises `RuntimeError` rather than hand back an
  empty bundle. `discover.load("fred")` is strict (raises `LookupError` if not installed);
  `discover.iter_providers()` lists installed packages without importing them.

Each task runs four discovery moves before the fetch: **bootstrap** the framework, **route** to a
connector, **inspect** it, **find** the id.

## 0. Bootstrap — make sure the framework itself is present

Confirm `parsimony-core` is importable in the active environment before anything else:

```python
import importlib.util
if importlib.util.find_spec("parsimony") is None:
    ...  # not installed — install it below before continuing
```

If it is missing, install it into the **active** environment — the same interpreter or virtualenv
the task is already running in, not a new one — with whichever of `python -m pip install
parsimony-core` or `uv pip install parsimony-core` matches how the project manages dependencies.
Never add `--break-system-packages` to force past a refusal. If the install fails with a PEP 668
"externally-managed-environment" guard, or with a permissions error, **stop and tell the user**
that the environment needs a virtualenv (or an explicit override they choose to run themselves)
rather than forcing the install. Re-import to confirm before moving on.

## 1. Route — installed → registry → author

Which connectors exist depends on what is pip-installed in **this** environment, so read it live
instead of assuming a list. This step has three escalating moves — try each in order and stop at
the first that covers the source.

### 1a. Installed and project-local

```python
connectors = discover.load_all()
print(connectors.describe())               # one line per connector: name + what it does
```

If the project keeps a local connector library (a `connectors/` package exporting a `CONNECTORS` bundle — see [the authoring guide](references/authoring.md)), merge it into the same bundle first:
`connectors = discover.load_all() + LOCAL`. Local and installed connectors then behave
identically everywhere below.

Pick the connector whose description matches the domain you need — the description names the
institution (e.g. "Banco de España", "Swiss National Bank"), which the short code (`bde`, `snb`)
alone does not. Most providers separate **discovery from retrieval**: one connector searches or
lists what exists, another fetches once you know the id. **Which is which is in the description,
not the name** — read the description, don't pattern-match the verb. The shape varies (one search
+ one fetch; resolve-an-id-then-fetch; a two-stage search; several kinds of search), so let the
descriptions tell you. Search-then-fetch is the *workflow* to keep in mind, not a naming rule.

### 1b. Registry — an existing package not yet installed here

If none of the installed or local connectors covers the provider you need — the environment may
simply not have it yet — query the official registry of installable `parsimony-<name>` packages
programmatically instead of guessing a name or reading a repo by hand:

```python
from parsimony.registry import list_available, RegistryError

try:
    registry = list_available()
except RegistryError as exc:
    ...  # neither the live registry nor the bundled snapshot loaded — see the exception message

for c in registry.connectors:
    print(c.package, c.provider, c.entry_point, c.connector_count, c.keyless, c.requires)
```

(`parsimony list --available` on the command line does the same thing for a quick human check.)
`list_available()` always tries the canonical `https://parsimony.dev/connectors.json` endpoint
first and falls back to a read-only snapshot bundled in this release if that fetch fails;
`registry.source` (`"remote"` or `"bundled"`) says which one you got, and a warning is logged when
it's the bundled one. Surface that fallback status rather than staying silent about it.

Match on `provider` (and `entry_point`) the same way you'd read a connector's description — by the
institution or source it names, not just the short code. Once you've picked a match:

1. Install it into the active environment the same safe way as bootstrap above (`python -m pip
   install <package>` or `uv pip install <package>`; never `--break-system-packages`; stop and ask
   the user on a PEP 668 or permissions failure).
2. If the package is not `keyless`, resolving a credential also needs the user — ask them to set
   the env var(s) named in `c.requires` (e.g. `FRED_API_KEY`) rather than guessing a name.
3. Reload and verify: re-run `discover.load_all()` (or `discover.load(entry_point)`) and confirm
   the new provider now appears before calling any of its connectors.

Otherwise ask the user only when the install or a credential genuinely needs their input — not to
confirm a choice the registry data already answers.

### 1c. Author — no registry entry covers the source

Only when no installed, local, *or* registry entry covers the source at all, author a
project-local connector: read [the authoring guide](references/authoring.md) for the contract and
the conventions. Author and test the connector first, then run the analysis through it.

## 2. Inspect a connector before you call it

A connector is a plain function; its parameters **are** the call surface. Inspect it before every
call — never invoke one you have not read:

```python
print(connectors["sdmx_datasets_search"].describe())
```

`.describe()` is a connector's own introspection API — it gives the parameters (required/optional,
with namespace hints), the output schema (column names and roles) when the connector declares one,
and the full description. Knowing the params and outputs up front is what keeps you from guessing
an argument name — or guessing an id you don't actually have.

## 3. Find the code — search the catalog

Series ids are opaque (`UNRATE`; SDMX keys like `D.USD.EUR.SP00.A`). When you don't know the
exact id, **search before fetching**. Call the provider's search connector with a text query,
then read the returned result to pick the id:

```python
hits = connectors["sdmx_datasets_search"](query="euro area unemployment rate", limit=5)
print(hits.to_llm())         # bounded preview: the result's columns + first rows
ids = hits.raw               # the full result frame; columns vary by connector — read them, don't assume
```

Every ranked catalog search ends with the same three columns — read them before trusting row
order, not after: `coverage` is the fraction of the query's tokens a row's indexed values
literally satisfy (all-or-nothing per value) — `1.0` is a verified fact, not a guess, and explains
a row ranked above a higher `score`; `score` is similarity relative to *this query's* best hit, not
comparable across queries or catalogs; `matched` says whether that evidence was `lexical`,
`semantic`, or `both` — an all-`semantic` page means nothing you typed was found verbatim anywhere,
so rephrase rather than trust the order. A search call returns a relevance-ranked top-N, not the
whole catalog; to read a slice exhaustively instead, drop `query=` and pass `filter=` (an exact AND
on the result columns, e.g. `{"code": "..."}`) with a higher `limit` — that enumerates from the
same cached catalog, no re-crawl.

A fetch parameter shown with a `namespace=` hint in `.describe()` means its legal values come
from that catalog namespace — search there first.

A first search pays two one-time costs, both of them normal — **neither is a hang, and retrying
only starts them over**:

- **The catalog downloads** (typically a few seconds). Paid once per catalog, not once per
  session: a provider may expose many catalogs, and each is fetched the first time you search it,
  so a task spanning several pays it several times.
- **The embedding model loads** on the first semantic search in each process, and downloads once
  per machine (~90 MB), which is the slower of the two on a fresh install.

Both are then cached and every later search is instant. The cache persists across sessions. If a
catalog has been republished (or a cached copy is corrupt), bust it from the shell and re-run —
the next search re-downloads it. Clearing is targeted: list what's cached, then drop one repo:

```bash
parsimony cache info --repos                       # cached catalogs, by Hugging Face repo + size
parsimony cache clear --repo parsimony-dev/sdmx    # drop just that provider's catalogs
```

To tell a slow step apart from a genuine stall rather than wait it out, turn on logging before the
call. Each catalog fetch, model download, and model load logs a line when it starts and one when
it finishes, with size and elapsed time — so a start with no finish is still running, not stuck:

```python
import logging; logging.basicConfig(level=logging.INFO)
```

## Errors (every failure is typed; messages carry the next action)

All operational failures derive from `ConnectorError` (has `.provider`); default messages embed
agent-loop directives. Programmer mistakes stay as `TypeError` / `ValueError`.

| Error | Means | Do in the loop |
|---|---|---|
| `UnauthorizedError` | 401/403; has `env_var` hint | Set the named key env var. **Do not retry** as-is. |
| `PaymentRequiredError` | 402 / plan restriction | Stop; the data needs a paid plan. Don't retry. |
| `RateLimitError` | 429; has `retry_after`, `quota_exhausted` | Back off `retry_after` seconds, then retry (unless `quota_exhausted`). |
| `ProviderError` | 5xx / 4xx / timeout; has `status_code` (408=timeout) | Transient — retry with backoff a few times. |
| `EmptyDataError` | 200 but no rows; has `query_params` | Query was valid but empty — adjust params, don't hammer. |
| `ParseError` | 200 but unparseable body | Provider returned something unexpected; don't blindly retry. |
| `InvalidParameterError` | bad call-time args | Fix the arguments; retrying the same call won't help. |
| `CatalogNotFoundError` | catalog snapshot missing | Install the connector's catalog extra / data. |

## Credentials

Keyless connectors just work. Keyed ones take the key as a normal parameter — `bind` it once so it
stays out of provenance and off the call surface:

```python
fred = connectors["fred_fetch"].bind(api_key="...")   # or bind the whole bundle: connectors.bind(api_key=...)
result = fred(series_id="UNRATE")
```

---

In short: **bootstrap** `parsimony-core` if it's missing, **route** with
`discover.load_all().describe()` — falling back to `parsimony.registry.list_available()` and a
safe install when nothing installed covers it — **inspect** the chosen connector with
`connectors["x"].describe()`, **search** the catalog for the opaque id, then **fetch**. And when
no connector — installed, local, or in the registry — covers the source, **author** a local one
([authoring guide](references/authoring.md)).
