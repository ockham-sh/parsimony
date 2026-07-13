<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-brand-dark.png" />
  <img src="docs/assets/parsimony-brand-light.png" alt="parsimony" width="460" />
</picture>

**Typed connectors and a portable hybrid-search catalog for financial data.**

[![PyPI](https://img.shields.io/pypi/v/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)

</div>


<p align="center">
  <img src="docs/assets/parsimony-hero.gif" alt="parsimony: decorate a function with @connector, bind the operator's API key, and call it — the result comes back as a typed Result carrying both the data and full provenance (source, call-time params, fetch time), with the bound api_key kept out of the record." width="900" />
</p>

---

## What it is

Parsimony is the kernel of a connector ecosystem for financial and economic data. It gives you two things:

1. **A connector model.** A connector is a plain Python function plus metadata. You fetch by calling it (`result = conn(series_id="GDP")`); the framework wraps the raw `DataFrame` your function returns into a typed `Result` with automatic provenance. Operational failures surface through a small, agent-facing error taxonomy (`UnauthorizedError`, `RateLimitError`, `ProviderError`, …) instead of raw HTTP exceptions.

2. **A hybrid-search catalog.** When you need to *discover* what data exists — search across thousands of series codes, titles, and descriptions — Parsimony ships a portable `Catalog` that combines BM25 keyword indexes and FAISS vector indexes, fused into a single ranked result and snapshot-able to disk or a Hugging Face dataset.

The kernel ships **zero connectors in-tree**. Each connector (e.g. `parsimony-fred`, `parsimony-sdmx`) is a separate `parsimony-<name>` distribution, discovered at runtime through the `parsimony.providers` entry-point group; the official library of them is **[parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)**. `import parsimony` stays cheap: the heavy catalog stack (torch, FAISS, sentence-transformers) is an **optional** install and loads lazily on first use.

## Key features

- **Connectors are just functions.** The function's own parameters *are* the connector's call surface — no separate params schema to wire up.
- **Typed, provenance-tagged results.** Return a raw `pandas` `DataFrame`; the framework builds the `Result` and a `Provenance` record (source, description, UTC fetch time, call params).
- **Declarative output specs.** `OutputSpec` + `Column` + `ColumnRole` (`DATA`/`KEY`/`TITLE`/`METADATA`) annotate what a connector returns — the data itself is never renamed or coerced — and drive the `result.to_entities()` catalog projection.
- **Agent-facing error taxonomy.** A single `ConnectorError` base with subclasses whose default messages embed retry directives — built for autonomous agent loops, not just humans.
- **Credential injection by composition.** `bind(api_key=...)` fixes a parameter, removes it from the call surface, and keeps it out of provenance.
- **HTTP transport helpers.** `HttpClient` plus `fetch_json` / `fetch_csv` / `fetch_text` (and `check_status`) translate `httpx` errors into the typed taxonomy — including a non-JSON/non-CSV body surfacing as `ParseError` — with secret redaction in logs and transient retry built in.
- **Plugin discovery + conformance.** Plugins register under `parsimony.providers`; `parsimony list` enumerates them and `--strict` runs a conformance suite.
- **Hybrid search.** BM25 + FAISS vector indexes fused with Z-score / min-max / RRF rankers, with adaptive FAISS index selection by row count.
- **Swappable embedders.** Local PyTorch, faster ONNX (int8), or hosted (litellm) — each behind its own optional extra.
- **Lean default install.** The mandatory footprint is `pydantic`, `pandas`, `pyarrow`, `httpx`, `platformdirs`. No torch, no FAISS unless you ask for them.

## Install

```bash
pip install parsimony-core               # kernel: connectors, results, errors, transport
pip install 'parsimony-core[catalog]'    # + the hybrid-search Catalog (pulled by catalog-backed connectors)
pip install parsimony-fred parsimony-sdmx  # individual connectors (each its own distribution)
```

The default install pulls only the lean kernel deps. The Catalog and its embedders are opt-in:

| Extra | Adds | Unlocks |
|---|---|---|
| `catalog` | `faiss-cpu`, `rank-bm25`, `sentence-transformers`, `huggingface_hub` | `Catalog`, `BM25Index`, `VectorIndex`, `HybridIndex`, the default local embedder, and the `hf://` snapshot loader |
| `standard-onnx` | `catalog` + `optimum[onnxruntime]`, `onnxruntime` | `OnnxEmbedder` — 2–3× faster CPU inference via int8 quantization, ~4× smaller on disk |
| `litellm` | `litellm` | `LiteLLMEmbeddingProvider` — hosted embeddings (OpenAI, Gemini, Cohere, Voyage, Bedrock) |
| `all` | `catalog` + `standard-onnx` + `litellm` | Everything |
| `dev` | pytest, ruff, mypy, pip-audit (+ `catalog`, `litellm`) | The full test/lint toolchain |

Requires Python 3.11+.

## Quickstart

The intended first run is through your coding agent. Install parsimony and the connectors you want, then install the **agent skill** — a single `SKILL.md` that teaches Claude Code / Cursor / Codex how to drive the library: the discover/search/fetch idiom, runtime connector discovery, the catalog query DSL, the typed errors, and the silent-truncation traps. Because the library isn't in any model's training data, the skill is how an off-the-shelf agent learns it.

```bash
pip install parsimony-core parsimony-fred parsimony-sdmx

npx skills add ockham-sh/parsimony          # installs the agent skill
```

> No Node? `uvx npx-skills add ockham-sh/parsimony` does the same without a global Node install. It's a standard Agent Skills repo — the skill lives at [`skills/parsimony/SKILL.md`](skills/parsimony/SKILL.md), so any installer works (`gh skill install`, etc.).

Or just tell your agent *"install the parsimony skill from ockham-sh/parsimony"*. Then ask for data — *"compare euro-area and US inflation since 2015"* — and it searches the catalog, fetches the right series truncation-guarded, and hands back typed results instead of hand-rolling `requests`.

### Use an installed connector directly

You don't need an agent — every connector is a plain importable function. Connectors are separate distributions; with `parsimony-fred` installed:

```python
from parsimony_fred import fred_fetch, fred_search

# fred reads FRED_API_KEY from the environment (or bind it: fred_fetch.bind(api_key=...))
hits = fred_search(search_text="US unemployment rate")
print(hits.to_llm())             # governed, bounded preview — columns vary by connector
print(hits.data.head())          # .data is the canonical payload (a DataFrame here)

result = fred_fetch(series_id="UNRATE")   # -> Result
print(result.data.tail())        # tabular payload; .frame / .df are convenience aliases
print(result.provenance.source)  # 'fred_fetch'
```

### Define your own connector

A connector is a plain `def` decorated with `@connector`. Return raw data — the framework builds the typed envelope.

> `parsimony-core` is just the framework. Production connectors live in **[parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)** — to ship your own, package it as a `parsimony-<name>` distribution that registers under the `parsimony.providers` entry point (see the [authoring guide](docs/plugins/authoring.md)). The snippets below show the mechanism.

```python
import pandas as pd
from parsimony import connector

@connector
def my_data_source(category: str) -> pd.DataFrame:
    """Return sample rows for a category (replace with a real HTTP call)."""
    return pd.DataFrame(
        {
            "code": ["A1", "A2", "A3"],
            "label": [f"{category} - Alpha", f"{category} - Beta", f"{category} - Gamma"],
            "score": [0.95, 0.87, 0.73],
        }
    )

result = my_data_source(category="widgets")
print(result.data)               # the canonical payload (a DataFrame here)
print(result.provenance.source)  # 'my_data_source'
```

Annotate fetch parameters whose legal values come from a catalog namespace with `Namespace(...)` inside `typing.Annotated` — the framework surfaces this on connector cards as a symbology hint for agents and humans:

```python
from typing import Annotated

from parsimony import Namespace, connector

@connector
def fetch_series(series_id: Annotated[str, Namespace("fred")]) -> pd.DataFrame:
    """Fetch one FRED series by id."""
    ...
```

For catalog-backed flows, attach an [`OutputSpec`](docs/connectors/results.md) declaring each column's role. The spec is annotation only — your function returns already-shaped data, and the framework never renames or coerces it:

```python
from parsimony import Column, ColumnRole, OutputSpec, connector

CUSTOM_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="my_source"),
        Column(name="label", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

@connector(output=CUSTOM_OUTPUT, tags=["custom"])
def my_data_source_schematized(category: str) -> pd.DataFrame:
    """Return sample rows for a category (replace with a real HTTP call)."""
    return pd.DataFrame(
        {
            "code": ["A1", "A2", "A3"],
            "label": [f"{category} - Alpha", f"{category} - Beta", f"{category} - Gamma"],
            "score": [0.95, 0.87, 0.73],
        }
    )
```

Connectors **must** be synchronous (`def`, not `async def`) and **must** have a description (docstring or `description=`). They **must** return raw data — returning a `Result` or `(data, properties)` tuple raises `TypeError`. Provider facts belong in `DataFrame` columns, never in `provenance.properties` (which is framework-only).

### Compose connectors into a bundle

`Connectors` is an immutable, composable collection. Combine bundles with `+`, then run the real **discover → search → fetch** idiom: search returns candidate ids, you pick one, then fetch — never hand-type an opaque code. `bind()` scopes a credential to only the connectors that accept it (`conns.bind(api_key=…)` reaches `fred_*`; `sdmx` ignores it — `fred` also reads `FRED_API_KEY` from the environment).

```python
from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX

conns = FRED + SDMX
print(conns.names())

# US — FRED: search → fetch
hits = conns["fred_search"](search_text="US consumer price index all items city average")
print(hits.data[["id", "title"]].head())     # inspect candidates, pick the id -> CPIAUCNS
us = conns["fred_fetch"](
    series_id="CPIAUCNS",                     # the id you picked from the search
    observation_start="2013-01-01",
    observation_end="2025-12-31",
).data                                        # date + value (monthly index), plus series metadata

# Euro area — SDMX (Eurostat): datasets_search → series_search → fetch
flows = conns["sdmx_datasets_search"](
    query="HICP all-items monthly annual rate of change inflation",
    agency="ESTAT", limit=10,
)
print(flows.data[["flow_id", "title"]])       # inspect + pick PRC_HICP_MANR

series = conns["sdmx_series_search"](
    agency="ESTAT", dataset_id="PRC_HICP_MANR",
    query="euro area all items annual rate of change",
    limit=10,
)
print(series.data[["key", "title"]])          # -> M.RCH_A.CP00.EA19 (2015-2022), M.RCH_A.CP00.EA20 (2023->)
# Eurostat gives the euro area only as fixed compositions — stitch EA19 (->2022) + EA20 (2023->)
# so the composition change (Croatia joined the euro in 2023) stays explicit, not silently chained.
ea19 = conns["sdmx_fetch"](
    dataset_ref="ESTAT-PRC_HICP_MANR", series_ref="M.RCH_A.CP00.EA19",
    start_period="2015", end_period="2022",
).data
ea20 = conns["sdmx_fetch"](
    dataset_ref="ESTAT-PRC_HICP_MANR", series_ref="M.RCH_A.CP00.EA20",
    start_period="2023", end_period="2026",
).data                                        # TIME_PERIOD, value, + decoded dimension columns

print(us.tail())
print(ea20.tail())
```

### Build an HTTP connector with the transport helpers

The transport layer maps `httpx` errors (`401`/`402`/`429`/`5xx`/timeout) into the typed `parsimony.errors` taxonomy and redacts secrets in logs:

```python
import pandas as pd
from parsimony import Column, ColumnRole, OutputSpec, connector
from parsimony.transport.helpers import fetch_json, make_api_key_client

OUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="acme"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

@connector(output=OUT, secrets=("api_key",))
def acme_fetch(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch an ACME time series by id."""
    http = make_api_key_client("https://api.acme.test", provider="acme", api_key=api_key)
    payload = fetch_json(http, path=f"series/{series_id}", op_name="series")
    return pd.DataFrame(payload["observations"])
```

`secrets=("api_key",)` strips that parameter from provenance even when passed at call time, and validates the name is a real parameter at decoration.

## Catalog and hybrid search

When you need to discover *which* series exist, build a `Catalog` over `Entity` rows. Each `Entity` is identified by `(namespace, code)` and carries a `title` plus free-form `metadata`. Field indexes (`BM25Index`, `VectorIndex`, `HybridIndex`) are keyed by a logical search surface; queries are either broad plain text (routed to the default field) or structured `field: value` clauses.

> The Catalog stack requires `pip install 'parsimony-core[catalog]'` (catalog-backed connector packages declare this dependency). Importing `Catalog`/`BM25Index`/`VectorIndex` from `parsimony` always works (lazy PEP 562), but `build()` raises an actionable error on first use without the extra.

```python
from parsimony import BM25Index, Catalog, Entity, HybridIndex, VectorIndex
from parsimony.ranking import ZScoreFusion

entries = [
    Entity(namespace="fred", code="GDPC1", title="Real Gross Domestic Product",
           metadata={"description": "Inflation-adjusted US output and real growth."}),
    Entity(namespace="fred", code="UNRATE", title="Unemployment Rate",
           metadata={"description": "Monthly civilian unemployment rate."}),
]

catalog = Catalog(
    "macro",
    indexes={
        "code": BM25Index(),
        "title": HybridIndex(
            components=[BM25Index(), VectorIndex()],  # VectorIndex() defaults to all-MiniLM-L6-v2
            fusion=ZScoreFusion(weights={"bm25": 0.5, "vector": 1.0}),
        ),
    },
    default_field="title",
)
catalog.set_entities(entries)
catalog.build()  # MUST build before search/save

hits = catalog.search("inflation adjusted output", limit=5)   # broad
print([(h.code, round(h.score, 3)) for h in hits])

hits2 = catalog.search("code: UNRATE", limit=1)                   # structured, exact match
print(hits2[0].title)

catalog.save("file:///tmp/macro-catalog", builder="readme-example")
reloaded = Catalog.load("file:///tmp/macro-catalog")
print(len(reloaded))
```

A few important details, grounded in the code:

- **The catalog must be built before search or save.** Call `catalog.build()` after construction and after any `set_entities` / `set_indexes` / `delete_many` — `search()` and `save()` raise `ValueError` until rebuilt.
- **`search(query, limit, *, namespaces=None)`** — `limit` is positional and required.
- **Default index policy.** `Catalog(name, indexes=None)` auto-creates BM25 indexes for `code`, `title`, and every metadata key at `build()` time. Pass an explicit `indexes` dict for full control.
- **Exact value matches win.** A case-insensitive exact value match short-circuits to a sentinel score that dominates fuzzy BM25/cosine scores — ideal for code lookups.
- **Portable, integrity-checked snapshots.** A saved catalog is a directory of Parquet (zstd) files plus `meta.json`; `Catalog.load` recomputes a content SHA-256 over the data files and rejects a mismatch (an anti-corruption check, not a signature — trust the source of any snapshot you load). Only `file://` (or a bare path) and `hf://` (Hugging Face dataset) schemes are wired in. An `hf://` URL may pin a revision — `hf://<org>/<repo>@<commit-sha>` — for a reproducible, tamper-resistant remote load; without one it tracks the dataset's default branch.

### Building entities from connector output

A role-annotated tabular result projects into entities via `result.to_entities()`, which returns the catalog-ready `list[Entity]` (`catalog.set_entities(result.to_entities())`): rows sharing the `KEY` value become one `Entity`, in first-appearance order. The single `KEY` column (which must declare a `namespace`) becomes the `code`, `TITLE` becomes the `title`, and `METADATA` columns become `metadata`; role invariants are validated at projection time, never during connector execution. This is how an enumerator connector feeds the catalog. For a bare DataFrame outside a `Result`, `parsimony.catalog.source.entities_from_raw(df, output_spec)` runs the same projection.

### Swapping the embedder

```python
from parsimony import BM25Index, HybridIndex, VectorIndex
from parsimony.embedder import LiteLLMEmbeddingProvider, OnnxEmbedder

# Faster CPU path — requires parsimony-core[standard-onnx]
onnx_title = HybridIndex(components=[BM25Index(), VectorIndex(embedder=OnnxEmbedder())])

# Hosted embeddings — requires parsimony-core[litellm]; you supply model + dim
hosted = LiteLLMEmbeddingProvider(model="text-embedding-3-small", dimension=1536)
hosted_title = HybridIndex(components=[BM25Index(), VectorIndex(embedder=hosted)])
```

Any object satisfying the `EmbeddingProvider` protocol works. An embedder's identity `(model, dim, normalize)` is persisted in a snapshot; `VectorIndex.load` rejects a mismatched embedder, so rebuild if you change models.

## Core concepts

### Connector / Connectors

A `Connector` is a frozen dataclass wrapping a synchronous function plus metadata. Call it to fetch.

- `conn(**kwargs)` → `Result` (raw `__call__`; `call_raw(**kwargs)` returns the unwrapped function output).
- `conn.bind(**kwargs)` → a new connector with parameters fixed and removed from `exposed_signature`.
- `conn.describe()` / `conn.to_llm()` → human- and LLM-readable cards.

`Connectors([...])` is the immutable collection: `+` to combine (rejects duplicate names), `bundle[name]` / `bundle.get(name)` to index, `names()`, `filter(predicate, tags=[...])`, `bind(**kwargs)`, `describe()`, `to_llm()`, and `to_entities()` (one catalogable `Entity` per connector, namespace `"connectors"` by default) for explicit catalog-backed connector discovery. There is no `merge` classmethod — use `+`. There is no collection-level `search()` — for free-text discovery over a large bundle, build a `Catalog` over `to_entities()` and search that.

### Three decorators

| Decorator | Purpose | Output contract |
|---|---|---|
| `@connector` | General-purpose fetch | `output` optional |
| `@enumerator(output=...)` | Entity/series discovery | exactly one `KEY` (with `namespace`), at least one `TITLE`, **no** `DATA`; function must annotate a `pd.DataFrame` return; returned columns strictly validated |
| `@loader(output=...)` | Observation-data fetch | exactly one `KEY` (with `namespace`), at least one `DATA`, **no** `TITLE`/`METADATA` |

### Result / Provenance

`Result` is the single envelope for every connector output. Its canonical payload is `result.data: Any` — a `pandas` `DataFrame` for tabular fetches, but it may also be a `Series`, scalar, `dict`, `str`, or `bytes` — and it is exactly what the connector returned; the framework never renames, coerces, or validates it. `result.is_tabular` is `True` exactly when `data` is a `DataFrame`; in that case `result.frame` (alias `result.df`) returns it (and raises if the result is not tabular), and an optional `result.output_spec` (an `OutputSpec`) carries column roles, namespaces, and `exclude_from_llm_view` governance. When the spec declares a namespaced `KEY` column, `result.to_entities()` projects the frame into entity records (see "Building entities from connector output" above).

- `result.to_llm()` renders a governed, bounded preview — honest row/column counts and the first rows for tabular payloads, a structural type/shape preview for opaque ones. Use it for LLM context.
- `result.text` coerces the payload to a string.
- Tabular results round-trip through Arrow/Parquet (`to_arrow`/`from_arrow`/`to_parquet`/`from_parquet`), embedding provenance and the output spec in table metadata; `Result.from_dataframe(df)` builds a tabular result with no spec.

`result.provenance` is a framework-built `Provenance` (`source`, `source_description`, `params`, `fetched_at`, plus a framework-only `properties` map) — connectors never construct it.

### Error taxonomy

All operational failures derive from `ConnectorError` (carries `.provider`). Default messages embed agent-loop directives (e.g. "DO NOT retry"). Programmer errors stay as `TypeError`/`ValueError`/`ValidationError`.

| Error | Maps to | Notable fields |
|---|---|---|
| `UnauthorizedError` | 401 / 403 | `env_var` hint |
| `PaymentRequiredError` | 402 / plan restriction | — |
| `RateLimitError` | 429 | `retry_after` (rejects values > 86400), `quota_exhausted` |
| `ProviderError` | 5xx / 4xx / timeout | `status_code` (408 for timeouts) |
| `EmptyDataError` | 200, no rows | `query_params` |
| `ParseError` | 200, unparseable | — |
| `InvalidParameterError` | invalid call-time args | — |
| `CatalogNotFoundError` | missing catalog bundle | — |

### The `parsimony.providers` plugin contract

A connector plugin is a `parsimony-<name>` distribution that:

1. Exports a module-level `CONNECTORS` (a `Connectors` instance).
2. Registers under the `parsimony.providers` entry-point group.

```python
# my_plugin/__init__.py
from parsimony import Connectors
from .connectors import acme_fetch, enumerate_demo

CONNECTORS = Connectors([acme_fetch, enumerate_demo])
```

```toml
# pyproject.toml of the parsimony-acme distribution
[project.entry-points.'parsimony.providers']
acme = "my_plugin"
```

Consumers discover plugins via `parsimony.discover`:

```python
from parsimony import discover

providers = list(discover.iter_providers())  # metadata only, no imports
bundle = discover.load("acme")               # strict: raises LookupError if not installed
everything = discover.load_all()             # forgiving: logs and skips broken plugins
```

`iter_providers()` raises `RuntimeError` if two installed distributions register the same provider name.

### Conformance testing for plugin authors

`parsimony.testing` gives plugin authors a conformance suite. Subclass `ProviderTestSuite` in a pytest file to inherit conformance plus an installation check, or call `assert_plugin_valid(module)` procedurally:

```python
# tests/test_conformance.py in the plugin repo
from parsimony.testing import ProviderTestSuite
import my_plugin

class TestMyPlugin(ProviderTestSuite):
    module = my_plugin
    entry_point_name = "acme"   # also verifies registration under parsimony.providers
```

The same checks run from the shell via `parsimony list --strict`.

## The `parsimony` CLI

The package installs a `parsimony` console script (`parsimony = parsimony.cli:main`) with three verbs:

```bash
# Enumerate installed plugins (name, version, connector count)
parsimony list
parsimony list --json
parsimony list --strict          # import each plugin, run conformance; non-zero exit on failure

# Inspect or clear the global cache (subdirs: catalogs, models, connectors, staging)
parsimony cache path
parsimony cache info [--json]
parsimony cache clear [--subdir NAME] [--yes]
```

The cache root resolves through `PARSIMONY_CACHE_DIR`, defaulting to `platformdirs.user_cache_dir("parsimony")`. Hugging Face catalog snapshots land under the `catalogs` subdir; ONNX models under `models`; connector-owned scratch under `connectors`; and per-provider catalog build staging (`staging_dir(provider)`) under `staging`.

| Env var | Purpose |
|---|---|
| `PARSIMONY_CACHE_DIR` | Override the cache root (must point at a user-private directory; world/group-writable dirs are refused) |
| `PARSIMONY_FAISS_IVF_THRESHOLD` | Row count at/above which FAISS index construction switches to `IndexIVFFlat` (default 500000) |

## Where it fits

Parsimony is the foundation that the rest of the parsimony / Ockham ecosystem builds on. The dependency direction is one-way:

```
parsimony-core   →   parsimony-<name> connectors   →   parsimony-agents   →   applications
```

- This package (`parsimony-core`) depends on nothing else in the ecosystem.
- The connectors are **[parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)** — one `parsimony-<name>` distribution per provider, each depending on `parsimony-core` and registering through the `parsimony.providers` entry point.
- The agent layer is **[parsimony-agents](https://github.com/ockham-sh/parsimony-agents)** — an AI agent framework that, given these connectors, answers data questions by writing and executing Python and returns typed artifacts (datasets, charts, reports), not prose.

## Development

```bash
make install     # uv pip install -e ".[dev]"
make test        # pytest tests/ -x --tb=short -q
make test-cov    # pytest with coverage (--cov-fail-under=80)
make lint        # ruff check + ruff format --check (parsimony/ tests/ examples/)
make format      # ruff format + ruff check --fix
make typecheck   # mypy parsimony/
make check       # lint + typecheck + test
```

Tests enforce an 80% coverage floor. Two pytest markers gate heavier tests:

- `integration` — hits live APIs (may be slow, requires env vars).
- `slow` — heavy local tests; opt-in.

To exercise the full FAISS + BM25 + sentence-transformers paths during development, install with the `catalog` extra (the `dev` extra already pulls it in).

## License

Apache-2.0. See [LICENSE](LICENSE).
