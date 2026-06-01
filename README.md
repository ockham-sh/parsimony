<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-brand-dark.png" />
  <img src="docs/assets/parsimony-brand-light.png" alt="parsimony" width="460" />
</picture>
</div>

**Typed connectors and a portable hybrid-search catalog for financial data.**

[![PyPI](https://img.shields.io/pypi/v/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/pypi/pyversions/parsimony-core.svg)](https://pypi.org/project/parsimony-core/)

```bash
pip install parsimony-core
```

Distribution: `parsimony-core` on PyPI. Import name: `parsimony`.

<p align="center">
  <img src="docs/assets/parsimony-hero.gif" alt="parsimony: decorate an async function with @connector, bind the operator's API key, and call it — the result comes back as a typed Result carrying both the data and full provenance (connector, source, call-time args, fetch time), with the bound api_key kept out of the record." width="900" />
</p>

---

## What it is

Parsimony is the kernel of a connector ecosystem for financial and economic data. It gives you two things:

1. **A connector model.** A connector is an `async` Python function plus metadata. You fetch by awaiting it (`await conn(series_id="GDP")`); the framework wraps the raw `DataFrame` your function returns into a typed `Result`/`TabularResult` with automatic provenance. Operational failures surface through a small, agent-facing error taxonomy (`UnauthorizedError`, `RateLimitError`, `ProviderError`, …) instead of raw HTTP exceptions.

2. **A hybrid-search catalog.** When you need to *discover* what data exists — search across thousands of series codes, titles, and descriptions — Parsimony ships a portable `Catalog` that combines BM25 keyword indexes and FAISS vector indexes, fused into a single ranked result and snapshot-able to disk or a Hugging Face dataset.

The kernel ships **zero connectors in-tree**. Each connector (e.g. `parsimony-fred`, `parsimony-sdmx`) is a separate `parsimony-<name>` distribution, discovered at runtime through the `parsimony.providers` entry-point group. `import parsimony` stays cheap: the heavy catalog stack (torch, FAISS, sentence-transformers) is an **optional** install and loads lazily on first use.

## Key features

- **Connectors are just async functions.** The function's own parameters *are* the connector's call surface — no separate params schema to wire up.
- **Typed, provenance-tagged results.** Return a raw `pandas` `DataFrame`; the framework builds the `Result`/`TabularResult` and a `Provenance` record (source, description, UTC fetch time, call params).
- **Declarative output schemas.** `OutputConfig` + `Column` + `ColumnRole` (`DATA`/`KEY`/`TITLE`/`METADATA`) shape results *and* drive catalog-entity extraction.
- **Agent-facing error taxonomy.** A single `ConnectorError` base with subclasses whose default messages embed retry directives — built for autonomous agent loops, not just humans.
- **Credential injection by composition.** `bind(api_key=...)` fixes a parameter, removes it from the call surface, and keeps it out of provenance.
- **HTTP transport helpers.** `HttpClient`, `fetch_json`, and `map_http_error` translate `httpx` errors into the typed taxonomy, with secret redaction in logs and transient retry built in.
- **Plugin discovery + conformance.** Plugins register under `parsimony.providers`; `parsimony list` enumerates them and `--strict` runs a conformance suite.
- **Hybrid search.** BM25 + FAISS vector indexes fused with Z-score / min-max / RRF rankers, with adaptive FAISS index selection by row count.
- **Swappable embedders.** Local PyTorch, faster ONNX (int8), or hosted (litellm) — each behind its own optional extra.
- **Lean default install.** The mandatory footprint is `pydantic`, `pandas`, `pyarrow`, `httpx`, `platformdirs`. No torch, no FAISS unless you ask for them.

## Install

```bash
pip install parsimony-core               # kernel: connectors, results, errors, transport
pip install 'parsimony-core[standard]'   # + the hybrid-search Catalog
pip install parsimony-fred parsimony-sdmx  # individual connectors (each its own distribution)
```

The default install pulls only the lean kernel deps. The Catalog and its embedders are opt-in:

| Extra | Adds | Unlocks |
|---|---|---|
| `standard` | `faiss-cpu`, `rank-bm25`, `sentence-transformers`, `huggingface_hub` | `Catalog`, `BM25Index`, `VectorIndex`, `HybridIndex`, the default local embedder, and the `hf://` snapshot loader |
| `standard-onnx` | `standard` + `optimum[onnxruntime]`, `onnxruntime` | `OnnxEmbedder` — 2–3× faster CPU inference via int8 quantization, ~4× smaller on disk |
| `litellm` | `litellm` | `LiteLLMEmbeddingProvider` — hosted embeddings (OpenAI, Gemini, Cohere, Voyage, Bedrock) |
| `s3` | `s3fs` | Installs the dependency for `s3://` catalog URLs. **Note:** the `s3://` source handler is still a stub — adding this extra does not yet enable the scheme. |
| `all` | `standard` + `standard-onnx` + `litellm` + `s3` | Everything |
| `dev` | pytest, ruff, mypy, pip-audit (+ `standard`, `litellm`) | The full test/lint toolchain |

Requires Python 3.11+.

## Quickstart

### Use an installed connector

Connectors are separate distributions. With `parsimony-fred` installed and a free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html):

```python
import asyncio
import os

from parsimony_fred import fred_fetch, fred_search

async def main() -> None:
    api_key = os.environ["FRED_API_KEY"]

    # bind() fixes api_key and removes it from the call surface. The bound
    # value is NOT recorded in provenance.params.
    search = fred_search.bind(api_key=api_key)
    fetch = fred_fetch.bind(api_key=api_key)

    search_result = await search(search_text="US gross domestic product")
    print(search_result.df[["id", "title"]].head())

    result = await fetch(series_id="GDP")          # -> TabularResult
    print(result.df[["date", "value"]].tail())
    print(result.provenance.source)                # 'fred_fetch'

asyncio.run(main())
```

### Define your own connector

A connector is an `async` function decorated with `@connector`. Return raw data — the framework builds the typed envelope.

```python
import asyncio
import pandas as pd
from parsimony import Column, ColumnRole, OutputConfig, connector

CUSTOM_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="my_source"),
        Column(name="label", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA, dtype="numeric"),
    ]
)

@connector(output=CUSTOM_OUTPUT, tags=["custom"])
async def my_data_source(category: str) -> pd.DataFrame:
    """Return sample rows for a category (replace with a real HTTP call)."""
    return pd.DataFrame(
        {
            "code": ["A1", "A2", "A3"],
            "label": [f"{category} - Alpha", f"{category} - Beta", f"{category} - Gamma"],
            "score": [0.95, 0.87, 0.73],
        }
    )

# The framework wraps the raw DataFrame into a TabularResult with provenance.
result = asyncio.run(my_data_source(category="widgets"))
print(result.df)
print(result.provenance.source)  # 'my_data_source'
```

Connectors **must** be `async` and **must** have a description (docstring or `description=`). They **must** return raw data — returning a `Result`, `TabularResult`, or `(data, properties)` tuple raises `TypeError`. Provider facts belong in `DataFrame` columns, never in `provenance.properties` (which is framework-only).

### Compose connectors into a bundle

`Connectors` is an immutable, composable collection. Combine bundles with `+` and scope a credential across only the connectors that accept it:

```python
import os
from parsimony import Connectors
from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX

api_key = os.environ["FRED_API_KEY"]

# Combine bundles with the + operator. Connectors.bind scopes api_key only to
# connectors that actually accept it (FRED), leaving SDMX untouched.
bundle = (FRED + SDMX).bind(api_key=api_key)
print(bundle.names())

gdp = await bundle["fred_fetch"](series_id="GDP")
fx = await bundle["sdmx_fetch"](dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A")
```

### Build an HTTP connector with the transport helpers

The transport layer maps `httpx` errors (`401`/`402`/`429`/`5xx`/timeout) into the typed `parsimony.errors` taxonomy and redacts secrets in logs:

```python
import pandas as pd
from parsimony import Column, ColumnRole, OutputConfig, connector
from parsimony.transport.helpers import fetch_json, make_api_key_client

OUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="acme"),
        Column(name="value", role=ColumnRole.DATA, dtype="numeric"),
    ]
)

@connector(output=OUT, secrets=("api_key",))
async def acme_fetch(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch an ACME time series by id."""
    http = make_api_key_client("https://api.acme.test", api_key=api_key)
    payload = await fetch_json(http, path=f"series/{series_id}", provider="acme", op_name="series")
    return pd.DataFrame(payload["observations"])
```

`secrets=("api_key",)` strips that parameter from provenance even when passed at call time, and validates the name is a real parameter at decoration.

## Catalog and hybrid search

When you need to discover *which* series exist, build a `Catalog` over `Entity` rows. Each `Entity` is identified by `(namespace, code)` and carries a `title` plus free-form `metadata`. Field indexes (`BM25Index`, `VectorIndex`, `HybridIndex`) are keyed by a logical search surface; queries are either broad plain text (routed to the default field) or structured `field: value` clauses.

> The Catalog stack requires `pip install 'parsimony-core[standard]'`. Importing `Catalog`/`BM25Index`/`VectorIndex` from `parsimony` always works (lazy PEP 562), but `build()` raises `ImportError` on first use without the extra.

```python
import asyncio
from parsimony import BM25Index, Catalog, Entity, HybridIndex, VectorIndex
from parsimony.ranking import ZScoreFusion

async def main() -> None:
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
    await catalog.build()  # MUST build before search/save

    hits, diag = await catalog.search("inflation adjusted output", limit=5)   # broad
    print(diag.mode, [(h.code, round(h.score, 3)) for h in hits])

    hits2, _ = await catalog.search("code: UNRATE", limit=1)                   # structured, exact match
    print(hits2[0].title)

    await catalog.save("file:///tmp/macro-catalog", builder="readme-example")
    reloaded = await Catalog.load("file:///tmp/macro-catalog")
    print(len(reloaded))

asyncio.run(main())
```

A few important details, grounded in the code:

- **The catalog API is async.** `build`, `search`, `save`, `load`, `get`, `delete_many`, and embedder methods are all coroutines. You must `await catalog.build()` after construction and after any `set_entities` / `set_indexes` / `delete_many` — `search()` and `save()` raise `ValueError` until rebuilt.
- **`search(query, limit, *, namespaces=None)`** — `limit` is positional and required.
- **Default index policy.** `Catalog(name, indexes=None)` auto-creates BM25 indexes for `code`, `title`, and every metadata key at `build()` time. Pass an explicit `indexes` dict for full control.
- **Exact value matches win.** A case-insensitive exact value match short-circuits to a sentinel score that dominates fuzzy BM25/cosine scores — ideal for code lookups.
- **Portable, integrity-checked snapshots.** A saved catalog is a directory of Parquet (zstd) files plus `meta.json`; `Catalog.load` recomputes a content SHA-256 over every file and rejects a mismatch. Only `file://` (or a bare path) and `hf://` (Hugging Face dataset) schemes are wired in.

### Building entities from connector output

`OutputConfig.build_entities(df)` projects a `DataFrame` into `Entity` rows using column roles — the single `KEY` column (which must declare a `namespace`) becomes the `code`, `TITLE` becomes the `title`, and `METADATA` columns become `metadata`. This is how an enumerator connector feeds the catalog.

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

A `Connector` is a frozen dataclass wrapping an async function plus metadata. Await it to fetch.

- `await conn(**kwargs)` → `Result` (raw `__call__`; `call_raw(**kwargs)` returns the unwrapped function output).
- `conn.bind(**kwargs)` → a new connector with parameters fixed and removed from `exposed_signature`.
- `conn.with_callback(cb)` → adds a post-fetch observer (exceptions are logged and swallowed, never propagated).
- `conn.describe()` / `conn.to_llm()` → human- and LLM-readable cards.

`Connectors([...])` is the immutable collection: `+` to combine (rejects duplicate names), `bundle[name]` / `bundle.get(name)` to index, `names()`, `filter(pred)`, `search(query, *, tags=None, **properties)`, `bind(**kwargs)`, `with_callback(cb)`, `describe()`, `to_llm()`. There is no `merge` classmethod — use `+`.

### Three decorators

| Decorator | Purpose | Output contract |
|---|---|---|
| `@connector` | General-purpose fetch | `output` optional |
| `@enumerator(output=...)` | Entity/series discovery | exactly one `KEY` (with `namespace`), at least one `TITLE`, **no** `DATA`; function must annotate a `pd.DataFrame` return; returned columns strictly validated |
| `@loader(output=...)` | Observation-data fetch | exactly one `KEY` (with `namespace`), at least one `DATA`, **no** `TITLE`/`METADATA` |

### Result / TabularResult / Provenance

`Result(data, provenance)` is the opaque envelope; `TabularResult` adds `.df` and an optional `output_schema`, and round-trips through Arrow/Parquet (`to_arrow`/`from_arrow`/`to_parquet`/`from_parquet`) embedding provenance and the column schema in table metadata. `Provenance` is framework-only — connectors never construct it.

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

The package installs a `parsimony` console script (`parsimony = parsimony.cli:main`) with two verbs:

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
- Each connector distribution depends on `parsimony-core` and registers through the `parsimony.providers` entry point.
- Higher-level packages (e.g. `parsimony-agents`) build on top, consuming connectors and the catalog through the public API here.

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

Tests run with `asyncio_mode=auto` and an 80% coverage floor. Two pytest markers gate heavier tests:

- `integration` — hits live APIs (may be slow, requires env vars).
- `slow` — heavy local tests; opt-in.

To exercise the full FAISS + BM25 + sentence-transformers paths during development, install with the `standard` extra (the `dev` extra already pulls it in).

## License

Apache-2.0. See [LICENSE](LICENSE).
