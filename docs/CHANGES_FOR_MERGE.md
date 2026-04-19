# Parsimony — Pending working-tree changes

> **Purpose.** A neutral inventory of every uncommitted change currently sitting
> in the `parsimony` working tree, written so the merge can be reasoned about
> file-by-file. Authorship is not asserted — the diffs only tell us *what*
> changed, not *who* changed it. The changes from the most recent assistant
> session (the `to_llm()` / connector-tagging work) are explicitly flagged as a
> subsection so they don't get lost in the larger overhaul.
>
> Run `git status` and `git diff --stat` for the live picture; this doc gives
> the *shape* and *intent* of each cluster of changes.

---

## 1. The shape of the pending changes

`git diff --stat` summary: **85 unstaged files, +2 025 / −6 155 lines**, plus
13 fully untracked directories/files. Logically the working tree contains
five overlapping refactors:

| # | Refactor | Posture |
|---|---|---|
| 1 | **Monorepo split** — `parsimony/* → packages/parsimony/parsimony/*` plus six new sibling packages | Foundational; everything else sits on top of it |
| 2 | **Catalog rewrite** — `Catalog` (sqlite-backed) → `BaseCatalog` (ABC) + canonical `_standard/Catalog` (Parquet + FAISS + BM25 + RRF) | Replaces the persistence + search engine |
| 3 | **Store / extras simplification** — drop `sqlite_catalog`, `catalog_store`, `embeddings/`, in-tree `mcp/`, `connectors/{sec_edgar,financial_reports}.py`; new `standard` / `litellm` / `s3` extras | Removes ~5 KLOC; deletes the `sec`, `financial-reports`, `mcp`, `search` extras |
| 4 | **Plugin / provider registry** — `plugins.py` + rewritten `connectors/__init__.py` exposing `ProviderSpec` / `iter_providers` over the `parsimony.providers` entry-point group | New extension axis for connectors |
| 5 | **Prompt-authoring boundary** *(this session)* — `Connectors.to_llm()` becomes pure serialization; framing moves to hosts | Smallest of the five; described in §3 |

Refactors 1–4 are deeply interlocking (deletions in core depend on the new
sibling packages picking up the workload, etc.). Refactor 5 is largely
orthogonal except where it touches `connector.py` and the connector modules.

---

## 2. File-by-file inventory

### 2.1 Monorepo split (refactor #1)

**Staged renames** (87 files, 0 content delta):

- `parsimony/* → packages/parsimony/parsimony/*` — all sources, tests,
  examples.
- `parsimony/connectors/sdmx.py → packages/parsimony-sdmx/parsimony_sdmx/_connectors.py`
  and matching test/example renames.
- `pyproject.toml → packages/parsimony/pyproject.toml`.

**New top-level scaffolding** (untracked):

- `pyproject.toml` (workspace root)
- `release-please-config.json`, `.release-please-manifest.json`
- `.github/workflows/release.yml`
- Deleted: `.github/workflows/publish.yml`; modified: `.github/workflows/test.yml`

**New sibling packages** (untracked directories):

- `packages/parsimony-edgar/` — extracts `connectors/sec_edgar.py`
- `packages/parsimony-financial-reports/` — extracts `connectors/financial_reports.py`
- `packages/parsimony-mcp/` — extracts `parsimony/mcp/`
- `packages/parsimony-plugin-tests/` — integration tests for the plugin axis
- `packages/parsimony-sdmx/` — README, examples, conformance tests, plugin entry-point (the source modules are the staged renames above)
- `packages/parsimony-starter/` — template package

### 2.2 Catalog rewrite (refactor #2)

**`packages/parsimony/parsimony/catalog/catalog.py`** *(modified, ≈600-line diff)*:
the old concrete `Catalog` (sqlite + lazy namespace population + platformdirs
cache) is replaced by `BaseCatalog`, an abstract base class. Persistence and
search are now a single contract on the implementation: a catalog implementation
*owns its embedder* so query- and index-time models cannot drift. URL-based
load/push (`from_url`, `push`) dispatches to the implementation class via
`load_from_url` / `save_to_url` abstract methods. The module no longer imports
`sqlite3`, `os`, `pathlib.Path`, `platformdirs`, `parsimony.errors.ConnectorError`,
or `CatalogStore`.

**`packages/parsimony/parsimony/catalog/__init__.py`** *(modified)*: drops
`EmbeddingProvider`, `normalize_series_catalog_row`, `build_embedding_text`,
the lazy `Catalog` `__getattr__`. Adds `BaseCatalog`, `EmbedderInfo`,
`catalog_key`, `normalize_entity_code`.

**`packages/parsimony/parsimony/catalog/models.py`** *(modified)*: removes
`EmbeddingProvider` ABC, `normalize_series_catalog_row` (Supabase-shaped row
parser), the `properties` / `observable_id` fields on `SeriesEntry`, and the
JSON-import that backed them. Adds `SeriesEntry.embedding_text()` — a fixed
representation an embedder indexes for the entry.

**`packages/parsimony/parsimony/catalog/builder.py`** *(deleted)*.

**New, untracked under `packages/parsimony/parsimony/catalog/`:**

- `embedder_info.py` — value object describing the embedder model that
  produced a catalog's vectors (used to gate query/index compatibility).

**New, untracked: `packages/parsimony/parsimony/_standard/`** — the canonical
`Catalog` implementation, deliberately namespaced under an underscore to
signal "implementation, not API":

- `__init__.py` — re-exports `Catalog`, `EmbeddingProvider`,
  `SentenceTransformerEmbedder`, `LiteLLMEmbeddingProvider`, and the
  `CatalogMeta` / `BuildInfo` / filename constants.
- `catalog.py` — Parquet rows + FAISS vectors + BM25 keywords; flat IP for
  small catalogs, HNSW above a row threshold; atomic save via temp-dir
  rename; reciprocal-rank-fusion over the two indexes.
- `embedder.py` — `EmbeddingProvider` ABC, `SentenceTransformerEmbedder`
  (default), `LiteLLMEmbeddingProvider` (OpenAI/Gemini/Cohere/Voyage/Bedrock).
- `indexes.py` — FAISS read/write/query helpers; BM25 build/query; tokenize;
  RRF fuse.
- `meta.py` — `CatalogMeta`, `BuildInfo`, `SCHEMA_VERSION`, the on-disk
  filename constants.
- `sources/{__init__,local,hf,s3}.py` — URL-scheme dispatch. `file://`
  works, `hf://` works (requires `huggingface-hub`), `s3://` is a
  placeholder that raises an actionable `NotImplementedError`. The module
  docstring is explicit that schemes are *not* a plugin axis — adding one
  means editing this directory.

**`packages/parsimony/parsimony/__init__.py`** *(modified)*: rebuilt around
PEP 562 lazy imports. Light symbols (`Connector`, `BaseCatalog`,
`SeriesEntry`, errors) load eagerly; heavy symbols (`Catalog`,
`SentenceTransformerEmbedder`, `LiteLLMEmbeddingProvider`) load on first
attribute access so `import parsimony` doesn't pull torch / faiss /
huggingface-hub.

**Test churn for the catalog rewrite:**

- `test_lazy_catalog.py`, `test_catalog_coverage.py`, `test_catalog_namespaces.py` — **deleted** (the old `Catalog`'s sqlite-backed lazy population semantics no longer exist).
- `test_series_catalog_models.py` — modified (drops `EmbeddingProvider`, adds `embedding_text` coverage).
- `test_indexing.py` — modified (large reduction; replaces tests of the old in-process indexer).
- `test_standard_catalog.py` — **new**, untracked.
- `test_catalog_url_dispatch.py` — **new**, untracked.

### 2.3 Store / extras simplification (refactor #3)

**Stores.** `packages/parsimony/parsimony/stores/__init__.py` shrinks to:

```python
from parsimony.stores.data_store import DataStore, LoadResult
from parsimony.stores.memory_data import InMemoryDataStore
__all__ = ["DataStore", "InMemoryDataStore", "LoadResult"]
```

with `parsimony/stores/catalog_store.py` (66 lines) and `parsimony/stores/sqlite_catalog.py`
(539 lines) **deleted**. Catalog persistence is now a method on the catalog
implementation itself, not a separate `Store` abstraction.

**Extras / dependencies.** `packages/parsimony/pyproject.toml`:

- `sdmx1` removed from base dependencies (now lives in `parsimony-sdmx`).
- `numpy>=1.26` added to base.
- Optional-dependency groups rewritten:
  - **removed:** `search` (`litellm` + `sqlite-vec`), `sec` (`edgartools`),
    `financial-reports`, `mcp`.
  - **added:** `standard` (`faiss-cpu`, `rank-bm25`, `sentence-transformers`,
    `huggingface-hub`), `litellm`, `s3` (`s3fs`).
  - `all` is now `parsimony[standard,litellm,s3]`.
- Coverage gate (`--cov-fail-under=80`) removed from `addopts`.
- `description` rewritten ("Connector framework for financial data — typed
  fetch, hybrid-search catalogs, distribute via Hugging Face Hub or S3.").

**In-tree MCP removal.** Whole subtree deleted: `parsimony/mcp/__init__.py`,
`__main__.py`, `bridge.py`, `server.py`. Tests deleted: `tests/test_mcp/`
(four files), `tests/test_mcp_server_coverage.py`.

**Embeddings removal.** `parsimony/embeddings/__init__.py` and
`parsimony/embeddings/litellm.py` deleted (the embedder lives at
`_standard/embedder.py` now). Test deleted: `test_litellm_embedding.py`.

**Connectors leaving core.**

- `parsimony/connectors/sec_edgar.py` deleted (now `parsimony-edgar`).
- `parsimony/connectors/financial_reports.py` deleted (now
  `parsimony-financial-reports`).
- `tests/test_new_connectors_integration.py` deleted (472 lines —
  superseded by per-package conformance tests).
- `tests/test_supabase_catalog_fts.py`, `tests/test_sqlite_store.py` deleted.

**Result-system tweak.** `parsimony/result.py` *(modified, ≈40-line diff,
authorship unclear)*: `to_arrow` / `from_arrow` now treat `output_schema`
as optional — schemaless `Result`s round-trip as plain tabular results
with provenance only. `test_result.py` gains coverage for the
schemaless path.

### 2.4 Plugin / provider registry (refactor #4)

**`packages/parsimony/parsimony/plugins.py`** *(new, untracked)* — entry-point
discovery for the `parsimony.providers` group. Failures surface as a
`RegistryWarning` rather than silent `ImportError`.

**`packages/parsimony/parsimony/connectors/__init__.py`** *(rewritten)*:

- `ProviderSpec` now a `(name, connectors, module, env_vars)` dataclass —
  bundled providers carry a `module` path that lazy-loads `CONNECTORS`;
  plugin providers carry a populated `connectors` field directly.
- New `iter_providers()` yields built-ins **then** entry-point-discovered
  providers.
- `build_connectors_from_env(...)` rebuilt around `iter_providers`.

**`tests/test_plugins.py`** *(new, untracked)*.

### 2.5 Prompt-authoring boundary (this session — refactor #5)

This is the smallest cluster and the only one we can directly attribute to
the most recent assistant session.

**`packages/parsimony/parsimony/connector.py`** *(modified, −82 / +25 lines,
all inside `Connectors`)*:

- Removed `_CODE_HEADER` and `_MCP_HEADER` class constants (≈40 lines of
  prose previously injected into agent system prompts).
- Removed the `context: str = "code"` parameter from `Connectors.to_llm()`.
- Body is now pure serialization — no header, no usage prose, no binding
  name, no "Connectors (N)" / "Tools (N)" section labels.
- Two docstring examples updated `"fred_fetch" → "fred"` (consistent with
  the rename below).

**`packages/parsimony/parsimony/connectors/*.py`** *(every connector module)*:

1. **Single-fetch sources renamed** `<src>_fetch → <src>`: `bde`, `bdf`,
   `bdp`, `bls`, `boc`, `boj`, `destatis`, `eia`, `fred`, `rba`,
   `riksbank`, `snb`, `treasury`. Polymarket factory updated to the same
   pattern.
2. **`"search"` tag added** to every connector returning discovery payloads:
   `alpha_vantage`, `coingecko`, `eodhd`, `finnhub`, `fmp`, `fmp_screener`,
   `fred` (`fred_search`), `tiingo`. Existing tags preserved.

**Tests reflecting the boundary:**

- `tests/test_connector.py` — `TestConnectorsToLlm` rewritten: asserts the
  prose is *absent*, `Connectors([]).to_llm() == ""`.
- `tests/test_connector_describe.py` — same shape; adds
  `test_no_framework_authored_header` and
  `test_concatenation_separates_with_blank_line` as contract pins.
- `tests/test_fetch_connectors_factory.py` — updated for the `<src>_fetch → <src>` rename. Note this file *also* has unrelated SDMX-related deletions (refactor #1/#3). Both must survive a merge.

**Doc edit:** `docs/mcp-setup.md` bullet 4 of "How It Works" rewritten:

```diff
- 4. Server instructions … are generated from `connectors.to_llm(context="mcp")`
+ 4. Server instructions … are composed in `parsimony_mcp.server`, combining
+    MCP-specific framing with `connectors.to_llm()`
```

**Untracked file we authored:** `packages/parsimony-mcp/parsimony_mcp/server.py`
*(this file lives inside the untracked `parsimony-mcp` package skeleton from
refactor #1)*. We added the `_MCP_SERVER_INSTRUCTIONS` constant — the prose
that previously lived in `Connectors._MCP_HEADER` — and made `create_server`
compose it with `connectors.to_llm()`:

```python
_MCP_SERVER_INSTRUCTIONS = """\
# Parsimony — financial data discovery tools
…
## Tools

{catalog}
"""

def create_server(connectors: Connectors) -> Server:
    instructions = _MCP_SERVER_INSTRUCTIONS.format(catalog=connectors.to_llm())
    server = Server("parsimony-data", instructions=instructions)
    …
```

If the `parsimony-mcp` package is committed at any point without these two
additions, `create_server` will call the no-longer-existing
`connectors.to_llm(context="mcp")` and fail at server boot.

### 2.6 Documentation

Every file under `docs/` is modified (`api-reference.md`, `architecture.md`,
`cookbook.md`, `connector-implementation-guide.md`, `faq.md`, `index.md`,
`internal-connectors.md`, `quickstart.md`, `user-guide.md`) plus new
`docs/extracting-a-bundled-connector.md`. Most of this churn is downstream
of refactors #1–#4 (rewriting examples to use the new package boundaries
and the `Catalog` API). Our session only touched `docs/mcp-setup.md`
(refactor #5).

`README.md` and `Makefile` are also modified at the top level — same lineage.

---

## 3. Where conflicts will land

The merge surface is dominated by refactors #1–#4. Refactor #5 is small and
its conflict footprint is contained:

| File | Conflict source | Action |
|---|---|---|
| `packages/parsimony/parsimony/connector.py` | Session edit + the rename also touches connector docstrings | Keep the trimmed `to_llm()` body; drop `_CODE_HEADER` / `_MCP_HEADER` for good. |
| `packages/parsimony/parsimony/connectors/*.py` | Session rename + `"search"` tags vs. any restructure-driven import / type-hint changes | Keep the `<src>_fetch → <src>` renames *and* the `"search"` tags; preserve any unrelated import or type changes around them. |
| `packages/parsimony/tests/test_connector.py` | Likely session-only inside `TestConnectorsToLlm` | Keep the new contract tests. |
| `packages/parsimony/tests/test_connector_describe.py` | Likely session-only inside `TestConnectorsToLlm` | Keep the new contract tests. |
| `packages/parsimony/tests/test_fetch_connectors_factory.py` | Session rename + restructure-driven SDMX deletions | Both edits must survive — independent regions of the file. |
| `docs/mcp-setup.md` | Session bullet-4 rewrite + restructure-driven name updates elsewhere | Trivial three-way merge. |
| `packages/parsimony-mcp/parsimony_mcp/server.py` | Untracked; session-edited inside an untracked package | If/when the `parsimony-mcp` package is committed, verify `_MCP_SERVER_INSTRUCTIONS` and the `create_server` composition survive. |

Everything else in §2.1–§2.4 is large but internally consistent — those
files are either fully rewritten or fully deleted, so there is little
session work to interleave with them.

---

## 4. Verification

After the working tree is committed (in whatever shape), the following must
pass:

```bash
cd parsimony && uv run pytest packages/parsimony/tests -q --no-cov
cd parsimony && uv run pytest packages/parsimony-mcp -q --no-cov
```

Behavioral invariants for refactor #5 (the session work):

```python
from parsimony.connector import Connectors
assert Connectors([]).to_llm() == ""        # empty = empty string
text = some_real_collection.to_llm()
assert "Data connectors" not in text         # no framework prose
assert "client[" not in text                 # no binding name
```

Behavioral invariant for refactor #2 (the catalog rewrite):

```python
from parsimony import Catalog, BaseCatalog
assert issubclass(Catalog, BaseCatalog)
# Round-trip a catalog through file:// to confirm the standard impl is wired.
```

If either invariant fails after a merge, the corresponding refactor's
intent has been dropped.
