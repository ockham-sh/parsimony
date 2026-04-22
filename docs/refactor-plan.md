# Parsimony refactor — formal plan

**Status:** Approved direction, not yet executed.
**Scope:** Kernel (`parsimony-core`) refactor; external plugins (`parsimony-fred`,
`parsimony-sdmx`) migrated in lockstep.
**Target:** ~2,730 LOC from 7,343 (~63% reduction). 12 flat modules from 31
files across 8 subpackages.
**Version cut:** `0.3.0` — single breaking release, no deprecation cycle (no
external users yet).

---

## 1. Goals

- Cut kernel LOC to ~2,700 without losing functional capability.
- Replace nominal inheritance (`BaseCatalog` ABC) with structural typing
  (`CatalogBackend` Protocol) — smaller contract, lower adoption barrier.
- Collapse the bundle / spec / plan abstraction into plain Python
  (`CATALOGS: list[tuple[str, Callable]]` or async factory).
- Flat module layout: one file per concern, no subpackages.
- Two CLI verbs (`list`, `publish`) instead of four.
- Three conformance checks instead of seven.
- Make the partitioning question (see
  [catalog-partitioning-design.md](catalog-partitioning-design.md)) tractable
  to answer later — the refactor does not prejudge it.

## 2. Target architecture

```
parsimony/
  __init__.py          ~80 LOC    public surface (lazy re-exports)
  connector.py        ~500 LOC    @connector / @enumerator / @loader + Connector + Connectors
  result.py           ~300 LOC    Result + Provenance + OutputConfig + Column (role + namespace sentinel pattern)
  catalog.py          ~500 LOC    CatalogBackend Protocol + Catalog concrete + Entry/Match/IndexResult + url parser
  embedder.py         ~250 LOC    EmbeddingProvider Protocol + SentenceTransformer + LiteLLM
  indexes.py          ~120 LOC    FAISS + BM25 + RRF pure functions
  discovery.py        ~120 LOC    entry-point scan + build_connectors_from_env + errors
  errors.py            ~80 LOC    ConnectorError hierarchy
  http.py             ~180 LOC    HttpClient (transport)
  publish.py          ~150 LOC    publish(module, target=...) orchestrator
  stores.py           ~150 LOC    InMemoryDataStore + LoadResult
  testing.py          ~100 LOC    assert_plugin_valid (3 checks) + ProviderTestSuite (4 methods)
  cli.py              ~200 LOC    single file, two verbs
```

**Deleted subpackages:** `bundles/`, `catalog/`, `_standard/`, `discovery/`,
`stores/`, `transport/`, `cli/`.

## 3. Agreed design decisions (consolidated)

| # | Decision | Rationale |
|---|---|---|
| 1 | Replace `BaseCatalog` ABC with `CatalogBackend` Protocol (2 methods: `add`, `search`) | Smaller pluggability contract; structural over nominal typing |
| 2 | Concrete `Catalog` class is the sole reference implementation; keeps all distribution-format methods (`save`/`load`/`push`/`from_url`/…) | Separates "pluggable live-catalog" from "opinionated canonical format" |
| 3 | Delete `@enumerator(catalog=CatalogSpec(...))` plumbing; replace with `CATALOGS` list or async factory on the plugin module | Plugin author owns namespace grammar in plain Python; no mini-language |
| 4 | Optional `RESOLVE_CATALOG(namespace) -> Callable \| None` plugin hook for on-demand catalog build | Replaces `LazyNamespaceCatalog` + namespace-template reverse-regex |
| 5 | Delete `LazyNamespaceCatalog`; provide userland recipe (~15 LOC) | Too specific to live in kernel |
| 6 | Delete namespace-template engine (`{placeholder}` substitution, `namespace_placeholders`, reverse regex) | Plugin builds namespace string directly |
| 7 | Replace `ProviderCatalogURL` (254 LOC) with `parse_catalog_url(url) -> (scheme, root, sub)` helper | ~25 LOC, no repo-prefix enforcement, sandboxing comes from `Path.resolve().is_relative_to(...)` |
| 8 | Kill `namespaces.json` manifest + content hashing + resume logic | Always rebuild or `--only`; re-add if measured cost demands |
| 9 | Kill upload-retry with 429/5xx / `Retry-After`; rely on `huggingface_hub` client | |
| 10 | Kill `CONTRACT_VERSION` constant + `parsimony-contract-v1` keyword reading | Premature at 0.x.y |
| 11 | Keep 3 decorators (`@connector` / `@enumerator` / `@loader`) | They're thin aliases; early validation has real value |
| 12 | Keep full `OutputConfig` surface (`dtype`, `mapped_name`, `param_key`, `description`, `exclude_from_llm_view`, `namespace`, role) | Each field serves a real case |
| 13 | Merge `SemanticTableResult(Result)` subclass into `Result` via pydantic validator | Structural cleanup |
| 14 | Replace `Namespace("x")` annotation class with string sentinel `Annotated[str, "ns:x"]` | Same agent-cross-ref value, less machinery |
| 15 | Delete `ResultCallback` + `with_callback`; ship as userland recipe | Minor feature, plugin authors wrap call sites |
| 16 | 7 conformance checks → 3: `connectors_exported`, `descriptions_non_empty`, `env_vars_map_to_deps` | Drop micro-checks; keep integrity checks |
| 17 | 4 CLI verbs → 2: `parsimony list` (plugins + catalogs) + `parsimony publish` | Drop `bundles list`, fold `conformance verify` into `list --strict` |
| 18 | Flat module layout, no subpackages | Reader sees protocol + impl in one scroll |

## 4. Phase plan

No deprecation cycle — no external users. One breaking release, done in three
PRs landing in order. Green CI between each.

### Phase 1 — kernel rewrite (~3–4 days)

Single PR. Delete old, write new, fix tests.

**Delete (as directories) in one commit:**
- `parsimony/bundles/` — entire subpackage (6 files, ~1,580 LOC)
- `parsimony/catalog/` — subpackage (3 files, ~532 LOC)
- `parsimony/_standard/` — subpackage (10 files, ~1,020 LOC)
- `parsimony/discovery/` — subpackage (4 files, ~394 LOC)
- `parsimony/stores/` — subpackage (2 files, ~164 LOC)
- `parsimony/transport/` — subpackage (3 files, ~265 LOC)
- `parsimony/cli/` — subpackage (5 files, ~897 LOC)
- `parsimony/templates/` — if still present

**Also delete:**
- `tests/test_bundles_*.py`, `test_lazy_namespace_catalog.py`, `test_provider_catalog_url.py`, `test_provider_manifest.py`, `test_template_namespaces.py`, `test_publish_property.py`, `test_publish_hostile.py`, `test_publish_conformance.py`
- `docs/migration-catalog-publish.md`

**Write (flat modules):**
- `parsimony/catalog.py` — `CatalogBackend` Protocol + `parse_catalog_url` + `SeriesEntry` / `SeriesMatch` / `IndexResult` + concrete `Catalog` class with `save`/`load`/`from_url`/`push`/`add`/`search`/`get`/`delete`/`list`/`list_namespaces`/`add_from_result`. URL handlers (`file://` / `hf://`) inlined as module-private helpers.
- `parsimony/embedder.py` — `EmbeddingProvider` Protocol + `SentenceTransformerEmbedder` + `LiteLLMEmbeddingProvider`.
- `parsimony/indexes.py` — FAISS + BM25 + RRF helpers (move verbatim from `_standard/indexes.py`).
- `parsimony/publish.py` — `publish(module, target=..., only=..., dry_run=...)` reading `CATALOGS` (list | async generator function) + optional `RESOLVE_CATALOG`.
- `parsimony/discovery.py` — entry-point scan + `DiscoveredProvider` + `build_connectors_from_env` + `PluginError` / `PluginImportError` / `PluginContractError`.
- `parsimony/stores.py` — `InMemoryDataStore` + `LoadResult` (no wrapper package).
- `parsimony/http.py` — `HttpClient` (verbatim).
- `parsimony/cli.py` — two verbs (`list`, `publish`).
- `parsimony/testing.py` — 3 checks, 4 test methods.

**Modify:**
- `parsimony/connector.py`:
  - Drop `Namespace` class; replace call sites with `Annotated[str, "ns:x"]` sentinel-string pattern (update `_resolve_type` / presentation helpers to read the string).
  - Drop `ResultCallback`, `_invoke_result_callbacks`, `Connector._callbacks` field, `Connector.with_callback`, `Connectors.with_callback`.
  - Keep 3 decorators; `@enumerator` drops the `catalog=` kwarg entirely.
- `parsimony/result.py`:
  - Merge `SemanticTableResult` into `Result` via `@model_validator` that requires `output_schema` when `data` is a DataFrame with declared schema.
  - Drop `namespace_placeholders`, `resolve_namespace_template`, the placeholder-substitution machinery on `OutputConfig.__post_init__`.
  - Make `Column.namespace` optional on KEY columns (see §9.1 resolution).
- `parsimony/errors.py`:
  - Drop `BundleNotFoundError`.
- `parsimony/__init__.py`:
  - Drop `CONTRACT_VERSION`, `Namespace`, `SemanticTableResult`, `BundleNotFoundError`, `ResultCallback` from exports.
  - Drop lazy-import machinery for `Catalog` / embedders (they live in flat modules now; lazy imports stay if we still want cheap `import parsimony`).

**Success gate:**
- Full test suite green.
- `pip install .` works.
- `parsimony list` enumerates installed plugins.
- `python -c "from parsimony import Catalog, connector, Connectors, Result; print('ok')"` works.

### Phase 2 — plugin rewrites (~1 day each, parallel)

One PR per plugin repo. No need to stagger — the kernel break is already live.

**`parsimony-fred`:**
- Drop `from parsimony.bundles import CatalogSpec`.
- Drop `catalog=CatalogSpec.static(...)` kwarg.
- Add `CATALOGS = [("fred", fred_enumerate)]` to `parsimony_fred/__init__.py`.
- Bump dependency: `parsimony-core>=0.3,<0.5`.
- Drop `parsimony-contract-v1` keyword from pyproject.
- Release `parsimony-fred 0.3.0`.

**`parsimony-sdmx`:**
- Drop `CatalogSpec(plan=to_async(...))` usage.
- Drop `{placeholder}` templates in `Column.namespace`.
- Add async `CATALOGS` generator function (static + dynamic fan-out).
- Add `RESOLVE_CATALOG` function.
- Bump dependency: `parsimony-core>=0.3,<0.5`.
- Drop `parsimony-contract-v1` keyword.
- Release `parsimony-sdmx 0.3.0`.

Concrete migration deltas are in §6.

**Success gate:**
- `parsimony publish --provider fred --target file:///tmp/fred` produces a working catalog that `Catalog.from_url("file:///tmp/fred/fred")` can open and search.
- Same for sdmx with a 2-namespace dry run.

### Phase 3 — docs (~1 day)

Lands with Phase 1 PR or as a follow-up.

- Rewrite `docs/contract.md`:
  - Remove §10 (catalog-publish distribution)
  - Remove CONTRACT_VERSION section
  - Drop keyword ABI gate section
  - Update kernel API surface table to match new exports
- Rewrite `docs/catalog-publish.md` around `CATALOGS`
- Delete `docs/migration-catalog-publish.md`
- Update `README.md` quickstart (new install + `CATALOGS` plugin shape)
- Update `docs/building-a-private-connector.md` with the `CATALOGS` + `RESOLVE_CATALOG` pattern
- Keep `docs/catalog-partitioning-design.md` as an open-question note

## 5. File-by-file kernel deltas

| File | Before LOC | After LOC | Notes |
|---|---:|---:|---|
| `__init__.py` | 153 | 80 | Rewrite `__all__`; drop lazy catalog machinery; drop CONTRACT_VERSION |
| `connector.py` | 802 | 500 | Drop `Namespace` class, `ResultCallback`, `with_callback`, observer logic; keep 3 decorators |
| `result.py` | 501 | 300 | Drop `SemanticTableResult` subclass; drop namespace-template helpers; keep OutputConfig surface |
| `catalog/catalog.py` | 347 | → merged | `BaseCatalog` deleted; `entries_from_result` moves to new `catalog.py` |
| `catalog/models.py` | 157 | → merged | into `catalog.py` |
| `catalog/embedder_info.py` | 28 | → merged | into `catalog.py` |
| `_standard/catalog.py` | 403 | → merged | into `catalog.py` as the concrete `Catalog` class |
| `_standard/embedder.py` | 278 | → `embedder.py` (~250) | unchanged logic, move file |
| `_standard/indexes.py` | 114 | → `indexes.py` (~120) | unchanged logic, move file |
| `_standard/meta.py` | 60 | → merged | into `catalog.py` |
| `_standard/provider_manifest.py` | 189 | deleted | manifest/resume killed |
| `_standard/sources/local.py` | 31 | → merged | into `catalog.py` as `Catalog.save/load` |
| `_standard/sources/hf.py` | 184 | → merged | into `catalog.py` as `Catalog.push/from_url(hf://)` |
| `_standard/sources/s3.py` | 25 | deleted | stub, not shipping |
| `_standard/sources/__init__.py` | 49 | → merged | URL dispatch inlined |
| `bundles/spec.py` | 241 | deleted | `CatalogSpec`/`CatalogPlan` killed |
| `bundles/urls.py` | 254 | → `~25` LOC in `catalog.py` | `parse_catalog_url` helper |
| `bundles/publish.py` | 611 | → `publish.py` (~150) | strip resume + manifest + retries |
| `bundles/lazy_catalog.py` | 276 | deleted | userland recipe in docs |
| `bundles/discovery.py` | 71 | deleted | folded into `publish.collect_catalogs()` |
| `bundles/errors.py` | 127 | deleted | no upload retry needed |
| `bundles/__init__.py` | 34 | deleted | |
| `discovery/_scan.py` | 212 | → `discovery.py` (~120) | merge with compose |
| `discovery/_compose.py` | 94 | → merged | |
| `discovery/errors.py` | 88 | → merged | |
| `discovery/__init__.py` | 47 | → merged | |
| `cli/__init__.py` | 102 | → `cli.py` (~200) | merge with subcommands |
| `cli/publish.py` | 377 | → merged | kill force-scope / staging-dir flags if unused |
| `cli/bundles.py` | 75 | deleted | `bundles list` merged into `list` |
| `cli/conformance.py` | 184 | → merged | fold into `list --strict` |
| `cli/list_plugins.py` | 159 | → merged | |
| `cli/__main__.py` | (small) | → merged | |
| `testing.py` | 384 | 100 | 7 checks → 3, 8 test methods → 4 |
| `stores/data_store.py` | 151 | → `stores.py` (~150) | move file |
| `stores/__init__.py` | 13 | → merged | |
| `transport/http.py` | 176 | → `http.py` (~180) | move file |
| `transport/json_helpers.py` | 89 | delete or inline | check usage; likely delete |
| `transport/__init__.py` | 8 | → merged | |
| `errors.py` | 166 | 80 | drop `BundleNotFoundError` (bundles gone) |

**Estimated total: ~2,730 LOC (from 7,343).**

## 6. Plugin contract — before / after

### `parsimony-fred` (static single-namespace)

**Before** (`parsimony_fred/connectors.py`):
```python
from parsimony import OutputConfig, Column, ColumnRole, enumerator
from parsimony.bundles import CatalogSpec

FRED_ENUM_OUTPUT = OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="frequency", role=ColumnRole.METADATA),
])

@enumerator(
    output=FRED_ENUM_OUTPUT,
    catalog=CatalogSpec.static(namespace="fred"),
)
async def fred_enumerate(params: FredEnumerateParams, *, api_key: str) -> Result:
    ...
```

**After** (`parsimony_fred/connectors.py`):
```python
from parsimony import OutputConfig, Column, ColumnRole, enumerator

FRED_ENUM_OUTPUT = OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="frequency", role=ColumnRole.METADATA),
])

@enumerator(output=FRED_ENUM_OUTPUT)          # <-- drop catalog= kwarg
async def fred_enumerate(params: FredEnumerateParams, *, api_key: str) -> Result:
    ...
```

**After** (`parsimony_fred/__init__.py`):
```python
from parsimony import Connectors
from parsimony_fred.connectors import fred_enumerate, fred_fetch, fred_search

CONNECTORS = Connectors([fred_enumerate, fred_fetch, fred_search])
ENV_VARS = {"api_key": "FRED_API_KEY"}

CATALOGS = [("fred", fred_enumerate)]          # <-- new
```

**Diff per plugin file:** ~3 lines modified.

### `parsimony-sdmx` (dynamic many-namespace)

**Before** (`parsimony_sdmx/connectors.py`):
```python
from parsimony.bundles import CatalogSpec, CatalogPlan, to_async

def _agencies() -> Iterator[CatalogPlan]:
    for a in ("ECB", "ESTAT", "IMF_DATA", "WB_WDI"):
        yield CatalogPlan(namespace="sdmx_datasets", params={"agency": a})

SDMX_SERIES_OUTPUT = OutputConfig(columns=[
    Column(
        name="series_id",
        role=ColumnRole.KEY,
        namespace="sdmx_series_{agency}_{dataset_id}",   # template
    ),
    Column(name="agency", role=ColumnRole.METADATA),
    Column(name="dataset_id", role=ColumnRole.METADATA),
    Column(name="title", role=ColumnRole.TITLE),
])

@enumerator(
    output=SDMX_SERIES_OUTPUT,
    catalog=CatalogSpec(plan=to_async(_agencies)),
)
async def enumerate_sdmx_series(params: SdmxSeriesParams, *, ...) -> Result:
    ...
```

**After** (`parsimony_sdmx/connectors.py`):
```python
from parsimony import OutputConfig, Column, ColumnRole, enumerator

def sdmx_series_output_for(agency: str, dataset_id: str) -> OutputConfig:
    """One OutputConfig per (agency, dataset_id) — namespace is plain string."""
    ns = f"sdmx_series_{agency.lower()}_{dataset_id.lower()}"
    return OutputConfig(columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace=ns),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
    ])

@enumerator(output=sdmx_series_output_for_base)   # base shape; namespace filled per-call
async def enumerate_sdmx_series(params: SdmxSeriesParams, *, ...) -> Result:
    """Emit series rows for one (agency, dataset_id)."""
    ...
```

**After** (`parsimony_sdmx/__init__.py`):
```python
from functools import partial
from typing import AsyncIterator, Callable, Awaitable
from parsimony import Connectors
from parsimony_sdmx.connectors import (
    enumerate_sdmx_series,
    enumerate_sdmx_datasets,
    ...
)

CONNECTORS = Connectors([enumerate_sdmx_series, enumerate_sdmx_datasets, ...])

# Static catalogs known at import time:
_STATIC_CATALOGS: list[tuple[str, Callable[[], Awaitable]]] = [
    ("sdmx_datasets", enumerate_sdmx_datasets),
]

async def CATALOGS() -> AsyncIterator[tuple[str, Callable[[], Awaitable]]]:
    """Dynamic + static catalogs for SDMX. Called by `parsimony publish`."""
    for item in _STATIC_CATALOGS:
        yield item
    async for agency in _fetch_agencies():
        async for dataflow in _fetch_dataflows(agency):
            ns = f"sdmx_series_{agency.lower()}_{dataflow.id.lower()}"
            yield ns, partial(enumerate_sdmx_series, agency=agency, dataset_id=dataflow.id)


def RESOLVE_CATALOG(namespace: str) -> Callable[[], Awaitable] | None:
    """On-demand catalog build for a namespace we haven't published yet."""
    if namespace == "sdmx_datasets":
        return enumerate_sdmx_datasets
    if not namespace.startswith("sdmx_series_"):
        return None
    rest = namespace.removeprefix("sdmx_series_")
    try:
        agency, dataset_id = rest.split("_", 1)
    except ValueError:
        return None
    return partial(
        enumerate_sdmx_series,
        agency=agency.upper(),
        dataset_id=dataset_id.upper(),
    )
```

**Diff per plugin:** ~40 lines rewritten, ~120 lines net-deleted (`CatalogPlan` + `to_async` import / usage gone; template regex-reverse logic no longer needed anywhere).

## 7. Test surface migration

### Delete (features removed)
- `tests/test_bundles_spec.py` (247 LOC)
- `tests/test_bundles_discovery.py` (179 LOC)
- `tests/test_bundles_retrieval_eval.py`
- `tests/test_lazy_namespace_catalog.py` (221 LOC)
- `tests/test_provider_catalog_url.py` (249 LOC)
- `tests/test_provider_manifest.py` (188 LOC)
- `tests/test_template_namespaces.py`
- `tests/test_publish_property.py` / `test_publish_hostile.py` / `test_publish_conformance.py` (much folded into new `test_publish.py`)

### Consolidate
- `tests/test_publish.py` + fragments of the 3 above → single `test_publish.py` (~200 LOC target; was 424 + 200 + 188 + smaller)
- `tests/test_cli_publish.py` + `test_cli_list_plugins.py` + `test_cli_conformance.py` → single `test_cli.py` (~250 LOC target; was 376 + 194 + 195)
- `tests/test_hf_source.py` + `test_series_catalog_models.py` + `test_catalog_rewrite_invariants.py` → single `test_catalog.py`

### Rewrite (3-check conformance)
- `tests/test_testing_conformance.py`: drop 4 check tests, keep 3
- `tests/test_provider_test_suite.py`: drop 4 test methods

### Keep largely intact
- `tests/test_connector.py` (core)
- `tests/test_connector_describe.py`
- `tests/test_result.py` + `test_result_arrow.py`
- `tests/test_data_store.py`
- `tests/test_discovery.py`
- `tests/test_http_source_log_redaction.py`
- `tests/test_json_helpers.py` (verify still used — may delete)
- `tests/test_kernel_purity.py`
- `tests/test_framework_hardening.py` (review — may delete checks for deleted features)

## 8. Breaking changes for plugin authors (summary table)

| Old (0.2.x) | New (0.4+) |
|---|---|
| `@enumerator(catalog=CatalogSpec.static(namespace="x"))` | `@enumerator(...)` + `CATALOGS = [("x", fn)]` in `__init__.py` |
| `CatalogSpec(plan=to_async(_sync_iter))` | `async def CATALOGS(): yield ...` (or `CATALOGS = list`) |
| `Column(namespace="x_{a}_{b}")` templates | Plugin builds namespace as plain Python string |
| `LazyNamespaceCatalog(base, bundle_loader=..., connectors=...)` | Userland recipe (docs); or `RESOLVE_CATALOG` hook |
| `class MyCatalog(BaseCatalog):` | `class MyCatalog:` matching `CatalogBackend` Protocol |
| `ProviderCatalogURL.parse(url)` | `parse_catalog_url(url) -> (scheme, root, sub)` |
| `Annotated[str, Namespace("fred")]` | `Annotated[str, "ns:fred"]` |
| `conn.with_callback(observer)` | Userland wrapper (see cookbook) |
| `keywords = [..., "parsimony-contract-v1"]` in pyproject | Just remove the token |
| `dependencies = ["parsimony-core>=0.1.0,<0.3"]` | `dependencies = ["parsimony-core>=0.4,<0.6"]` |
| `parsimony bundles list` | `parsimony list` |
| `parsimony conformance verify <dist>` | `parsimony list --strict` (exit 0/1 on conformance) |
| `parsimony.BundleNotFoundError` | `FileNotFoundError` (publish side) / `CatalogNotFound` if we add one |

## 9. Open design decisions within the refactor

Flagged for resolution before code lands:

### 9.1 `Column(role=KEY).namespace` — required or optional?

Two options:

- **(A) Required (status quo).** Every KEY column declares its namespace. Plugin authors for dynamic cases build the OutputConfig per-call (SDMX example above).
- **(B) Optional, catalog supplies default.** If omitted on KEY, `Catalog(name=ns).add_from_result(result)` uses `ns` as the namespace. Plugin authors write `Column(role=KEY)` without repeating the namespace.

Recommendation: **(B).** Cleaner for the dynamic case, backward-compatible for static case (explicit wins).

### 9.2 `SeriesEntry` / `SeriesMatch` → rename to `Entry` / `Match`?

Current names reflect a finance-lineage ("series"). With `CatalogBackend` being
provider-agnostic, plain `Entry` / `Match` is more honest. But it's a rename
churn — every plugin importing `SeriesEntry` has to update.

Recommendation: **keep `SeriesEntry` / `SeriesMatch`** for this refactor (one
breaking change at a time); revisit in a later minor if the Protocol gets wide
external adoption.

### 9.3 Where does `parse_catalog_url` live?

Options:
- Top-level `parsimony.url_parse` (discoverable, but creates a module for ~25 LOC).
- Inside `parsimony.catalog` (co-located with `Catalog.from_url`).
- Inside `parsimony.publish` (co-located with publish target parsing).

Recommendation: **`parsimony.catalog`**, since it's used by both `Catalog.from_url` (read) and `publish()` (write).

### 9.4 `EmbeddingProvider` — ABC or Protocol?

Same question as `BaseCatalog`. Currently ABC; three implementations exist
(`SentenceTransformerEmbedder`, `LiteLLMEmbeddingProvider`, a test fake).

Recommendation: **Protocol.** Consistency with `CatalogBackend`; easier to write test fakes.

### 9.5 CLI `--force` flag semantics post-refactor

Current: `--force=all` rebuilds every namespace; `--force --only X` rebuilds
one. Complex.

Recommendation: drop `--force=all`; `--only X` by itself is unambiguous and
already always-rebuilds (since resume is killed). No force flag at all.

## 10. Release / versioning

Single breaking release. No alpha, no deprecation cycle.

| Package | Version | State |
|---|---|---|
| `parsimony-core` | `0.3.0` | Phase 1 + 3 merged: new kernel, new docs, old surface gone |
| `parsimony-fred` | `0.3.0` | Phase 2 merged: uses `CATALOGS` |
| `parsimony-sdmx` | `0.3.0` | Phase 2 merged: uses `CATALOGS` + `RESOLVE_CATALOG` |

- No `CONTRACT_VERSION` / `parsimony-contract-v*` keyword anywhere.
- Plugin `dependencies` pin: `parsimony-core>=0.3,<0.5`.

## 11. Risks

No external users, so backward-compatibility risk is gone. Remaining risks:

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `CATALOGS` async-factory vs list dispatch trips us up in testing | medium | low | Accept both shapes; one unit test per shape |
| Merging `SemanticTableResult` into `Result` changes arrow-roundtrip behaviour | low | medium | Keep `test_result_arrow.py`; add regression check on the validator |
| `CatalogBackend` Protocol turns out too narrow for a real backend | low | medium | Informally sanity-check against "how would I implement this over Postgres+pgvector" as a mental exercise before merging |
| Partitioning decision (post-refactor) invalidates `CATALOGS` shape | low | medium | `CATALOGS` is granularity-agnostic; any partition count works |
| Flattening modules creates circular-import surprises | medium | low | Audit `from parsimony.X import ...` after each merge; defer heavy imports in `__init__.py` |

## 12. Out of scope

- The partitioning question (see `catalog-partitioning-design.md`).
- Any new feature. This is a subtraction, not addition.
- Rewriting `parsimony-mcp` (separate repo, already uses public surface).
- Benchmarking. If something regresses measurably we'll find out; no prophylactic perf work.

## 13. Checklist for execution

**Before starting:**
- [ ] Confirm Column `namespace` required-vs-optional decision (§9.1)
- [ ] Confirm `parse_catalog_url` location (§9.3)
- [ ] Confirm `EmbeddingProvider` Protocol (§9.4)
- [ ] Confirm `--force` flag removal (§9.5)
- [ ] Confirm `SeriesEntry` rename deferral (§9.2)

**Phase 1 — kernel rewrite (one PR):**
- [ ] Delete subpackages: `bundles/`, `catalog/`, `_standard/`, `discovery/`, `stores/`, `transport/`, `cli/`
- [ ] Delete obsolete tests (bundles/lazy/url/manifest/template/publish-property/publish-hostile/publish-conformance)
- [ ] Write flat modules: `catalog.py`, `embedder.py`, `indexes.py`, `publish.py`, `discovery.py`, `stores.py`, `http.py`, `cli.py`
- [ ] Modify `connector.py`: drop `Namespace`, drop `ResultCallback` + `with_callback`, drop `catalog=` kwarg
- [ ] Modify `result.py`: merge `SemanticTableResult` → `Result` validator, drop namespace-template helpers, make `Column.namespace` optional on KEY
- [ ] Modify `errors.py`: drop `BundleNotFoundError`
- [ ] Modify `__init__.py`: rewrite exports, drop `CONTRACT_VERSION`
- [ ] Rewrite `testing.py` with 3 checks + 4 test methods
- [ ] Rewrite `cli.py` with 2 verbs
- [ ] Full test suite green (new + kept-intact)
- [ ] Release `parsimony-core 0.3.0`

**Phase 2 — plugin rewrites (one PR per plugin, in parallel):**
- [ ] `parsimony-fred`: drop `CatalogSpec`, add `CATALOGS = [("fred", fred_enumerate)]`, bump dep, release 0.3.0
- [ ] `parsimony-sdmx`: drop `CatalogSpec` + templates, add `CATALOGS` async fn + `RESOLVE_CATALOG`, bump dep, release 0.3.0
- [ ] Validate: `parsimony publish --provider fred --target file:///tmp/fred` produces a working catalog
- [ ] Validate: `parsimony publish --provider sdmx --target file:///tmp/sdmx --only sdmx_datasets` same

**Phase 3 — docs (bundle with Phase 1 PR or follow-up):**
- [ ] Rewrite `docs/contract.md` (remove §10, CONTRACT_VERSION, keyword ABI)
- [ ] Rewrite `docs/catalog-publish.md` (CATALOGS mental model)
- [ ] Delete `docs/migration-catalog-publish.md`
- [ ] Update `README.md` (new plugin shape in quickstart)
- [ ] Update `docs/building-a-private-connector.md` (CATALOGS + RESOLVE_CATALOG)

---

*End of plan.*
