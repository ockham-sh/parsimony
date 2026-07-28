"""Catalog & connector walkthrough — evolving complexity.

Nine self-contained stages that go from a bare function to the full
search-then-fetch pattern used by real connector packages.

No real network calls. No API keys required. Run with:

    uv run python examples/catalog_walkthrough.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Annotated

import pandas as pd

from parsimony.catalog import Catalog
from parsimony.catalog.indexes import BM25Index
from parsimony.catalog.search import make_local_search_connector
from parsimony.connector import Connectors, connector, enumerator
from parsimony.entity import Entity
from parsimony.errors import EmptyDataError
from parsimony.namespace import Namespace
from parsimony.result import Column, ColumnRole, OutputSpec
from parsimony.transport.helpers import require_key

# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Minimal connector
#
# Any synchronous Python function becomes a connector with one decorator.
# The framework builds the Result envelope; you return raw data.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 1 — Minimal connector")
print("=" * 70)


@connector
def gdp_trend(country: str, years: int = 5) -> pd.DataFrame:
    """Return synthetic GDP trend data for a country."""
    return pd.DataFrame(
        {
            "year": list(range(2020, 2020 + years)),
            "gdp": [round(20 + i * 0.3 + hash(country) % 3 * 0.1, 2) for i in range(years)],
        }
    )


result = gdp_trend(country="US")

assert result.is_tabular
assert list(result.raw.columns) == ["year", "gdp"]
assert result.provenance.source == "gdp_trend"
assert result.provenance.params == {"country": "US", "years": 5}

print(f"connector:  {gdp_trend!r}")
print(f"raw:\n{result.raw.to_string(index=False)}")
print(f"provenance: source={result.provenance.source!r}, params={result.provenance.params}")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — OutputSpec: declare column roles
#
# OutputSpec maps raw DataFrame columns to semantic roles:
#   KEY      — unique identifier (can be used as a catalog code)
#   TITLE    — human-readable label
#   DATA     — numeric/string payload
#   METADATA — filtering context, not a primary analysis target
#
# OutputSpec is purely declarative — it never renames, coerces, reorders, or
# drops a column. Whatever the connector body returns is exactly what lands
# on result.raw. If a provider hands you strings, coerce them yourself
# (pd.to_datetime, pd.to_numeric) before returning.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 2 — OutputSpec: schema and roles")
print("=" * 70)

SERIES_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="acme"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="freq", role=ColumnRole.METADATA),
        Column(name="obs_date", role=ColumnRole.DATA),
        Column(name="obs_value", role=ColumnRole.DATA),
    ]
)


@connector(output=SERIES_OUTPUT, tags=["acme", "data"])
def acme_fetch(series_id: str, start: str = "2024-01-01") -> pd.DataFrame:
    """Fetch Acme time series observations."""
    df = pd.DataFrame(
        {
            "series_id": ["A001", "A001"],
            "name": ["Acme Index", "Acme Index"],
            "freq": ["monthly", "monthly"],
            "obs_date": ["2024-01-01", "2024-02-01"],
            "obs_value": ["100.5", "102.3"],  # strings — the connector coerces, not the framework
        }
    )
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df["obs_value"] = pd.to_numeric(df["obs_value"])
    return df


result2 = acme_fetch(series_id="A001")

assert pd.api.types.is_numeric_dtype(result2.raw["obs_value"])
assert pd.api.types.is_datetime64_any_dtype(result2.raw["obs_date"])

cols_info = [(c.name, c.role.value) for c in result2.columns]
print(f"columns: {cols_info}")
print(f"raw:\n{result2.raw.to_string(index=False)}")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Secrets, require_key, and bind
#
# secrets=(...) marks parameters whose values are excluded from provenance
# and safe_dump().  require_key() resolves api_key from the argument or an
# env var fallback, raising UnauthorizedError when neither is set.
# bind() produces a new Connector with parameters fixed; bound params are
# removed from the exposed call surface and call-time provenance.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 3 — Secrets, require_key, and bind")
print("=" * 70)


@connector(secrets=("api_key",))
def secured_fetch(series_id: str, api_key: str = "") -> pd.DataFrame:
    """Fetch data; api_key is resolved from arg or ACME_KEY env var."""
    _key = require_key(api_key, env_var="ACME_KEY", provider="acme")
    return pd.DataFrame({"series_id": [series_id], "value": [42.0]})


os.environ["ACME_KEY"] = "env-key-for-test"

# api_key resolved from env; excluded from provenance
r3 = secured_fetch(series_id="X1")
assert "api_key" not in r3.provenance.params
print(f"provenance params (no api_key): {r3.provenance.params}")

# bind() fixes the key; new connector hides it from the call surface
bound = secured_fetch.bind(api_key="hardcoded-key")
assert "api_key" not in bound.exposed_signature.parameters
r3b = bound(series_id="X2")
assert r3b.provenance.params == {"series_id": "X2"}
print(f"bound connector surface: {list(bound.exposed_signature.parameters)}")

# safe_dump redacts any param matching the secret name pattern
safe = r3.provenance.safe_dump()
assert safe.get("params", {}).get("api_key") is None
print(f"safe_dump params: {safe['params']}")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 4 — Namespace annotation
#
# Annotate a fetch parameter with Namespace("ns_name") inside Annotated[T, ...].
# The hint is recorded on the Connector and surfaces in:
#   describe()   — human-readable output
#   to_llm()     — LLM card shows [ns:ns_name]
#
# Agents use this hint to know: "run the search connector for namespace
# 'acme' first, then pass the resulting code as series_id here."
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 4 — Namespace annotation")
print("=" * 70)

FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="acme_dates"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


@connector(output=FETCH_OUTPUT, tags=["acme", "data"], secrets=("api_key",))
def acme_fetch_v2(
    series_id: Annotated[str, Namespace("acme")],  # ← legal values come from the "acme" namespace
    start_date: str = "2024-01-01",
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch Acme series by ID from the acme namespace."""
    _key = require_key(api_key, env_var="ACME_KEY", provider="acme")
    return pd.DataFrame({"date": ["2024-01-01"], "value": [99.0]})


assert acme_fetch_v2.namespace_hints == {"series_id": "acme"}

desc = acme_fetch_v2.describe()
assert "namespace='acme'" in desc
print(desc)

card = acme_fetch_v2.to_llm()
assert "[ns:acme]" in card
print(f"\nLLM card:\n{card}")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 5 — Connectors collection
#
# Connectors is an immutable, composable bundle.  Key operations:
#   +          concatenate two bundles (unique names enforced)
#   bind()     fix parameters across every matching connector
#   filter()   subset by predicate and/or tags
#   search()   substring match over name + description
#   to_llm()   render an LLM-ready prompt section
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 5 — Connectors collection")
print("=" * 70)

SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="acme"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

LIST_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="acme"),
        Column(name="label", role=ColumnRole.TITLE),
        Column(name="freq", role=ColumnRole.METADATA),
    ]
)


@connector(output=SEARCH_OUTPUT, tags=["acme", "tool"])
def acme_search(query: str, limit: int = 10) -> pd.DataFrame:
    """Search the Acme catalog for series matching a keyword."""
    return pd.DataFrame({"code": ["A001", "A002"], "title": ["Alpha", "Beta"], "score": [0.9, 0.7]})


@enumerator(output=LIST_OUTPUT)
def acme_list() -> pd.DataFrame:
    """Enumerate all available Acme series."""
    return pd.DataFrame(
        {
            "code": ["A001", "A002", "A003"],
            "label": ["Alpha Index", "Beta Rate", "Gamma Spread"],
            "freq": ["monthly", "quarterly", "daily"],
        }
    )


bundle = Connectors([acme_search, acme_fetch_v2, acme_list])
print(bundle.describe())

# bind api_key across all connectors that accept it
bound_bundle = bundle.bind(api_key="bundle-key")
assert "api_key" not in bound_bundle["acme_fetch_v2"].exposed_signature.parameters

# filter by tags
tool_only = bundle.filter(tags=["tool"])
assert tool_only.names() == ["acme_search"]
print(f"filter(tags=['tool']): {tool_only.names()}")

# + operator — unique names across both bundles
other = Connectors([gdp_trend])
combined = bundle + other
assert "gdp_trend" in combined
print(f"combined bundle: {combined.names()}")

# LLM prompt section
llm_text = bundle.to_llm(header="Acme data connectors")
assert "acme_search" in llm_text and "[ns:acme]" in llm_text
print(f"\nto_llm() excerpt (first 300 chars):\n{llm_text[:300]}…")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 6 — Building a Catalog
#
# Catalog holds Entity entries and one or more named CatalogIndex instances.
# Index types:
#   BM25Index   — keyword matching (no embedding model required)
#   VectorIndex — semantic similarity via sentence-transformers + FAISS
#   HybridIndex — BM25 + vector components fused with Reciprocal Rank Fusion (RRF)
#
# indexes=None  → framework auto-creates BM25 indexes for code, title, and
#                 every metadata key observed in the entries at build time.
# A query is always literal text. It targets the "title" index by convention —
# no constructor argument needed — and any other indexed field is reached with
# field="name". Exact constraints are never expressed in the query text: they go
# in filter=, which excludes non-matching rows instead of merely re-ranking them.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 6 — Building a Catalog (BM25, literal queries, exact filters)")
print("=" * 70)

entries = [
    Entity(namespace="acme", code="INFL", title="Inflation Rate", metadata={"freq": "monthly", "source": "govt"}),
    Entity(namespace="acme", code="UNEMP", title="Unemployment Rate", metadata={"freq": "monthly", "source": "govt"}),
    Entity(
        namespace="acme", code="GDP", title="Gross Domestic Product", metadata={"freq": "quarterly", "source": "govt"}
    ),
    Entity(
        namespace="acme", code="CREDIT", title="Consumer Credit Growth", metadata={"freq": "monthly", "source": "bank"}
    ),
    Entity(
        namespace="acme", code="YIELD", title="Government Bond Yield 10yr", metadata={"freq": "daily", "source": "mkt"}
    ),
    Entity(namespace="acme", code="FOREX", title="USD/EUR Exchange Rate", metadata={"freq": "daily", "source": "mkt"}),
]

# Explicit indexes gives full control; a "title" index enables plain-text search.
cat = Catalog(
    "acme",
    indexes={
        "title": BM25Index(),
        "code": BM25Index(),
        "freq": BM25Index(),
    },
)
cat.set_entities(entries)
cat.build()

# Broad search — targets the "title" index by convention
hits = cat.search("inflation rate", limit=5)
assert hits and hits[0].code == "INFL"

# Exact constraint — filter=, the replacement for any "field: value" spelling
hits2 = cat.search(filter={"freq": "daily"}, limit=5)
assert {"YIELD", "FOREX"} == {h.code for h in hits2}

# Score one named field instead of the default
hits3 = cat.search("GDP", field="code", limit=3)
assert hits3 and hits3[0].code == "GDP"
print(f"field='code' 'GDP':       {[(h.code, round(h.score, 2)) for h in hits3]}")

# Point lookup
e = cat.get("acme", "GDP")
assert e and e.title == "Gross Domestic Product"
print(f"get('acme', 'GDP'):       {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 7 — Catalog persistence: save and load
#
# save() writes an atomic snapshot to a directory:
#   entries.parquet       — all Entity rows as Parquet
#   indexes/<field>/      — serialised index (BM25 postings or FAISS vectors)
#   meta.json             — schema, index fields, builder metadata
# Catalog.load() reads it back; no rebuild needed.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 7 — Catalog persistence: save() / load()")
print("=" * 70)

with tempfile.TemporaryDirectory() as tmp:
    snapshot_path = Path(tmp) / "acme_catalog"

    cat.save(snapshot_path)
    saved_files = sorted(f.relative_to(snapshot_path) for f in snapshot_path.rglob("*") if f.is_file())
    print("snapshot files:\n  " + "\n  ".join(str(f) for f in saved_files))

    # Load from disk — no re-build required
    cat_loaded = Catalog.load(snapshot_path)
    hits_l = cat_loaded.search("consumer credit", limit=3)
    assert any(h.code == "CREDIT" for h in hits_l)
    print(f"\nloaded catalog search 'consumer credit': {[h.code for h in hits_l]}")

    # Add a new entry, save again — set_entities replaces the full set
    new_entries = cat_loaded.entities + [
        Entity(
            namespace="acme",
            code="CPI",
            title="Consumer Price Index",
            metadata={"freq": "monthly", "source": "govt"},
        )
    ]
    cat_loaded.set_entities(new_entries)
    cat_loaded.build()
    cat_loaded.save(snapshot_path)  # atomic overwrite

    cat_v2 = Catalog.load(snapshot_path)
    assert cat_v2.get("acme", "CPI") is not None
    print("add + save + reload: CPI present ✓")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 8 — Enumerator → Catalog: building from connector output
#
# An enumerator connector returns entity-discovery data.  Its OutputSpec
# declares a KEY column (with namespace=), TITLE column(s), and optional
# METADATA columns.  Result.entities projects the result against that
# OutputSpec into Entity instances ready for catalog ingestion — a lazy,
# ref-keyed Mapping[EntityRef, Entity].
#
# Real connector packages use this pattern to build their snapshots:
#   1. Call the enumerator to get a Result.
#   2. Read result.entities.values() to get the Entity list.
#   3. Populate and build the Catalog.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 8 — Enumerator → Catalog build")
print("=" * 70)

ENUM_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="acme2"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="update_freq", role=ColumnRole.METADATA),
    ]
)


@enumerator(output=ENUM_OUTPUT)
def acme_enumerate() -> pd.DataFrame:
    """Enumerate the Acme series universe."""
    return pd.DataFrame(
        {
            "code": ["CORE_CPI", "CORE_PCE", "NONFARM", "ISM_MFG", "IND_PROD"],
            "name": ["Core CPI", "Core PCE", "Nonfarm Payrolls", "ISM Manufacturing", "Industrial Production"],
            "category": ["prices", "prices", "labor", "sentiment", "output"],
            "update_freq": ["monthly", "monthly", "monthly", "monthly", "monthly"],
        }
    )


# Call the enumerator — produces a Result like any connector
enum_result = acme_enumerate()
assert enum_result.is_tabular

# .entities projects the declared OutputSpec against .raw into Entity objects
entities = list(enum_result.entities.values())
assert len(entities) == 5
assert entities[0].namespace == "acme2"
assert entities[0].code == "CORE_CPI"
assert entities[0].metadata["category"] == "prices"
print("entities from enumerator:\n  " + "\n  ".join(f"{e.code}: {e.metadata}" for e in entities))

# Build a catalog from the enumerator output.
# indexes=None → auto-create BM25 indexes for code, title, and all metadata keys.
cat2 = Catalog("acme2")
cat2.set_entities(entities)
cat2.build()

hits_e = cat2.search("payrolls", limit=3)
assert any(h.code == "NONFARM" for h in hits_e)
print(f"\ncatalog search 'payrolls': {[h.code for h in hits_e]}")

# Metadata indexes are auto-created because indexes=None, so every metadata key
# is addressable — as a filter column here, or as field= for scoring.
hits_cat = cat2.search(filter={"category": "prices"}, limit=5)
price_codes = sorted(h.code for h in hits_cat)
assert set(price_codes) == {"CORE_CPI", "CORE_PCE"}
print(f"filter category=prices:   {price_codes}")

# ─────────────────────────────────────────────────────────────────────────────
# Stage 9 — Full search-then-fetch pattern with make_local_search_connector
#
# Real provider packages wire three things together:
#   1. An enumerator that populates a local snapshot catalog.
#   2. A search connector built with make_local_search_connector() that:
#        • loads (or lazily builds) the catalog snapshot on first call,
#        • runs BM25/hybrid search,
#        • returns a DataFrame with KEY column in the provider's namespace,
#        • caches the loaded Catalog in a CatalogLRU for cheap repeat calls.
#   3. A fetch connector whose series_id: Annotated[str, Namespace("ns")]
#      links back to the search result KEY column.
#
# The Namespace annotation is the machine-readable contract that tells agents
# and humans: "run the search connector, take the 'code' KEY column, pass it
# as series_id to the fetch connector."
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Stage 9 — Full search-then-fetch with make_local_search_connector")
print("=" * 70)

FETCH_V2_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY, namespace="acme2_obs"),
        Column(name="value", role=ColumnRole.DATA),
    ]
)


@connector(output=FETCH_V2_OUTPUT, tags=["acme2", "data"])
def acme2_fetch(
    series_id: Annotated[str, Namespace("acme2")],  # ← must come from search result KEY
    start_date: str = "2024-01-01",
) -> pd.DataFrame:
    """Fetch Acme2 observations for a series from the acme2 namespace."""
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "value": [2.5, 2.7, 2.9],
        }
    )


# Build a local catalog snapshot from the enumerator (stage 8) and wire a
# search connector to it using the factory helper.
with tempfile.TemporaryDirectory() as tmp2:
    snap = Path(tmp2) / "acme2_catalog"
    cat2.save(snap)

    # make_local_search_connector returns a fully-formed Connector that loads
    # the catalog at call time (first call only) and caches it in a CatalogLRU.
    acme_search_v2 = make_local_search_connector(
        provider="acme2",
        default_url=str(snap),  # would be "hf://acme-org/acme2-catalog" in prod
        catalog_url_env_var="ACME2_CATALOG_URL",
        tags=["acme2", "tool"],
        description="Search the Acme2 catalog for economic series.",
        # score/search_detail are appended automatically — declare only the
        # provider-specific columns here. METADATA roles are projected from
        # each match's metadata bag onto the hit table.
        output=OutputSpec(
            columns=[
                Column(name="code", role=ColumnRole.KEY, namespace="acme2"),
                Column(name="title", role=ColumnRole.TITLE),
                Column(name="category", role=ColumnRole.METADATA),
                Column(name="update_freq", role=ColumnRole.METADATA),
            ]
        ),
    )
    print(f"search connector: {acme_search_v2!r}")
    print(f"tags: {acme_search_v2.tags}")

    # First call loads catalog from snap path, runs BM25 search
    r_search = acme_search_v2(query="core inflation prices")
    assert r_search.is_tabular
    print(f"\nsearch 'core inflation prices':\n{r_search.raw.to_string(index=False)}")

    # EmptyDataError when no results match
    try:
        acme_search_v2(query="zzznomatch999")
    except EmptyDataError as exc:
        print(f"\nno-match → EmptyDataError: {exc}")

# Wire the two connectors into a bundle — this is what a connector package
# exports as its CONNECTORS constant.
bundle2 = Connectors([acme_search_v2, acme2_fetch])
print(f"\nfinal bundle: {bundle2.names()}")
print(bundle2.to_llm())

print("\n" + "=" * 70)
print("All stages completed successfully.")
print("=" * 70)
