# Quickstart

Get from zero to fetching macroeconomic data in under five minutes — no API
keys required.

## Install

```bash
pip install parsimony-core parsimony-sdmx
```

> **Python 3.11+** required. SDMX providers (ECB, Eurostat, IMF, World Bank)
> ship as the separate `parsimony-sdmx` plugin — install it alongside the
> kernel to get `sdmx_fetch` and catalog bundles for SDMX namespaces.

---

## Step 1: Fetch ECB exchange rates (no API key)

Once `parsimony-sdmx` is installed, the SDMX connectors are discovered
automatically via the `parsimony.providers` entry point. Fetch the daily
USD/EUR spot rate from the ECB:

**Script**

```python
import asyncio
from parsimony_sdmx import CONNECTORS as SDMX

async def main():
    result = await SDMX["sdmx_fetch"](
        dataset_key="ECB-EXR",
        series_key="D.USD.EUR.SP00.A",
        start_period="2024-01",
    )
    print(result.data.tail())
    print(result.provenance)

asyncio.run(main())
```

**Jupyter**

```python
from parsimony_sdmx import CONNECTORS as SDMX

result = await SDMX["sdmx_fetch"](
    dataset_key="ECB-EXR",
    series_key="D.USD.EUR.SP00.A",
    start_period="2024-01",
)
result.data.tail()
```

Expected output:

```text
  series_key         title                ... TIME_PERIOD    value
  D.USD.EUR.SP00.A   US dollar/Euro       ... 2024-12-27   1.0427
  D.USD.EUR.SP00.A   US dollar/Euro       ... 2024-12-30   1.0389
  ...
```

---

## Step 2: Inspect the result

Every connector call returns a `Result` carrying the DataFrame and its
provenance:

```python
result.data          # pandas DataFrame
result.provenance    # Provenance(source="sdmx_fetch", params={...}, fetched_at=...)
```

When a connector declares an `OutputConfig` (every `@enumerator` and
`@loader` does; `@connector` does optionally), the result also carries
that schema. The schema-aware accessors expose typed column groups:

```python
result.output_schema     # OutputConfig (or None)
result.entity_keys       # DataFrame subset: KEY columns
result.data_columns      # [Column(name="TIME_PERIOD", ...), Column(name="value", ...)]
result.metadata_columns  # [Column(name="...", role=METADATA)]
```

When no schema is attached, these accessors return empty DataFrames / lists.

---

## Step 3: Explore available series

The `parsimony-sdmx` plugin publishes pre-built Catalog snapshots on
Hugging Face Hub — one per namespace. Load the bundle you want with
`Catalog.from_url` and search it directly:

```python
from parsimony import Catalog

# Find datasets semantically
datasets_catalog = await Catalog.from_url("hf://ockham/catalog-sdmx_datasets")
for m in await datasets_catalog.search("euro area exchange rates", limit=10):
    print(f"  [{m.namespace}:{m.code}] {m.title}")

# Drill into a specific dataset's series bundle once you have the key
series_catalog = await Catalog.from_url("hf://ockham/catalog-sdmx_series_ecb_exr")
for m in await series_catalog.search("daily USD EUR", limit=10):
    print(f"  [{m.namespace}:{m.code}] {m.title}")
```

Each dataset has its own per-dataset series namespace following the
template `sdmx_series_{agency}_{dataset_id}` (lowercased). The first
`from_url` call downloads the bundle (~50–200 MB) and loads the embedder;
subsequent calls hit the local Hugging Face cache.

---

## Step 4: Add FRED (free API key)

FRED provides US macroeconomic data. Get a free key at
[fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html), then:

```bash
pip install parsimony-fred
export FRED_API_KEY="your-key-here"
```

**Search for series:**

```python
from parsimony_fred import CONNECTORS as fred_connectors

fred = fred_connectors.bind_env()      # reads FRED_API_KEY from os.environ

search = await fred["fred_search"](search_text="US unemployment rate")
print(search.data[["id", "title"]].head())
```

```text
          id                                   title
0      UNRATE             Unemployment Rate
1    UNRATENSA  Unemployment Rate (Not Seasonally Adjusted)
...
```

**Fetch a series:**

```python
result = await fred["fred_fetch"](
    series_id="UNRATE",
    observation_start="2020-01-01",
)
print(result.data.tail())
```

```text
  series_id                 title  ... date        value
  UNRATE     Unemployment Rate    ... 2024-11-01    4.2
  UNRATE     Unemployment Rate    ... 2024-12-01    4.1
```

---

## Step 5: Build and search a catalog

`parsimony.Catalog` is a Parquet + FAISS + BM25 hybrid-search catalog
with reciprocal rank fusion. Install the `[standard]` extra to get the
runtime:

```bash
pip install 'parsimony-core[standard]'
```

### Load a published bundle

```python
from parsimony import Catalog

catalog = await Catalog.from_url("hf://ockham/catalog-snb")
matches = await catalog.search("policy rate", limit=5)
for m in matches:
    print(f"  [{m.namespace}:{m.code}] {m.title}  (sim={m.similarity:.3f})")
```

### Build your own catalog

Build a catalog from any `@enumerator` result and save it locally or
publish to Hugging Face Hub:

```python
from parsimony import Catalog
from parsimony_fred import CONNECTORS as fred_connectors

fred = fred_connectors.bind_env()      # reads FRED_API_KEY
catalog = Catalog("fred")

# Enumerate all series in FRED release 10 (Employment Situation)
enum_result = await fred["enumerate_fred_release"](release_id=10)
await catalog.add_from_result(enum_result)

await catalog.push("file:///tmp/catalog-fred")      # local directory
# await catalog.push("hf://myorg/catalog-fred")     # Hugging Face Hub
```

The canonical on-disk layout is three files in one directory: `meta.json`
+ `entries.parquet` + `embeddings.faiss`. Writes are atomic (temp-dir
rename). `Catalog.from_url` picks the scheme handler (`file://`, `hf://`,
`s3://` planned) automatically.

Custom backends (Postgres + pgvector, Redis, in-memory mocks) match the
`CatalogBackend` `Protocol` in `parsimony.catalog` — two methods: `add`
and `search`. No subclassing required.

### Publish from the CLI

For plugin authors: a single command builds every namespace declared by a
plugin's `CATALOGS` export and pushes each to the URL template:

```bash
parsimony publish --provider fred --target 'hf://myorg/catalog-{namespace}'
parsimony publish --provider sdmx --target 'file:///tmp/sdmx/{namespace}' --only sdmx_datasets
```

See [`building-a-private-connector.md`](building-a-private-connector.md)
for the `CATALOGS` / `RESOLVE_CATALOG` plugin shape.

---

## Common patterns

### Script vs Jupyter

All connectors are async. In scripts, wrap with `asyncio.run()`. In
Jupyter, use `await` directly (notebooks run an event loop).

### Dict vs keyword params

Connectors accept keyword arguments or a typed Pydantic model:

```python
# Keyword arguments (recommended)
await SDMX["sdmx_fetch"](dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A")

# Pydantic model (for programmatic use)
from parsimony_sdmx.connectors.fetch import SdmxFetchParams
await SDMX["sdmx_fetch"](SdmxFetchParams(dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A"))
```

### Composing multiple sources

```python
from parsimony import Connectors
from parsimony_fred import CONNECTORS as fred
from parsimony_sdmx import CONNECTORS as sdmx

all_connectors = Connectors.merge(fred, sdmx).bind_env()
result = await all_connectors["fred_fetch"](series_id="GDP")
```

Or use `discover.load_all()` to compose every installed plugin in one
call, reading env vars automatically:

```python
from parsimony import discover

connectors = discover.load_all().bind_env()
result = await connectors["fred_fetch"](series_id="GDP")
```

Connectors whose required env vars are not set stay in the collection but
raise `UnauthorizedError` on call — list them via `connectors.unbound`.

---

## Next steps

- [User Guide](user-guide.md) — detailed walkthrough of all features
- [Architecture](architecture.md) — connector, result, and catalog design
- [API Reference](api-reference.md) — full class and function documentation
- [Plugin Contract](contract.md) — what every `parsimony-<name>` package implements
