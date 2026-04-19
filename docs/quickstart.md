# Quickstart

Get from zero to fetching macroeconomic data in under five minutes -- no API keys required.

## Install

```bash
pip install parsimony-core parsimony-sdmx
```

> **Python 3.11+** required. SDMX providers (ECB, Eurostat, IMF, World Bank)
> ship as the separate `parsimony-sdmx` plugin — install it alongside the
> kernel to get `sdmx_fetch` and catalog bundles for SDMX namespaces.

---

## Step 1: Fetch ECB Exchange Rates (No API Key)

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

**Jupyter notebook**

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

```
  series_key                          title  ... TIME_PERIOD    value
  D.USD.EUR.SP00.A  US dollar/Euro (EXR)    ... 2024-12-27   1.0427
  D.USD.EUR.SP00.A  US dollar/Euro (EXR)    ... 2024-12-30   1.0389
  ...
```

---

## Step 2: Inspect the Result

Every connector call returns a `Result` (or `SemanticTableResult`) carrying the data and its provenance:

```python
result.data          # pandas DataFrame
result.provenance    # Provenance(source="sdmx", params={...}, fetched_at=...)
```

For schema-aware connectors like `sdmx_fetch`, the result is a `SemanticTableResult` with typed column roles:

```python
result.columns          # all Column objects with role, dtype, namespace
result.entity_keys      # DataFrame subset: columns with role == KEY
result.data_columns     # [Column(name="TIME_PERIOD", ...), Column(name="value", ...)]
result.metadata_columns # [Column(name="...", role=METADATA)]
```

---

## Step 3: Explore Available Series

The `parsimony-sdmx` plugin publishes pre-built Catalog snapshots on
HuggingFace Hub — one per namespace. Load the bundle you want with
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
`from_url` call downloads the bundle (~50–200 MB) and loads the
embedder; subsequent calls hit the local Hugging Face cache.

---

## Step 4: Add FRED (Free API Key)

FRED provides US macroeconomic data. Get a free key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html), then:

```bash
export FRED_API_KEY="your-key-here"
```

**Search for series:**

```python
from parsimony_fred import CONNECTORS as FRED

fred = FRED.bind_deps(api_key="your-key-here")

search = await fred["fred_search"](search_text="US unemployment rate")
print(search.data[["id", "title"]].head())
```

```
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

```
  series_id                 title  ... date        value
  UNRATE     Unemployment Rate    ... 2024-11-01    4.2
  UNRATE     Unemployment Rate    ... 2024-12-01    4.1
```

---

## Step 5: Search the Catalog

`parsimony.Catalog` is a Parquet + FAISS + BM25 hybrid-search catalog
with reciprocal rank fusion. Install the `[standard]` extra to get the
runtime:

```bash
pip install 'parsimony-core[standard]'
```

Load a published bundle by URL:

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

catalog = Catalog("my_catalog")
ecb_result = await SDMX["sdmx_fetch"](
    dataset_key="ECB-EXR",
    series_key="D.USD.EUR.SP00.A",
)
await catalog.index_result(ecb_result)

await catalog.push("file:///tmp/my_catalog")      # local directory
await catalog.push("hf://myorg/catalog-mine")     # Hugging Face Hub
```

The canonical on-disk layout is three files in one directory: `meta.json`
+ `entries.parquet` + `embeddings.faiss`. Writes are atomic (temp-dir
rename). `Catalog.from_url` picks the scheme handler (`file://`, `hf://`,
`s3://` planned) automatically.

Custom backends (Postgres + pgvector, Redis, in-memory mocks) subclass
`parsimony.BaseCatalog` directly — there is no plugin axis for catalogs.

---

## Common Patterns

### Script vs Jupyter

All connectors are async. In scripts, wrap with `asyncio.run()`. In Jupyter, use `await` directly (notebooks already run an event loop).

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
from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX

all_connectors = FRED.bind_deps(api_key="your-key") + SDMX
result = await all_connectors["fred_fetch"](series_id="GDP")
```

---

## Next Steps

- [User Guide](user-guide.md) -- custom connectors, enumerators, loaders, and data stores
- [Architecture](architecture.md) -- connector, result, and catalog design
- [API Reference](api-reference.md) -- full class and function documentation
