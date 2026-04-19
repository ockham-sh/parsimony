# Quickstart

Get from zero to fetching macroeconomic data in under five minutes -- no API keys required.

## Install

```bash
pip install parsimony-core
```

> **Python 3.11+** required. SDMX providers (ECB, Eurostat, IMF, World Bank) are included in the base install.

---

## Step 1: Fetch ECB Exchange Rates (No API Key)

SDMX connectors work out of the box. Fetch the daily USD/EUR spot rate from the ECB:

**Script**

```python
import asyncio
from parsimony.connectors.sdmx import CONNECTORS as SDMX

async def main():
    result = await SDMX["sdmx"](
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
from parsimony.connectors.sdmx import CONNECTORS as SDMX

result = await SDMX["sdmx"](
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

For schema-aware connectors like `sdmx`, the result is a `SemanticTableResult` with typed column roles:

```python
result.columns          # all Column objects with role, dtype, namespace
result.entity_keys      # DataFrame subset: columns with role == KEY
result.data_columns     # [Column(name="TIME_PERIOD", ...), Column(name="value", ...)]
result.metadata_columns # [Column(name="...", role=METADATA)]
```

---

## Step 3: Explore Available Series

Discover what data is available before fetching:

```python
# List all ECB datasets
datasets = await SDMX["sdmx_list_datasets"](agency="ECB")
print(datasets.data.head(10))
```

```
       dataset_id                                       name
0             AME  AMECO - Annual macro-economic database ...
1             BKN                   Banknote statistics (BKN)
2             BLS               Bank Lending Survey (BLS)
...
```

```python
# Inspect the exchange rate dataset structure
dsd = await SDMX["sdmx_dsd"](dataset_key="ECB-EXR")
print(dsd.data)
```

```
   position dimension_id   concept_name  codelist_size
0         0         FREQ      Frequency              9
1         1     CURRENCY       Currency            350
2         2  CURRENCY_DE  Currency deno            350
3         3     EXR_TYPE  Exchange rate             33
4         4   EXR_SUFFIX  Series varia             15
```

```python
# See valid codes for a dimension
codes = await SDMX["sdmx_codelist"](dataset_key="ECB-EXR", dimension="CURRENCY")
print(codes.data.head(5))
```

---

## Step 4: Add FRED (Free API Key)

FRED provides US macroeconomic data. Get a free key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html), then:

```bash
export FRED_API_KEY="your-key-here"
```

**Search for series:**

```python
from parsimony.connectors.fred import CONNECTORS as FRED

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
result = await fred["fred"](
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

## Step 5: Build a Mini Catalog

Index fetched series into a searchable catalog:

```python
from parsimony import Catalog

catalog = Catalog("ecb_exr")

# Index the ECB exchange rate result
ecb_result = await SDMX["sdmx"](
    dataset_key="ECB-EXR",
    series_key="D.USD.EUR.SP00.A",
)
summary = await catalog.index_result(ecb_result)
print(f"Indexed: {summary.indexed}, Skipped: {summary.skipped}")

# Search the catalog (BM25 + vector via FAISS, fused with RRF)
matches = await catalog.search("dollar euro exchange", limit=5)
for m in matches:
    print(f"  [{m.namespace}:{m.code}] {m.title}")
```

```
Indexed: 1, Skipped: 0
  [sdmx_ecb_exr:D.USD.EUR.SP00.A] US dollar/Euro (EXR)
```

`parsimony.Catalog` requires the standard extra (FAISS + BM25 + sentence-transformers):

```bash
pip install 'parsimony-core[standard]'        # local sentence-transformers embedder
# pip install 'parsimony-core[standard,litellm]'  # also enable hosted-API embedder (OpenAI, Gemini, ...)
```

To use a different sentence-transformers model, pass an embedder explicitly:

```python
from parsimony import Catalog, SentenceTransformerEmbedder

catalog = Catalog("ecb_exr", embedder=SentenceTransformerEmbedder(model="BAAI/bge-base-en-v1.5"))
```

To use a hosted API (OpenAI, Gemini, Cohere, Voyage, Bedrock, ...) via litellm:

```python
from parsimony import Catalog, LiteLLMEmbeddingProvider

catalog = Catalog("ecb_exr", embedder=LiteLLMEmbeddingProvider(
    model="openai/text-embedding-3-small",
    dimension=1536,
))
```

To pull a published catalog instead of building one:

```python
catalog = await Catalog.from_url("hf://ockham/ecb-yc-catalog")
```

---

## Common Patterns

### Script vs Jupyter

All connectors are async. In scripts, wrap with `asyncio.run()`. In Jupyter, use `await` directly (notebooks already run an event loop).

### Dict vs keyword params

Connectors accept keyword arguments or a typed Pydantic model:

```python
# Keyword arguments (recommended)
await SDMX["sdmx"](dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A")

# Pydantic model (for programmatic use)
from parsimony.connectors.sdmx import SdmxFetchParams
await SDMX["sdmx"](SdmxFetchParams(dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A"))
```

### Composing multiple sources

```python
from parsimony import Connectors
from parsimony.connectors.fred import CONNECTORS as FRED
from parsimony.connectors.sdmx import CONNECTORS as SDMX

all_connectors = FRED.bind_deps(api_key="your-key") + SDMX
result = await all_connectors["fred"](series_id="GDP")
```

---

## Next Steps

- [User Guide](user-guide.md) -- custom connectors, enumerators, loaders, and data stores
- [Architecture](architecture.md) -- connector, result, and catalog design
- [API Reference](api-reference.md) -- full class and function documentation
