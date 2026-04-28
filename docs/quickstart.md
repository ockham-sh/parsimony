# Quickstart

Install parsimony and run your first fetch — no API key required.

## Install

```bash
pip install parsimony-core parsimony-sdmx
```

Python 3.11+. SDMX providers (ECB, Eurostat, IMF, World Bank, BIS, OECD,
ILO) ship as the separate `parsimony-sdmx` plugin and require no
credentials.

## Fetch ECB exchange rates

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

In Jupyter, drop the `asyncio.run()` wrapper and `await` directly — the
notebook already runs an event loop.

Every call returns a `Result` carrying the DataFrame and its provenance:

```python
result.data          # pandas DataFrame
result.provenance    # Provenance(source="sdmx_fetch", params={...}, fetched_at=...)
```

## Discover series via the catalog

The plugin publishes pre-built FAISS catalogs on Hugging Face Hub. Load a
bundle and search it:

```python
from parsimony import Catalog

catalog = await Catalog.from_url("hf://ockham/catalog-sdmx_datasets")
for m in await catalog.search("euro area exchange rates", limit=5):
    print(f"  [{m.namespace}:{m.code}] {m.title}")
```

The first call downloads the bundle into the local Hugging Face cache;
subsequent calls hit the cache.

## Add a credentialed source (FRED)

```bash
pip install parsimony-fred
export FRED_API_KEY="your-key"
```

Free key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html).

```python
from parsimony_fred import CONNECTORS as fred_connectors
fred = fred_connectors.bind_env()      # reads FRED_API_KEY

result = await fred["fred_fetch"](series_id="UNRATE", observation_start="2020-01-01")
```

## Next

- **[Guide](guide.md)** — composing connectors, error handling, the catalog
- **[Connectors](connectors/index.md)** — every available data source
