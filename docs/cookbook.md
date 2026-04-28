# Recipes

Six end-to-end examples. Each is self-contained — install the relevant
plugin alongside `parsimony-core` and run. Replace `bind(api_key=...)`
with `bind_env()` if you've exported the env var.

---

## 1. Plot US GDP with Altair

```python
import asyncio
import altair as alt
from parsimony_fred import CONNECTORS as FRED

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    result = await fred["fred_fetch"](series_id="GDPC1")
    chart = alt.Chart(result.data).mark_line().encode(
        x=alt.X("date:T"), y=alt.Y("value:Q", title="Real GDP"),
    )
    chart.save("us_gdp.html")

asyncio.run(main())
```

---

## 2. Compare EUR/USD across two sources

```python
import asyncio
import pandas as pd
from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    fred_r, ecb_r = await asyncio.gather(
        fred["fred_fetch"](series_id="DEXUSEU", observation_start="2023-01-01"),
        SDMX["sdmx_fetch"](dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A",
                           start_period="2023-01"),
    )
    fred_df = fred_r.data[["date", "value"]].rename(columns={"value": "fred"})
    ecb_df = ecb_r.data.rename(columns={"TIME_PERIOD": "date", "value": "ecb"})
    ecb_df["date"] = pd.to_datetime(ecb_df["date"])
    merged = fred_df.merge(ecb_df[["date", "ecb"]], on="date")
    print(f"correlation: {merged['fred'].corr(merged['ecb']):.4f}")

asyncio.run(main())
```

---

## 3. Build a searchable FRED employment catalog

```python
import asyncio
from parsimony import Catalog
from parsimony_fred import CONNECTORS as FRED

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    catalog = Catalog("fred")
    result = await fred["enumerate_fred_release"](release_id=50)   # Employment Situation
    await catalog.add_from_result(result)
    for m in await catalog.search("unemployment rate", limit=5):
        print(f"  {m.code:15s}  {m.title}")

asyncio.run(main())
```

---

## 4. Compute financial ratios from FMP statements

```python
import asyncio
from parsimony_fmp import CONNECTORS as FMP

async def main():
    fmp = FMP.bind(api_key="YOUR_FMP_KEY")
    params = {"symbol": "AAPL", "period": "annual", "limit": 3}
    inc, bs = await asyncio.gather(
        fmp["fmp_income_statements"](**params),
        fmp["fmp_balance_sheet_statements"](**params),
    )
    inc.data["margin"] = inc.data["netIncome"] / inc.data["revenue"]
    bs.data["de_ratio"] = bs.data["totalDebt"] / bs.data["totalStockholdersEquity"]
    print(inc.data[["date", "margin"]])
    print(bs.data[["date", "de_ratio"]])

asyncio.run(main())
```

---

## 5. Track Polymarket prediction markets (no credentials)

```python
import asyncio
from parsimony_polymarket import CONNECTORS as POLY

async def main():
    result = await POLY["polymarket_gamma_fetch"](query="Bitcoin", limit=10)
    print(result.data[["title", "slug", "liquidity", "volume"]])

asyncio.run(main())
```

---

## 6. Two-hop SDMX discovery (datasets → series → fetch)

Search the published `sdmx_datasets` catalog, then the per-dataset
`sdmx_series_{agency}_{dataset_id}` catalog, then fetch.

```python
import asyncio
from parsimony import Catalog
from parsimony_sdmx import CONNECTORS as SDMX

async def main():
    datasets = await Catalog.from_url("hf://ockham/catalog-sdmx_datasets")
    ds_hits = await datasets.search("Euro area yield curve", limit=5)
    for m in ds_hits:
        print(f"  [{m.code}] {m.title}")
    # Pick ECB|YC from the matches above
    agency, dataset_id = "ECB", "YC"

    series_ns = f"sdmx_series_{agency.lower()}_{dataset_id.lower()}"
    series = await Catalog.from_url(f"hf://ockham/catalog-{series_ns}")
    for m in await series.search("10-year German bond yield", limit=5):
        print(f"  [{m.code}] {m.title}")

    result = await SDMX["sdmx_fetch"](
        dataset_key=f"{agency}-{dataset_id}",
        series_key="B.DE.EUR.4F.G_N_C.SV_C_YM.SR_10Y",
        start_period="2023-01",
    )
    print(f"Fetched {len(result.data)} observations")

asyncio.run(main())
```
