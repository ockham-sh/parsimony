# Cookbook

Ten practical recipes. Each is self-contained and runnable as a standalone
script. All recipes assume the relevant plugin is installed alongside
`parsimony-core`.

---

### Recipe 1: Fetch US GDP and plot with Altair

_Fetch quarterly real GDP from FRED and render a line chart._

```python
import asyncio
import altair as alt
from parsimony_fred import CONNECTORS as FRED

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    result = await fred["fred_fetch"](series_id="GDPC1")
    df = result.data
    chart = alt.Chart(df).mark_line().encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("value:Q", title="Billions of Chained 2017 $"),
    ).properties(title="US Real GDP (GDPC1)")
    chart.save("us_gdp.html")
    print(f"Saved chart with {len(df)} observations")

asyncio.run(main())
```

---

### Recipe 2: Compare EUR/USD from ECB vs FRED

_Fetch the same exchange rate from two sources and merge for comparison._

```python
import asyncio
import pandas as pd
from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    fred_result = await fred["fred_fetch"](
        series_id="DEXUSEU", observation_start="2023-01-01",
    )
    sdmx_result = await SDMX["sdmx_fetch"](
        dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A",
        start_period="2023-01",
    )
    fred_df = fred_result.data[["date", "value"]].rename(columns={"value": "fred"})
    ecb_df = sdmx_result.data.rename(columns={"TIME_PERIOD": "date", "value": "ecb"})
    ecb_df["date"] = pd.to_datetime(ecb_df["date"])
    merged = pd.merge(fred_df, ecb_df[["date", "ecb"]], on="date", how="inner")
    print(f"Merged {len(merged)} rows; correlation: {merged['fred'].corr(merged['ecb']):.4f}")

asyncio.run(main())
```

---

### Recipe 3: Screen for undervalued tech stocks

_Use FMP quotes to filter stocks by PE ratio._

```python
import asyncio
from parsimony_fmp import CONNECTORS as FMP

async def main():
    fmp = FMP.bind(api_key="YOUR_FMP_KEY")
    tickers = ["AAPL", "MSFT", "GOOGL", "META", "INTC", "AMD", "NVDA", "CRM", "ORCL", "IBM"]
    # Fetch quotes for each ticker concurrently
    results = await asyncio.gather(*[
        fmp["fmp_quotes"](symbol=t) for t in tickers
    ])
    rows = []
    for r in results:
        rows.extend(r.data.to_dict("records"))
    import pandas as pd
    df = pd.DataFrame(rows)
    if "pe" in df.columns:
        undervalued = df[df["pe"].between(0, 25)]
        print(f"Stocks with PE < 25:\n{undervalued[['symbol', 'name', 'price', 'pe']].to_string()}")
    else:
        print(f"Quotes:\n{df.head()}")

asyncio.run(main())
```

---

### Recipe 4: Build a searchable FRED employment catalog

_Enumerate a FRED release into an in-memory catalog and search it._

```python
import asyncio
from parsimony import Catalog
from parsimony_fred import CONNECTORS as FRED

async def main():
    catalog = Catalog("fred")
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    # Release 50 = Employment Situation
    result = await fred["enumerate_fred_release"](release_id=50)
    await catalog.add_from_result(result)
    matches = await catalog.search("unemployment rate", limit=5, namespaces=["fred"])
    for m in matches:
        print(f"  {m.code:15s}  {m.title}")

asyncio.run(main())
```

---

### Recipe 5: Fetch company financials and compute ratios

_Pull income statement and balance sheet, then compute profit margin and
debt-to-equity._

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
    inc_df, bs_df = inc.data, bs.data
    if "revenue" in inc_df.columns and "netIncome" in inc_df.columns:
        inc_df["margin"] = inc_df["netIncome"] / inc_df["revenue"]
        print("Profit margins:", inc_df[["date", "margin"]].to_string(index=False))
    if "totalDebt" in bs_df.columns and "totalStockholdersEquity" in bs_df.columns:
        bs_df["de_ratio"] = bs_df["totalDebt"] / bs_df["totalStockholdersEquity"]
        print("D/E ratios:", bs_df[["date", "de_ratio"]].to_string(index=False))

asyncio.run(main())
```

---

### Recipe 6: Track prediction market probabilities

_Fetch a Polymarket event and list its market outcomes._

```python
import asyncio
from parsimony_polymarket import CONNECTORS as POLYMARKET

async def main():
    # No API key required for Polymarket
    result = await POLYMARKET["polymarket_gamma_fetch"](
        query="Bitcoin", limit=10,
    )
    df = result.data
    cols = [c for c in ["title", "slug", "liquidity", "volume"] if c in df.columns]
    print(df[cols].head(10).to_string())

asyncio.run(main())
```

---

### Recipe 7: Multi-source macro dashboard

_Combine FRED inflation and ECB interest rates into one DataFrame._

```python
import asyncio
import pandas as pd
from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    cpi_task = fred["fred_fetch"](series_id="CPIAUCSL", observation_start="2020-01-01")
    ecb_task = SDMX["sdmx_fetch"](
        dataset_key="ECB-FM",
        series_key="M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
        start_period="2020-01",
    )
    cpi_result, ecb_result = await asyncio.gather(cpi_task, ecb_task)
    cpi = cpi_result.data[["date", "value"]].rename(columns={"value": "us_cpi"})
    euribor = ecb_result.data.rename(columns={"TIME_PERIOD": "date", "value": "euribor_3m"})
    euribor["date"] = pd.to_datetime(euribor["date"])
    dashboard = pd.merge(cpi, euribor[["date", "euribor_3m"]], on="date", how="outer").sort_values("date")
    print(f"Dashboard: {len(dashboard)} rows, {dashboard.columns.tolist()}")
    print(dashboard.tail(5).to_string(index=False))

asyncio.run(main())
```

---

### Recipe 8: Export data to Parquet and round-trip with schema

_Fetch FRED data, write to Parquet, and read it back with full schema._

```python
import asyncio
from parsimony import Result
from parsimony_fred import CONNECTORS as FRED

async def main():
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    result = await fred["fred_fetch"](series_id="UNRATE")
    # fred_fetch is decorated with an OutputConfig, so result carries a schema
    result.to_parquet("unrate.parquet")
    print(f"Wrote {len(result.data)} rows to unrate.parquet")
    # Read back — schema and provenance are embedded in Arrow metadata
    restored = Result.from_parquet("unrate.parquet")
    print(f"Restored: {len(restored.data)} rows, source={restored.provenance.source}")
    if restored.output_schema is not None:
        print(f"Columns: {[c.name for c in restored.output_schema.columns]}")

asyncio.run(main())
```

---

### Recipe 9: Batch enumerate multiple FRED releases

_Loop over several releases, index all into one catalog, then search across them._

```python
import asyncio
from parsimony import Catalog
from parsimony_fred import CONNECTORS as FRED

RELEASES = {50: "Employment", 53: "GDP", 10: "CPI"}

async def main():
    catalog = Catalog("fred")
    fred = FRED.bind(api_key="YOUR_FRED_KEY")
    for release_id, label in RELEASES.items():
        result = await fred["enumerate_fred_release"](release_id=release_id)
        idx = await catalog.add_from_result(result)
        print(f"  {label} (release {release_id}): indexed {idx.indexed}, skipped {idx.skipped}")
    namespaces = await catalog.list_namespaces()
    print(f"Namespaces: {namespaces}")
    matches = await catalog.search("consumer price index", limit=5, namespaces=["fred"])
    for m in matches:
        print(f"  {m.code:15s}  {m.title}")

asyncio.run(main())
```

---

### Recipe 10: Discover and fetch SDMX yield curves

_Two-hop catalog flow: search published SDMX bundles for a dataset, then
search that dataset's per-series bundle, then fetch the chosen series._

Requires `pip install parsimony-sdmx`. Dataset metadata lives in the
`sdmx_datasets` namespace; each dataset's series keys live in
`sdmx_series_{agency}_{dataset_id}` (lowercased).

```python
import asyncio
from parsimony import Catalog
from parsimony_sdmx import CONNECTORS as SDMX

async def main():
    # Hop 1: find the yield-curve dataset
    datasets = await Catalog.from_url("hf://ockham/catalog-sdmx_datasets")
    ds_hits = await datasets.search(
        "Euro area government bond yield curve",
        limit=5,
        namespaces=["sdmx_datasets"],
    )
    print("Dataset candidates:")
    for m in ds_hits:
        print(f"  [{m.code}] {m.title}")
    # pick ECB|YC from the matches
    agency, dataset_id = "ECB", "YC"

    # Hop 2: search series inside that dataset's per-dataset series bundle
    series_ns = f"sdmx_series_{agency.lower()}_{dataset_id.lower()}"
    series = await Catalog.from_url(f"hf://ockham/catalog-{series_ns}")
    sk_hits = await series.search(
        "10-year German government bond yield",
        limit=5,
        namespaces=[series_ns],
    )
    for m in sk_hits:
        print(f"  [{m.namespace}:{m.code}] {m.title}")

    # Hop 3: fetch the chosen series
    result = await SDMX["sdmx_fetch"](
        dataset_key=f"{agency}-{dataset_id}",
        series_key="B.DE.EUR.4F.G_N_C.SV_C_YM.SR_10Y",
        start_period="2023-01",
    )
    print(f"\nFetched {len(result.data)} observations")

asyncio.run(main())
```
