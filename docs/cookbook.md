# Parsimony Cookbook

Ten practical recipes. Each is self-contained and runnable as a standalone script.

---

### Recipe 1: Fetch US GDP and Plot with Altair

_Fetch quarterly real GDP from FRED and render a line chart._

```python
import asyncio
import altair as alt
from parsimony.connectors.fred import fred_fetch, FredFetchParams

async def main():
    conn = fred_fetch.bind_deps(api_key="YOUR_FRED_KEY")
    result = await conn(FredFetchParams(series_id="GDPC1"))
    df = result.df
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
from parsimony_fred import fred_fetch, FredFetchParams
from parsimony_sdmx.connectors.fetch import sdmx_fetch, SdmxFetchParams

async def main():
    fred_conn = fred_fetch.bind_deps(api_key="YOUR_FRED_KEY")
    fred_result = await fred_conn(FredFetchParams(
        series_id="DEXUSEU", observation_start="2023-01-01",
    ))
    sdmx_result = await sdmx_fetch(SdmxFetchParams(
        dataset_key="ECB-EXR", series_key="D.USD.EUR.SP00.A",
        start_period="2023-01",
    ))
    fred_df = fred_result.df[["date", "value"]].rename(columns={"value": "fred"})
    ecb_df = sdmx_result.df.rename(columns={"TIME_PERIOD": "date", "value": "ecb"})
    ecb_df["date"] = pd.to_datetime(ecb_df["date"])
    merged = pd.merge(fred_df, ecb_df[["date", "ecb"]], on="date", how="inner")
    print(f"Merged {len(merged)} rows; correlation: {merged['fred'].corr(merged['ecb']):.4f}")

asyncio.run(main())
```

---

### Recipe 3: Screen for Undervalued Tech Stocks

_Use FMP taxonomy and quotes to filter stocks by sector and PE ratio._

```python
import asyncio
from parsimony.connectors.fmp import fmp_quotes, FmpSymbolsParams

async def main():
    conn = fmp_quotes.bind_deps(api_key="YOUR_FMP_KEY")
    symbols = "AAPL,MSFT,GOOGL,META,INTC,AMD,NVDA,CRM,ORCL,IBM"
    result = await conn(FmpSymbolsParams(symbols=symbols))
    df = result.df
    # Filter for PE under 25 (where available)
    if "pe" in df.columns:
        undervalued = df[df["pe"].between(0, 25)]
        print(f"Stocks with PE < 25:\n{undervalued[['symbol', 'name', 'price', 'pe']].to_string()}")
    else:
        print(f"Quotes:\n{df.head()}")

asyncio.run(main())
```

---

### Recipe 4: Build a Searchable FRED Employment Catalog

_Enumerate a FRED release into an in-memory catalog and search it._

```python
import asyncio
from parsimony import Catalog, SQLiteCatalogStore
from parsimony.connectors.fred import enumerate_fred_release, FredEnumerateParams

async def main():
    catalog = Catalog(SQLiteCatalogStore(":memory:"))
    enum_conn = enumerate_fred_release.bind_deps(api_key="YOUR_FRED_KEY")
    # Release 50 = Employment Situation
    result = await enum_conn(FredEnumerateParams(release_id=50))
    await catalog.index_result(result, embed=False)
    matches = await catalog.search("unemployment rate", limit=5, namespaces=["fred"])
    for m in matches:
        print(f"  {m.code:15s}  {m.title}")

asyncio.run(main())
```

---

### Recipe 5: Fetch Company Financials and Compute Ratios

_Pull income statement and balance sheet, then compute profit margin and debt-to-equity._

```python
import asyncio
from parsimony.connectors.fmp import (
    fmp_income_statements, fmp_balance_sheet_statements,
    FmpFinancialStatementParams,
)

async def main():
    deps = dict(api_key="YOUR_FMP_KEY")
    inc_conn = fmp_income_statements.bind_deps(**deps)
    bs_conn = fmp_balance_sheet_statements.bind_deps(**deps)
    params = FmpFinancialStatementParams(symbol="AAPL", period="annual", limit=3)
    inc, bs = await asyncio.gather(inc_conn(params), bs_conn(params))
    inc_df, bs_df = inc.df, bs.df
    if "revenue" in inc_df.columns and "netIncome" in inc_df.columns:
        inc_df["margin"] = inc_df["netIncome"] / inc_df["revenue"]
        print("Profit margins:", inc_df[["date", "margin"]].to_string(index=False))
    if "totalDebt" in bs_df.columns and "totalStockholdersEquity" in bs_df.columns:
        bs_df["de_ratio"] = bs_df["totalDebt"] / bs_df["totalStockholdersEquity"]
        print("D/E ratios:", bs_df[["date", "de_ratio"]].to_string(index=False))

asyncio.run(main())
```

---

### Recipe 6: Track Prediction Market Probabilities

_Fetch a Polymarket event and list its market outcomes._

```python
import asyncio
from parsimony.connectors.polymarket import POLYMARKET_GAMMA, PolymarketFetchParams

async def main():
    result = await POLYMARKET_GAMMA(PolymarketFetchParams(
        path="/events",
        response_path="",
        method="GET",
    ))
    df = result.df
    cols = [c for c in ["title", "slug", "liquidity", "volume"] if c in df.columns]
    print(df[cols].head(10).to_string())

asyncio.run(main())
```

---

### Recipe 7: Multi-Source Macro Dashboard

_Combine FRED inflation and ECB interest rates into one DataFrame._

```python
import asyncio
import pandas as pd
from parsimony_fred import fred_fetch, FredFetchParams
from parsimony_sdmx.connectors.fetch import sdmx_fetch, SdmxFetchParams

async def main():
    fred_conn = fred_fetch.bind_deps(api_key="YOUR_FRED_KEY")
    cpi_task = fred_conn(FredFetchParams(series_id="CPIAUCSL", observation_start="2020-01-01"))
    ecb_task = sdmx_fetch(SdmxFetchParams(
        dataset_key="ECB-FM", series_key="M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
        start_period="2020-01",
    ))
    cpi_result, ecb_result = await asyncio.gather(cpi_task, ecb_task)
    cpi = cpi_result.df[["date", "value"]].rename(columns={"value": "us_cpi"})
    euribor = ecb_result.df.rename(columns={"TIME_PERIOD": "date", "value": "euribor_3m"})
    euribor["date"] = pd.to_datetime(euribor["date"])
    dashboard = pd.merge(cpi, euribor[["date", "euribor_3m"]], on="date", how="outer").sort_values("date")
    print(f"Dashboard: {len(dashboard)} rows, {dashboard.columns.tolist()}")
    print(dashboard.tail(5).to_string(index=False))

asyncio.run(main())
```

---

### Recipe 8: Export Data to Parquet

_Fetch FRED data with OutputConfig, write to Parquet, and read it back with full schema._

```python
import asyncio
from parsimony.connectors.fred import fred_fetch, FredFetchParams
from parsimony.result import SemanticTableResult

async def main():
    conn = fred_fetch.bind_deps(api_key="YOUR_FRED_KEY")
    result = await conn(FredFetchParams(series_id="UNRATE"))
    # fred_fetch uses output=FETCH_OUTPUT, so result is a SemanticTableResult
    result.to_parquet("unrate.parquet")
    print(f"Wrote {len(result.df)} rows to unrate.parquet")
    # Read it back -- schema and provenance are embedded in Arrow metadata
    restored = SemanticTableResult.from_parquet("unrate.parquet")
    print(f"Restored: {len(restored.df)} rows, source={restored.provenance.source}")
    print(f"Columns: {[c.name for c in restored.columns]}")

asyncio.run(main())
```

---

### Recipe 9: Batch Enumerate Multiple FRED Releases

_Loop over several releases, index all into one catalog, then search across them._

```python
import asyncio
from parsimony import Catalog, SQLiteCatalogStore
from parsimony.connectors.fred import enumerate_fred_release, FredEnumerateParams

RELEASES = {50: "Employment", 53: "GDP", 10: "CPI"}

async def main():
    catalog = Catalog(SQLiteCatalogStore(":memory:"))
    enum_conn = enumerate_fred_release.bind_deps(api_key="YOUR_FRED_KEY")
    for release_id, label in RELEASES.items():
        result = await enum_conn(FredEnumerateParams(release_id=release_id))
        idx = await catalog.index_result(result, embed=False)
        print(f"  {label} (release {release_id}): indexed {idx.indexed}, skipped {idx.skipped}")
    namespaces = await catalog.list_namespaces()
    print(f"Namespaces: {namespaces}")
    matches = await catalog.search("consumer price index", limit=5, namespaces=["fred"])
    for m in matches:
        print(f"  {m.code:15s}  {m.title}")

asyncio.run(main())
```

---

### Recipe 10: Discover and Fetch SDMX Yield Curves

_Two-hop catalog flow: search published SDMX bundles for a dataset, then
search that dataset's per-series bundle, then fetch the chosen series._

Requires `pip install parsimony-sdmx`. Dataset metadata lives in the
`sdmx_datasets` namespace; each dataset's series keys live in
`sdmx_series_{agency}_{dataset_id}` (lowercased).

```python
import asyncio
from parsimony import Catalog
from parsimony.stores import HFBundleCatalogStore
from parsimony_sdmx.connectors.fetch import sdmx_fetch, SdmxFetchParams

async def main():
    catalog = Catalog(store=HFBundleCatalogStore(...))

    # Hop 1: find the yield-curve dataset
    ds_hits = await catalog.search(
        "Euro area government bond yield curve",
        limit=5,
        namespaces=["sdmx_datasets"],
    )
    print("Dataset candidates:")
    for m in ds_hits:
        print(f"  [{m.code}] {m.title}")
    # pick ECB|YC from the matches
    agency, dataset_id = "ECB", "YC"

    # Hop 2: search series inside that dataset
    series_ns = f"sdmx_series_{agency.lower()}_{dataset_id.lower()}"
    sk_hits = await catalog.search(
        "10-year German government bond yield",
        limit=5,
        namespaces=[series_ns],
    )
    for m in sk_hits:
        print(f"  [{m.namespace}:{m.code}] {m.title}")

    # Hop 3: fetch the chosen series
    result = await sdmx_fetch(SdmxFetchParams(
        dataset_key=f"{agency}-{dataset_id}",
        series_key="B.DE.EUR.4F.G_N_C.SV_C_YM.SR_10Y",
        start_period="2023-01",
    ))
    print(f"\nFetched {len(result.df)} observations")

asyncio.run(main())
```
