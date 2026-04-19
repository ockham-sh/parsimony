"""FMP stock screening: screener, company profile, and historical prices.

Demonstrates the FMP equity research workflow:
1. Screen for large-cap NASDAQ tech stocks.
2. Fetch a company profile for the top hit.
3. Fetch recent historical prices for that symbol.

Setup:
    pip install parsimony-core
    export FMP_API_KEY="your-key-here"

    FMP API keys are available at https://financialmodelingprep.com/

Expected output:
    A screener table of tech stocks, a company profile summary, and a
    short historical price series for the first symbol found.

Run:
    python examples/fmp_screening.py
"""

from __future__ import annotations

import asyncio
import os

from parsimony.connectors.fmp import CONNECTORS as FMP
from parsimony.connectors.fmp_screener import CONNECTORS as FMP_SCREENER


async def main() -> None:
    api_key = os.environ["FMP_API_KEY"]

    bundle = FMP.bind_deps(api_key=api_key) + FMP_SCREENER.bind_deps(api_key=api_key)

    # 1. Screen for large-cap NASDAQ tech stocks
    screen = await bundle["fmp_screener"](
        sector="Technology",
        exchange="NASDAQ",
        market_cap_min=50_000_000_000,
        limit=10,
    )
    print("--- FMP Screener: Large-cap NASDAQ Tech ---")
    cols = [c for c in ["symbol", "companyName", "marketCap", "sector"] if c in screen.df.columns]
    print(screen.df[cols].head(5).to_string(index=False))
    print()

    # Pick the first symbol for deeper research.
    symbol = str(screen.df["symbol"].iloc[0])

    # 2. Company profile
    profile = await bundle["fmp_company_profile"](symbol=symbol)
    print(f"--- Company Profile: {symbol} ---")
    print(profile.df.T.to_string())
    print()

    # 3. Historical prices (last 30 trading days)
    prices = await bundle["fmp_prices"](
        symbol=symbol,
        from_date="2024-11-01",
        to_date="2024-12-01",
    )
    print(f"--- Historical Prices: {symbol} ---")
    price_cols = [c for c in ["date", "close", "volume"] if c in prices.df.columns]
    print(prices.df[price_cols].tail(5).to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
