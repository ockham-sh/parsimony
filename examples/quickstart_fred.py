"""Quickstart: Search and fetch US GDP from FRED.

Setup:
    pip install parsimony-core
    export FRED_API_KEY="your-key-here"

    Register for a free FRED API key at:
    https://fred.stlouisfed.org/docs/api/api_key.html

Expected output:
    1. A search result table showing GDP-related series with id and title.
    2. A time series of quarterly GDP observations with date and value columns.

Run:
    python examples/quickstart_fred.py
"""

from __future__ import annotations

import os

from parsimony_fred import fred_fetch, fred_search


def main() -> None:
    api_key = os.environ["FRED_API_KEY"]

    # Bind the API key once; use the bound connector for all calls.
    search = fred_search.bind(api_key=api_key)
    fetch = fred_fetch.bind(api_key=api_key)

    # 1. Search for GDP series
    search_result = search(search_text="US gross domestic product")
    print("--- FRED Search: US GDP ---")
    print(search_result.raw[["id", "title"]].head(5).to_string(index=False))
    print()

    # 2. Fetch the headline GDP series
    result = fetch(series_id="GDP")
    print("--- GDP Observations (last 5 quarters) ---")
    print(result.raw[["date", "value"]].tail(5).to_string(index=False))


if __name__ == "__main__":
    main()
