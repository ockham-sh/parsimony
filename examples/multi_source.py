"""Multi-source: fetch from FRED and SDMX in one script.

Demonstrates Connectors composition with the + operator to build a unified
bundle spanning multiple data providers.

Setup:
    pip install parsimony-core parsimony-fred parsimony-sdmx
    export FRED_API_KEY="your-key-here"

Expected output:
    US GDP quarterly data from FRED, followed by ECB EUR/USD daily exchange
    rate from SDMX, both fetched through the same Connectors bundle.

Run:
    python examples/multi_source.py
"""

from __future__ import annotations

import os

from parsimony_fred import CONNECTORS as FRED
from parsimony_sdmx import CONNECTORS as SDMX


def main() -> None:
    api_key = os.environ["FRED_API_KEY"]

    # Compose: FRED (needs API key) + SDMX (no key required), via the + operator.
    bundle = FRED.bind(api_key=api_key) + SDMX
    print(f"Bundle connectors: {bundle.names()}")
    print()

    # --- FRED: US GDP ---
    gdp = bundle["fred_fetch"](series_id="GDP")
    print("--- FRED: US GDP (last 3 quarters) ---")
    print(gdp.raw[["date", "value"]].tail(3).to_string(index=False))
    print()

    # --- SDMX: ECB EUR/USD ---
    fx = bundle["sdmx_fetch"](
        dataset_ref="ECB-EXR",
        series_ref="D.USD.EUR.SP00.A",
        start_period="2024-06-01",
        end_period="2024-06-30",
    )
    print("--- SDMX: ECB EUR/USD (June 2024) ---")
    print(fx.raw[["TIME_PERIOD", "value"]].tail(5).to_string(index=False))


if __name__ == "__main__":
    main()
