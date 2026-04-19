"""Quickstart: Fetch ECB EUR/USD exchange rate via SDMX (no API key required).

Setup:
    pip install parsimony-sdmx

Expected output:
    A DataFrame of daily EUR/USD exchange rate observations with columns
    including series_key, title, TIME_PERIOD, and value, plus provenance
    metadata showing the SDMX source and parameters used.

Run:
    python examples/quickstart_sdmx.py
"""

from __future__ import annotations

import asyncio

from parsimony_sdmx import sdmx


async def main() -> None:
    # SDMX connectors have no dependencies to bind -- call directly.
    # ECB exchange rate dataset: daily USD/EUR spot rate.
    result = await sdmx(
        dataset_key="ECB-EXR",
        series_key="D.USD.EUR.SP00.A",
        start_period="2024-01-01",
        end_period="2024-12-31",
    )

    print("--- EUR/USD Daily Exchange Rate (2024) ---")
    print(result.df.tail(10).to_string(index=False))
    print()
    print("--- Provenance ---")
    print(f"  source: {result.provenance.source}")
    print(f"  params: {result.provenance.params}")


if __name__ == "__main__":
    asyncio.run(main())
