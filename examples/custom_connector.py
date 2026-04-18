"""Custom connector: build your own data source and compose it with built-in connectors.

Demonstrates:
1. Defining a Pydantic params model.
2. Using the @connector decorator with an OutputConfig schema.
3. Composing the custom connector into a Connectors bundle alongside FRED.

Setup:
    pip install parsimony-core
    export FRED_API_KEY="your-key-here"

Expected output:
    A DataFrame from the custom connector with KEY, TITLE, and DATA columns,
    followed by proof that it lives alongside fred_fetch in the same bundle.

Run:
    python examples/custom_connector.py
"""

from __future__ import annotations

import asyncio
import os

import pandas as pd
from parsimony_fred import fred_fetch
from pydantic import BaseModel, Field

from parsimony import Column, ColumnRole, Connectors, OutputConfig, connector

CUSTOM_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="my_source"),
        Column(name="label", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA, dtype="numeric"),
    ]
)


class MyParams(BaseModel):
    category: str = Field(..., description="Category to look up")


@connector(output=CUSTOM_OUTPUT, tags=["custom"])
async def my_data_source(params: MyParams) -> pd.DataFrame:
    """Return sample rows for a category (replace with a real HTTP call)."""
    return pd.DataFrame(
        {
            "code": ["A1", "A2", "A3"],
            "label": [f"{params.category} - Alpha", f"{params.category} - Beta", f"{params.category} - Gamma"],
            "score": [0.95, 0.87, 0.73],
        }
    )


async def main() -> None:
    api_key = os.environ["FRED_API_KEY"]

    # Compose custom + built-in connectors into one bundle.
    fred = Connectors([fred_fetch]).bind_deps(api_key=api_key)
    bundle = fred + Connectors([my_data_source])

    print(f"Bundle connectors: {bundle.names()}")
    print()

    # Call the custom connector through the bundle.
    result = await bundle["my_data_source"](category="widgets")
    print("--- Custom connector result ---")
    print(result.df.to_string(index=False))
    print(f"Provenance source: {result.provenance.source}")


if __name__ == "__main__":
    asyncio.run(main())
