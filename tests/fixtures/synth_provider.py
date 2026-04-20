"""A synthetic provider module conforming to the plugin contract.

Used as a kernel-test fixture wherever previous tests imported a real
in-tree connector (e.g. ``parsimony.connectors.treasury``). Exports the
full contract surface: ``CONNECTORS``, ``ENV_VARS``, ``PROVIDER_METADATA``.

This module lives under ``tests/fixtures/`` rather than in an external
package because the kernel test suite must be self-contained — tests that
exercise discovery against a real installed plugin belong in the
parsimony-plugin-template CI (Task 21) or in a connector package's own
conformance test, not here.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result

ENV_VARS: dict[str, str] = {}  # no credentials required

PROVIDER_METADATA: dict[str, Any] = {
    "homepage": "https://example.invalid/synth",
    "pricing": "free",
}


class SynthFetchParams(BaseModel):
    """Parameters for the synthetic fetch connector."""

    key: str = Field(..., min_length=1, description="Synthetic entity key.")


class SynthEnumerateParams(BaseModel):
    """Parameters for the synthetic catalog-enumeration connector."""

    limit: int = Field(default=10, ge=1, le=1000)


SYNTH_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, param_key="key", namespace="synth"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

SYNTH_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="synth"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


@connector(output=SYNTH_FETCH_OUTPUT, tags=["synth", "tool"])
async def synth_fetch(params: SynthFetchParams) -> Result:
    """Fetch a synthetic observation series. Returns a two-row example table."""
    df = pd.DataFrame(
        [
            {"key": params.key, "title": f"Synthetic: {params.key}", "date": "2024-01-01", "value": 1.0},
            {"key": params.key, "title": f"Synthetic: {params.key}", "date": "2024-02-01", "value": 2.0},
        ]
    )
    return SYNTH_FETCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="synth", params={"key": params.key}),
        params={"key": params.key},
    )


@enumerator(output=SYNTH_ENUMERATE_OUTPUT, tags=["synth"])
async def enumerate_synth(params: SynthEnumerateParams) -> pd.DataFrame:
    """Enumerate up to ``limit`` synthetic catalog entries."""
    return pd.DataFrame([{"key": f"k{i}", "title": f"Item {i}"} for i in range(params.limit)])


CONNECTORS = Connectors([synth_fetch, enumerate_synth])
