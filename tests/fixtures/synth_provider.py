"""A synthetic provider module conforming to the plugin contract.

Used as a kernel-test fixture wherever a test needs an example connector
module. Exports the contract surface required by the plugin contract:
``CONNECTORS``.

This module lives under ``tests/fixtures/`` rather than in an external
package because the kernel test suite must be self-contained — tests that
exercise discovery against a real installed plugin belong in the
parsimony-plugin-template CI (Task 21) or in a connector package's own
conformance test, not here.
"""

from __future__ import annotations

import pandas as pd

from parsimony.connector import Connectors, connector, enumerator
from parsimony.result import Column, ColumnRole, OutputSpec

SYNTH_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="synth"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

SYNTH_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="synth"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


@connector(output=SYNTH_FETCH_OUTPUT, tags=["synth", "tool"])
def synth_fetch(key: str) -> pd.DataFrame:
    """Fetch a synthetic observation series. Returns a two-row example table."""
    df = pd.DataFrame(
        [
            {"key": key, "title": f"Synthetic: {key}", "date": "2024-01-01", "value": 1.0},
            {"key": key, "title": f"Synthetic: {key}", "date": "2024-02-01", "value": 2.0},
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


@enumerator(output=SYNTH_ENUMERATE_OUTPUT, tags=["synth"])
def enumerate_synth(limit: int = 10) -> pd.DataFrame:
    """Enumerate up to ``limit`` synthetic catalog entries."""
    return pd.DataFrame([{"key": f"k{i}", "title": f"Item {i}"} for i in range(limit)])


CONNECTORS = Connectors([synth_fetch, enumerate_synth])
