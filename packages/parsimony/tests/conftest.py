"""Shared test fixtures for parsimony test suite."""

from __future__ import annotations

import pytest

from parsimony.catalog.models import SeriesEntry


@pytest.fixture
def sample_entries() -> list[SeriesEntry]:
    return [
        SeriesEntry(
            namespace="fred",
            code="GDPC1",
            title="Real Gross Domestic Product",
            tags=["macro"],
            metadata={"units": "Billions of Chained 2017 Dollars"},
        ),
        SeriesEntry(
            namespace="fred",
            code="UNRATE",
            title="Unemployment Rate",
            tags=["macro", "employment"],
            metadata={"units": "Percent"},
        ),
        SeriesEntry(
            namespace="fmp",
            code="AAPL",
            title="Apple Inc.",
            tags=["equities"],
            metadata={"exchange": "NASDAQ"},
        ),
    ]
