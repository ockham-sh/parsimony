"""Shared test fixtures for parsimony test suite."""

from __future__ import annotations

import pytest

from parsimony.catalog.models import EmbeddingProvider, SeriesEntry
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


class MockEmbeddingProvider(EmbeddingProvider):
    """In-memory embedding provider for tests."""

    def __init__(self, dimension: int = 3) -> None:
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[float(i + 1)] * self._dimension for i in range(len(texts))]

    async def embed_query(self, query: str) -> list[float]:
        return [1.0] * self._dimension


@pytest.fixture
def mock_embeddings() -> MockEmbeddingProvider:
    return MockEmbeddingProvider(dimension=3)


@pytest.fixture
def sqlite_store() -> SQLiteCatalogStore:
    return SQLiteCatalogStore(":memory:", embedding_dim=3)


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
