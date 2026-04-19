"""Tests for Catalog indexing, namespace connectors, and SDMX key enumeration."""

from __future__ import annotations

import builtins

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.catalog.catalog import Catalog
from parsimony.catalog.models import (
    EmbeddingProvider,
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    code_token,
)
from parsimony.connector import Connectors, enumerator
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    SemanticTableResult,
)
from parsimony.stores.catalog_store import CatalogStore


class _AutoIndexFetchParams(BaseModel):
    q: str = "x"


def test_code_token_normalizes() -> None:
    assert code_token("ECB-YC") == "ecb_yc"
    assert code_token("  SR.3M  ") == "sr_3m"


class _FixedEmbeddings(EmbeddingProvider):
    def __init__(self, dim: int = 4) -> None:
        self._dim = dim

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[1.0] + [0.0] * (self._dim - 1) for _ in texts]

    async def embed_query(self, query: str) -> list[float]:
        return [1.0] + [0.0] * (self._dim - 1)


class _ShortEmbeddings(_FixedEmbeddings):
    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if len(texts) <= 1:
            return await super().embed_texts(texts)
        return [[1.0] + [0.0] * (self._dim - 1) for _ in texts[:-1]]


class _RecordingStore(CatalogStore):
    def __init__(self, existing: set[tuple[str, str]] | None = None) -> None:
        self.existing: set[tuple[str, str]] = set(existing or set())
        self.upsert_batches: list[list[SeriesEntry]] = []

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        self.upsert_batches.append(list(entries))
        for e in entries:
            self.existing.add(catalog_key(e.namespace, e.code))

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        return None

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        return {k for k in keys if catalog_key(k[0], k[1]) in self.existing}

    async def delete(self, namespace: str, code: str) -> None:
        self.existing.discard(catalog_key(namespace, code))

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
        query_embedding: builtins.list[float] | None = None,
    ) -> builtins.list[SeriesMatch]:
        return []

    async def list_namespaces(self) -> builtins.list[str]:
        return []

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        return [], 0


@pytest.mark.asyncio
async def test_series_catalog_ingest_inserts_new_with_embeddings() -> None:
    store = _RecordingStore()
    emb = _FixedEmbeddings(dim=4)
    entries = [
        SeriesEntry(
            namespace="fred",
            code="GDPC1",
            title="GDP",
        )
    ]
    catalog = Catalog(store, embeddings=emb)
    result = await catalog.ingest(entries, embed=True)
    assert isinstance(result, IndexResult)
    assert result.total == 1
    assert result.indexed == 1
    assert result.skipped == 0
    assert result.errors == 0
    assert len(store.upsert_batches) == 1
    entry = store.upsert_batches[0][0]
    assert entry.namespace == "fred"
    assert entry.code == "GDPC1"
    assert entry.embedding is not None
    assert len(entry.embedding) == 4


@pytest.mark.asyncio
async def test_series_catalog_ingest_skips_existing_keys() -> None:
    store = _RecordingStore(existing={catalog_key("fred", "GDPC1")})
    emb = _FixedEmbeddings(dim=4)
    catalog = Catalog(store, embeddings=emb)
    entries = [
        SeriesEntry(
            namespace="fred",
            code="GDPC1",
            title="GDP",
        )
    ]
    result = await catalog.ingest(entries, embed=True)
    assert result.indexed == 0
    assert result.skipped == 1
    assert store.upsert_batches == []


@pytest.mark.asyncio
async def test_series_catalog_ingest_force_upserts_existing_keys() -> None:
    store = _RecordingStore(existing={catalog_key("fred", "GDPC1")})
    emb = _FixedEmbeddings(dim=4)
    catalog = Catalog(store, embeddings=emb)
    entries = [
        SeriesEntry(
            namespace="fred",
            code="GDPC1",
            title="GDP",
        )
    ]
    result = await catalog.ingest(entries, embed=True, force=True)
    assert result.indexed == 1
    assert result.skipped == 0
    assert len(store.upsert_batches) == 1


@pytest.mark.asyncio
async def test_index_result_requires_key_namespace() -> None:
    """``index_result`` needs KEY column ``namespace=...`` in the schema."""
    store = _RecordingStore()
    catalog = Catalog(store, embeddings=_FixedEmbeddings(dim=4))
    df = pd.DataFrame({"k": ["x"], "t": ["T"]})
    table = SemanticTableResult(
        data=df,
        provenance=Provenance(source="x"),
        output_schema=OutputConfig(
            columns=[
                Column(name="k", role=ColumnRole.KEY),
                Column(name="t", role=ColumnRole.TITLE),
            ]
        ),
    )
    with pytest.raises(ValueError, match="namespace"):
        await catalog.index_result(table)


def _dry_run_sample_table() -> SemanticTableResult:
    return SemanticTableResult(
        data=pd.DataFrame({"code_col": ["A", "B"], "title_col": ["One", "Two"]}),
        provenance=Provenance(source="dry_test"),
        output_schema=OutputConfig(
            columns=[
                Column(name="code_col", role=ColumnRole.KEY, namespace="test_ns"),
                Column(name="title_col", role=ColumnRole.TITLE),
            ]
        ),
    )


@pytest.mark.asyncio
async def test_index_result_dry_run_matches_live_dedupe_counts() -> None:
    table = _dry_run_sample_table()
    store = _RecordingStore()
    catalog = Catalog(store, embeddings=None)
    dry = await catalog.index_result(table, embed=False, dry_run=True)
    live = await catalog.index_result(table, embed=False)
    assert dry.total == live.total == 2
    assert dry.indexed == live.indexed == 2
    assert dry.skipped == live.skipped == 0
    assert len(store.upsert_batches) == 1


@pytest.mark.asyncio
async def test_index_result_dry_run_after_ingest_skips_all() -> None:
    table = _dry_run_sample_table()
    store = _RecordingStore()
    catalog = Catalog(store, embeddings=None)
    await catalog.index_result(table, embed=False)
    dry = await catalog.index_result(table, embed=False, dry_run=True)
    assert dry.total == 2
    assert dry.indexed == 0
    assert dry.skipped == 2


@pytest.mark.asyncio
async def test_index_result_dry_run_force_counts_all_indexed() -> None:
    table = _dry_run_sample_table()
    store = _RecordingStore()
    catalog = Catalog(store, embeddings=None)
    await catalog.index_result(table, embed=False)
    dry = await catalog.index_result(table, embed=False, dry_run=True, force=True)
    assert dry.total == 2
    assert dry.indexed == 2
    assert dry.skipped == 0


@pytest.mark.asyncio
async def test_index_result_dry_run_does_not_upsert() -> None:
    table = _dry_run_sample_table()
    store = _RecordingStore()
    catalog = Catalog(store, embeddings=None)
    await catalog.index_result(table, embed=False, dry_run=True)
    assert store.upsert_batches == []


@pytest.mark.asyncio
async def test_series_catalog_ingest_without_embed() -> None:
    store = _RecordingStore()
    emb = _FixedEmbeddings(dim=4)
    catalog = Catalog(store, embeddings=emb)
    entries = [SeriesEntry(namespace="fred", code="X", title="X")]
    result = await catalog.ingest(entries, embed=False)
    assert result.indexed == 1
    assert store.upsert_batches[0][0].embedding is None


@pytest.mark.asyncio
async def test_index_result_as_callback() -> None:
    """``with_callback(catalog.index_result)`` indexes using KEY column ``namespace=...``."""

    @enumerator(
        output=OutputConfig(
            columns=[
                Column(name="code_col", role=ColumnRole.KEY, namespace="test_ns"),
                Column(name="title_col", role=ColumnRole.TITLE),
            ],
        ),
    )
    async def fetch_multi(_p: _AutoIndexFetchParams) -> pd.DataFrame:
        """Multi-row fetch for auto-index test."""
        return pd.DataFrame({"code_col": ["A", "B", "A"], "title_col": ["One", "Two", "One"]})

    store = _RecordingStore()
    catalog = Catalog(store, embeddings=_FixedEmbeddings(dim=4))

    wired = Connectors([fetch_multi.with_callback(catalog.index_result)])
    await wired["fetch_multi"](q="x")
    assert len(store.upsert_batches) == 1
    batch = store.upsert_batches[0]
    assert len(batch) == 2
    by_code = {e.code: e for e in batch}
    assert by_code["A"].title == "One"
    assert by_code["B"].title == "Two"
    assert by_code["A"].namespace == "test_ns"
    assert by_code["A"].embedding is not None
    assert len(by_code["A"].embedding) == 4


# fred-specific enumerate tests moved to packages/fred/tests/ in the
# parsimony-connectors monorepo; the kernel no longer depends on any
# connector module and the tests would fail to import here.
