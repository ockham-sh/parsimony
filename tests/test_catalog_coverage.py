"""Tests for parsimony.catalog.catalog — dispatcher logic, CRUD, embedding backfill."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from parsimony.catalog.catalog import Catalog, _find_enumerator
from parsimony.catalog.models import EmbeddingProvider, SeriesEntry
from parsimony.errors import ConnectorError
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore

# ---------------------------------------------------------------------------
# Mock embedding provider
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store() -> SQLiteCatalogStore:
    return SQLiteCatalogStore(":memory:", embedding_dim=3)


@pytest.fixture
def embeddings() -> MockEmbeddingProvider:
    return MockEmbeddingProvider(dimension=3)


@pytest.fixture
def catalog(store: SQLiteCatalogStore, embeddings: MockEmbeddingProvider) -> Catalog:
    return Catalog(store, embeddings=embeddings)


@pytest.fixture
def catalog_no_embed(store: SQLiteCatalogStore) -> Catalog:
    return Catalog(store)


def _make_entries(namespace: str = "fred", count: int = 3) -> list[SeriesEntry]:
    return [
        SeriesEntry(
            namespace=namespace,
            code=f"SER{i}",
            title=f"Series {i}",
            tags=["test"],
            metadata={"unit": "index"},
        )
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# CRUD pass-throughs
# ---------------------------------------------------------------------------


class TestStorePassthroughs:
    """The Catalog class no longer re-exports these store CRUD methods; callers use
    ``catalog.store.*`` directly. These tests cover the store interface via that path."""

    @pytest.mark.asyncio
    async def test_list_namespaces(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        await store.upsert(_make_entries("fred", 1))
        await store.upsert(_make_entries("fmp", 1))
        ns = await catalog.store.list_namespaces()
        assert set(ns) == {"fred", "fmp"}

    @pytest.mark.asyncio
    async def test_list_entries(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        entries = _make_entries("fred", 5)
        await store.upsert(entries)
        results, total = await catalog.store.list(namespace="fred", limit=3)
        assert len(results) <= 3
        assert total >= 3

    @pytest.mark.asyncio
    async def test_get_entry(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        entries = _make_entries("fred", 1)
        await store.upsert(entries)
        entry = await catalog.store.get("fred", "SER0")
        assert entry is not None
        assert entry.code == "SER0"

    @pytest.mark.asyncio
    async def test_get_entry_missing(self, catalog: Catalog) -> None:
        entry = await catalog.store.get("fred", "NONEXISTENT")
        assert entry is None

    @pytest.mark.asyncio
    async def test_delete_entry(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        await store.upsert(_make_entries("fred", 1))
        await catalog.store.delete("fred", "SER0")
        entry = await catalog.store.get("fred", "SER0")
        assert entry is None

    @pytest.mark.asyncio
    async def test_upsert_entries(self, catalog: Catalog) -> None:
        entries = _make_entries("fred", 2)
        await catalog.store.upsert(entries)
        result = await catalog.store.get("fred", "SER0")
        assert result is not None


# ---------------------------------------------------------------------------
# search() with embeddings (hybrid path)
# ---------------------------------------------------------------------------


class TestSearchWithEmbeddings:
    @pytest.mark.asyncio
    async def test_search_embeds_query(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        entries = _make_entries("fred", 3)
        embedded = [e.model_copy(update={"embedding": [1.0, 1.0, 1.0]}) for e in entries]
        await store.upsert(embedded)

        results = await catalog.search("Series", limit=5, namespaces=["fred"])
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_search_without_embeddings(self, catalog_no_embed: Catalog, store: SQLiteCatalogStore) -> None:
        await store.upsert(_make_entries("fred", 2))
        results = await catalog_no_embed.search("Series", limit=5, namespaces=["fred"])
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_search_with_namespace_filter(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        await store.upsert(_make_entries("fred", 2))
        await store.upsert(_make_entries("fmp", 2))
        results = await catalog.search("Series", limit=10, namespaces=["fmp"])
        for r in results:
            assert r.namespace == "fmp"


# ---------------------------------------------------------------------------
# _ensure_namespace() — thin dispatcher: populated -> store -> remote -> enumerate
# ---------------------------------------------------------------------------


class TestEnsureNamespace:
    """The dispatcher caches resolution outcomes (success AND confirmed miss)
    so a cold namespace is not re-probed on every query. These tests assert
    that behaviour via the observable side-effect (store calls), not via the
    private cache attribute name."""

    @pytest.mark.asyncio
    async def test_resolved_namespace_not_reprobed(
        self, catalog: Catalog, store: SQLiteCatalogStore, monkeypatch: Any
    ) -> None:
        await store.upsert(_make_entries("fred", 1))
        calls: list[None] = []
        original = store.list_namespaces

        async def counting_list_namespaces() -> list[str]:
            calls.append(None)
            return await original()

        monkeypatch.setattr(store, "list_namespaces", counting_list_namespaces)
        await catalog._ensure_namespace("fred")
        await catalog._ensure_namespace("fred")
        await catalog._ensure_namespace("fred")
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_unknown_namespace_not_reprobed(
        self, catalog: Catalog, store: SQLiteCatalogStore, monkeypatch: Any
    ) -> None:
        """Confirmed miss is cached — cold queries for the same namespace
        don't re-probe the store on every call."""
        calls: list[None] = []
        original = store.list_namespaces

        async def counting_list_namespaces() -> list[str]:
            calls.append(None)
            return await original()

        monkeypatch.setattr(store, "list_namespaces", counting_list_namespaces)
        await catalog._ensure_namespace("unknown_ns")
        await catalog._ensure_namespace("unknown_ns")
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_invalidate_forces_reprobe(
        self, catalog: Catalog, store: SQLiteCatalogStore, monkeypatch: Any
    ) -> None:
        await store.upsert(_make_entries("fred", 1))
        calls: list[None] = []
        original = store.list_namespaces

        async def counting_list_namespaces() -> list[str]:
            calls.append(None)
            return await original()

        monkeypatch.setattr(store, "list_namespaces", counting_list_namespaces)
        await catalog._ensure_namespace("fred")
        catalog.invalidate("fred")
        await catalog._ensure_namespace("fred")
        assert len(calls) == 2


# ---------------------------------------------------------------------------
# _try_enumerate()
# ---------------------------------------------------------------------------


@dataclass
class _FakeOutputConfig:
    columns: list[Any]


@dataclass
class _FakeConnector:
    output_config: _FakeOutputConfig | None
    param_type: Any = None
    _call_result: Any = None

    async def __call__(self, params: Any) -> Any:
        return self._call_result


class TestTryEnumerate:
    @pytest.mark.asyncio
    async def test_no_connectors_returns_false(
        self, store: SQLiteCatalogStore, embeddings: MockEmbeddingProvider
    ) -> None:
        catalog = Catalog(store, embeddings=embeddings, connectors=None)
        result = await catalog._try_enumerate("fred")
        assert result is False

    @pytest.mark.asyncio
    async def test_no_matching_enumerator(
        self, store: SQLiteCatalogStore, embeddings: MockEmbeddingProvider
    ) -> None:
        col = Column(name="ticker", role=ColumnRole.KEY, namespace="fmp")
        title_col = Column(name="name", role=ColumnRole.TITLE)
        connector = _FakeConnector(output_config=_FakeOutputConfig(columns=[col, title_col]))
        catalog = Catalog(store, embeddings=embeddings, connectors=[connector])
        result = await catalog._try_enumerate("fred")
        assert result is False

    @pytest.mark.asyncio
    async def test_enumerator_runs_and_ingests(
        self, store: SQLiteCatalogStore, embeddings: MockEmbeddingProvider
    ) -> None:
        import pandas as pd

        from parsimony.result import Provenance, SemanticTableResult

        df = pd.DataFrame({"ticker": ["AAPL", "MSFT"], "name": ["Apple", "Microsoft"]})
        schema = OutputConfig(
            columns=[
                Column(name="ticker", role=ColumnRole.KEY, namespace="fmp"),
                Column(name="name", role=ColumnRole.TITLE),
            ]
        )
        prov = Provenance(source="test_enum")
        table_result = SemanticTableResult(data=df, output_schema=schema, provenance=prov)

        col = Column(name="ticker", role=ColumnRole.KEY, namespace="fmp")
        title_col = Column(name="name", role=ColumnRole.TITLE)
        connector = _FakeConnector(
            output_config=_FakeOutputConfig(columns=[col, title_col]),
            _call_result=table_result,
        )
        catalog = Catalog(store, embeddings=embeddings, connectors=[connector])
        result = await catalog._try_enumerate("fmp")
        assert result is True

        ns = await store.list_namespaces()
        assert "fmp" in ns

    @pytest.mark.asyncio
    async def test_enumerator_connector_error(
        self, store: SQLiteCatalogStore, embeddings: MockEmbeddingProvider, caplog: Any
    ) -> None:
        """A connector raising a ConnectorError returns False and logs the provider name."""

        @dataclass
        class _FailingConnector:
            output_config: _FakeOutputConfig
            param_type: Any = None

            async def __call__(self, params: Any) -> Any:
                raise ConnectorError("boom", provider="test_provider")

        col = Column(name="code", role=ColumnRole.KEY, namespace="fred")
        title_col = Column(name="name", role=ColumnRole.TITLE)
        connector = _FailingConnector(output_config=_FakeOutputConfig(columns=[col, title_col]))
        catalog = Catalog(store, embeddings=embeddings, connectors=[connector])

        with caplog.at_level("WARNING"):
            result = await catalog._try_enumerate("fred")
        assert result is False


# ---------------------------------------------------------------------------
# _find_enumerator()
# ---------------------------------------------------------------------------


class TestFindEnumerator:
    def test_finds_matching_enumerator(self) -> None:
        col = Column(name="code", role=ColumnRole.KEY, namespace="fred")
        title_col = Column(name="name", role=ColumnRole.TITLE)
        conn = _FakeConnector(output_config=_FakeOutputConfig(columns=[col, title_col]))
        result = _find_enumerator([conn], "fred")
        # New signature: returns (enumerator, extracted_params) — static namespace → empty dict.
        assert result is not None
        found_conn, extracted = result
        assert found_conn is conn
        assert extracted == {}

    def test_skips_data_role_connector(self) -> None:
        col = Column(name="code", role=ColumnRole.KEY, namespace="fred")
        data_col = Column(name="value", role=ColumnRole.DATA)
        title_col = Column(name="name", role=ColumnRole.TITLE)
        conn = _FakeConnector(output_config=_FakeOutputConfig(columns=[col, data_col, title_col]))
        result = _find_enumerator([conn], "fred")
        assert result is None

    def test_skips_no_title_connector(self) -> None:
        col = Column(name="code", role=ColumnRole.KEY, namespace="fred")
        conn = _FakeConnector(output_config=_FakeOutputConfig(columns=[col]))
        result = _find_enumerator([conn], "fred")
        assert result is None

    def test_skips_none_output_config(self) -> None:
        conn = _FakeConnector(output_config=None)
        result = _find_enumerator([conn], "fred")
        assert result is None

    def test_no_match_returns_none(self) -> None:
        col = Column(name="code", role=ColumnRole.KEY, namespace="fmp")
        title_col = Column(name="name", role=ColumnRole.TITLE)
        conn = _FakeConnector(output_config=_FakeOutputConfig(columns=[col, title_col]))
        result = _find_enumerator([conn], "fred")
        assert result is None


# ---------------------------------------------------------------------------
# embed_pending()
# ---------------------------------------------------------------------------


class TestEmbedPending:
    @pytest.mark.asyncio
    async def test_embed_pending_backfills(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        entries = _make_entries("fred", 3)
        await store.upsert(entries)

        count = await catalog.embed_pending(namespace="fred")
        assert count == 3

        for e in entries:
            stored = await store.get("fred", e.code)
            assert stored is not None
            assert stored.embedding is not None

    @pytest.mark.asyncio
    async def test_embed_pending_no_missing(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        entries = [e.model_copy(update={"embedding": [1.0, 2.0, 3.0]}) for e in _make_entries("fred", 2)]
        await store.upsert(entries)
        count = await catalog.embed_pending(namespace="fred")
        assert count == 0

    @pytest.mark.asyncio
    async def test_embed_pending_no_provider_raises(self, catalog_no_embed: Catalog) -> None:
        with pytest.raises(RuntimeError, match="embed_pending requires"):
            await catalog_no_embed.embed_pending()

    @pytest.mark.asyncio
    async def test_embed_pending_with_limit(self, catalog: Catalog, store: SQLiteCatalogStore) -> None:
        entries = _make_entries("fred", 5)
        await store.upsert(entries)
        count = await catalog.embed_pending(limit=2)
        assert count <= 5


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestClose:
    @pytest.mark.asyncio
    async def test_close_invokes_store_close_when_available(self) -> None:
        """Integration test: when the store exposes close(), Catalog.close forwards to it."""
        store = SQLiteCatalogStore(":memory:", embedding_dim=3)
        closed = {"count": 0}
        original = store.close

        async def counting_close() -> None:
            closed["count"] += 1
            await original()

        store.close = counting_close  # type: ignore[method-assign]
        catalog = Catalog(store)
        await catalog.close()
        assert closed["count"] == 1

    @pytest.mark.asyncio
    async def test_close_without_store_close_method_is_noop(self) -> None:
        """A store that doesn't implement close() must not crash Catalog.close."""

        class _NoCloseStore:
            pass

        catalog = Catalog(_NoCloseStore())  # type: ignore[arg-type]
        await catalog.close()  # no raise
