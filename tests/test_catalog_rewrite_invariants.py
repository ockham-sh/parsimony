"""Phase 2 verification invariants for the catalog rewrite.

These are behavioural pins, not unit tests — each one corresponds to a
promise in PLAN-juraj-merge.md §2. If any of these break, the rewrite's
intent has been dropped.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from parsimony import (
    CONTRACT_VERSION,
    BaseCatalog,
    Catalog,
    SeriesEntry,
)
from parsimony._standard.embedder import EmbeddingProvider
from parsimony.catalog.embedder_info import EmbedderInfo


class _FakeEmbedder(EmbeddingProvider):
    """Deterministic 4-dim embedder so tests don't need torch."""

    _DIM = 4

    @property
    def dimension(self) -> int:
        return self._DIM

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        # Signature vector per text: a normalized unit vector where the
        # nonzero slot is hash(text) % 4, so similar texts cluster and
        # different texts land in different lanes.
        return [_signature(t) for t in texts]

    async def embed_query(self, query: str) -> list[float]:
        return _signature(query)

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="fake", dim=self._DIM, normalize=True, package="tests")


def _signature(text: str) -> list[float]:
    slot = hash(text) % 4
    vec = [0.0] * 4
    vec[slot] = 1.0
    return vec


def _new() -> Catalog:
    return Catalog("demo", embedder=_FakeEmbedder())


# ---------------------------------------------------------------------------
# 1. Catalog ABC split holds.
# ---------------------------------------------------------------------------


def test_catalog_is_basecatalog_subclass() -> None:
    assert issubclass(Catalog, BaseCatalog)


def test_contract_version_is_one() -> None:
    assert CONTRACT_VERSION == "1"


# ---------------------------------------------------------------------------
# 2. file:// round-trip works and preserves entries.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_url_roundtrip(tmp_path: Path) -> None:
    src = _new()
    await src.upsert(
        [
            SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment rate"),
            SeriesEntry(namespace="fred", code="GDPC1", title="Real GDP"),
        ]
    )
    target = tmp_path / "bundle"
    await src.push(f"file://{target}")

    loaded = await Catalog.from_url(f"file://{target}", embedder=_FakeEmbedder())
    assert loaded.name == "demo"
    assert {(e.namespace, e.code) for e in loaded.entries} == {
        ("fred", "UNRATE"),
        ("fred", "GDPC1"),
    }


# ---------------------------------------------------------------------------
# 3. Hybrid search: BM25 catches short code queries that vector alone misses.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bm25_leg_ranks_keyword_matches_first() -> None:
    """BM25 indexes SeriesEntry.embedding_text() (title + metadata + tags);
    a distinctive keyword in the title should rank that entry first even with
    a random-ish embedder.
    """
    catalog = _new()
    await catalog.upsert(
        [
            SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment rate"),
            SeriesEntry(namespace="fred", code="GDPC1", title="Real gross domestic product"),
            SeriesEntry(namespace="fred", code="CPIAUCSL", title="Consumer price index"),
        ]
    )
    matches = await catalog.search("unemployment", limit=3)
    assert matches[0].code == "UNRATE"


@pytest.mark.asyncio
async def test_search_with_namespace_filter() -> None:
    catalog = _new()
    await catalog.upsert(
        [
            SeriesEntry(namespace="fred", code="UNRATE", title="Unemployment rate"),
            SeriesEntry(namespace="bls", code="LNS14000000", title="Unemployment rate (BLS)"),
        ]
    )
    matches = await catalog.search("unemployment", limit=5, namespaces=["fred"])
    assert all(m.namespace == "fred" for m in matches)


# ---------------------------------------------------------------------------
# 4. list / list_namespaces / exists / delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_namespaces_returns_sorted_distinct() -> None:
    catalog = _new()
    await catalog.upsert(
        [
            SeriesEntry(namespace="b_ns", code="1", title="t"),
            SeriesEntry(namespace="a_ns", code="2", title="t"),
            SeriesEntry(namespace="b_ns", code="3", title="t"),
        ]
    )
    assert await catalog.list_namespaces() == ["a_ns", "b_ns"]


@pytest.mark.asyncio
async def test_exists_returns_existing_subset() -> None:
    catalog = _new()
    await catalog.upsert([SeriesEntry(namespace="fred", code="UNRATE", title="U")])
    existing = await catalog.exists([("fred", "UNRATE"), ("fred", "MISSING")])
    assert existing == {("fred", "UNRATE")}


@pytest.mark.asyncio
async def test_delete_removes_entry() -> None:
    catalog = _new()
    await catalog.upsert([SeriesEntry(namespace="fred", code="UNRATE", title="U")])
    await catalog.delete("fred", "UNRATE")
    assert await catalog.get("fred", "UNRATE") is None


# ---------------------------------------------------------------------------
# 5. BundleNotFoundError is exported from parsimony.errors.
# ---------------------------------------------------------------------------


def test_bundle_not_found_error_importable() -> None:
    from parsimony.errors import BundleNotFoundError

    exc = BundleNotFoundError("hf://x/y")
    assert exc.url == "hf://x/y"
    assert "hf://x/y" in str(exc)
