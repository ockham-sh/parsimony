"""Tests for ``Catalog.load_entities_only`` and the LRU's entities-only path.

The entities-only load exists so browse/list flows (e.g. paging a small codelist
to pick a code by hand) skip the FAISS vector read, the embedder hydration, and
the whole-dir integrity hash — the costs that dominate per-catalog load latency.
The contract pinned here: it returns entities, it does NOT load indexes (proven by
loading after the ``indexes/`` dir is deleted), and ``search()`` raises so an
entities-only catalog can never be mistaken for a searchable one.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from parsimony.catalog import BM25Index, Catalog, Entity, HybridIndex, VectorIndex
from parsimony.catalog.models import BroadSearchUnavailableError
from parsimony.catalog.search import CatalogLRU
from parsimony.embedder import EmbedderInfo


class _StubEmbedder:
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] if "alpha" in text else [0.0, 1.0] for text in texts]

    def embed_query(self, query: str) -> list[float]:
        return [1.0, 0.0] if "alpha" in query else [0.0, 1.0]

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub", dim=2, normalize=True, package="test")


def _save_vector_catalog(target: Path) -> None:
    """Build + save a catalog whose index is a vector index (the costly kind)."""
    catalog = Catalog(
        "codelist",
        indexes={"description": HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder())])},
        default_field="description",
    )
    catalog.set_entities(
        [
            Entity(namespace="codelist", code="A", title="alpha", metadata={"description": "alpha signal"}),
            Entity(namespace="codelist", code="B", title="beta", metadata={"description": "beta signal"}),
        ]
    )
    catalog.build()
    catalog.save(f"file://{target}")


def _save_bm25_catalog(target: Path) -> None:
    """A BM25-only catalog reloads and searches without an embedder — used where
    the test does a full reload-and-search (a vector index can't rehydrate its
    stub embedder from disk)."""
    catalog = Catalog("codelist", indexes={"title": BM25Index()}, default_field="title")
    catalog.set_entities(
        [
            Entity(namespace="codelist", code="A", title="alpha"),
            Entity(namespace="codelist", code="B", title="beta"),
        ]
    )
    catalog.build()
    catalog.save(f"file://{target}")


def test_load_entities_only_returns_entities_without_indexes(tmp_path: Path) -> None:
    """Entities load even after the index dir is deleted — proving it skips index load."""
    snapshot = tmp_path / "codelist"
    _save_vector_catalog(snapshot)

    # Delete the indexes: a full load now cannot succeed, but entities-only must.
    shutil.rmtree(snapshot / "indexes")

    catalog = Catalog.load_entities_only(f"file://{snapshot}")
    assert [e.code for e in catalog.entities] == ["A", "B"]
    assert catalog.get("codelist", "A").title == "alpha"

    # A normal load needs the (now-missing) index — entities-only genuinely differs.
    # Deleting indexes/ changes the dir, so the content-SHA check trips first.
    with pytest.raises((ValueError, FileNotFoundError, OSError)):
        Catalog.load(f"file://{snapshot}")


def test_entities_only_catalog_cannot_search(tmp_path: Path) -> None:
    """``search`` on an entities-only catalog raises rather than silently misbehaving."""
    snapshot = tmp_path / "codelist"
    _save_vector_catalog(snapshot)

    catalog = Catalog.load_entities_only(f"file://{snapshot}")
    with pytest.raises(BroadSearchUnavailableError):
        catalog.search("alpha", limit=5)


def test_lru_keys_entities_only_separately_from_full(tmp_path: Path) -> None:
    """Browse (entities-only) and search (full) loads of one URL coexist in the LRU."""
    snapshot = tmp_path / "codelist"
    _save_bm25_catalog(snapshot)
    url = f"file://{snapshot}"
    lru = CatalogLRU(size=4)

    browse = lru.get_or_load(url, entities_only=True)
    full = lru.get_or_load(url, entities_only=False)

    assert browse is not full
    # The full one searches; the entities-only one refuses.
    hits = full.search("alpha", limit=5)
    assert [h.code for h in hits] == ["A"]
    with pytest.raises(BroadSearchUnavailableError):
        browse.search("alpha", limit=5)

    # Re-requesting each returns the same cached instance.
    assert lru.get_or_load(url, entities_only=True) is browse
    assert lru.get_or_load(url, entities_only=False) is full
