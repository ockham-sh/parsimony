from __future__ import annotations

from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    CatalogEntry,
    VectorIndex,
)
from parsimony.embedder import EmbedderInfo


class _SimpleEmbedder:
    DIM = 4

    @property
    def dimension(self) -> int:
        return self.DIM

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="simple", dim=self.DIM, normalize=True, package="test")

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

    async def embed_query(self, query: str) -> list[float]:
        return [1.0, 0.0, 0.0, 0.0]


@pytest.mark.asyncio
async def test_catalog_embedder_separation() -> None:
    # 1. Catalog ctor should not accept embedder
    with pytest.raises(TypeError):
        Catalog("test", embedder=_SimpleEmbedder())  # type: ignore

    # 2. VectorIndex accepts embedder
    idx = VectorIndex("title_vector", field="title", embedder=_SimpleEmbedder())
    assert idx._embedder is not None

    # 3. VectorIndex.load works without passing embedder and lazy-initializes it when required
    cat = Catalog("test", indexes=[idx])
    cat.set_entries([CatalogEntry(namespace="ns", code="A", title="Testing")])
    await cat.build()


@pytest.mark.asyncio
async def test_catalog_url_unified(tmp_path: Path) -> None:
    # 1. Save and load using Path objects and file scheme falling back
    cat = Catalog("test", indexes=[BM25Index("title_bm25", field="title")])
    cat.set_entries([CatalogEntry(namespace="ns", code="A", title="Hello World")])
    await cat.build()

    # Save to a Path object
    await cat.save(tmp_path / "cat_path")
    assert (tmp_path / "cat_path" / "meta.json").exists()

    # Load from a Path object
    loaded1 = await Catalog.load(tmp_path / "cat_path")
    assert loaded1.name == "test"

    # Save to string without scheme (bare path)
    str_path = str(tmp_path / "cat_str")
    await cat.save(str_path)
    assert (tmp_path / "cat_str" / "meta.json").exists()

    # Load from string without scheme (bare path)
    loaded2 = await Catalog.load(str_path)
    assert loaded2.name == "test"


@pytest.mark.asyncio
async def test_snapshot_integrity(tmp_path: Path) -> None:
    cat = Catalog("test", indexes=[BM25Index("title_bm25", field="title")])
    cat.set_entries([CatalogEntry(namespace="ns", code="A", title="Secure snapshot")])
    await cat.build()

    save_path = tmp_path / "snapshot"
    await cat.save(save_path)

    # Corrupting the snapshot by appending extra data to a data file (e.g. entries.parquet)
    entries_file = save_path / "entries.parquet"
    assert entries_file.exists()
    entries_content = entries_file.read_bytes()
    entries_file.write_bytes(entries_content + b"\x00corrupt\x00")

    # Loading corrupted snapshot should raise ValueError because of SHA256 mismatch
    with pytest.raises(ValueError, match="Catalog snapshot integrity check failed"):
        await Catalog.load(save_path)


@pytest.mark.asyncio
async def test_bm25_self_contained(tmp_path: Path) -> None:
    cat = Catalog("test", indexes=[BM25Index("title_bm25", field="title")])
    cat.set_entries(
        [
            CatalogEntry(namespace="ns", code="A", title="Unique token identifier"),
            CatalogEntry(namespace="ns", code="B", title="Another unrelated title"),
            CatalogEntry(namespace="ns", code="C", title="Something completely different"),
            CatalogEntry(namespace="ns", code="D", title="More filler content"),
            CatalogEntry(namespace="ns", code="E", title="And another one"),
        ]
    )
    await cat.build()

    save_path = tmp_path / "bm25_snapshot"
    await cat.save(save_path)

    # Verify tokens.parquet exists in index directory
    tokens_file = save_path / "indexes" / "title_bm25" / "tokens.parquet"
    assert tokens_file.exists()

    # Load the catalog and perform search
    loaded = await Catalog.load(save_path)
    results, _ = await loaded.search("Unique token", limit=5)
    assert len(results) == 1
    assert results[0].code == "A"


@pytest.mark.asyncio
async def test_bm25_overlap_fallback_on_tiny_corpus() -> None:
    """Dev-time BM25 catalogs with very few rows still return token-overlap hits."""
    cat = Catalog("test", indexes=[BM25Index("title_bm25", field="title")])
    cat.set_entries([CatalogEntry(namespace="ns", code="FXUSDCAD", title="USD/CAD")])
    await cat.build()

    results, _ = await cat.search("USD", limit=5)
    assert len(results) == 1
    assert results[0].code == "FXUSDCAD"
