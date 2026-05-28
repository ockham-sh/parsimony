from __future__ import annotations

from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    Entity,
    VectorIndex,
)
from parsimony.catalog.storage import VALUES_FILENAME
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
    with pytest.raises(TypeError):
        Catalog("test", embedder=_SimpleEmbedder())  # type: ignore[call-arg]

    idx = VectorIndex(embedder=_SimpleEmbedder())
    assert idx._embedder is not None

    cat = Catalog("test", indexes={"title": idx})
    cat.set_entities([Entity(namespace="ns", code="A", title="Testing")])
    await cat.build()


@pytest.mark.asyncio
async def test_catalog_url_unified(tmp_path: Path) -> None:
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="A", title="Hello World")])
    await cat.build()

    await cat.save(tmp_path / "cat_path")
    assert (tmp_path / "cat_path" / "meta.json").exists()

    loaded1 = await Catalog.load(tmp_path / "cat_path")
    assert loaded1.name == "test"

    str_path = str(tmp_path / "cat_str")
    await cat.save(str_path)
    assert (tmp_path / "cat_str" / "meta.json").exists()

    loaded2 = await Catalog.load(str_path)
    assert loaded2.name == "test"


@pytest.mark.asyncio
async def test_snapshot_integrity(tmp_path: Path) -> None:
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="A", title="Secure snapshot")])
    await cat.build()

    save_path = tmp_path / "snapshot"
    await cat.save(save_path)

    entries_file = save_path / "entries.parquet"
    assert entries_file.exists()
    entries_content = entries_file.read_bytes()
    entries_file.write_bytes(entries_content + b"\x00corrupt\x00")

    with pytest.raises(ValueError, match="Catalog snapshot integrity check failed"):
        await Catalog.load(save_path)


@pytest.mark.asyncio
async def test_bm25_self_contained(tmp_path: Path) -> None:
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities(
        [
            Entity(namespace="ns", code="A", title="Unique token identifier"),
            Entity(namespace="ns", code="B", title="Another unrelated title"),
            Entity(namespace="ns", code="C", title="Something completely different"),
            Entity(namespace="ns", code="D", title="More filler content"),
            Entity(namespace="ns", code="E", title="And another one"),
        ]
    )
    await cat.build()

    save_path = tmp_path / "bm25_snapshot"
    await cat.save(save_path)

    values_file = save_path / "indexes" / "title" / VALUES_FILENAME
    assert values_file.exists()

    loaded = await Catalog.load(save_path)
    results, _ = await loaded.search("Unique token", limit=5)
    assert len(results) == 1
    assert results[0].code == "A"


@pytest.mark.asyncio
async def test_bm25_overlap_fallback_on_tiny_corpus() -> None:
    """Dev-time BM25 catalogs with very few rows still return token-overlap hits."""
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="FXUSDCAD", title="USD/CAD")])
    await cat.build()

    results, _ = await cat.search("USD", limit=5)
    assert len(results) == 1
    assert results[0].code == "FXUSDCAD"
