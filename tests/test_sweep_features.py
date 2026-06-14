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

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

    def embed_query(self, query: str) -> list[float]:
        return [1.0, 0.0, 0.0, 0.0]


def test_catalog_embedder_separation() -> None:
    with pytest.raises(TypeError):
        Catalog("test", embedder=_SimpleEmbedder())  # type: ignore[call-arg]

    idx = VectorIndex(embedder=_SimpleEmbedder())
    assert idx._embedder is not None

    cat = Catalog("test", indexes={"title": idx})
    cat.set_entities([Entity(namespace="ns", code="A", title="Testing")])
    cat.build()


def test_catalog_url_unified(tmp_path: Path) -> None:
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="A", title="Hello World")])
    cat.build()

    cat.save(tmp_path / "cat_path")
    assert (tmp_path / "cat_path" / "meta.json").exists()

    loaded1 = Catalog.load(tmp_path / "cat_path")
    assert loaded1.name == "test"

    str_path = str(tmp_path / "cat_str")
    cat.save(str_path)
    assert (tmp_path / "cat_str" / "meta.json").exists()

    loaded2 = Catalog.load(str_path)
    assert loaded2.name == "test"


def test_snapshot_integrity(tmp_path: Path) -> None:
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="A", title="Secure snapshot")])
    cat.build()

    save_path = tmp_path / "snapshot"
    cat.save(save_path)

    entries_file = save_path / "entries.parquet"
    assert entries_file.exists()
    entries_content = entries_file.read_bytes()
    entries_file.write_bytes(entries_content + b"\x00corrupt\x00")

    with pytest.raises(ValueError, match="Catalog snapshot integrity check failed"):
        Catalog.load(save_path)


def test_bm25_self_contained(tmp_path: Path) -> None:
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
    cat.build()

    save_path = tmp_path / "bm25_snapshot"
    cat.save(save_path)

    values_file = save_path / "indexes" / "title" / VALUES_FILENAME
    assert values_file.exists()

    loaded = Catalog.load(save_path)
    results, _ = loaded.search("Unique token", limit=5)
    assert len(results) == 1
    assert results[0].code == "A"


def test_bm25_overlap_fallback_on_tiny_corpus() -> None:
    """Dev-time BM25 catalogs with very few rows still return token-overlap hits."""
    cat = Catalog("test", indexes={"title": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="FXUSDCAD", title="USD/CAD")])
    cat.build()

    results, _ = cat.search("USD", limit=5)
    assert len(results) == 1
    assert results[0].code == "FXUSDCAD"
