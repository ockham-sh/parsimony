"""Tests for entity projection, catalog policy, and catalog-not-found errors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.embedder import EmbedderInfo
from parsimony.entity import Entity
from parsimony.errors import CatalogNotFoundError
from parsimony.result import Column, ColumnRole, OutputSpec, Result


class _FakeEmbedder:
    """Offline stand-in so index construction never loads a real model."""

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="fake", dim=2, normalize=False, package="test")

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[0.0, 0.0] for _ in texts]

    def embed_query(self, query: str) -> list[float]:
        return [0.0, 0.0]


def _sample_output() -> OutputSpec:
    return OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="demo"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="topic", role=ColumnRole.METADATA),
        ]
    )


def test_entities_from_tabular_result() -> None:
    output = _sample_output()
    result = Result(
        raw=pd.DataFrame({"code": ["a"], "title": ["Alpha"], "topic": ["prices"]}),
        output_spec=output,
    )
    entities = list(result.entities.values())
    assert len(entities) == 1
    assert entities[0].namespace == "demo"
    assert entities[0].code == "a"


def test_entities_rejects_non_tabular_data() -> None:
    output = _sample_output()
    result = Result(raw=[Entity(namespace="demo", code="a", title="Alpha")], output_spec=output)
    with pytest.raises(TypeError, match="tabular"):
        _ = result.entities


def test_discovery_indexes_follow_role_regardless_of_entry_count() -> None:
    """Index kind follows the field's role, not how many entries a provider has.

    ``code`` is a BM25 identifier index; ``title`` and ``description`` are
    always hybrid (BM25 + vector), whether the catalog has one entry or many.
    A fake embedder keeps construction offline.
    """
    embedder = _FakeEmbedder()
    for count in (1, 500):
        entries = [Entity(namespace="demo", code=f"c{i}", title=f"title {i}", metadata={}) for i in range(count)]
        indexes = discovery_indexes(entries, embedder=embedder)
        assert indexes["code"].__class__.__name__ == "BM25Index"
        assert indexes["title"].__class__.__name__ == "HybridIndex"
        assert indexes["description"].__class__.__name__ == "HybridIndex"


def test_discovery_indexes_omits_description_when_excluded() -> None:
    indexes = discovery_indexes([], include_description=False, embedder=_FakeEmbedder())
    assert "description" not in indexes
    assert set(indexes) == {"code", "title"}


def test_catalog_not_found_from_missing_file(tmp_path: Path) -> None:
    from parsimony.catalog.search import load_or_build_catalog

    missing = tmp_path / "nope"
    with pytest.raises(CatalogNotFoundError, match="DO NOT retry"):
        load_or_build_catalog(f"file://{missing}", cache_path=missing / "cache", build=None)


def test_concurrent_save_uses_unique_temp_dirs(tmp_path: Path) -> None:
    entries = [Entity(namespace="demo", code="a", title="Alpha", metadata={})]

    def _save(name: str) -> None:
        # BM25-only (indexes=None's default) — this test is about save()'s temp-dir
        # isolation, not search quality, so it has no business pulling in a real
        # embedder. The role-based discovery_indexes() would give title a HybridIndex
        # with a vector component and hit the network on a cold model cache, which is
        # exactly the live-dependency the rest of the suite avoids via stub embedders
        # (see test_hybrid_index.py, test_catalog_lifecycle.py).
        catalog = Catalog(name)
        catalog.set_entities(entries)
        catalog.build()
        catalog.save(str(tmp_path / name))

    _save("one")
    _save("two")
    assert (tmp_path / "one").is_dir()
    assert (tmp_path / "two").is_dir()
