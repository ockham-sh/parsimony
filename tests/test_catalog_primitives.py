"""Tests for entity projection, catalog policy, and catalog-not-found errors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from parsimony.catalog import Catalog
from parsimony.catalog.policy import (
    HYBRID_UNIQUE_VALUE_LIMIT,
    discovery_indexes,
)
from parsimony.entity import Entity
from parsimony.errors import CatalogNotFoundError
from parsimony.result import Column, ColumnRole, OutputSpec, Result


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


def test_discovery_indexes_switch_at_threshold() -> None:
    small = [
        Entity(namespace="demo", code=f"c{i}", title=f"title {i}", metadata={})
        for i in range(HYBRID_UNIQUE_VALUE_LIMIT - 1)
    ]
    large = [
        Entity(namespace="demo", code=f"c{i}", title=f"title {i}", metadata={})
        for i in range(HYBRID_UNIQUE_VALUE_LIMIT + 1)
    ]
    assert discovery_indexes(small)["title"].__class__.__name__ == "HybridIndex"
    assert discovery_indexes(large)["title"].__class__.__name__ == "BM25Index"


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
        # embedder. discovery_indexes(entries) would default to a HybridIndex here
        # (1 unique value is below HYBRID_UNIQUE_VALUE_LIMIT) and hit the network on
        # a cold model cache, which is exactly the live-dependency the rest of the
        # suite avoids via stub embedders (see test_hybrid_index.py, test_catalog_lifecycle.py).
        catalog = Catalog(name)
        catalog.set_entities(entries)
        catalog.build()
        catalog.save(str(tmp_path / name))

    _save("one")
    _save("two")
    assert (tmp_path / "one").is_dir()
    assert (tmp_path / "two").is_dir()
