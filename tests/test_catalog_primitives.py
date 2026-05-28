"""Tests for entity projection, catalog policy, and catalog-not-found errors."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd
import pytest

from parsimony.catalog import Catalog
from parsimony.catalog.policy import (
    HYBRID_UNIQUE_VALUE_LIMIT,
    discovery_indexes,
)
from parsimony.catalog.source import entities_from_raw
from parsimony.entity import Entity
from parsimony.errors import CatalogNotFoundError
from parsimony.result import Column, ColumnRole, OutputConfig, TabularResult


def _sample_output() -> OutputConfig:
    return OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="demo"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="topic", role=ColumnRole.METADATA),
        ]
    )


def test_entities_from_tabular_result() -> None:
    output = _sample_output()
    raw = TabularResult(
        data=pd.DataFrame({"code": ["a"], "title": ["Alpha"], "topic": ["prices"]}),
        output_schema=output,
    )
    entities = entities_from_raw(raw, output)
    assert len(entities) == 1
    assert entities[0].namespace == "demo"
    assert entities[0].code == "a"


def test_entities_from_raw_rejects_entity_list() -> None:
    output = _sample_output()
    with pytest.raises(TypeError, match="list\\[Entity\\]"):
        entities_from_raw([Entity(namespace="demo", code="a", title="Alpha")], output)


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


@pytest.mark.asyncio
async def test_catalog_not_found_from_missing_file(tmp_path: Path) -> None:
    from parsimony.catalog.search import load_or_build_catalog

    missing = tmp_path / "nope"
    with pytest.raises(CatalogNotFoundError, match="DO NOT retry"):
        await load_or_build_catalog(f"file://{missing}", cache_path=missing / "cache", build=None)


@pytest.mark.asyncio
async def test_concurrent_save_uses_unique_temp_dirs(tmp_path: Path) -> None:
    entries = [Entity(namespace="demo", code="a", title="Alpha", metadata={})]

    async def _save(name: str) -> None:
        catalog = Catalog(name, indexes=discovery_indexes(entries))
        catalog.set_entities(entries)
        await catalog.build()
        await catalog.save(str(tmp_path / name))

    await asyncio.gather(_save("one"), _save("two"))
    assert (tmp_path / "one").is_dir()
    assert (tmp_path / "two").is_dir()
