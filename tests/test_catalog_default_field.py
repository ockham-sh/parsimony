"""Tests for Catalog default_field."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    BroadSearchConfigError,
    BroadSearchUnavailableError,
    Catalog,
    Entity,
)


@pytest.mark.asyncio
async def test_catalog_default_field_none_resolves_title() -> None:
    cat = Catalog("test_cat")
    assert cat.default_field is None
    assert cat._resolve_default_field() == "title"

    cat.set_entities([Entity(namespace="ns", code="A", title="Title", metadata={})])
    await cat.build()
    matches, diag = await cat.search("Title", limit=5)
    assert len(matches) >= 1
    assert diag.mode == "broad"


@pytest.mark.asyncio
async def test_explicit_default_field() -> None:
    cat2 = Catalog("test_cat", default_field="desc")
    assert cat2.default_field == "desc"


@pytest.mark.asyncio
async def test_build_raises_when_explicit_default_has_no_index() -> None:
    cat3 = Catalog("test_cat", default_field="desc")
    with pytest.raises(BroadSearchConfigError, match="default_field"):
        cat3.set_indexes({"title": BM25Index()})


@pytest.mark.asyncio
async def test_broad_search_unavailable_without_title_index() -> None:
    cat = Catalog("test_cat", indexes={"code": BM25Index()})
    cat.set_entities([Entity(namespace="ns", code="A", title="Title", metadata={"code": "x"})])
    await cat.build()

    with pytest.raises(BroadSearchUnavailableError, match="structured"):
        await cat.search("plain text", limit=5)


@pytest.mark.asyncio
async def test_save_load_roundtrip_default_field() -> None:
    cat4 = Catalog("test_cat", indexes={"title": BM25Index()}, default_field="title")
    cat4.set_entities([Entity(namespace="ns", code="A", title="Title", metadata={"desc": "Description"})])
    await cat4.build()

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "snapshot"
        await cat4.save(path)

        loaded = await Catalog.load(path)
        assert loaded.default_field == "title"
        assert loaded.name == "test_cat"
