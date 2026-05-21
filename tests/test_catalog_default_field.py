"""Tests for Catalog default_field."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    CatalogEntry,
)


@pytest.mark.asyncio
async def test_catalog_default_field_behavior() -> None:
    cat = Catalog("test_cat")
    assert cat.default_field == "title"

    cat2 = Catalog("test_cat", default_field="desc")
    assert cat2.default_field == "desc"

    cat3 = Catalog("test_cat", default_field="desc")
    cat3.set_indexes([BM25Index("title_bm25", field="title")])
    cat3.set_entries([CatalogEntry(namespace="ns", code="A", title="Title", metadata={"desc": "Description"})])

    with pytest.raises(ValueError, match="No index configured for default_field"):
        await cat3.build()

    # Pass the right index and default_field at construction
    cat4 = Catalog("test_cat", indexes=[BM25Index("title_bm25", field="title")], default_field="title")
    cat4.set_entries([CatalogEntry(namespace="ns", code="A", title="Title", metadata={"desc": "Description"})])
    await cat4.build()

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "snapshot"
        await cat4.save(path)

        loaded = await Catalog.load(path)
        assert loaded.default_field == "title"
        assert loaded.name == "test_cat"
