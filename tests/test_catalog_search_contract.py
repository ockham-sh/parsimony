"""Tests the `Catalog.search` contract: namespaces= is required, migration UX."""

from __future__ import annotations

import pytest

from parsimony.catalog.catalog import Catalog
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


@pytest.mark.asyncio
async def test_search_requires_namespaces_not_none():
    store = SQLiteCatalogStore(":memory:", embedding_dim=4)
    catalog = Catalog(store)
    with pytest.raises(ValueError, match="namespaces=\\[\\.\\.\\.\\]"):
        await catalog.search("gdp")


@pytest.mark.asyncio
async def test_search_requires_non_empty_namespaces():
    store = SQLiteCatalogStore(":memory:", embedding_dim=4)
    catalog = Catalog(store)
    with pytest.raises(ValueError, match="non-empty"):
        await catalog.search("gdp", namespaces=[])


@pytest.mark.asyncio
async def test_error_message_names_migration_path():
    store = SQLiteCatalogStore(":memory:", embedding_dim=4)
    catalog = Catalog(store)
    with pytest.raises(ValueError) as excinfo:
        await catalog.search("gdp")
    message = str(excinfo.value)
    # Must name the old and new shapes so developers can fix without reading source.
    assert "Catalog.search(query)" in message
    assert "namespaces=" in message
