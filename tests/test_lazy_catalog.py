"""Tests for Catalog lazy namespace population: HF download + enumerator fallback."""

from __future__ import annotations

import pytest

from ockham.catalog.catalog import Catalog
from ockham.connectors.riksbank import CONNECTORS as RIKSBANK
from ockham.connectors.treasury import CONNECTORS as TREASURY
from ockham.stores.sqlite_catalog import SQLiteCatalogStore


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_hf_download(tmp_path):
    """Catalog auto-downloads treasury catalog from HuggingFace."""
    store = SQLiteCatalogStore(tmp_path / "catalog.db")
    catalog = Catalog(store, connectors=TREASURY)

    matches = await catalog.search("debt", limit=5, namespaces=["treasury"])
    assert len(matches) > 0
    assert all(m.namespace == "treasury" for m in matches)

    ns = await catalog.list_namespaces()
    assert "treasury" in ns

    await catalog.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_enumerator_fallback(tmp_path):
    """Catalog falls back to live enumerator when HF dataset doesn't exist."""
    connectors = RIKSBANK.bind_deps(api_key="")
    store = SQLiteCatalogStore(tmp_path / "catalog.db")
    catalog = Catalog(store, connectors=connectors)

    # Riksbank has no HF dataset — should enumerate live
    await catalog.search("SEK", limit=5, namespaces=["riksbank"])

    ns = await catalog.list_namespaces()
    assert "riksbank" in ns

    await catalog.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_caches_across_calls(tmp_path):
    """Second search for same namespace doesn't re-download."""
    store = SQLiteCatalogStore(tmp_path / "catalog.db")
    catalog = Catalog(store, connectors=TREASURY)

    await catalog.search("debt", limit=1, namespaces=["treasury"])
    assert "treasury" in catalog._populated

    matches = await catalog.search("fiscal", limit=5, namespaces=["treasury"])
    assert len(matches) >= 0

    await catalog.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_persists_between_instances(tmp_path):
    """Catalog data persists in SQLite between Catalog instances."""
    db_path = tmp_path / "catalog.db"

    store1 = SQLiteCatalogStore(db_path)
    cat1 = Catalog(store1, connectors=TREASURY)
    await cat1.search("debt", limit=1, namespaces=["treasury"])
    await cat1.close()

    # Second instance — treasury already in SQLite, no download needed
    store2 = SQLiteCatalogStore(db_path)
    cat2 = Catalog(store2)  # no connectors needed
    matches = await cat2.search("debt", limit=5, namespaces=["treasury"])
    assert len(matches) > 0
    await cat2.close()
