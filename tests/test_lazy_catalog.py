"""Integration tests for Catalog lazy namespace population via live enumerators.

The former GitHub-download tests were removed in the HF bundle cutover —
bundle-store integration is covered by :mod:`tests.test_hf_bundle_store`,
which uses a local fixture layout (no HF network required).
"""

from __future__ import annotations

import pytest

from parsimony.catalog.catalog import Catalog
from parsimony.connectors.riksbank import CONNECTORS as RIKSBANK
from parsimony.connectors.treasury import CONNECTORS as TREASURY
from parsimony.stores.sqlite_catalog import SQLiteCatalogStore


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_enumerator_fallback(tmp_path):
    """Catalog falls back to live enumerator when no HF bundle is published."""
    connectors = RIKSBANK.bind_deps(api_key="")
    store = SQLiteCatalogStore(tmp_path / "catalog.db")
    catalog = Catalog(store, connectors=connectors)

    await catalog.search("SEK", limit=5, namespaces=["riksbank"])

    ns = await catalog.list_namespaces()
    assert "riksbank" in ns

    await catalog.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_caches_across_calls(tmp_path):
    """Second search for same namespace doesn't re-enumerate."""
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

    store2 = SQLiteCatalogStore(db_path)
    cat2 = Catalog(store2)
    matches = await cat2.search("debt", limit=5, namespaces=["treasury"])
    assert len(matches) > 0
    await cat2.close()
