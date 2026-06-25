"""Tests for catalog load-or-build and lazy cache."""

from __future__ import annotations

from pathlib import Path

import pytest

from parsimony.catalog import BM25Index, Catalog
from parsimony.catalog.models import Entity
from parsimony.catalog.search import CatalogLRU, load_or_build_catalog
from parsimony.catalog.source import lazy_catalog_dir


@pytest.fixture
def sample_entries() -> list[Entity]:
    return [
        Entity(namespace="demo", code="a", title="alpha widget"),
        Entity(namespace="demo", code="b", title="beta widget"),
    ]


def test_load_or_build_uses_lazy_cache(tmp_path: Path, sample_entries: list[Entity]) -> None:
    configured = tmp_path / "missing-configured"
    cache = tmp_path / "lazy-cache"
    build_calls = 0

    def build() -> Catalog:
        nonlocal build_calls
        build_calls += 1
        catalog = Catalog("demo", indexes={"code": BM25Index(), "title": BM25Index()}, default_field="title")
        catalog.set_entities(sample_entries)
        catalog.build()
        return catalog

    first = load_or_build_catalog(
        f"file://{configured}",
        cache_path=cache,
        build=build,
    )
    assert build_calls == 1
    assert len(first) == 2

    second = load_or_build_catalog(
        f"file://{configured}",
        cache_path=cache,
        build=build,
    )
    assert build_calls == 1
    assert len(second) == 2
    assert (cache / "meta.json").is_file()


def test_catalog_lru_reuses_memory(tmp_path: Path, sample_entries: list[Entity]) -> None:
    lru = CatalogLRU(size=2)
    url = f"file://{tmp_path / 'missing'}"
    cache = tmp_path / "lazy"

    def build() -> Catalog:
        catalog = Catalog("demo", indexes={"code": BM25Index(), "title": BM25Index()}, default_field="title")
        catalog.set_entities(sample_entries)
        catalog.build()
        return catalog

    first = lru.get_or_load(url, cache_path=cache, build=build)
    second = lru.get_or_load(url, cache_path=cache, build=build)
    assert first is second


def test_catalog_lru_refresh_bypasses_memory(tmp_path: Path, sample_entries: list[Entity]) -> None:
    lru = CatalogLRU(size=2)
    url = f"file://{tmp_path / 'missing'}"
    cache = tmp_path / "lazy"

    def build() -> Catalog:
        catalog = Catalog("demo", indexes={"code": BM25Index(), "title": BM25Index()}, default_field="title")
        catalog.set_entities(sample_entries)
        catalog.build()
        return catalog

    first = lru.get_or_load(url, cache_path=cache, build=build)
    assert lru.get_or_load(url, cache_path=cache, build=build) is first  # served from memory
    refreshed = lru.get_or_load(url, cache_path=cache, build=build, refresh=True)
    assert refreshed is not first  # refresh dropped the in-memory copy and re-loaded
    assert len(refreshed) == 2


def test_lazy_catalog_dir_under_connectors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSIMONY_CACHE_DIR", str(tmp_path))
    path = lazy_catalog_dir("treasury", "treasury")
    assert path.endswith("connectors/treasury/catalogs/treasury")
