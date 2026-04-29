"""Tests for :class:`parsimony.catalog.CatalogCache`.

Locks in the in-memory caching contract used by every connector that
queries a published catalog: lazy load, LRU eviction, single load
under concurrent gets.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from parsimony.catalog import Catalog, CatalogCache


class _FakeCatalog:
    """Sentinel — identity is what the cache tests compare on."""

    def __init__(self, url: str) -> None:
        self.url = url


@pytest.mark.asyncio
async def test_first_get_loads_subsequent_gets_hit_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def _from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        calls.append(url)
        return _FakeCatalog(url)

    monkeypatch.setattr(Catalog, "from_url", _from_url)

    cache = CatalogCache(max_size=1)
    a1 = await cache.get("hf://o/r/a")
    a2 = await cache.get("hf://o/r/a")

    assert calls == ["hf://o/r/a"]
    assert a1 is a2
    assert "hf://o/r/a" in cache
    assert len(cache) == 1


@pytest.mark.asyncio
async def test_lru_evicts_oldest_at_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Filling past ``max_size`` evicts the least-recently-used entry."""
    calls: list[str] = []

    async def _from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        calls.append(url)
        return _FakeCatalog(url)

    monkeypatch.setattr(Catalog, "from_url", _from_url)

    cache = CatalogCache(max_size=2)
    await cache.get("a")
    await cache.get("b")
    await cache.get("a")  # touch — moves to MRU
    await cache.get("c")  # evicts "b" (oldest), keeps "a" + "c"

    assert "a" in cache
    assert "b" not in cache
    assert "c" in cache
    assert len(cache) == 2

    # Re-fetching "b" triggers another from_url; "a" + "c" don't.
    await cache.get("b")
    assert calls == ["a", "b", "c", "b"]


@pytest.mark.asyncio
async def test_concurrent_first_load_calls_from_url_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent gets for the same URL trigger exactly one load —
    the lock holds the second call until the first finishes."""
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    async def _slow_from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        calls.append(url)
        started.set()
        await release.wait()
        return _FakeCatalog(url)

    monkeypatch.setattr(Catalog, "from_url", _slow_from_url)

    cache = CatalogCache(max_size=2)
    t1 = asyncio.create_task(cache.get("hf://o/r/x"))
    await started.wait()
    t2 = asyncio.create_task(cache.get("hf://o/r/x"))

    # Give t2 a tick to attempt acquiring the lock.
    await asyncio.sleep(0)
    release.set()

    a, b = await asyncio.gather(t1, t2)

    assert calls == ["hf://o/r/x"]
    assert a is b


@pytest.mark.asyncio
async def test_clear_drops_all_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        return _FakeCatalog(url)

    monkeypatch.setattr(Catalog, "from_url", _from_url)

    cache = CatalogCache(max_size=4)
    await cache.get("a")
    await cache.get("b")
    assert len(cache) == 2

    cache.clear()

    assert len(cache) == 0
    assert "a" not in cache


@pytest.mark.asyncio
async def test_loader_errors_propagate_unwrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``Catalog.from_url`` errors propagate raw — wrapping policy
    belongs to the caller (each provider has its own directive)."""

    async def _raise(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        raise FileNotFoundError("not on disk")

    monkeypatch.setattr(Catalog, "from_url", _raise)

    cache = CatalogCache(max_size=1)

    with pytest.raises(FileNotFoundError):
        await cache.get("file:///missing")

    # Failed load is not cached — a retry hits the loader again.
    with pytest.raises(FileNotFoundError):
        await cache.get("file:///missing")


def test_max_size_zero_rejected() -> None:
    with pytest.raises(ValueError, match=">= 1"):
        CatalogCache(max_size=0)
