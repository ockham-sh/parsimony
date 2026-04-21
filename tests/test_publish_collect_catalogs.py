"""Unit tests for :func:`parsimony.publish.collect_catalogs` dispatch rules.

The function resolves a plugin's declared catalogs into an ``(namespace, fn)``
list, routing between ``CATALOGS`` (full fan-out) and ``RESOLVE_CATALOG``
(targeted lookup) based on whether ``--only`` was supplied.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

import pytest

from parsimony.publish import collect_catalogs


def _module(**attrs: Any) -> ModuleType:
    """Build a throw-away module object with the given plugin surface."""
    m = ModuleType("fake_plugin")
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


def _fn(name: str):  # noqa: ANN202
    """Return a distinct sentinel callable for each catalog name."""

    def _impl() -> None:
        return None

    _impl.__name__ = f"enumerate_{name}"
    return _impl


# ---------------------------------------------------------------------------
# CATALOGS fan-out (no --only)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_only_walks_entire_catalogs_list() -> None:
    fn_a, fn_b = _fn("a"), _fn("b")
    mod = _module(CATALOGS=[("a", fn_a), ("b", fn_b)])

    entries = await collect_catalogs(mod)
    assert entries == [("a", fn_a), ("b", fn_b)]


@pytest.mark.asyncio
async def test_no_only_ignores_resolve_catalog() -> None:
    fn_a = _fn("a")

    def _resolve(ns: str):  # noqa: ANN202
        raise AssertionError(f"RESOLVE_CATALOG should not fire without --only (got {ns!r})")

    mod = _module(CATALOGS=[("a", fn_a)], RESOLVE_CATALOG=_resolve)

    entries = await collect_catalogs(mod)
    assert entries == [("a", fn_a)]


# ---------------------------------------------------------------------------
# --only with RESOLVE_CATALOG short-circuit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_with_full_resolve_skips_catalogs_walk() -> None:
    """When RESOLVE_CATALOG handles every wanted namespace, CATALOGS must NOT iterate.

    This is the core perf motivation — a plugin whose ``CATALOGS`` live-queries
    an upstream API shouldn't pay that cost when the caller already named the
    exact namespace.
    """
    fn_target = _fn("target")

    def _resolve(ns: str):  # noqa: ANN202
        return fn_target if ns == "target" else None

    async def _catalogs():  # noqa: ANN202
        raise AssertionError("CATALOGS must not be walked when RESOLVE_CATALOG covers --only")
        yield  # pragma: no cover — unreachable; keeps this an async generator

    mod = _module(CATALOGS=_catalogs, RESOLVE_CATALOG=_resolve)

    entries = await collect_catalogs(mod, only=["target"])
    assert entries == [("target", fn_target)]


@pytest.mark.asyncio
async def test_only_falls_back_to_catalogs_for_unknown_namespace() -> None:
    """Namespaces RESOLVE_CATALOG rejects must still get a chance via CATALOGS."""
    fn_known, fn_fallback = _fn("known"), _fn("fallback")

    def _resolve(ns: str):  # noqa: ANN202
        return fn_known if ns == "known" else None

    mod = _module(CATALOGS=[("known", _fn("stale")), ("fallback", fn_fallback)], RESOLVE_CATALOG=_resolve)

    entries = await collect_catalogs(mod, only=["known", "fallback"])
    names = {ns: fn for ns, fn in entries}

    # ``known`` comes from RESOLVE_CATALOG (the resolver-returned fn,
    # not the CATALOGS-registered one — the walk short-circuits on
    # already-``seen`` names).
    assert names["known"] is fn_known
    # ``fallback`` came from the CATALOGS walk.
    assert names["fallback"] is fn_fallback


@pytest.mark.asyncio
async def test_only_without_resolve_catalog_uses_catalogs_walk() -> None:
    fn_a, fn_b = _fn("a"), _fn("b")
    mod = _module(CATALOGS=[("a", fn_a), ("b", fn_b)])

    entries = await collect_catalogs(mod, only=["b"])
    assert entries == [("b", fn_b)]


@pytest.mark.asyncio
async def test_only_with_resolver_returning_all_none_still_walks_catalogs() -> None:
    """If RESOLVE_CATALOG recognises nothing, CATALOGS must still run."""
    fn_a = _fn("a")

    def _resolve(_ns: str):  # noqa: ANN202
        return None

    mod = _module(CATALOGS=[("a", fn_a)], RESOLVE_CATALOG=_resolve)

    entries = await collect_catalogs(mod, only=["a"])
    assert entries == [("a", fn_a)]
