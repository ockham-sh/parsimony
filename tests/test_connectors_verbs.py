"""Tests for :class:`Connectors` collection verbs."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from parsimony.connector import Connector, Connectors, connector


def _public(name: str, **kwargs: Any) -> Connector:
    async def _fn(x: str = "y") -> dict[str, Any]:
        return {"ok": True, "x": x}

    _fn.__doc__ = f"Public connector {name}."
    _fn.__name__ = name
    return connector(**kwargs)(_fn)


def _keyed(name: str) -> Connector:
    async def _fn(x: str, api_key: str) -> dict[str, Any]:
        return {"ok": True, "x": x, "key": api_key}

    _fn.__doc__ = f"Keyed connector {name}."
    _fn.__name__ = name
    return connector(_fn)


def test_add_two_collections() -> None:
    a = Connectors([_public("a1"), _public("a2")])
    b = Connectors([_public("b1")])

    merged = a + b
    assert merged.names() == ["a1", "a2", "b1"]
    assert a.names() == ["a1", "a2"]
    assert b.names() == ["b1"]


def test_add_raises_on_duplicate_name_across_collections() -> None:
    a = Connectors([_public("shared"), _public("a_extra")])
    b = Connectors([_public("shared")])

    with pytest.raises(ValueError, match="Duplicate connector names"):
        _ = a + b


def test_bind_applies_matching_arguments_across_collection() -> None:
    k = _keyed("keyed_fetch")
    p = _public("public_fetch")

    bound = Connectors([k, p]).bind(api_key="secret")

    assert list(bound["keyed_fetch"].exposed_signature.parameters) == ["x"]
    assert list(bound["public_fetch"].exposed_signature.parameters) == ["x"]
    result = asyncio.run(bound["keyed_fetch"](x="hello"))
    assert result.data == {"ok": True, "x": "hello", "key": "secret"}
    assert result.provenance.params == {"x": "hello"}


def test_filter_predicate() -> None:
    coll = Connectors([_public("loader_fetch", tags=["loader"]), _public("safe_fetch", tags=["tool"])])
    safe = coll.filter(lambda c: "loader" not in c.tags)
    assert safe.names() == ["safe_fetch"]
