"""Tests for the new :class:`Connectors` verbs.

Covers ``merge``, ``bind_env``, ``unbound``, ``env_vars``, ``replace``, and
the ``filter(predicate)`` overload. Also verifies the keep-but-unbound
credentialing model from DESIGN §5 scenario 9.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from parsimony.connector import Connector, Connectors, connector
from parsimony.errors import UnauthorizedError


class _TP(BaseModel):
    x: str = "y"


def _public(name: str, **kwargs: Any) -> Connector:
    async def _fn(params: _TP) -> dict[str, Any]:
        return {"ok": True}

    _fn.__doc__ = f"Public connector {name}."
    _fn.__name__ = name
    return connector(**kwargs)(_fn)


def _keyed(name: str, *, env: dict[str, str]) -> Connector:
    async def _fn(params: _TP, *, api_key: str) -> dict[str, Any]:
        return {"ok": True, "key": api_key}

    _fn.__doc__ = f"Keyed connector {name}."
    _fn.__name__ = name
    return connector(env=env)(_fn)


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


def test_merge_two_collections() -> None:
    a = Connectors([_public("a1"), _public("a2")])
    b = Connectors([_public("b1")])

    merged = Connectors.merge(a, b)
    assert merged.names() == ["a1", "a2", "b1"]
    # Verify identity — original collections untouched.
    assert a.names() == ["a1", "a2"]
    assert b.names() == ["b1"]


def test_merge_raises_on_duplicate_name_across_collections() -> None:
    a = Connectors([_public("shared"), _public("a_extra")])
    b = Connectors([_public("shared")])

    with pytest.raises(ValueError, match="Duplicate connector names"):
        Connectors.merge(a, b)


# ---------------------------------------------------------------------------
# bind_env
# ---------------------------------------------------------------------------


def test_bind_env_success(monkeypatch: pytest.MonkeyPatch) -> None:
    k = _keyed("keyed_fetch", env={"api_key": "MYKEY"})
    monkeypatch.setenv("MYKEY", "secret-value")

    bound = Connectors([k]).bind_env()
    assert bound.unbound == ()
    # And the connector is directly callable. The wrapped function returns a
    # dict; parsimony surfaces it on ``result.data``.
    result = asyncio.run(bound["keyed_fetch"](x="hello"))
    assert result.data["ok"] is True
    assert result.data["key"] == "secret-value"


def test_bind_env_missing_required_marks_unbound(monkeypatch: pytest.MonkeyPatch) -> None:
    k = _keyed("needy_fetch", env={"api_key": "MISSING_KEY"})
    monkeypatch.delenv("MISSING_KEY", raising=False)

    result = Connectors([k]).bind_env()
    assert result.unbound == ("needy_fetch",)
    # The connector is still in the collection.
    assert "needy_fetch" in result


def test_unbound_connector_raises_unauthorized_on_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    k = _keyed("needy_fetch", env={"api_key": "ALSO_MISSING"})
    monkeypatch.delenv("ALSO_MISSING", raising=False)

    coll = Connectors([k]).bind_env()
    with pytest.raises(UnauthorizedError) as exc_info:
        asyncio.run(coll["needy_fetch"](x="hello"))
    assert "ALSO_MISSING" in str(exc_info.value)


# ---------------------------------------------------------------------------
# env_vars
# ---------------------------------------------------------------------------


def test_env_vars_aggregates_across_connectors() -> None:
    a = _keyed("a_fetch", env={"api_key": "A_KEY"})
    b = _keyed("b_fetch", env={"api_key": "B_KEY"})
    c = _public("c_fetch")  # no env

    coll = Connectors([a, b, c])
    assert coll.env_vars() == frozenset({"A_KEY", "B_KEY"})


# ---------------------------------------------------------------------------
# replace
# ---------------------------------------------------------------------------


def test_replace_swaps_entry() -> None:
    original = _public("orig_fetch")
    replacement = _public("orig_fetch")  # same name, different function

    coll = Connectors([original, _public("other")])
    swapped = coll.replace("orig_fetch", replacement)

    assert swapped.names() == ["orig_fetch", "other"]
    # The new entry is a distinct Connector object.
    assert swapped["orig_fetch"] is replacement
    # Original collection unchanged.
    assert coll["orig_fetch"] is original


def test_replace_unknown_name_raises() -> None:
    coll = Connectors([_public("only")])
    with pytest.raises(KeyError, match="ghost"):
        coll.replace("ghost", _public("ghost"))


# ---------------------------------------------------------------------------
# filter (predicate overload)
# ---------------------------------------------------------------------------


def test_filter_predicate_overload() -> None:
    a = _public("public_fetch")
    loader_fn = _public("loader_fetch", tags=["loader"])
    enum_fn = _public("enum_fetch", tags=["enumerator"])

    coll = Connectors([a, loader_fn, enum_fn])
    safe = coll.filter(lambda c: "loader" not in c.tags)
    assert safe.names() == ["enum_fetch", "public_fetch"]
