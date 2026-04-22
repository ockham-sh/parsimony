"""Tests for :mod:`parsimony.discover` — metadata-only entry-point discovery."""

from __future__ import annotations

import logging
import sys
import types
from importlib.metadata import EntryPoint
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector
from parsimony.discover import Provider, iter_providers, load, load_all


class _TP(BaseModel):
    x: str = "y"


def _toy(name: str) -> Any:
    async def _fn(params: _TP) -> dict[str, Any]:
        return {"ok": True}

    _fn.__doc__ = "A toy connector."
    _fn.__name__ = name
    return connector()(_fn)


def _make_fake_dist(name: str, version: str) -> Any:
    dist = types.SimpleNamespace()
    dist.version = version
    dist.metadata = {"Name": name}
    return dist


def _make_ep(name: str, value: str, dist_name: str, version: str) -> EntryPoint:
    ep = EntryPoint(name=name, value=value, group="parsimony.providers")
    # EntryPoint is a NamedTuple — .dist is a post-init attribute we set manually.
    object.__setattr__(ep, "dist", _make_fake_dist(dist_name, version))
    return ep


def test_iter_providers_empty() -> None:
    with patch("parsimony.discover.importlib.metadata.entry_points", return_value=[]):
        assert list(iter_providers()) == []


def test_iter_providers_duplicate_name_raises() -> None:
    ep1 = _make_ep("fred", "parsimony_fred_a", "parsimony-fred", "0.1.0")
    ep2 = _make_ep("fred", "parsimony_fred_b", "parsimony-fred-fork", "0.2.0")

    with patch("parsimony.discover.importlib.metadata.entry_points", return_value=[ep1, ep2]):
        with pytest.raises(RuntimeError, match="two distributions register provider 'fred'"):
            list(iter_providers())


def test_load_strict_missing_name() -> None:
    ep = _make_ep("fred", "parsimony_fred", "parsimony-fred", "0.1.0")
    mod = types.ModuleType("parsimony_fred")
    mod.CONNECTORS = Connectors([_toy("fred_fetch")])

    with patch("parsimony.discover.importlib.metadata.entry_points", return_value=[ep]):
        with patch("parsimony.discover.importlib.import_module", return_value=mod):
            with pytest.raises(LookupError) as exc_info:
                load("ghost")
            assert "ghost" in str(exc_info.value)
            assert "Available" in str(exc_info.value)


def test_load_all_forgiving_on_import_error(caplog: pytest.LogCaptureFixture) -> None:
    ep1 = _make_ep("good", "parsimony_good", "parsimony-good", "0.1.0")
    ep2 = _make_ep("broken", "parsimony_broken", "parsimony-broken", "0.1.0")

    good_mod = types.ModuleType("parsimony_good")
    good_mod.CONNECTORS = Connectors([_toy("good_fetch")])

    def _import_module(name: str) -> Any:
        if name == "parsimony_good":
            return good_mod
        raise ImportError("no module named 'missing_dep'")

    with patch("parsimony.discover.importlib.metadata.entry_points", return_value=[ep1, ep2]):
        with patch("parsimony.discover.importlib.import_module", side_effect=_import_module):
            with caplog.at_level(logging.WARNING, logger="parsimony.discover"):
                result = load_all()

    assert len(result) == 1
    assert result.names() == ["good_fetch"]
    assert any("broken" in r.message for r in caplog.records)


def test_load_contract_violation() -> None:
    ep = _make_ep("noexport", "parsimony_noexport", "parsimony-noexport", "0.1.0")
    mod = types.ModuleType("parsimony_noexport")  # no CONNECTORS

    with patch("parsimony.discover.importlib.metadata.entry_points", return_value=[ep]):
        with patch("parsimony.discover.importlib.import_module", return_value=mod):
            with pytest.raises(TypeError, match="must export CONNECTORS: Connectors"):
                load("noexport")


def test_provider_load_idempotent() -> None:
    # `load()` goes through importlib, whose module cache makes repeated calls
    # return the same object — the Provider.load contract is idempotent.
    ep = _make_ep("double", "pkg_double", "parsimony-double", "0.1.0")
    mod = types.ModuleType("pkg_double")
    mod.CONNECTORS = Connectors([_toy("double_fetch")])
    sys.modules["pkg_double"] = mod

    try:
        with patch("parsimony.discover.importlib.metadata.entry_points", return_value=[ep]):
            p = next(iter_providers())
            assert isinstance(p, Provider)
            first = p.load()
            second = p.load()
            assert list(first) == list(second)
            assert first.names() == second.names()
    finally:
        sys.modules.pop("pkg_double", None)
