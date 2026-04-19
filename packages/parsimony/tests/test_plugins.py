"""Smoke tests for entry-point discovery in :mod:`parsimony.plugins`.

Only one plugin axis exists: ``parsimony.providers``. Catalog backends are
not plugin-discovered; users subclass :class:`parsimony.BaseCatalog` directly
or use :class:`parsimony.Catalog`.
"""

from __future__ import annotations

from importlib.metadata import EntryPoint
from unittest.mock import patch

import pytest

from parsimony.plugins import PROVIDERS_GROUP, RegistryWarning, discover_providers


def _make_ep(name: str, value: str, group: str) -> EntryPoint:
    return EntryPoint(name=name, value=value, group=group)


def test_group_name_is_stable() -> None:
    assert PROVIDERS_GROUP == "parsimony.providers"


def test_discover_providers_yields_loaded_objects() -> None:
    fake_provider = object()
    ep = _make_ep("fake", "fake.module:Cls", PROVIDERS_GROUP)
    with (
        patch("parsimony.plugins._entry_points", return_value=[ep]),
        patch.object(EntryPoint, "load", autospec=True, return_value=fake_provider),
    ):
        result = list(discover_providers())
    assert result == [fake_provider]


def test_discover_providers_returns_empty_when_none_registered() -> None:
    with patch("parsimony.plugins._entry_points", return_value=[]):
        assert list(discover_providers()) == []


def test_load_failure_emits_warning_not_exception() -> None:
    ep = _make_ep("broken", "no.such.module:Cls", PROVIDERS_GROUP)

    def raise_import_error(*_args: object, **_kwargs: object) -> object:
        raise ImportError("missing dep")

    with (
        patch("parsimony.plugins._entry_points", return_value=[ep]),
        patch.object(EntryPoint, "load", autospec=True, side_effect=raise_import_error),
        pytest.warns(RegistryWarning, match="Failed to load entry point"),
    ):
        result = list(discover_providers())
    assert result == []
