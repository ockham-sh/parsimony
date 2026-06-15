"""Conformance guard for flat public connector parameters."""

from __future__ import annotations

from types import ModuleType

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector
from parsimony.testing import ConformanceError, assert_plugin_valid


class _BundledParams(BaseModel):
    country: str


@connector()
def _bundled_macro(params: _BundledParams) -> pd.DataFrame:
    """Toy connector with bundled params for conformance testing."""
    return pd.DataFrame({"country": [params.country]})


@connector()
def _flat_macro(country: str) -> pd.DataFrame:
    """Toy connector with flat params for conformance testing."""
    _BundledParams(country=country)
    return pd.DataFrame({"country": [country]})


def _fake_module(connectors: Connectors) -> ModuleType:
    mod = ModuleType("fake_plugin")
    mod.CONNECTORS = connectors  # type: ignore[attr-defined]
    return mod


class TestFlatPublicParamsConformance:
    def test_rejects_bundled_params_connector(self) -> None:
        mod = _fake_module(Connectors([_bundled_macro]))
        with pytest.raises(ConformanceError, match="check_flat_public_params"):
            assert_plugin_valid(mod)

    def test_accepts_flat_connector(self) -> None:
        mod = _fake_module(Connectors([_flat_macro]))
        assert_plugin_valid(mod)

    def test_flat_call_records_flat_provenance(self) -> None:
        result = _flat_macro(country="USA")
        assert result.provenance.params == {"country": "USA"}
