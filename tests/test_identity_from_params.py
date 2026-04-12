"""Tests for Namespace-based identity extraction from connector params."""

from __future__ import annotations

from typing import Annotated

import pytest
from pydantic import BaseModel

from parsimony.catalog.identity_from_params import first_namespace_field, identity_from_params
from parsimony.connector import Namespace
from parsimony.connectors.fred import FredFetchParams


class NoNsParams(BaseModel):
    x: str = "a"


class MultiNsParams(BaseModel):
    """Multiple identity annotations are rejected."""

    period: str = "annual"
    symbol: Annotated[str, Namespace("fmp_symbols")]
    series_id: Annotated[str, Namespace("fred")]


def test_first_namespace_field_fred_fetch() -> None:
    assert first_namespace_field(FredFetchParams) == ("series_id", "fred")


def test_first_namespace_field_multi() -> None:
    with pytest.raises(ValueError, match="multiple Namespace-annotated fields"):
        first_namespace_field(MultiNsParams)


def test_first_namespace_field_none() -> None:
    assert first_namespace_field(NoNsParams) is None


def test_identity_from_params_extracts() -> None:
    p = FredFetchParams(series_id="GDPC1")
    assert identity_from_params(FredFetchParams, p) == ("fred", "GDPC1")


def test_identity_from_params_empty_series_id() -> None:
    class Loose(BaseModel):
        series_id: Annotated[str, Namespace("fred")] = ""

    p = Loose(series_id="   ")
    assert identity_from_params(Loose, p) is None


def test_identity_from_params_no_annotation_model() -> None:
    p = NoNsParams()
    assert identity_from_params(NoNsParams, p) is None
