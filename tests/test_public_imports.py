"""Smoke tests for documented top-level ``parsimony`` imports."""

from __future__ import annotations


def test_readme_import_surface() -> None:
    from parsimony import (
        Column,
        ColumnRole,
        Connectors,
        EntityRef,
        OutputSpec,
        Provenance,
        Result,
        connector,
        discover,
    )

    assert connector is not None
    assert Connectors is not None
    assert Result is not None
    assert discover is not None
    assert Column(name="x", role=ColumnRole.KEY, namespace="ns").role == ColumnRole.KEY
    assert OutputSpec is not None
    assert Provenance is not None
    assert EntityRef("ns", "code") == ("ns", "code")


def test_contract_error_imports() -> None:
    from parsimony import ConnectorError, UnauthorizedError

    assert issubclass(UnauthorizedError, ConnectorError)
