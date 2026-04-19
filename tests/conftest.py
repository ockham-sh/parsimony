"""Shared test fixtures for parsimony test suite."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_plugin_discovery_cache() -> None:
    """Clear the plugin discovery cache before every test.

    Monkeypatched entry-points in some tests can otherwise leak cached
    mock results into subsequent tests that expect the real registry.
    """
    from parsimony.discovery import _scan

    _scan._clear_cache()
