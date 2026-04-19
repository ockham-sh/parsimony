"""Shared pytest conformance suite for parsimony plugins.

Plugin authors verify their package satisfies the parsimony provider contract
by adding one file::

    # tests/test_conformance.py
    from parsimony_plugin_tests import ProviderTestSuite
    from parsimony_mybank import PROVIDER

    class TestMyBankProvider(ProviderTestSuite):
        provider = PROVIDER
        entry_point_name = "mybank"

Pytest's standard test discovery picks up the inherited methods. Every check
is structural (no network, no credentials), so the suite runs in CI for any
plugin without environment configuration.
"""

from __future__ import annotations

from parsimony_plugin_tests.provider import ProviderTestSuite

__all__ = ["ProviderTestSuite"]
