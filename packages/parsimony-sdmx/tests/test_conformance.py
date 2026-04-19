"""Run the parsimony provider conformance suite against parsimony-sdmx."""

from __future__ import annotations

from parsimony_plugin_tests import ProviderTestSuite
from parsimony_sdmx import PROVIDER


class TestSdmxProviderConformance(ProviderTestSuite):
    provider = PROVIDER
    entry_point_name = "sdmx"
