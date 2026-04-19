from __future__ import annotations

from parsimony_edgar import PROVIDER
from parsimony_plugin_tests import ProviderTestSuite


class TestEdgarProviderConformance(ProviderTestSuite):
    provider = PROVIDER
    entry_point_name = "sec_edgar"
