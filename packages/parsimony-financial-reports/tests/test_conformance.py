from __future__ import annotations

from parsimony_financial_reports import PROVIDER
from parsimony_plugin_tests import ProviderTestSuite


class TestFinancialReportsProviderConformance(ProviderTestSuite):
    provider = PROVIDER
    entry_point_name = "financial_reports"
