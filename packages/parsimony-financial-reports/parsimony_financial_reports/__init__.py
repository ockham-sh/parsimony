"""FinancialReports connectors for parsimony.

Registered with parsimony core via the ``parsimony.providers`` entry point::

    [project.entry-points."parsimony.providers"]
    financial_reports = "parsimony_financial_reports:PROVIDER"
"""

from __future__ import annotations

from parsimony.connectors import ProviderSpec

from parsimony_financial_reports._connectors import (
    CONNECTORS,
    ENV_VARS,
    financial_reports_companies_search,
    financial_reports_company_retrieve,
    financial_reports_filing_history,
    financial_reports_filing_markdown,
    financial_reports_filing_retrieve,
    financial_reports_filings_search,
    financial_reports_isic_browse,
    financial_reports_isin_lookup,
    financial_reports_next_annual_report,
    financial_reports_reference_data,
)

__all__ = [
    "CONNECTORS",
    "ENV_VARS",
    "PROVIDER",
    "financial_reports_companies_search",
    "financial_reports_company_retrieve",
    "financial_reports_filing_history",
    "financial_reports_filing_markdown",
    "financial_reports_filing_retrieve",
    "financial_reports_filings_search",
    "financial_reports_isic_browse",
    "financial_reports_isin_lookup",
    "financial_reports_next_annual_report",
    "financial_reports_reference_data",
]

PROVIDER = ProviderSpec(name="financial_reports", connectors=CONNECTORS, env_vars=ENV_VARS)
