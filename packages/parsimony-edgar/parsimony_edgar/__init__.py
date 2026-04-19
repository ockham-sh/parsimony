"""SEC EDGAR connectors for parsimony.

Registered with parsimony core via the ``parsimony.providers`` entry point::

    [project.entry-points."parsimony.providers"]
    sec_edgar = "parsimony_edgar:PROVIDER"

Direct use::

    from parsimony_edgar import CONNECTORS, sec_edgar_find_company
"""

from __future__ import annotations

from parsimony.connectors import ProviderSpec

from parsimony_edgar._connectors import (
    CONNECTORS,
    sec_edgar_balance_sheet,
    sec_edgar_cashflow_statement,
    sec_edgar_company_facts,
    sec_edgar_company_profile,
    sec_edgar_filing_document,
    sec_edgar_filing_item,
    sec_edgar_filing_metadata,
    sec_edgar_filing_sections,
    sec_edgar_filing_table,
    sec_edgar_filing_tables,
    sec_edgar_filings,
    sec_edgar_find_company,
    sec_edgar_income_statement,
    sec_edgar_insider_trades,
    sec_edgar_search_filings,
)

__all__ = [
    "CONNECTORS",
    "PROVIDER",
    "sec_edgar_balance_sheet",
    "sec_edgar_cashflow_statement",
    "sec_edgar_company_facts",
    "sec_edgar_company_profile",
    "sec_edgar_filing_document",
    "sec_edgar_filing_item",
    "sec_edgar_filing_metadata",
    "sec_edgar_filing_sections",
    "sec_edgar_filing_table",
    "sec_edgar_filing_tables",
    "sec_edgar_filings",
    "sec_edgar_find_company",
    "sec_edgar_income_statement",
    "sec_edgar_insider_trades",
    "sec_edgar_search_filings",
]

PROVIDER = ProviderSpec(name="sec_edgar", connectors=CONNECTORS)
