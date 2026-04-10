"""SEC EDGAR source: company search, profiles, financials, filings, and documents via edgartools.

Provides 15 connectors covering the full SEC EDGAR surface:
company lookup, profiles, financial statements (income/balance/cashflow),
filing search and listing, company facts, filing document/metadata/sections/items/tables,
and insider trades.
"""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, Field

from ockham.connector import Connectors, connector
from ockham.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SEC_EDGAR_USER_AGENT = os.getenv("SEC_EDGAR_USER_AGENT", "YourApp your-email@example.com")


def _ensure_edgar_identity() -> None:
    from edgar.core import set_identity

    set_identity(os.getenv("EDGAR_IDENTITY", _SEC_EDGAR_USER_AGENT))


def _is_company_search_results(value: Any) -> bool:
    return (
        type(value).__name__ == "CompanySearchResults"
        and hasattr(value, "results")
        and hasattr(value, "__len__")
    )


def _resolve_company(identifier: str) -> Any:
    from edgar import find

    query = str(identifier).strip()
    if not query:
        raise ValueError("identifier is required")
    _ensure_edgar_identity()
    result = find(query)
    if result is None:
        raise ValueError(f"No SEC company found for '{query}'")
    return result


def _resolve_to_entity(identifier: str) -> Any:
    result = _resolve_company(identifier)
    if _is_company_search_results(result):
        if len(result) == 0:
            raise ValueError(f"No SEC company found for '{identifier}'")
        return result[0]
    return result


_DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _period_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if _DATE_COL_RE.match(str(c))]


def _sanitize_detailed_statement_df(df: pd.DataFrame) -> pd.DataFrame:
    periods = _period_columns(df)
    if not periods:
        return df
    out = df.copy()
    for col in periods:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _to_dataframe(value: Any) -> pd.DataFrame:
    if value is None:
        raise ValueError("No data returned")
    if isinstance(value, pd.DataFrame):
        return value
    if hasattr(value, "to_dataframe"):
        out = value.to_dataframe()
        return out if isinstance(out, pd.DataFrame) else pd.DataFrame(out)
    if hasattr(value, "to_pandas"):
        out = value.to_pandas()
        return out if isinstance(out, pd.DataFrame) else pd.DataFrame(out)
    if hasattr(value, "data") and isinstance(value.data, pd.DataFrame):
        return value.data
    raise ValueError("Unsupported output type")


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

FIND_COMPANY_OUTPUT = OutputConfig(
    columns=[
        Column(name="cik", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="ticker", role=ColumnRole.METADATA),
    ]
)

FILINGS_OUTPUT = OutputConfig(
    columns=[
        Column(name="accession_number", role=ColumnRole.KEY),
        Column(name="form", role=ColumnRole.TITLE),
        Column(name="filing_date", role=ColumnRole.DATA),
        Column(name="company", mapped_name="company_name", role=ColumnRole.METADATA),
    ]
)

SEARCH_FILINGS_OUTPUT = OutputConfig(
    columns=[
        Column(name="accession", role=ColumnRole.KEY),
        Column(name="form", role=ColumnRole.TITLE),
        Column(name="filed", role=ColumnRole.DATA),
        Column(name="company", role=ColumnRole.METADATA),
        Column(name="cik", role=ColumnRole.METADATA),
    ]
)


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class SecEdgarFindCompanyParams(BaseModel):
    """Parameters for searching SEC companies by name, ticker, or CIK."""

    identifier: str = Field(
        ..., description="Company name, ticker symbol, or CIK number to search for"
    )


class SecEdgarCompanyProfileParams(BaseModel):
    """Parameters for retrieving a company profile from SEC EDGAR."""

    identifier: str = Field(
        ..., description="Company name, ticker symbol, or CIK number"
    )


class SecEdgarFinancialStatementParams(BaseModel):
    """Parameters for retrieving financial statements from SEC 10-K/10-Q XBRL data."""

    identifier: str = Field(
        ..., description="Company name, ticker symbol, or CIK number"
    )
    periods: int = Field(
        default=4, ge=1, le=20, description="Number of filing periods to include (default 4)"
    )
    annual: bool = Field(
        default=True,
        description="True for annual (10-K only), False to include quarterly (10-K + 10-Q)",
    )
    view: Literal["summary", "detailed"] = Field(
        default="summary",
        description="'summary' for multi-period comparison, 'detailed' for single latest filing with full line items",
    )


class SecEdgarSearchFilingsParams(BaseModel):
    """Parameters for full-text search across all SEC filings."""

    query: str = Field(..., description="Full-text search query (e.g. 'artificial intelligence risk')")
    forms: list[str] | None = Field(
        default=None, description="Filter by form types (e.g. ['10-K', '8-K'])"
    )
    start_date: str | None = Field(
        default=None, description="Start date for filing range (YYYY-MM-DD)"
    )
    end_date: str | None = Field(
        default=None, description="End date for filing range (YYYY-MM-DD)"
    )
    limit: int = Field(default=20, ge=1, le=100, description="Maximum results to return")


class SecEdgarFilingsParams(BaseModel):
    """Parameters for listing filings from SEC EDGAR."""

    identifier: str | None = Field(
        default=None,
        description="Company name, ticker, or CIK. If omitted, returns recent filings across all companies.",
    )
    form: str | None = Field(
        default=None, description="Filter by form type (e.g. '10-K', '8-K', '10-Q')"
    )
    filing_date: str | None = Field(
        default=None, description="Filter by filing date or date range (YYYY-MM-DD or YYYY-MM-DD:YYYY-MM-DD)"
    )
    limit: int = Field(default=20, ge=1, le=100, description="Maximum results to return")


class SecEdgarCompanyFactsParams(BaseModel):
    """Parameters for retrieving XBRL company facts from SEC EDGAR."""

    identifier: str = Field(
        ..., description="Company name, ticker symbol, or CIK number"
    )


class SecEdgarFilingDocumentParams(BaseModel):
    """Parameters for retrieving a filing's full content as markdown."""

    accession_number: str = Field(
        ..., description="Filing accession number (e.g. '0001140361-26-006577')"
    )


class SecEdgarFilingMetadataParams(BaseModel):
    """Parameters for retrieving structured metadata from a parsed SEC filing."""

    accession_number: str = Field(
        ..., description="Filing accession number (e.g. '0001140361-26-006577')"
    )


class SecEdgarFilingSectionsParams(BaseModel):
    """Parameters for listing the sections/items (table of contents) of a SEC filing."""

    accession_number: str = Field(
        ..., description="Filing accession number (e.g. '0001140361-26-006577')"
    )


class SecEdgarFilingItemParams(BaseModel):
    """Parameters for retrieving a specific section/item from a SEC filing."""

    accession_number: str = Field(
        ..., description="Filing accession number (e.g. '0001140361-26-006577')"
    )
    item: str = Field(
        ...,
        description=(
            "Item identifier. Supports multiple formats: "
            "'1A' or 'Item 1A' (10-K Risk Factors), "
            "'risk_factors' or 'mda' (friendly names), "
            "'2.02' (8-K items), "
            "'Part I, Item 1' (part-qualified for 10-Q)"
        ),
    )


class SecEdgarFilingTablesParams(BaseModel):
    """Parameters for listing all tables in a SEC filing."""

    accession_number: str = Field(
        ..., description="Filing accession number (e.g. '0001140361-26-006577')"
    )
    item: str | None = Field(
        default=None,
        description="Optional item identifier to scope tables to a specific section (e.g. '1A', '8')",
    )


class SecEdgarFilingTableParams(BaseModel):
    """Parameters for retrieving a specific table from a SEC filing as a DataFrame."""

    accession_number: str = Field(
        ..., description="Filing accession number (e.g. '0001140361-26-006577')"
    )
    table_index: int = Field(
        ..., ge=0, description="Zero-based table index from sec_edgar_filing_tables results"
    )
    item: str | None = Field(
        default=None,
        description="Optional item identifier — must match the item used in sec_edgar_filing_tables",
    )


class SecEdgarInsiderTradesParams(BaseModel):
    """Parameters for retrieving structured insider trades (Form 4) from SEC EDGAR."""

    identifier: str = Field(
        ..., description="Company name, ticker symbol, or CIK number"
    )
    start_date: str | None = Field(
        default=None, description="Start date for filing range (YYYY-MM-DD)"
    )
    end_date: str | None = Field(
        default=None, description="End date for filing range (YYYY-MM-DD)"
    )
    limit: int = Field(
        default=10, ge=1, le=50, description="Maximum number of Form 4 filings to process"
    )


# ---------------------------------------------------------------------------
# Statement helper (shared by 3 financial statement connectors)
# ---------------------------------------------------------------------------


def _statement_from_single_filing(
    *,
    identifier: str,
    statement_type: Literal["income_statement", "balance_sheet", "cashflow_statement"],
    annual: bool,
    view: str,
) -> pd.DataFrame:
    entity = _resolve_to_entity(identifier)
    forms = ["10-K"] if annual else ["10-K", "10-Q"]
    filings = entity.get_filings(form=forms)
    if filings is None or len(filings) == 0:
        raise ValueError("No filings found")
    filing = filings[0]
    xbrl = filing.xbrl()
    if xbrl is None:
        raise ValueError("No XBRL data found")
    stmt_getter = {
        "income_statement": xbrl.statements.income_statement,
        "balance_sheet": xbrl.statements.balance_sheet,
        "cashflow_statement": xbrl.statements.cash_flow_statement,
    }[statement_type]
    statement = stmt_getter()
    df = _to_dataframe(statement.to_dataframe(view=view))
    return _sanitize_detailed_statement_df(df)


def _statement_from_multi_filings(
    *,
    identifier: str,
    statement_type: Literal["income_statement", "balance_sheet", "cashflow_statement"],
    periods: int,
    annual: bool,
    view: str,
) -> pd.DataFrame:
    from edgar import MultiFinancials

    entity = _resolve_to_entity(identifier)
    forms = ["10-K"] if annual else ["10-K", "10-Q"]
    filings = entity.get_filings(form=forms)
    if filings is None or len(filings) == 0:
        raise ValueError("No filings found")
    selected = filings.head(max(int(periods), 1))
    multi = MultiFinancials.extract(selected)
    method = {
        "income_statement": multi.income_statement,
        "balance_sheet": multi.balance_sheet,
        "cashflow_statement": multi.cashflow_statement,
    }[statement_type]
    df = _to_dataframe(method(view=view))
    return _sanitize_detailed_statement_df(df)


async def _fetch_statement(
    statement_type: Literal["income_statement", "balance_sheet", "cashflow_statement"],
    params: SecEdgarFinancialStatementParams,
) -> pd.DataFrame:
    if params.view == "detailed":
        return await asyncio.to_thread(
            _statement_from_single_filing,
            identifier=params.identifier,
            statement_type=statement_type,
            annual=params.annual,
            view=params.view,
        )
    return await asyncio.to_thread(
        _statement_from_multi_filings,
        identifier=params.identifier,
        statement_type=statement_type,
        periods=params.periods,
        annual=params.annual,
        view=params.view,
    )


# ---------------------------------------------------------------------------
# Filing retrieval helper
# ---------------------------------------------------------------------------


def _get_filing_by_accession(accession_number: str) -> Any:
    from edgar import get_by_accession_number

    accession = str(accession_number).strip()
    if not accession:
        raise ValueError("accession_number is required")
    _ensure_edgar_identity()
    filing = get_by_accession_number(accession)
    if filing is None:
        raise ValueError(f"Filing not found: {accession}")
    return filing


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=FIND_COMPANY_OUTPUT, tags=["sec_edgar", "tool"])
async def sec_edgar_find_company(params: SecEdgarFindCompanyParams) -> Result:
    """Search SEC EDGAR for companies by name, ticker symbol, or CIK number.

    Returns matching companies with CIK, name, and ticker.
    Use the ticker or CIK in other sec_edgar_* connectors to get filings and financials.
    """
    result = await asyncio.to_thread(_resolve_company, params.identifier)
    if _is_company_search_results(result):
        df = result.results.copy()
        if "company" in df.columns and "name" not in df.columns:
            df = df.rename(columns={"company": "name"})
        if df.empty:
            raise ValueError(f"No companies found for '{params.identifier}'")
    else:
        entity = result
        tickers = getattr(entity, "tickers", None) or []
        df = pd.DataFrame([{
            "ticker": tickers[0] if tickers else "",
            "cik": str(getattr(entity, "cik", "")).zfill(10),
            "name": getattr(entity, "name", ""),
        }])
    return FIND_COMPANY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar", "tool"])
async def sec_edgar_company_profile(params: SecEdgarCompanyProfileParams) -> Result:
    """Retrieve a company's SEC EDGAR profile: name, CIK, ticker, industry, SIC code, and fiscal year end.

    Use this to get structured company information before fetching financials or filings.
    """
    entity = await asyncio.to_thread(_resolve_to_entity, params.identifier)
    tickers = getattr(entity, "tickers", None) or []
    df = pd.DataFrame([{
        "name": getattr(entity, "name", ""),
        "cik": str(getattr(entity, "cik", "")).zfill(10),
        "ticker": tickers[0] if tickers else "",
        "industry": getattr(entity, "industry", ""),
        "sic": getattr(entity, "sic", ""),
        "fiscal_year_end": getattr(entity, "fiscal_year_end", ""),
    }])
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_income_statement(params: SecEdgarFinancialStatementParams) -> Result:
    """Retrieve income statement data from SEC 10-K/10-Q XBRL filings.

    Returns revenue, expenses, net income, and other P&L line items.
    Use view='summary' for multi-period comparison or view='detailed' for full line items from the latest filing.
    """
    df = await _fetch_statement("income_statement", params)
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_balance_sheet(params: SecEdgarFinancialStatementParams) -> Result:
    """Retrieve balance sheet data from SEC 10-K/10-Q XBRL filings.

    Returns assets, liabilities, equity, and other balance sheet line items.
    Use view='summary' for multi-period comparison or view='detailed' for full line items from the latest filing.
    """
    df = await _fetch_statement("balance_sheet", params)
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_cashflow_statement(params: SecEdgarFinancialStatementParams) -> Result:
    """Retrieve cash flow statement data from SEC 10-K/10-Q XBRL filings.

    Returns operating, investing, and financing cash flows.
    Use view='summary' for multi-period comparison or view='detailed' for full line items from the latest filing.
    """
    df = await _fetch_statement("cashflow_statement", params)
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(output=SEARCH_FILINGS_OUTPUT, tags=["sec_edgar", "tool"])
async def sec_edgar_search_filings(params: SecEdgarSearchFilingsParams) -> Result:
    """Full-text search across all SEC EDGAR filings.

    Search for specific topics, risk factors, or disclosures across all public companies.
    Returns form type, company, filing date, accession number, and CIK.
    """
    from edgar import search_filings

    _ensure_edgar_identity()
    results = await asyncio.to_thread(
        search_filings,
        params.query,
        forms=params.forms,
        start_date=params.start_date,
        end_date=params.end_date,
        limit=params.limit,
    )
    rows = [
        {
            "form": r.form,
            "company": r.company,
            "filed": r.filed,
            "accession": r.accession_number,
            "cik": r.cik,
        }
        for r in getattr(results, "results", [])
    ]
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(columns=["form", "company", "filed", "accession", "cik"])
    )
    if df.empty:
        raise ValueError(f"No filings found for search: '{params.query}'")
    return SEARCH_FILINGS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(output=FILINGS_OUTPUT, tags=["sec_edgar"])
async def sec_edgar_filings(params: SecEdgarFilingsParams) -> Result:
    """List filings from SEC EDGAR for a specific company or across all companies.

    Returns filing metadata: form type, filing date, accession number.
    Use the accession number with sec_edgar_filing_document or sec_edgar_filing_metadata.
    """
    from edgar import get_filings

    _ensure_edgar_identity()
    if params.identifier:
        entity = await asyncio.to_thread(_resolve_to_entity, params.identifier)
        filings = entity.get_filings(form=params.form, filing_date=params.filing_date)
    else:
        filings = await asyncio.to_thread(
            get_filings, form=params.form, filing_date=params.filing_date
        )
    if filings is None:
        raise ValueError("No filings found")
    df = await asyncio.to_thread(filings.head(params.limit).to_pandas)
    if "accession_no" in df.columns and "accession_number" not in df.columns:
        df = df.rename(columns={"accession_no": "accession_number"})
    if df.empty:
        raise ValueError("No filings found")
    return FILINGS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_company_facts(params: SecEdgarCompanyFactsParams) -> Result:
    """Retrieve all XBRL company facts from SEC EDGAR for a given company.

    Returns a comprehensive table of all reported financial data points (revenue, assets, shares, etc.)
    across all filing periods. Useful for building custom financial time series.
    """
    entity = await asyncio.to_thread(_resolve_to_entity, params.identifier)
    facts = await asyncio.to_thread(entity.get_facts)
    if facts is None:
        raise ValueError(f"No company facts found for '{params.identifier}'")
    df = await asyncio.to_thread(_to_dataframe, facts)
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"], result_type="text")
async def sec_edgar_filing_document(params: SecEdgarFilingDocumentParams) -> Result:
    """Retrieve a filing's full content as markdown text by accession number.

    Returns the filing document converted to markdown for reading and analysis.
    Use accession numbers from sec_edgar_filings or sec_edgar_search_filings results.
    """
    filing = await asyncio.to_thread(_get_filing_by_accession, params.accession_number)
    markdown = await asyncio.to_thread(filing.markdown)
    content = str(markdown or "").strip()
    if not content:
        raise ValueError(f"No content available for filing {params.accession_number}")
    return Result(
        data=content,
        provenance=Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"], result_type="text")
async def sec_edgar_filing_metadata(params: SecEdgarFilingMetadataParams) -> Result:
    """Retrieve AI-optimized metadata summary of a SEC filing by accession number.

    Returns structured metadata (markdown-KV format) covering all form-specific fields:
    company, CIK, form type, filing date, items, sections, and more depending on the
    filing type (8-K, 10-K, 10-Q, 13F, etc.). Use for quick inspection before reading
    the full document with sec_edgar_filing_document.
    """
    filing = await asyncio.to_thread(_get_filing_by_accession, params.accession_number)

    # Try obj().to_context() first — form-specific, richest metadata
    try:
        obj = await asyncio.to_thread(filing.obj)
        if obj is not None and hasattr(obj, "to_context"):
            content = await asyncio.to_thread(obj.to_context, detail="full")
            if content and content.strip():
                return Result(
                    data=content,
                    provenance=Provenance(source="sec_edgar", params=params.model_dump()),
                )
    except Exception:
        pass

    # Fall back to filing-level to_context()
    content = await asyncio.to_thread(filing.to_context, detail="full")
    if not content or not content.strip():
        raise ValueError(f"No metadata available for filing {params.accession_number}")
    return Result(
        data=content,
        provenance=Provenance(source="sec_edgar", params=params.model_dump()),
    )



@connector(tags=["sec_edgar"])
async def sec_edgar_filing_sections(params: SecEdgarFilingSectionsParams) -> Result:
    """List the sections/items (table of contents) of a SEC filing by accession number.

    Returns available section identifiers and titles. Use the item identifiers with
    sec_edgar_filing_item to fetch specific section text, or with sec_edgar_filing_tables
    to scope table extraction to a section.
    """
    filing = await asyncio.to_thread(_get_filing_by_accession, params.accession_number)
    obj = await asyncio.to_thread(_get_filing_obj, filing)

    items = getattr(obj, "items", None)
    if items is None or (isinstance(items, (list, tuple)) and len(items) == 0):
        raise ValueError(f"No sections found for filing {params.accession_number}")

    # Build a DataFrame from the items list
    if isinstance(items, (list, tuple)):
        rows = [{"item": str(i)} for i in items]
    else:
        rows = [{"item": str(items)}]

    # Try to get section details (title, confidence) if available
    sections = getattr(obj, "sections", None)
    if sections is not None and hasattr(sections, "items"):
        enriched_rows = []
        try:
            for key, section in sections.items():
                row: dict[str, Any] = {"item": str(key)}
                if hasattr(section, "title"):
                    row["title"] = str(section.title) if section.title else ""
                if hasattr(section, "confidence"):
                    row["confidence"] = section.confidence
                if hasattr(section, "detection_method"):
                    row["detection_method"] = str(section.detection_method)
                enriched_rows.append(row)
            if enriched_rows:
                rows = enriched_rows
        except Exception:
            pass  # Fall back to simple items list

    df = pd.DataFrame(rows)
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"], result_type="text")
async def sec_edgar_filing_item(params: SecEdgarFilingItemParams) -> Result:
    """Retrieve a specific section/item from a SEC filing as text.

    Supports flexible item lookup: '1A' or 'Item 1A' (10-K Risk Factors),
    'risk_factors' or 'mda' (friendly names), '2.02' (8-K items),
    'Part I, Item 1' (part-qualified for 10-Q). Use sec_edgar_filing_sections
    to discover available items first.
    """
    filing = await asyncio.to_thread(_get_filing_by_accession, params.accession_number)
    obj = await asyncio.to_thread(_get_filing_obj, filing)

    content = obj[params.item]
    if content is None or (isinstance(content, str) and not content.strip()):
        raise ValueError(
            f"Item '{params.item}' not found or empty in filing {params.accession_number}"
        )

    text = str(content).strip()
    return Result(
        data=text,
        provenance=Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_filing_tables(params: SecEdgarFilingTablesParams) -> Result:
    """List all tables in a SEC filing with their caption, type, and size.

    Returns a summary DataFrame with table_index, caption, is_financial, row_count,
    and col_count. Use table_index with sec_edgar_filing_table to fetch a specific
    table as a DataFrame. Optionally scope to a specific section with the item parameter.
    """
    filing = await asyncio.to_thread(_get_filing_by_accession, params.accession_number)
    tables = await asyncio.to_thread(_get_tables_from_filing, filing, params.item)

    if not tables:
        raise ValueError(
            f"No tables found in filing {params.accession_number}"
            + (f" section '{params.item}'" if params.item else "")
        )

    rows = []
    for i, table in enumerate(tables):
        rows.append({
            "table_index": i,
            "caption": getattr(table, "caption", None) or "",
            "is_financial": getattr(table, "is_financial_table", False),
            "row_count": getattr(table, "row_count", 0),
            "col_count": getattr(table, "col_count", 0),
        })

    df = pd.DataFrame(rows)
    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_filing_table(params: SecEdgarFilingTableParams) -> Result:
    """Retrieve a specific table from a SEC filing as a DataFrame.

    Use sec_edgar_filing_tables first to list available tables and find the table_index.
    Returns the table with proper column headers, numeric type handling, and
    colspan/rowspan resolution.
    """
    filing = await asyncio.to_thread(_get_filing_by_accession, params.accession_number)
    tables = await asyncio.to_thread(_get_tables_from_filing, filing, params.item)

    if not tables:
        raise ValueError(f"No tables found in filing {params.accession_number}")
    if params.table_index >= len(tables):
        raise ValueError(
            f"table_index {params.table_index} out of range (filing has {len(tables)} tables)"
        )

    table = tables[params.table_index]
    df = await asyncio.to_thread(table.to_dataframe)
    if df.empty:
        raise ValueError(
            f"Table {params.table_index} could not be converted to a DataFrame "
            "(may contain only headers or unsupported structure). Try a different table_index."
        )

    # Flatten MultiIndex columns to prevent downstream serialization errors
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" | ".join(str(c) for c in col if str(c).strip()) for col in df.columns]

    # Deduplicate column names (e.g., "2025", "2025" → "2025", "2025_2")
    seen: dict[str, int] = {}
    new_cols: list[str] = []
    for col in df.columns:
        col_str = str(col)
        if col_str in seen:
            seen[col_str] += 1
            new_cols.append(f"{col_str}_{seen[col_str]}")
        else:
            seen[col_str] = 1
            new_cols.append(col_str)
    if new_cols != list(df.columns):
        df.columns = new_cols

    # Coerce mixed-type columns (str + float) to strings to prevent Arrow errors
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if len(sample) > 0:
                types = set(type(v).__name__ for v in sample)
                if len(types) > 1:
                    df[col] = df[col].apply(lambda x: str(x) if x is not None else "")

    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


@connector(tags=["sec_edgar"])
async def sec_edgar_insider_trades(params: SecEdgarInsiderTradesParams) -> Result:
    """Retrieve structured insider trades (Form 4) for a company from SEC EDGAR.

    Returns a DataFrame with transaction details: owner name, relationship,
    transaction type (Purchase/Sale/Grant/etc.), date, shares, price, and
    shares owned after transaction. Extracts structured XML data from Form 4 filings.
    """
    entity = await asyncio.to_thread(_resolve_to_entity, params.identifier)

    # Get Form 4 filings with optional date filtering
    filing_date = None
    if params.start_date and params.end_date:
        filing_date = f"{params.start_date}:{params.end_date}"
    elif params.start_date:
        filing_date = f"{params.start_date}:"
    elif params.end_date:
        filing_date = f":{params.end_date}"

    filings = entity.get_filings(form="4", filing_date=filing_date)
    if filings is None or len(filings) == 0:
        raise ValueError(f"No Form 4 filings found for '{params.identifier}'")

    selected = filings.head(params.limit)
    all_dfs: list[pd.DataFrame] = []

    def _extract_form4_data() -> None:
        for i in range(len(selected)):
            try:
                filing = selected.get_filing_at(i)
                form4 = filing.obj()
                if form4 is None:
                    continue

                owner_name = str(getattr(form4, "insider_name", ""))
                position = str(getattr(form4, "position", ""))
                filing_date = str(getattr(filing, "filing_date", ""))
                accession = str(getattr(filing, "accession_number", ""))

                # Collect non-derivative transactions from the Form4 DataFrames
                for source_attr in ("common_stock_sales", "common_stock_purchases", "option_exercises"):
                    source_df = getattr(form4, source_attr, None)
                    if source_df is None or not isinstance(source_df, pd.DataFrame) or source_df.empty:
                        continue
                    df = source_df.copy()
                    df["owner"] = owner_name
                    df["position"] = position
                    df["filing_date"] = filing_date
                    df["accession_number"] = accession
                    all_dfs.append(df)
            except Exception:
                continue  # Skip filings that fail to parse

    await asyncio.to_thread(_extract_form4_data)

    if not all_dfs:
        raise ValueError(f"No insider transactions extracted for '{params.identifier}'")

    df = pd.concat(all_dfs, ignore_index=True)

    # Select and reorder columns for clarity
    keep_cols = [
        "owner", "position", "TransactionType", "Date", "Security",
        "Shares", "Price", "Remaining", "AcquiredDisposed", "Code",
        "filing_date", "accession_number",
    ]
    available = [c for c in keep_cols if c in df.columns]
    df = df[available]

    return Result.from_dataframe(
        df,
        Provenance(source="sec_edgar", params=params.model_dump()),
    )


# ---------------------------------------------------------------------------
# Filing content helpers
# ---------------------------------------------------------------------------


def _get_filing_obj(filing: Any) -> Any:
    """Get the parsed form object from a filing. Raises ValueError if unavailable."""
    obj = filing.obj()
    if obj is None:
        raise ValueError("This filing type does not support structured parsing")
    return obj


def _get_tables_from_filing(filing: Any, item: str | None) -> list[Any]:
    """Get TableNode list from a filing, optionally scoped to a section."""
    obj = _get_filing_obj(filing)
    if item is not None:
        sections = getattr(obj, "sections", None)
        if sections is None:
            raise ValueError("This filing does not have sections")
        section = sections.get_item(item) if hasattr(sections, "get_item") else None
        if section is None:
            raise ValueError(f"Section '{item}' not found in this filing")
        # section.tables() returns list of TableNode for section-scoped access
        tables_method = getattr(section, "tables", None)
        if tables_method is None:
            raise ValueError("Section does not support table extraction")
        return tables_method() if callable(tables_method) else tables_method
    # Document-level tables
    doc = getattr(obj, "document", None)
    if doc is None:
        raise ValueError("This filing does not have a parsed document")
    tables = getattr(doc, "tables", None)
    if tables is None:
        return []
    return tables if isinstance(tables, list) else list(tables)

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

CONNECTORS = Connectors([
    sec_edgar_find_company,
    sec_edgar_company_profile,
    sec_edgar_income_statement,
    sec_edgar_balance_sheet,
    sec_edgar_cashflow_statement,
    sec_edgar_search_filings,
    sec_edgar_filings,
    sec_edgar_company_facts,
    sec_edgar_filing_document,
    sec_edgar_filing_metadata,
    sec_edgar_filing_sections,
    sec_edgar_filing_item,
    sec_edgar_filing_tables,
    sec_edgar_filing_table,
    sec_edgar_insider_trades,
])
