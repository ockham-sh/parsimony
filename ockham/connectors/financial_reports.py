"""Financial Reports source: company filings and metadata from financialreports.eu.

Provides connectors covering the full Financial Reports API surface:
companies, filings (metadata + markdown content), reference data
(filing types, categories, languages, countries, sources), ISIC industry
classifications, ISIN lookup, filing history, and next-annual-report predictions.
"""

from __future__ import annotations

import asyncio
import json
import logging
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sdk_client(api_key: str) -> Any:
    """Return the async-context-manager SDK client class, bound to a key.

    Works around a SyntaxError in financial-reports-generated-client<=1.4.21
    where webhooks_management_api.py has a broken type annotation. We stub
    out the broken module in sys.modules before importing — we never use
    webhooks anyway. The stub is a no-op if the SDK is eventually fixed.
    """
    import sys
    import types

    # Pre-load a stub so the broken .py file is never parsed.
    _stub_key = "financial_reports_generated_client.api.webhooks_management_api"
    if _stub_key not in sys.modules:
        stub = types.ModuleType(_stub_key)

        class _WebhooksStub:
            """Stand-in for the broken WebhooksManagementApi."""

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

        stub.WebhooksManagementApi = _WebhooksStub  # type: ignore[attr-defined]
        sys.modules[_stub_key] = stub

    try:
        from financial_reports_generated_client import FinancialReports
    except ImportError as e:
        raise ImportError(
            "financial-reports-generated-client is required. "
            "Install with: pip install financial-reports-generated-client"
        ) from e
    return FinancialReports(api_key=api_key)


_MAX_BURST_RETRIES = 3


def _parse_retry_after(exc: Exception) -> float:
    """Extract retry_after_seconds from a 429 ApiException body."""
    body = getattr(exc, "body", None) or ""
    if isinstance(body, (str, bytes)):
        try:
            body_str = body if isinstance(body, str) else body.decode()
            parsed = json.loads(body_str)
            return float(parsed.get("retry_after_seconds", 2.0))
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
    return 2.0


async def _with_retry(coro_factory: Any, api_key: str) -> Any:
    """Execute an async SDK operation with retry on burst 429 errors.

    ``coro_factory`` receives the SDK client and returns an awaitable.
    Burst-limit 429s (retry_after < 60s) are retried up to 3 times.
    Quota-limit 429s raise immediately with a clear message.
    """
    for attempt in range(_MAX_BURST_RETRIES + 1):
        try:
            async with _sdk_client(api_key) as client:
                return await coro_factory(client)
        except Exception as exc:
            if getattr(exc, "status", None) != 429:
                raise
            retry_after = _parse_retry_after(exc)
            if retry_after > 60:
                raise ValueError(
                    "Financial Reports API quota exhausted for the current billing period. "
                    "Upgrade your plan or wait for the next billing cycle."
                ) from exc
            if attempt < _MAX_BURST_RETRIES:
                wait = max(retry_after, 1.0) * (2 ** attempt)
                logger.info("FR API burst limit hit, retrying in %.1fs (attempt %d/%d)", wait, attempt + 1, _MAX_BURST_RETRIES)
                await asyncio.sleep(wait)
            else:
                raise ValueError(
                    f"Financial Reports API rate limit exceeded after {_MAX_BURST_RETRIES} retries. "
                    "Try again in a few seconds."
                ) from exc
    raise RuntimeError("Unreachable")  # pragma: no cover


def _to_dataframe(resp: Any) -> pd.DataFrame:
    """Convert an SDK response (paginated or single object) to a flat DataFrame.

    Uses ``model_dump(mode="json")`` so datetimes become ISO strings (avoids
    pydantic ``TzInfo`` objects that lack ``.zone``).  After ``json_normalize``,
    any remaining unhashable cells (nested dicts/lists at depth > max_level)
    are serialized as JSON strings so they remain parseable downstream.
    """
    raw = resp.model_dump(mode="json") if hasattr(resp, "model_dump") else resp
    if isinstance(raw, dict) and "results" in raw:
        records = raw["results"]
    elif isinstance(raw, list):
        records = raw
    elif isinstance(raw, dict):
        records = [raw]
    else:
        return pd.DataFrame()
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records, max_level=2)
    # Serialize remaining unhashable cells as JSON (preserves structure)
    for col in df.columns:
        sample = df[col].dropna()
        if len(sample) == 0:
            continue
        first = sample.iloc[0]
        if isinstance(first, (list, tuple, dict)):
            df[col] = df[col].apply(
                lambda x: json.dumps(x, default=str)
                if isinstance(x, (list, tuple, dict))
                else x
            )
    return df


def _strip_none(params: dict[str, Any]) -> dict[str, Any]:
    """Remove None values from a dict for use as SDK kwargs."""
    return {k: v for k, v in params.items() if v is not None}


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

COMPANIES_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="country_code", role=ColumnRole.METADATA),
        Column(name="lei", role=ColumnRole.METADATA),
        Column(name="sub_industry_code", role=ColumnRole.METADATA),
    ]
)

COMPANY_RETRIEVE_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="lei", role=ColumnRole.METADATA),
        Column(name="country_code", role=ColumnRole.METADATA),
        Column(name="ticker", role=ColumnRole.METADATA),
        Column(name="isins", role=ColumnRole.METADATA),
        Column(name="headcount", role=ColumnRole.METADATA),
        Column(name="sector.name", mapped_name="sector", role=ColumnRole.METADATA),
        Column(name="sub_industry.name", mapped_name="sub_industry", role=ColumnRole.METADATA),
        Column(name="ir_link", role=ColumnRole.METADATA),
        Column(name="homepage_link", role=ColumnRole.METADATA),
        Column(name="description", role=ColumnRole.DATA),
    ]
)

FILINGS_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="release_datetime", role=ColumnRole.DATA),
        Column(name="company.name", mapped_name="company_name", role=ColumnRole.METADATA),
        Column(name="filing_type.code", mapped_name="filing_type", role=ColumnRole.METADATA),
        Column(name="file_extension", role=ColumnRole.METADATA),
    ]
)

FILING_RETRIEVE_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="release_datetime", role=ColumnRole.DATA),
        Column(name="company.name", mapped_name="company_name", role=ColumnRole.METADATA),
        Column(name="filing_type.code", mapped_name="filing_type", role=ColumnRole.METADATA),
        Column(name="fiscal_year", role=ColumnRole.METADATA),
        Column(name="fiscal_period", role=ColumnRole.METADATA),
        Column(name="period_ending_date", role=ColumnRole.METADATA),
        Column(name="language.name", mapped_name="language", role=ColumnRole.METADATA),
        Column(name="source.name", mapped_name="source", role=ColumnRole.METADATA),
        Column(name="document", role=ColumnRole.METADATA),
        Column(name="markdown_url", role=ColumnRole.METADATA),
        Column(name="filing_type_confidence", role=ColumnRole.METADATA),
    ]
)

FILING_HISTORY_OUTPUT = OutputConfig(
    columns=[
        Column(name="history_id", role=ColumnRole.KEY),
        Column(name="history_date", role=ColumnRole.TITLE),
        Column(name="history_type", role=ColumnRole.METADATA),
        Column(name="changes", role=ColumnRole.DATA),
    ]
)

ISIC_BROWSE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
    ]
)

ISIN_LOOKUP_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="is_primary", role=ColumnRole.METADATA),
        Column(name="company.name", mapped_name="company_name", role=ColumnRole.TITLE),
        Column(name="figi", role=ColumnRole.METADATA),
        Column(name="security_type", role=ColumnRole.METADATA),
        Column(name="exch_code", role=ColumnRole.METADATA),
    ]
)

REFERENCE_FILING_TYPES_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="code", role=ColumnRole.TITLE),
        Column(name="name", role=ColumnRole.METADATA),
        Column(name="description", role=ColumnRole.METADATA),
    ]
)

REFERENCE_COUNTRIES_OUTPUT = OutputConfig(
    columns=[
        Column(name="alpha_2", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
    ]
)

REFERENCE_GENERIC_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
    ]
)


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class FrCompaniesSearchParams(BaseModel):
    """Parameters for searching/listing companies on Financial Reports."""

    search: str | None = Field(default=None, description="Full-text search query (company name)")
    countries: str | None = Field(
        default=None, description="Comma-separated ISO Alpha-2 country codes (e.g. 'DE,GB,FR')"
    )
    isin: str | None = Field(default=None, description="ISIN identifier (case-insensitive)")
    lei: str | None = Field(default=None, description="LEI identifier (case-insensitive)")
    ticker: str | None = Field(default=None, description="Stock ticker (case-insensitive)")
    sector: str | None = Field(default=None, description="ISIC Section code (e.g. 'C' for Manufacturing)")
    industry_group: str | None = Field(default=None, description="ISIC Division code")
    industry: str | None = Field(default=None, description="ISIC Group code")
    sub_industry: str | None = Field(default=None, description="ISIC Class code")
    ordering: str | None = Field(
        default=None,
        description="Sort field: id, name, date_ipo, year_founded, country_iso__name (prefix '-' for desc)",
    )
    page: int | None = Field(default=None, ge=1, description="Page number")
    page_size: int | None = Field(default=None, ge=1, le=100, description="Results per page")
    view: Literal["full"] | None = Field(
        default=None, description="Set to 'full' for detailed company info"
    )


class FrCompanyRetrieveParams(BaseModel):
    """Parameters for retrieving a single company by ID."""

    id: int = Field(..., description="Internal company ID (from fr_companies_search results)")


class FrFilingsSearchParams(BaseModel):
    """Parameters for searching/listing filings on Financial Reports."""

    search: str | None = Field(default=None, description="Full-text search query")
    company: int | None = Field(default=None, description="Filter by company ID")
    company_isin: str | None = Field(default=None, description="Filter by company ISIN (case-insensitive)")
    lei: str | None = Field(default=None, description="Filter by LEI")
    countries: str | None = Field(
        default=None, description="Comma-separated ISO Alpha-2 country codes (e.g. 'US,GB,DE')"
    )
    type: str | None = Field(default=None, description="Single filing type code (e.g. '10-K')")
    types: str | None = Field(
        default=None, description="Comma-separated filing type codes (e.g. '10-K,10-Q')"
    )
    category: int | None = Field(default=None, description="Single filing category ID")
    categories: str | None = Field(
        default=None, description="Comma-separated filing category IDs"
    )
    language: str | None = Field(default=None, description="Single ISO 639-1 language code")
    languages: str | None = Field(
        default=None, description="Comma-separated ISO 639-1 language codes"
    )
    fiscal_year: int | None = Field(default=None, description="Fiscal year (e.g. 2024)")
    fiscal_period: Literal["FY", "Q1", "Q2", "Q3", "Q4", "H1", "H2"] | None = Field(
        default=None, description="Fiscal period (only for 10-K, 10-K-ESEF, IR, ER types)"
    )
    period_ending_date: str | None = Field(
        default=None, description="Exact period ending date (YYYY-MM-DD)"
    )
    period_ending_date_from: str | None = Field(
        default=None, description="Period ending date range start (YYYY-MM-DD)"
    )
    period_ending_date_to: str | None = Field(
        default=None, description="Period ending date range end (YYYY-MM-DD)"
    )
    release_datetime_from: str | None = Field(
        default=None, description="Release date range start (YYYY-MM-DDTHH:MM:SSZ)"
    )
    release_datetime_to: str | None = Field(
        default=None, description="Release date range end (YYYY-MM-DDTHH:MM:SSZ)"
    )
    extensions: str | None = Field(
        default=None, description="Comma-separated file extensions (e.g. 'PDF,XBRL')"
    )
    source: int | None = Field(default=None, description="Single source ID")
    sources: str | None = Field(default=None, description="Comma-separated source IDs")
    ordering: str | None = Field(
        default=None,
        description="Sort: id, release_datetime, added_to_platform (prefix '-' for desc)",
    )
    page: int | None = Field(default=None, ge=1, description="Page number")
    page_size: int | None = Field(default=None, ge=1, le=100, description="Results per page")
    view: Literal["full"] | None = Field(
        default=None, description="Set to 'full' for detailed filing info"
    )


class FrFilingRetrieveParams(BaseModel):
    """Parameters for retrieving a single filing by ID."""

    id: int = Field(..., description="Filing ID (from fr_filings_search results)")


class FrFilingMarkdownParams(BaseModel):
    """Parameters for retrieving a filing's content as markdown (Level 2 access)."""

    id: int = Field(..., description="Filing ID to retrieve markdown content for")


class FrFilingHistoryParams(BaseModel):
    """Parameters for retrieving the audit trail of a filing."""

    id: int = Field(..., description="Filing ID to retrieve change history for")


class FrNextAnnualReportParams(BaseModel):
    """Parameters for predicting a company's next annual report date."""

    id: int = Field(..., description="Company ID (from fr_companies_search results)")


class FrIsicBrowseParams(BaseModel):
    """Parameters for browsing ISIC industry classifications."""

    level: Literal["sections", "divisions", "groups", "classes"] = Field(
        ...,
        description=(
            "ISIC hierarchy level: sections (broadest, e.g. 'C' Manufacturing), "
            "divisions, groups, classes (most specific)"
        ),
    )
    page: int | None = Field(default=None, ge=1, description="Page number")
    page_size: int | None = Field(default=None, ge=1, le=100, description="Results per page")


class FrIsinLookupParams(BaseModel):
    """Parameters for looking up ISINs with OpenFIGI enrichment."""

    codes: str | None = Field(default=None, description="Comma-separated ISIN codes to look up")
    company: int | None = Field(default=None, description="Filter by company ID")
    search: str | None = Field(default=None, description="Search query")
    page: int | None = Field(default=None, ge=1, description="Page number")
    page_size: int | None = Field(default=None, ge=1, le=100, description="Results per page")


class FrReferenceDataParams(BaseModel):
    """Parameters for listing reference/lookup data from Financial Reports."""

    resource: Literal[
        "filing_types",
        "filing_categories",
        "languages",
        "countries",
        "sources",
    ] = Field(
        ...,
        description=(
            "Reference data resource to list. "
            "Use filing_types/filing_categories to discover valid filter values for fr_filings_search."
        ),
    )
    search: str | None = Field(
        default=None, description="Search query (filing_types only)"
    )
    category: int | None = Field(
        default=None, description="Filter by category ID (filing_types only)"
    )
    page: int | None = Field(default=None, ge=1, description="Page number")
    page_size: int | None = Field(default=None, ge=1, le=100, description="Results per page")


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=COMPANIES_SEARCH_OUTPUT, tags=["financial_reports", "tool"])
async def fr_companies_search(params: FrCompaniesSearchParams, *, api_key: str) -> Result:
    """Search companies on Financial Reports by name, country, ISIN, ticker, or industry.

    Returns company profiles with ID, name, country, and ticker.
    Use the company ID with fr_filings_search(company=id) to find filings,
    fr_next_annual_report(id=id) for report predictions, or
    fr_company_retrieve(id=id) for the full profile.
    Use fr_isic_browse to discover valid ISIC codes for sector/industry filters.
    """
    kwargs = _strip_none(params.model_dump())
    resp = await _with_retry(lambda c: c.companies.list(**kwargs), api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No companies found for: {params}")
    return COMPANIES_SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(output=COMPANY_RETRIEVE_OUTPUT, tags=["financial_reports"])
async def fr_company_retrieve(params: FrCompanyRetrieveParams, *, api_key: str) -> Result:
    """Retrieve full company profile by ID from Financial Reports.

    Returns detailed info: name, LEI, ISINs, country, address, industry
    classification, ticker, headcount, IPO date, website, description, and more.
    Use fr_filings_search(company=id) to find this company's filings.
    Use fr_isin_lookup(company=id) for ISIN/FIGI cross-references.
    """
    resp = await _with_retry(lambda c: c.companies.retrieve(id=params.id), api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No company found with id={params.id}")
    return COMPANY_RETRIEVE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(output=FILINGS_SEARCH_OUTPUT, tags=["financial_reports"])
async def fr_filings_search(params: FrFilingsSearchParams, *, api_key: str) -> Result:
    """Search filings on Financial Reports by company, type, date, country, and more.

    Returns filing metadata: ID, title, release date, company, filing type.
    Use the filing ID with fr_filing_retrieve(id=id) for full details,
    fr_filing_markdown(id=id) to read the document content, or
    fr_filing_history(id=id) for the audit trail.
    Discover valid filing type codes via fr_reference_data(resource='filing_types').
    """
    kwargs = _strip_none(params.model_dump())
    resp = await _with_retry(lambda c: c.filings.list(**kwargs), api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No filings found for: {params}")
    return FILINGS_SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(output=FILING_RETRIEVE_OUTPUT, tags=["financial_reports"])
async def fr_filing_retrieve(params: FrFilingRetrieveParams, *, api_key: str) -> Result:
    """Retrieve full filing details by ID from Financial Reports.

    Returns detailed metadata: title, release date, company, filing type,
    fiscal year/period, source, language, document URL, markdown URL, and more.
    Use fr_filing_markdown(id=id) to read the actual document content.
    Use fr_filing_history(id=id) for the audit trail of changes to this filing.
    """
    resp = await _with_retry(lambda c: c.filings.retrieve(id=params.id), api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No filing found with id={params.id}")
    return FILING_RETRIEVE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(tags=["financial_reports"], result_type="text")
async def fr_filing_markdown(params: FrFilingMarkdownParams, *, api_key: str) -> Result:
    """Retrieve a filing's full content as markdown text (requires Level 2 API access).

    Returns the filing document converted to markdown for reading and analysis.
    Use fr_filing_retrieve(id=id) first to check if markdown_url is available.
    """
    text = await _with_retry(lambda c: c.filings.markdown_retrieve(id=params.id), api_key)
    content = (
        text
        if isinstance(text, str)
        else (text.decode() if isinstance(text, bytes) else str(text or ""))
    )
    if not content.strip():
        raise ValueError(f"No markdown content available for filing {params.id}")
    return Result(
        data=content,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(output=FILING_HISTORY_OUTPUT, tags=["financial_reports"])
async def fr_filing_history(params: FrFilingHistoryParams, *, api_key: str) -> Result:
    """Retrieve the audit trail of changes to a filing (reclassifications, metadata corrections).

    Returns history entries with date, change type (+ created, ~ changed, - deleted),
    and a changes dict describing what fields were modified.
    Use fr_filing_retrieve(id=id) first to get the current state of the filing.
    """
    resp = await _with_retry(lambda c: c.filings.history_retrieve(id=params.id), api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No history found for filing {params.id}")
    return FILING_HISTORY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(tags=["financial_reports"])
async def fr_next_annual_report(params: FrNextAnnualReportParams, *, api_key: str) -> Result:
    """Predict when a company's next annual report will be published.

    Returns a date window (start_date, end_date), confidence score (0-100),
    and is_overdue flag. Higher confidence means the company has a consistent
    historical release pattern. Use fr_companies_search to find company IDs.
    """
    resp = await _with_retry(
        lambda c: c.companies.next_annual_report_retrieve(id=params.id), api_key
    )
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No annual report prediction available for company {params.id}")
    return Result.from_dataframe(
        df,
        Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(output=ISIC_BROWSE_OUTPUT, tags=["financial_reports", "tool"])
async def fr_isic_browse(params: FrIsicBrowseParams, *, api_key: str) -> Result:
    """Browse ISIC industry classifications to find valid filter codes.

    Returns code/name pairs at the chosen hierarchy level.
    Use the codes in fr_companies_search: sector (sections), industry_group
    (divisions), industry (groups), sub_industry (classes).
    Start with level='sections' for the broadest view, then drill down.
    """
    kwargs: dict[str, Any] = {}
    if params.page is not None:
        kwargs["page"] = params.page
    if params.page_size is not None:
        kwargs["page_size"] = params.page_size

    level = params.level

    async def _call(client: Any) -> Any:
        isic = client.isic if hasattr(client, "isic") else None
        if isic is None:
            from financial_reports_generated_client import ISICClassificationsApi

            isic = ISICClassificationsApi(client.api_client)

        method = {
            "sections": isic.isic_sections_list,
            "divisions": isic.isic_divisions_list,
            "groups": isic.isic_groups_list,
            "classes": isic.isic_classes_list,
        }.get(level)
        if method is None:
            raise ValueError(f"Unknown ISIC level: {level}")
        return await method(**kwargs)

    resp = await _with_retry(_call, api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No ISIC {params.level} data returned")
    return ISIC_BROWSE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


@connector(output=ISIN_LOOKUP_OUTPUT, tags=["financial_reports", "tool"])
async def fr_isin_lookup(params: FrIsinLookupParams, *, api_key: str) -> Result:
    """Look up ISINs with OpenFIGI enrichment (FIGI, security type, exchange).

    Returns ISIN codes with associated company and financial instrument data.
    Use fr_companies_search to find company IDs, then fr_isin_lookup(company=id)
    to get all ISINs for that company.
    """
    kwargs = _strip_none(params.model_dump())

    async def _call(client: Any) -> Any:
        if hasattr(client, "isins"):
            return await client.isins.isins_list(**kwargs)
        from financial_reports_generated_client import ISINsApi

        return await ISINsApi(client.api_client).isins_list(**kwargs)

    resp = await _with_retry(_call, api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No ISINs found for: {params}")
    return ISIN_LOOKUP_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


# Per-resource OutputConfig mapping for reference data
_REFERENCE_OUTPUT_MAP: dict[str, OutputConfig] = {
    "filing_types": REFERENCE_FILING_TYPES_OUTPUT,
    "countries": REFERENCE_COUNTRIES_OUTPUT,
    "filing_categories": REFERENCE_GENERIC_OUTPUT,
    "languages": REFERENCE_GENERIC_OUTPUT,
    "sources": REFERENCE_GENERIC_OUTPUT,
}


@connector(output=REFERENCE_GENERIC_OUTPUT, tags=["financial_reports"])
async def fr_reference_data(params: FrReferenceDataParams, *, api_key: str) -> Result:
    """List reference/lookup data: filing types, categories, languages, countries, or sources.

    Use this to discover valid filter values for other connectors:
    - filing_types: type codes for fr_filings_search(type=..., types=...)
    - countries: alpha_2 codes for fr_filings_search(countries=...) or fr_companies_search(countries=...)
    - filing_categories: category IDs for fr_filings_search(category=..., categories=...)
    - languages: ISO 639-1 codes for fr_filings_search(language=..., languages=...)
    - sources: source IDs for fr_filings_search(source=..., sources=...)
    """
    kwargs: dict[str, Any] = {}
    if params.page is not None:
        kwargs["page"] = params.page
    if params.page_size is not None:
        kwargs["page_size"] = params.page_size

    resource = params.resource

    async def _call(client: Any) -> Any:
        if resource == "filing_types":
            if params.search is not None:
                kwargs["search"] = params.search
            if params.category is not None:
                kwargs["category"] = params.category
            return await client.filing_types.list(**kwargs)
        elif resource == "filing_categories":
            return await client.filing_categories.list(**kwargs)
        elif resource == "languages":
            return await client.languages.list(**kwargs)
        elif resource == "countries":
            return await client.countries.list(**kwargs)
        elif resource == "sources":
            return await client.sources.list(**kwargs)
        raise ValueError(f"Unknown resource: {resource}")

    resp = await _with_retry(_call, api_key)
    df = _to_dataframe(resp)
    if df.empty:
        raise ValueError(f"No {params.resource} data returned")
    output = _REFERENCE_OUTPUT_MAP.get(params.resource, REFERENCE_GENERIC_OUTPUT)
    return output.build_table_result(
        df,
        provenance=Provenance(source="financial_reports", params=params.model_dump()),
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

FETCH_CONNECTORS = Connectors([
    fr_companies_search,
    fr_company_retrieve,
    fr_filings_search,
    fr_filing_retrieve,
    fr_filing_markdown,
    fr_filing_history,
    fr_next_annual_report,
    fr_isic_browse,
    fr_isin_lookup,
    fr_reference_data,
])

CONNECTORS = FETCH_CONNECTORS
