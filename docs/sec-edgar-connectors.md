# SEC EDGAR Connectors — Design & Investigation

## Overview

The SEC EDGAR connectors wrap the [edgartools](https://github.com/dgunning/edgartools) library,
exposing SEC filing data through the ockham connector framework. Each connector has typed
Pydantic parameters discoverable via `to_llm()` for agentic AI workflows.

## Current Connectors (15)

### Discovery & Profile
| Connector | Params | Returns | Description |
|-----------|--------|---------|-------------|
| `sec_edgar_find_company` | `identifier` | DataFrame | Search by name, ticker, or CIK |
| `sec_edgar_company_profile` | `identifier` | DataFrame | Name, CIK, ticker, industry, SIC, fiscal year end |

### Financial Statements (XBRL)
| Connector | Params | Returns | Description |
|-----------|--------|---------|-------------|
| `sec_edgar_income_statement` | `identifier`, `periods?`, `annual?`, `view?` | DataFrame | Revenue, expenses, net income |
| `sec_edgar_balance_sheet` | `identifier`, `periods?`, `annual?`, `view?` | DataFrame | Assets, liabilities, equity |
| `sec_edgar_cashflow_statement` | `identifier`, `periods?`, `annual?`, `view?` | DataFrame | Operating, investing, financing flows |

### Filing Search & Listing
| Connector | Params | Returns | Description |
|-----------|--------|---------|-------------|
| `sec_edgar_search_filings` | `query`, `forms?`, `start_date?`, `end_date?`, `limit?` | DataFrame | Full-text search across all filings |
| `sec_edgar_filings` | `identifier?`, `form?`, `filing_date?`, `limit?` | DataFrame | List filings for a company or all |
| `sec_edgar_company_facts` | `identifier` | DataFrame | All XBRL facts across all periods |

### Filing Content
| Connector | Params | Returns | Description |
|-----------|--------|---------|-------------|
| `sec_edgar_filing_document` | `accession_number` | text | Full filing as markdown |
| `sec_edgar_filing_metadata` | `accession_number` | text | AI-optimized metadata via `to_context()` |
| `sec_edgar_filing_sections` | `accession_number` | DataFrame | Table of contents (items/sections list) |
| `sec_edgar_filing_item` | `accession_number`, `item` | text | Specific section text (e.g., "1A" for Risk Factors) |
| `sec_edgar_filing_tables` | `accession_number`, `item?` | DataFrame | Table summary listing (caption, type, size) |
| `sec_edgar_filing_table` | `accession_number`, `table_index`, `item?` | DataFrame | Individual table as DataFrame |

### Insider Activity
| Connector | Params | Returns | Description |
|-----------|--------|---------|-------------|
| `sec_edgar_insider_trades` | `identifier`, `start_date?`, `end_date?`, `limit?` | DataFrame | Structured Form 4 transactions |

## Agent Workflow Patterns

### Pattern 1: Financial Analysis
```python
# Find company → get statements
company = await client["sec_edgar_find_company"](identifier="AAPL")
income = await client["sec_edgar_income_statement"](identifier="AAPL", periods=4)
```

### Pattern 2: Filing Deep Dive
```python
# List filings → inspect TOC → read specific section
filings = await client["sec_edgar_filings"](identifier="AAPL", form="10-K", limit=1)
sections = await client["sec_edgar_filing_sections"](accession_number=acc)
risk_factors = await client["sec_edgar_filing_item"](accession_number=acc, item="1A")
```

### Pattern 3: Table Extraction
```python
# List tables → fetch specific one
tables = await client["sec_edgar_filing_tables"](accession_number=acc)
balance_sheet = await client["sec_edgar_filing_table"](accession_number=acc, table_index=3)
```

### Pattern 4: Insider Activity
```python
trades = await client["sec_edgar_insider_trades"](identifier="AAPL", limit=20)
```

## edgartools Library Capabilities Audit

### What We Use

| edgartools Feature | Our Connector | Notes |
|-------------------|---------------|-------|
| `edgar.find()` | `sec_edgar_find_company` | Returns Company or CompanySearchResults |
| `entity.get_filings()` | `sec_edgar_filings` | Supports form, filing_date filters |
| `edgar.search_filings()` | `sec_edgar_search_filings` | EFTS full-text search |
| `filing.xbrl().statements` | `*_statement` connectors | Via MultiFinancials for multi-period |
| `entity.get_facts()` | `sec_edgar_company_facts` | Full XBRL facts table |
| `filing.markdown()` | `sec_edgar_filing_document` | Full document as markdown |
| `filing.obj().to_context()` | `sec_edgar_filing_metadata` | AI-optimized metadata (HasContext protocol) |
| `obj.items` / `obj.sections` | `sec_edgar_filing_sections` | TOC with confidence scores |
| `obj[item_id]` | `sec_edgar_filing_item` | Flexible lookup: "1A", "risk_factors", "2.02" |
| `document.tables` / `TableNode` | `sec_edgar_filing_tables/table` | Full colspan/rowspan handling |
| `Form4` class | `sec_edgar_insider_trades` | Owner, transactions, holdings |

### What We Don't Use (Future Candidates)

#### HIGH Priority

**Earnings Release Parsing** (`edgar/earnings.py`)
- `EarningsRelease` class with structured extraction from 8-K press releases
- `get_financial_tables()` → list of `FinancialTable` objects (income, balance, cashflow, segment)
- `guidance` property → forward-looking metrics
- `eps_reconciliation` → GAAP vs non-GAAP
- `get_key_metrics()` → automatic KPI extraction
- **Why**: Earnings season is the #1 agent use case for SEC data. Currently the agent must
  parse markdown text from 8-K exhibits to extract structured financial data.
- **Proposed connector**: `sec_edgar_earnings(accession_number)` → DataFrame of key metrics,
  or `sec_edgar_earnings(identifier, limit?)` to get latest earnings releases.

**XBRL Concept Discovery** (`edgar/entity/core.py`)
- `Company.list_concepts()` → all available XBRL concepts for a company
- Enables agents to discover what financial metrics are queryable before calling `company_facts`
- **Why**: `company_facts` returns thousands of rows. Agents need to know what's available
  to formulate targeted queries.
- **Proposed connector**: `sec_edgar_concepts(identifier, search?)` → DataFrame of concept names

#### MEDIUM Priority

**Subsidiaries** (`edgar/company_reports/subsidiaries.py`)
- `Company.subsidiaries` → `SubsidiaryList` from Exhibit 21
- Name, state, country, ownership percentage
- **Why**: Corporate structure analysis, M&A due diligence
- **Proposed connector**: `sec_edgar_subsidiaries(identifier)` → DataFrame

**TTM Financials** (`edgar/entity/core.py`)
- `Company.get_ttm()`, `get_ttm_revenue()`, `get_ttm_net_income()`
- Rolling 12-month metrics from XBRL
- **Why**: Annualized metrics are essential for valuation
- **Proposed connector**: Could be a parameter on existing statement connectors

**Institutional Holdings (13F)** (`edgar/thirteenf/models.py`)
- `ThirteenF.holdings` → DataFrame with CUSIP, shares, fair value
- `compare_holdings()` → QoQ position changes
- `holding_history()` → track a security across periods
- **Why**: Institutional investor tracking, smart money flow analysis
- **Proposed connector**: `sec_edgar_institutional_holdings(identifier_or_manager, limit?)`

**Beneficial Ownership (13D/13G)** (`edgar/beneficial_ownership/schedule13.py`)
- `Schedule13D` / `Schedule13G` with issuer, reporting persons, ownership %
- Has `to_context()` method
- **Why**: Activist investor tracking, major shareholder monitoring
- **Proposed connector**: `sec_edgar_beneficial_ownership(identifier)`

#### LOW Priority

**Securities Offerings** (`edgar/offerings/`)
- `FormD` (private placements), `FormC` (crowdfunding), `RegistrationS1` (IPOs), `Prospectus424B`
- **Why**: IPO pipeline, private market tracking
- **Proposed connector**: `sec_edgar_offerings(identifier?, form_type?)` → DataFrame

**Fund Data** (`edgar/funds/`)
- `FundReport` (N-PORT), `FundCompany`, `MoneyMarketFund` (N-MFP)
- **Why**: Fund portfolio analysis
- **Proposed connector**: `sec_edgar_fund_holdings(identifier)` → DataFrame

**Proxy / Executive Compensation** (`edgar/proxy/core.py`)
- `ProxyStatement.executive_compensation` → DataFrame
- `pay_vs_performance()` → comp vs shareholder returns
- **Why**: Corporate governance analysis
- **Proposed connector**: `sec_edgar_executive_comp(identifier)` → DataFrame

### edgartools MCP Tools (Reference)

edgartools ships its own MCP tools in `edgar/ai/mcp/tools/` — 12+ tools designed for
agentic use. These serve as design reference but are not used directly because our
connectors go through the ockham Result/Provenance pipeline.

Their tools: `edgar_company`, `edgar_ownership`, `edgar_proxy`, `edgar_filing`,
`edgar_read`, `edgar_trends`, `edgar_notes`, `edgar_compare`, `edgar_search`,
`edgar_screen`, `edgar_fund`, `edgar_monitor`, `edgar_text_search`.

## Design Decisions

### Why separate connectors (not dispatch pattern)
The old `sec_edgar_fetch(endpoint="...", **kwargs)` hid parameters from the agent.
Each connector has typed Pydantic params visible via `to_llm()`, enabling the agent
to discover available operations and their parameters without documentation.

### Why `to_context()` for metadata
edgartools' `HasContext` protocol provides form-aware, AI-optimized metadata that
adapts to each filing type (8-K, 10-K, 13F, etc.). Maintaining a hard-coded attribute
list would always lag behind the library's capabilities.

### Why `result_type="text"` for documents
Filing content and metadata are consumed by the agent as text for reasoning,
not as DataFrames for computation. The `result_type` field on `Connector` tells
the agent that `.data` is a string, preventing `.iloc[]` errors.

### Why table listing + individual fetch (not batch)
Filing tables have heterogeneous schemas — different column counts, types, and
structures. Returning all tables in one DataFrame is impossible without lossy
serialization. The summary listing lets the agent pick the right table by caption
and type before fetching it as a clean DataFrame.
