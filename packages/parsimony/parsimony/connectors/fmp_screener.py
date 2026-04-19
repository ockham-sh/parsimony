"""FMP Screener: company-screener pushdown + key-metrics-ttm + ratios-ttm enrichment.

Ported from the main branch's ``FMPCompositeTool.screener_join_query`` mode.
This is the most complex FMP connector: it fans out to three FMP endpoints,
joins the results, applies residual ``where_clause`` filtering via
``pd.DataFrame.query()``, and sorts/limits the output.

Separated from ``fmp.py`` to keep file sizes manageable.
"""

from __future__ import annotations

import asyncio
import difflib
import logging
import re
from typing import Any

import httpx
import pandas as pd
from pydantic import BaseModel, Field

from parsimony.connector import (
    Connectors,
    connector,
)
from parsimony.errors import (
    EmptyDataError,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    UnauthorizedError,
)
from parsimony.result import (
    Column,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport.http import HttpClient

logger = logging.getLogger(__name__)

ENV_VARS: dict[str, str] = {"api_key": "FMP_API_KEY"}

_SEMAPHORE_LIMIT = 10  # max concurrent FMP requests per screener call

# ---------------------------------------------------------------------------
# Column-source classification (determines which enrichment endpoints to hit)
# ---------------------------------------------------------------------------

_SCREENER_NATIVE_COLS: frozenset[str] = frozenset(
    {
        "symbol",
        "companyName",
        "sector",
        "industry",
        "country",
        "exchange",
        "exchangeShortName",
        "marketCap",
        "price",
        "beta",
        "volume",
        "lastAnnualDividend",
        "isEtf",
        "isFund",
        "isActivelyTrading",
    }
)

_KEY_METRICS_TTM_COLS: frozenset[str] = frozenset(
    {
        # Enterprise value
        "enterpriseValueTTM",
        "evToSalesTTM",
        "evToEBITDATTM",
        "evToOperatingCashFlowTTM",
        "evToFreeCashFlowTTM",
        # Profitability
        "returnOnEquityTTM",
        "returnOnAssetsTTM",
        "returnOnInvestedCapitalTTM",
        "returnOnCapitalEmployedTTM",
        "operatingReturnOnAssetsTTM",
        "returnOnTangibleAssetsTTM",
        "earningsYieldTTM",
        "freeCashFlowYieldTTM",
        "incomeQualityTTM",
        # Leverage / liquidity
        "netDebtToEBITDATTM",
        "currentRatioTTM",
        "taxBurdenTTM",
        "interestBurdenTTM",
        "workingCapitalTTM",
        "investedCapitalTTM",
        # Capital efficiency
        "capexToOperatingCashFlowTTM",
        "capexToDepreciationTTM",
        "capexToRevenueTTM",
        "stockBasedCompensationToRevenueTTM",
        "salesGeneralAndAdministrativeToRevenueTTM",
        "researchAndDevelopementToRevenueTTM",
        "intangiblesToTotalAssetsTTM",
        # Working capital days
        "daysOfSalesOutstandingTTM",
        "daysOfPayablesOutstandingTTM",
        "daysOfInventoryOutstandingTTM",
        "operatingCycleTTM",
        "cashConversionCycleTTM",
        # Balance sheet ($)
        "averageReceivablesTTM",
        "averagePayablesTTM",
        "averageInventoryTTM",
        "freeCashFlowToEquityTTM",
        "freeCashFlowToFirmTTM",
        "tangibleAssetValueTTM",
        "netCurrentAssetValueTTM",
        # Graham
        "grahamNumberTTM",
        "grahamNetNetTTM",
    }
)

_FINANCIAL_RATIOS_TTM_COLS: frozenset[str] = frozenset(
    {
        # Margins
        "grossProfitMarginTTM",
        "ebitdaMarginTTM",
        "ebitMarginTTM",
        "operatingProfitMarginTTM",
        "netProfitMarginTTM",
        "pretaxProfitMarginTTM",
        "bottomLineProfitMarginTTM",
        "continuousOperationsProfitMarginTTM",
        # Valuation multiples
        "priceToEarningsRatioTTM",
        "priceToBookRatioTTM",
        "priceToSalesRatioTTM",
        "priceToFreeCashFlowRatioTTM",
        "priceToOperatingCashFlowRatioTTM",
        "priceToEarningsGrowthRatioTTM",
        "forwardPriceToEarningsGrowthRatioTTM",
        "priceToFairValueTTM",
        "enterpriseValueMultipleTTM",
        # Efficiency
        "receivablesTurnoverTTM",
        "payablesTurnoverTTM",
        "inventoryTurnoverTTM",
        "assetTurnoverTTM",
        "fixedAssetTurnoverTTM",
        "workingCapitalTurnoverRatioTTM",
        # Liquidity
        "quickRatioTTM",
        "cashRatioTTM",
        "solvencyRatioTTM",
        # Debt
        "debtToEquityRatioTTM",
        "debtToAssetsRatioTTM",
        "debtToCapitalRatioTTM",
        "longTermDebtToCapitalRatioTTM",
        "financialLeverageRatioTTM",
        "debtToMarketCapTTM",
        # Coverage
        "interestCoverageRatioTTM",
        "debtServiceCoverageRatioTTM",
        "operatingCashFlowRatioTTM",
        "operatingCashFlowSalesRatioTTM",
        "operatingCashFlowCoverageRatioTTM",
        "freeCashFlowOperatingCashFlowRatioTTM",
        "capitalExpenditureCoverageRatioTTM",
        "shortTermOperatingCashFlowCoverageRatioTTM",
        "dividendPaidAndCapexCoverageRatioTTM",
        # Dividends
        "dividendPayoutRatioTTM",
        "dividendYieldTTM",
        # Per-share ($)
        "revenuePerShareTTM",
        "netIncomePerShareTTM",
        "bookValuePerShareTTM",
        "tangibleBookValuePerShareTTM",
        "freeCashFlowPerShareTTM",
        "operatingCashFlowPerShareTTM",
        "cashPerShareTTM",
        "shareholdersEquityPerShareTTM",
        "capexPerShareTTM",
        "interestDebtPerShareTTM",
        # Other
        "effectiveTaxRateTTM",
        "netIncomePerEBTTTM",
        "ebtPerEbitTTM",
    }
)


# ---------------------------------------------------------------------------
# Parameter model
# ---------------------------------------------------------------------------


class FmpScreenerParams(BaseModel):
    # Pushdown filters (applied at FMP screener API level)
    sector: str | None = Field(default=None, description="Filter by sector (e.g. 'Technology')")
    industry: str | None = Field(default=None, description="Filter by industry (e.g. 'Consumer Electronics')")
    country: str | None = Field(
        default=None, description="Country code (e.g. 'US', 'DE'). Single value; for multiple use where_clause."
    )
    exchange: str | None = Field(default=None, description="Exchange code (e.g. 'NASDAQ', 'NYSE'). Single value.")
    market_cap_min: float | None = Field(default=None, description="Minimum market cap")
    market_cap_max: float | None = Field(default=None, description="Maximum market cap")
    price_min: float | None = Field(default=None, description="Minimum stock price")
    price_max: float | None = Field(default=None, description="Maximum stock price")
    volume_min: float | None = Field(default=None, description="Minimum trading volume")
    volume_max: float | None = Field(default=None, description="Maximum trading volume")
    beta_min: float | None = Field(default=None, description="Minimum beta")
    beta_max: float | None = Field(default=None, description="Maximum beta")
    dividend_min: float | None = Field(default=None, description="Minimum last annual dividend")
    dividend_max: float | None = Field(default=None, description="Maximum last annual dividend")
    is_etf: bool | None = Field(default=None, description="Include (True) or exclude (False) ETFs")
    is_fund: bool | None = Field(default=None, description="Include (True) or exclude (False) funds")
    is_actively_trading: bool | None = Field(default=None, description="Restrict to actively trading (True)")

    # Enrichment / residual filtering
    where_clause: str | None = Field(
        default=None,
        description=(
            "pandas df.query() filter applied after enrichment."
            " Can reference screener, key-metrics-ttm, or ratios-ttm columns."
        ),
    )
    sort_by: str | None = Field(
        default=None, description="Column to sort by (e.g. 'marketCap', 'freeCashFlowYieldTTM')"
    )
    sort_order: str = Field(default="desc", description="Sort direction: 'asc' or 'desc'")
    limit: int = Field(default=100, description="Max rows to return (default 100)")
    prefilter_limit: int | None = Field(
        default=None,
        description=(
            "Max symbols from screener before enrichment. Default max(limit, 500)."
            " Increase to 1000-2000 for broad global searches sorted by TTM columns."
        ),
    )
    fields: list[str] | None = Field(
        default=None,
        description=(
            "Output columns to include. symbol always returned."
            " Omit for all columns. When specified, skips unnecessary enrichment calls."
        ),
    )


# ---------------------------------------------------------------------------
# Output config
# ---------------------------------------------------------------------------


SCREENER_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="companyName"),
        Column(name="sector"),
        Column(name="industry"),
        Column(name="country"),
        Column(name="exchange"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="price", dtype="numeric"),
        Column(name="beta", dtype="numeric"),
        Column(name="*"),
    ]
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str, base_url: str) -> HttpClient:
    return HttpClient(base_url, query_params={"apikey": api_key})


async def _fetch_json(
    http: HttpClient,
    path: str,
    params: dict[str, Any],
) -> list | dict:
    """Fetch JSON from FMP with error handling."""
    filtered = {k: v for k, v in params.items() if v is not None}
    try:
        response = await http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        match exc.response.status_code:
            case 401:
                raise UnauthorizedError(provider="fmp", message="Invalid or missing FMP API key") from exc
            case 402:
                raise ProviderError(
                    provider="fmp",
                    status_code=402,
                    message="Your FMP plan is not eligible for this data request",
                ) from exc
            case 429:
                raise ProviderError(
                    provider="fmp",
                    status_code=429,
                    message="FMP rate limit reached. Screener enrichment endpoints are rate-limited; retry shortly.",
                ) from exc
            case _:
                raise ProviderError(
                    provider="fmp",
                    status_code=exc.response.status_code,
                    message=f"FMP API error {exc.response.status_code} on endpoint '{path}'",
                ) from exc
    return response.json()  # type: ignore[no-any-return]


def _col_refs_source(
    sort_by: str | None,
    where_clause: str | None,
    fields: list[str] | None,
    col_set: frozenset[str],
) -> bool:
    """Return True if any column references overlap with col_set."""
    if sort_by and sort_by in col_set:
        return True
    if where_clause and any(col in where_clause for col in col_set):
        return True
    return bool(fields and any(f in col_set for f in fields))


def _extract_unknown_cols(error_msg: str, allowed: list[str]) -> list[str]:
    """Heuristically extract column names mentioned in a pandas query error."""
    names = re.findall(r"'([A-Za-z_]\w*)'", error_msg)
    return [n for n in names if n not in allowed]


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


_PUSHDOWN_MAP: dict[str, str] = {
    "market_cap_min": "marketCapMoreThan",
    "market_cap_max": "marketCapLowerThan",
    "sector": "sector",
    "industry": "industry",
    "beta_min": "betaMoreThan",
    "beta_max": "betaLowerThan",
    "price_min": "priceMoreThan",
    "price_max": "priceLowerThan",
    "dividend_min": "dividendMoreThan",
    "dividend_max": "dividendLowerThan",
    "volume_min": "volumeMoreThan",
    "volume_max": "volumeLowerThan",
    "exchange": "exchange",
    "country": "country",
    "is_etf": "isEtf",
    "is_fund": "isFund",
    "is_actively_trading": "isActivelyTrading",
}


@connector(output=SCREENER_OUTPUT, tags=["equity", "search", "tool"])
async def fmp_screener(
    params: FmpScreenerParams,
    *,
    api_key: str,
    base_url: str = "https://financialmodelingprep.com/stable",
) -> Result:
    """Screen the global equity universe by financial metrics.

    Use pushdown params (sector, country, market_cap_min, etc.) to narrow the
    universe, then where_clause for residual conditions on enriched TTM metrics
    (ratios, yields, multiples, margins). Enriches with key-metrics-ttm and
    financial-ratios-ttm. Use fields to restrict output and skip unnecessary
    enrichment. Use sort_by + limit for top-N. Increase prefilter_limit
    (1000-2000) for broad global searches sorted by TTM columns.
    """
    http = _make_http(api_key, base_url)

    where_clause = params.where_clause
    limit = params.limit
    sort_by = params.sort_by
    sort_order = (params.sort_order or "desc").lower()
    fields = params.fields
    screener_limit = int(params.prefilter_limit or max(limit or 500, 500))

    # Build pushdown params for FMP screener API
    screener_params: dict[str, Any] = {"limit": screener_limit}
    params_dict = params.model_dump(
        exclude={"where_clause", "sort_by", "sort_order", "limit", "prefilter_limit", "fields"}
    )
    for internal_key, upstream_key in _PUSHDOWN_MAP.items():
        value = params_dict.get(internal_key)
        if value is not None:
            screener_params[upstream_key] = value

    # Step 1: Screener pushdown
    screener_raw = await _fetch_json(http, "company-screener", screener_params)
    screener_df = pd.json_normalize(screener_raw) if screener_raw else pd.DataFrame()
    if screener_df.empty:
        raise EmptyDataError(
            provider="fmp", message="FMP company-screener returned no rows for the selected filter set."
        )
    if "symbol" not in screener_df.columns:
        raise ParseError(provider="fmp", message="Unexpected company-screener payload: missing 'symbol' column.")

    symbols = [s for s in screener_df["symbol"].dropna().astype(str).str.strip().tolist() if s]
    if not symbols:
        raise EmptyDataError(provider="fmp", message="FMP company-screener did not return any valid symbols.")

    # Step 2: Determine which enrichment endpoints are needed
    need_metrics = fields is None or _col_refs_source(sort_by, where_clause, fields, _KEY_METRICS_TTM_COLS)
    need_ratios = fields is None or _col_refs_source(sort_by, where_clause, fields, _FINANCIAL_RATIOS_TTM_COLS)

    logger.info(
        "fmp_screener: enrichment plan — %d symbols, metrics=%s, ratios=%s",
        len(symbols),
        need_metrics,
        need_ratios,
    )

    # Step 3: Concurrent enrichment with semaphore-limited fan-out and shared connection pool
    metrics_semaphore = asyncio.Semaphore(_SEMAPHORE_LIMIT)
    ratios_semaphore = asyncio.Semaphore(_SEMAPHORE_LIMIT)

    async def _run_enrichment(
        enrich_http: HttpClient,
    ) -> list:
        async def _fetch_enrich(semaphore: asyncio.Semaphore, path: str, symbol: str) -> pd.DataFrame:
            async with semaphore:
                data = await _fetch_json(enrich_http, path, {"symbol": symbol})
                if not data:
                    return pd.DataFrame()
                df_s = pd.json_normalize(data if isinstance(data, list) else [data])
                if df_s.empty:
                    return pd.DataFrame()
                if "symbol" not in df_s.columns:
                    df_s.insert(0, "symbol", symbol)
                return df_s

        gather_coros: list[Any] = []
        if need_metrics:
            metrics_tasks = [
                asyncio.create_task(_fetch_enrich(metrics_semaphore, "key-metrics-ttm", s)) for s in symbols
            ]
            gather_coros.append(asyncio.gather(*metrics_tasks, return_exceptions=True))
        if need_ratios:
            ratios_tasks = [asyncio.create_task(_fetch_enrich(ratios_semaphore, "ratios-ttm", s)) for s in symbols]
            gather_coros.append(asyncio.gather(*ratios_tasks, return_exceptions=True))

        return await asyncio.gather(*gather_coros) if gather_coros else []

    # Use a shared httpx.AsyncClient for connection pooling across enrichment requests
    async with httpx.AsyncClient(**http._client_kwargs()) as shared:
        enrich_http = http.with_shared_client(shared)
        gathered = await _run_enrichment(enrich_http)

    def _collect_dfs(results: list) -> pd.DataFrame:
        dfs: list[pd.DataFrame] = []
        errors: list[Exception] = []
        for r in results:
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)
            elif isinstance(r, Exception):
                errors.append(r)
        if errors:
            # Propagate auth and rate-limit errors — these are not transient
            for err in errors:
                if isinstance(err, (UnauthorizedError, PaymentRequiredError)):
                    raise err
            logger.warning(
                "fmp_screener: %d/%d enrichment requests failed (first: %s)",
                len(errors),
                len(results),
                errors[0],
            )
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=["symbol"])

    idx = 0
    metrics_df = pd.DataFrame(columns=["symbol"])
    ratios_df = pd.DataFrame(columns=["symbol"])
    if need_metrics:
        metrics_df = _collect_dfs(gathered[idx])
        idx += 1
    if need_ratios:
        ratios_df = _collect_dfs(gathered[idx])

    # Step 4: Join enrichment into screener base
    df = screener_df.drop_duplicates("symbol")

    if need_metrics and not metrics_df.empty:
        if "marketCap" in metrics_df.columns:
            metrics_df = metrics_df.drop(columns=["marketCap"])
        df = pd.merge(df, metrics_df, on="symbol", how="left")

    if need_ratios and not ratios_df.empty:
        dup_cols = [c for c in ratios_df.columns if c != "symbol" and c in df.columns]
        if dup_cols:
            ratios_df = ratios_df.drop(columns=dup_cols)
        df = pd.merge(df, ratios_df, on="symbol", how="left")

    # Step 5: Residual filtering
    allowed_cols = sorted(df.columns.tolist())
    if where_clause:
        try:
            df = df.query(where_clause)
        except Exception as exc:
            unknown = _extract_unknown_cols(str(exc), allowed_cols)
            suggestions = {c: difflib.get_close_matches(c, allowed_cols, n=3, cutoff=0.6) for c in unknown}
            sug_str = "; ".join(f"'{c}' → {v}" if v else f"'{c}' → no close match" for c, v in suggestions.items())
            raise ValueError(
                f"Invalid where_clause: {exc}\n"
                + (f"Unknown column(s): {unknown}. Suggestions: {sug_str}\n" if unknown else "")
                + f"Available columns ({len(allowed_cols)}): {allowed_cols}"
            ) from exc

    # Step 6: Sort and limit
    if sort_by is not None:
        if sort_by not in df.columns:
            sort_suggestions = difflib.get_close_matches(sort_by, allowed_cols, n=5, cutoff=0.6)
            raise ValueError(
                f"Invalid sort_by column: '{sort_by}'. "
                f"Suggestions: {sort_suggestions if sort_suggestions else 'no close matches'}. "
                f"Available columns ({len(allowed_cols)}): {allowed_cols}"
            )
        df = df.sort_values(by=sort_by, ascending=(sort_order == "asc"))

    if limit:
        df = df.head(limit)

    # Step 7: Field selection (symbol always kept)
    if fields is not None:
        keep = ["symbol"] + [f for f in fields if f != "symbol"]
        missing = [f for f in keep if f not in df.columns]
        if missing:
            raise ValueError(
                f"Unknown field(s) in 'fields': {missing}. Available columns ({len(allowed_cols)}): {allowed_cols}"
            )
        df = df[keep]

    if df.empty:
        raise EmptyDataError(provider="fmp", message="Screener returned no rows after applying all filters.")

    prov = Provenance(
        source="fmp_screener",
        params=params.model_dump(exclude_none=True),
    )
    return SCREENER_OUTPUT.build_table_result(df, provenance=prov, params=params.model_dump(exclude_none=True))


CONNECTORS = Connectors([fmp_screener])
