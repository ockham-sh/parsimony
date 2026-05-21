"""Benchmark a Sentence Transformers reranker for cross-catalog search results.

This is an exploratory script, not part of the parsimony runtime contract. It
answers one question: if several catalogs return a noisy pooled candidate set,
can a small cross-encoder reranker improve the final order fast enough for
interactive use?

The default model is ``cross-encoder/ms-marco-MiniLM-L6-v2``. It is a compact
MS MARCO cross-encoder available through sentence-transformers and is a strong
speed/quality default for CPU reranking. For higher-quality but slower trials,
try ``BAAI/bge-reranker-v2-m3`` or another Hugging Face sequence-classification
reranker that ``sentence_transformers.CrossEncoder`` can load.

Run:
    uv run python examples/reranker_catalog_benchmark.py

Optional:
    uv run python examples/reranker_catalog_benchmark.py --device cpu --repeats 5
    uv run python examples/reranker_catalog_benchmark.py --model BAAI/bge-reranker-v2-m3
    uv run python examples/reranker_catalog_benchmark.py --suite --repeats 1
"""

from __future__ import annotations

import argparse
import math
import statistics
import time
from dataclasses import dataclass
from typing import Any

from parsimony.catalog import CatalogEntry

DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L6-v2"
DEFAULT_MODEL_SUITE = (
    "cross-encoder/ms-marco-MiniLM-L2-v2",
    "cross-encoder/ms-marco-MiniLM-L4-v2",
    "cross-encoder/ms-marco-MiniLM-L6-v2",
    "cross-encoder/ms-marco-MiniLM-L12-v2",
)
DEFAULT_SIZES = (10, 50, 100)


@dataclass(frozen=True)
class QueryCase:
    query: str
    relevant: dict[tuple[str, str], int]


@dataclass(frozen=True)
class Candidate:
    entry: CatalogEntry
    baseline_rank: int
    baseline_score: float


@dataclass(frozen=True)
class BenchmarkResult:
    size: int
    mean_latency_ms: float
    p95_latency_ms: float
    docs_per_second: float
    baseline_ndcg: float
    reranked_ndcg: float
    baseline_mrr: float
    reranked_mrr: float


@dataclass(frozen=True)
class ModelSummary:
    model: str
    load_seconds: float
    results: list[BenchmarkResult]

    @property
    def mean_ndcg(self) -> float:
        return _mean([result.reranked_ndcg for result in self.results])

    @property
    def mean_mrr(self) -> float:
        return _mean([result.reranked_mrr for result in self.results])

    @property
    def mean_docs_per_second(self) -> float:
        return _mean([result.docs_per_second for result in self.results])

    @property
    def latency_100_ms(self) -> float | None:
        for result in self.results:
            if result.size == 100:
                return result.mean_latency_ms
        return None


def _doc_text(entry: CatalogEntry) -> str:
    parts = [
        f"catalog: {entry.namespace}",
        f"code: {entry.code}",
        f"title: {entry.title}",
    ]
    for key in ("description", "frequency", "unit", "source", "geography", "category"):
        value = entry.metadata.get(key)
        if value is not None:
            parts.append(f"{key}: {value}")
    return " | ".join(parts)


def _identity(entry: CatalogEntry) -> tuple[str, str]:
    return (entry.namespace, entry.code)


def _base_entries() -> list[CatalogEntry]:
    return [
        CatalogEntry(
            namespace="fred",
            code="GDPC1",
            title="Real Gross Domestic Product",
            metadata={
                "description": "Inflation-adjusted output and real growth of the United States economy.",
                "frequency": "quarterly",
                "unit": "billions of chained dollars",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "national accounts",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="GDP",
            title="Gross Domestic Product",
            metadata={
                "description": "Nominal market value of goods and services produced in the United States.",
                "frequency": "quarterly",
                "unit": "billions of dollars",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "national accounts",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="UNRATE",
            title="Unemployment Rate",
            metadata={
                "description": "Monthly civilian unemployment rate and labor market slack.",
                "frequency": "monthly",
                "unit": "percent",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "labor market",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="PAYEMS",
            title="All Employees, Total Nonfarm Payrolls",
            metadata={
                "description": "Monthly nonfarm payroll employment and jobs count.",
                "frequency": "monthly",
                "unit": "thousands of persons",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "labor market",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="CPIAUCSL",
            title="Consumer Price Index for All Urban Consumers",
            metadata={
                "description": "Monthly consumer price index used to measure CPI inflation.",
                "frequency": "monthly",
                "unit": "index 1982-1984=100",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "prices",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="CPILFESL",
            title="Consumer Price Index Less Food and Energy",
            metadata={
                "description": "Monthly core CPI inflation excluding food and energy prices.",
                "frequency": "monthly",
                "unit": "index 1982-1984=100",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "prices",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="DGS10",
            title="Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity",
            metadata={
                "description": "Daily 10 year Treasury yield and long-term interest rate.",
                "frequency": "daily",
                "unit": "percent",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "rates",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="MORTGAGE30US",
            title="30-Year Fixed Rate Mortgage Average in the United States",
            metadata={
                "description": "Weekly average interest rate for 30-year fixed-rate home mortgages.",
                "frequency": "weekly",
                "unit": "percent",
                "source": "Freddie Mac via FRED",
                "geography": "United States",
                "category": "housing",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="HOUST",
            title="Housing Starts: Total New Privately Owned Housing Units Started",
            metadata={
                "description": "Monthly new residential construction and housing starts.",
                "frequency": "monthly",
                "unit": "thousands of units",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "housing",
            },
        ),
        CatalogEntry(
            namespace="fred",
            code="RSAFS",
            title="Advance Retail Sales: Retail Trade and Food Services",
            metadata={
                "description": "Monthly retail sales and consumer spending.",
                "frequency": "monthly",
                "unit": "millions of dollars",
                "source": "Federal Reserve Economic Data",
                "geography": "United States",
                "category": "consumption",
            },
        ),
        CatalogEntry(
            namespace="sdmx",
            code="ECB_EXR_D_USD_EUR_SP00_A",
            title="ECB Euro foreign exchange reference rate: U.S. dollar",
            metadata={
                "description": "Daily EUR/USD spot exchange rate published by the European Central Bank.",
                "frequency": "daily",
                "unit": "USD per EUR",
                "source": "European Central Bank SDMX",
                "geography": "Euro area",
                "category": "foreign exchange",
            },
        ),
        CatalogEntry(
            namespace="sdmx",
            code="OECD_QNA_USA_B1_GE_CQR",
            title="OECD quarterly real GDP for the United States",
            metadata={
                "description": "Quarterly national accounts real gross domestic product, volume index.",
                "frequency": "quarterly",
                "unit": "index",
                "source": "OECD SDMX",
                "geography": "United States",
                "category": "national accounts",
            },
        ),
        CatalogEntry(
            namespace="sdmx",
            code="ECB_ICP_M_U2_N_000000_4_ANR",
            title="Euro area HICP inflation annual rate",
            metadata={
                "description": "Monthly harmonised index of consumer prices inflation rate for the euro area.",
                "frequency": "monthly",
                "unit": "annual percent change",
                "source": "European Central Bank SDMX",
                "geography": "Euro area",
                "category": "prices",
            },
        ),
        CatalogEntry(
            namespace="sdmx",
            code="ILO_UNE_DEU_M",
            title="Germany unemployment rate",
            metadata={
                "description": "Monthly unemployment rate for Germany from labor force statistics.",
                "frequency": "monthly",
                "unit": "percent",
                "source": "ILO SDMX",
                "geography": "Germany",
                "category": "labor market",
            },
        ),
        CatalogEntry(
            namespace="fmp",
            code="AAPL_INCOME_STATEMENT_ANNUAL",
            title="Apple annual income statement",
            metadata={
                "description": "Annual revenue, operating income, net income, and earnings for Apple.",
                "frequency": "annual",
                "unit": "USD",
                "source": "Financial Modeling Prep",
                "geography": "United States",
                "category": "company fundamentals",
            },
        ),
        CatalogEntry(
            namespace="fmp",
            code="MSFT_BALANCE_SHEET_QUARTERLY",
            title="Microsoft quarterly balance sheet",
            metadata={
                "description": "Quarterly assets, liabilities, cash, and shareholder equity for Microsoft.",
                "frequency": "quarterly",
                "unit": "USD",
                "source": "Financial Modeling Prep",
                "geography": "United States",
                "category": "company fundamentals",
            },
        ),
        CatalogEntry(
            namespace="fmp",
            code="SP500_CONSTITUENTS",
            title="S&P 500 constituents",
            metadata={
                "description": "List of companies and sectors included in the S&P 500 index.",
                "frequency": "current",
                "unit": "entities",
                "source": "Financial Modeling Prep",
                "geography": "United States",
                "category": "equities",
            },
        ),
        CatalogEntry(
            namespace="sec",
            code="AAPL_10K_FILINGS",
            title="Apple Form 10-K annual reports",
            metadata={
                "description": "SEC annual reports with business, risk factors, financial statements, and MD&A.",
                "frequency": "annual",
                "unit": "filing",
                "source": "SEC EDGAR",
                "geography": "United States",
                "category": "filings",
            },
        ),
        CatalogEntry(
            namespace="sec",
            code="BANK_10Q_FILINGS",
            title="Large bank Form 10-Q quarterly reports",
            metadata={
                "description": "SEC quarterly filings for large banks including balance sheet and risk disclosures.",
                "frequency": "quarterly",
                "unit": "filing",
                "source": "SEC EDGAR",
                "geography": "United States",
                "category": "filings",
            },
        ),
        CatalogEntry(
            namespace="treasury",
            code="DAILY_TREASURY_YIELD_CURVE",
            title="Daily Treasury par yield curve rates",
            metadata={
                "description": "Daily U.S. Treasury yield curve interest rates by maturity.",
                "frequency": "daily",
                "unit": "percent",
                "source": "U.S. Treasury",
                "geography": "United States",
                "category": "rates",
            },
        ),
    ]


def _expanded_entries(total: int) -> list[CatalogEntry]:
    entries = list(_base_entries())
    filler_topics = [
        ("imports", "Monthly goods imports by end-use category", "trade flows and imported goods"),
        ("exports", "Monthly goods exports by destination", "trade flows and exported goods"),
        ("industrial_production", "Industrial production index", "manufacturing output and capacity"),
        ("capacity_utilization", "Capacity utilization", "factory utilization and industrial slack"),
        ("claims", "Initial unemployment insurance claims", "weekly jobless claims"),
        ("ppi", "Producer Price Index", "producer prices and wholesale inflation"),
        ("oil", "West Texas Intermediate crude oil price", "daily crude oil spot price"),
        ("gas", "Henry Hub natural gas spot price", "daily natural gas price"),
        ("credit", "Commercial bank credit", "weekly bank lending and credit aggregates"),
        ("deposits", "Commercial bank deposits", "weekly deposit liabilities at banks"),
        ("fx_jpy", "USD JPY foreign exchange rate", "daily dollar yen exchange rate"),
        ("fx_gbp", "GBP USD foreign exchange rate", "daily sterling dollar exchange rate"),
        ("wages", "Average hourly earnings", "monthly wage growth and earnings"),
        ("labor_force", "Civilian labor force participation rate", "monthly labor force participation"),
        ("construction", "Construction spending", "monthly construction spending by sector"),
        ("inventories", "Business inventories", "monthly inventories for manufacturers and retailers"),
        ("orders", "Durable goods orders", "monthly orders for durable manufactured goods"),
        ("sentiment", "Consumer sentiment index", "survey measure of household sentiment"),
        ("confidence", "Consumer confidence index", "survey measure of consumer confidence"),
        ("productivity", "Nonfarm business productivity", "quarterly labor productivity growth"),
    ]
    i = 0
    namespaces = ("fred", "sdmx", "fmp", "sec", "treasury")
    frequencies = ("daily", "weekly", "monthly", "quarterly", "annual")
    while len(entries) < total:
        slug, title, description = filler_topics[i % len(filler_topics)]
        namespace = namespaces[i % len(namespaces)]
        frequency = frequencies[i % len(frequencies)]
        geography = "United States" if i % 3 else "Euro area"
        entries.append(
            CatalogEntry(
                namespace=namespace,
                code=f"{slug.upper()}_{i:03d}",
                title=f"{title} ({geography}, synthetic {i})",
                metadata={
                    "description": f"{description}; synthetic catalog distractor {i}.",
                    "frequency": frequency,
                    "unit": "index",
                    "source": f"Synthetic {namespace.upper()} fixture",
                    "geography": geography,
                    "category": slug,
                },
            )
        )
        i += 1
    return entries[:total]


def _queries() -> list[QueryCase]:
    return [
        QueryCase(
            query="quarterly inflation adjusted US GDP growth",
            relevant={
                ("fred", "GDPC1"): 3,
                ("sdmx", "OECD_QNA_USA_B1_GE_CQR"): 2,
                ("fred", "GDP"): 1,
            },
        ),
        QueryCase(
            query="monthly unemployment rate labor market slack",
            relevant={
                ("fred", "UNRATE"): 3,
                ("sdmx", "ILO_UNE_DEU_M"): 2,
                ("fred", "PAYEMS"): 1,
            },
        ),
        QueryCase(
            query="consumer price inflation excluding food and energy",
            relevant={
                ("fred", "CPILFESL"): 3,
                ("fred", "CPIAUCSL"): 2,
                ("sdmx", "ECB_ICP_M_U2_N_000000_4_ANR"): 1,
            },
        ),
        QueryCase(
            query="daily 10 year treasury yield interest rate curve",
            relevant={
                ("fred", "DGS10"): 3,
                ("treasury", "DAILY_TREASURY_YIELD_CURVE"): 3,
                ("fred", "MORTGAGE30US"): 1,
            },
        ),
        QueryCase(
            query="daily euro dollar foreign exchange reference rate",
            relevant={
                ("sdmx", "ECB_EXR_D_USD_EUR_SP00_A"): 3,
            },
        ),
        QueryCase(
            query="Apple annual revenue net income SEC report",
            relevant={
                ("fmp", "AAPL_INCOME_STATEMENT_ANNUAL"): 3,
                ("sec", "AAPL_10K_FILINGS"): 2,
            },
        ),
        QueryCase(
            query="quarterly bank balance sheet SEC filings liabilities risk",
            relevant={
                ("sec", "BANK_10Q_FILINGS"): 3,
                ("fmp", "MSFT_BALANCE_SHEET_QUARTERLY"): 1,
            },
        ),
        QueryCase(
            query="monthly new residential construction housing starts",
            relevant={
                ("fred", "HOUST"): 3,
                ("fred", "MORTGAGE30US"): 1,
            },
        ),
    ]


def _lexical_score(query: str, entry: CatalogEntry) -> float:
    query_terms = _tokens(query)
    doc_terms = _tokens(_doc_text(entry))
    overlap = len(query_terms & doc_terms)
    phrase_bonus = sum(1 for term in query_terms if term in entry.title.lower()) * 0.25
    source_bias = {"fred": 0.30, "sdmx": 0.20, "fmp": 0.10, "sec": 0.05, "treasury": 0.05}.get(entry.namespace, 0.0)
    return float(overlap) + phrase_bonus + source_bias


def _tokens(text: str) -> set[str]:
    normalized = "".join(char.lower() if char.isalnum() else " " for char in text)
    return {token for token in normalized.split() if len(token) > 2}


def _candidates(query_case: QueryCase, entries: list[CatalogEntry], size: int) -> list[Candidate]:
    scored = sorted(entries, key=lambda entry: (-_lexical_score(query_case.query, entry), entry.namespace, entry.code))
    chosen = _ensure_relevant_present(scored[:size], scored, query_case.relevant, size=size)
    return [
        Candidate(entry=entry, baseline_rank=rank, baseline_score=1.0 / (rank + 1.0))
        for rank, entry in enumerate(chosen)
    ]


def _ensure_relevant_present(
    chosen: list[CatalogEntry],
    all_entries: list[CatalogEntry],
    relevant: dict[tuple[str, str], int],
    *,
    size: int,
) -> list[CatalogEntry]:
    present = {_identity(entry) for entry in chosen}
    relevant_entries = [
        entry for entry in all_entries if _identity(entry) in relevant and _identity(entry) not in present
    ]
    if not relevant_entries:
        return chosen
    out = list(chosen)
    for entry in relevant_entries:
        if len(out) >= size:
            out[-1] = entry
        else:
            out.append(entry)
    return out[:size]


def _score_pairs(model: Any, query: str, candidates: list[Candidate]) -> list[float]:
    pairs = [(query, _doc_text(candidate.entry)) for candidate in candidates]
    raw_scores = model.predict(pairs, show_progress_bar=False)
    return [float(score) for score in raw_scores]


def _rerank(model: Any, query_case: QueryCase, candidates: list[Candidate]) -> list[Candidate]:
    scores = _score_pairs(model, query_case.query, candidates)
    return [
        candidate
        for _score, candidate in sorted(
            zip(scores, candidates, strict=True),
            key=lambda item: (-item[0], item[1].entry.namespace, item[1].entry.code),
        )
    ]


def _ndcg_at_k(query_case: QueryCase, ranked: list[Candidate], *, k: int = 10) -> float:
    gains = [query_case.relevant.get(_identity(candidate.entry), 0) for candidate in ranked[:k]]
    dcg = sum((2**gain - 1) / math.log2(idx + 2) for idx, gain in enumerate(gains))
    ideal = sorted(query_case.relevant.values(), reverse=True)[:k]
    idcg = sum((2**gain - 1) / math.log2(idx + 2) for idx, gain in enumerate(ideal))
    return dcg / idcg if idcg else 0.0


def _mrr_at_k(query_case: QueryCase, ranked: list[Candidate], *, k: int = 10) -> float:
    for idx, candidate in enumerate(ranked[:k]):
        if query_case.relevant.get(_identity(candidate.entry), 0) > 0:
            return 1.0 / (idx + 1)
    return 0.0


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[idx]


def _run_size(
    model: Any,
    entries: list[CatalogEntry],
    query_cases: list[QueryCase],
    size: int,
    repeats: int,
) -> BenchmarkResult:
    latency_ms: list[float] = []
    baseline_ndcg: list[float] = []
    reranked_ndcg: list[float] = []
    baseline_mrr: list[float] = []
    reranked_mrr: list[float] = []

    for query_case in query_cases:
        candidates = _candidates(query_case, entries, size)
        baseline_ndcg.append(_ndcg_at_k(query_case, candidates))
        baseline_mrr.append(_mrr_at_k(query_case, candidates))

        reranked: list[Candidate] = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            reranked = _rerank(model, query_case, candidates)
            latency_ms.append((time.perf_counter() - t0) * 1000.0)

        reranked_ndcg.append(_ndcg_at_k(query_case, reranked))
        reranked_mrr.append(_mrr_at_k(query_case, reranked))

    mean_latency = _mean(latency_ms)
    return BenchmarkResult(
        size=size,
        mean_latency_ms=mean_latency,
        p95_latency_ms=_p95(latency_ms),
        docs_per_second=(size / (mean_latency / 1000.0)) if mean_latency > 0 else 0.0,
        baseline_ndcg=_mean(baseline_ndcg),
        reranked_ndcg=_mean(reranked_ndcg),
        baseline_mrr=_mean(baseline_mrr),
        reranked_mrr=_mean(reranked_mrr),
    )


def _load_cross_encoder(model_name: str, device: str | None) -> Any:
    try:
        from sentence_transformers import CrossEncoder
    except ImportError as exc:
        raise SystemExit(
            "sentence-transformers is required. Install it with:\n"
            "    uv sync --extra standard\n"
            "or:\n"
            "    pip install 'parsimony-core[standard]'"
        ) from exc
    kwargs: dict[str, str] = {}
    if device is not None:
        kwargs["device"] = device
    return CrossEncoder(model_name, **kwargs)


def _print_results(results: list[BenchmarkResult]) -> None:
    print()
    header = (
        "candidate_count  mean_ms  p95_ms  docs/sec  baseline_ndcg@10  "
        "reranked_ndcg@10  baseline_mrr@10  reranked_mrr@10"
    )
    print(header)
    for result in results:
        print(
            f"{result.size:15d}  "
            f"{result.mean_latency_ms:7.1f}  "
            f"{result.p95_latency_ms:6.1f}  "
            f"{result.docs_per_second:8.1f}  "
            f"{result.baseline_ndcg:16.3f}  "
            f"{result.reranked_ndcg:16.3f}  "
            f"{result.baseline_mrr:15.3f}  "
            f"{result.reranked_mrr:15.3f}"
        )


def _print_model_comparison(summaries: list[ModelSummary]) -> None:
    if len(summaries) <= 1:
        return

    print()
    print("model                                      load_s  mean_ndcg@10  mean_mrr@10  mean_docs/sec  mean_ms@100")
    for summary in sorted(summaries, key=lambda item: (-item.mean_ndcg, item.latency_100_ms or float("inf"))):
        latency_100 = summary.latency_100_ms
        latency_text = f"{latency_100:11.1f}" if latency_100 is not None else "        n/a"
        print(
            f"{summary.model[:42]:42s}  "
            f"{summary.load_seconds:6.1f}  "
            f"{summary.mean_ndcg:12.3f}  "
            f"{summary.mean_mrr:11.3f}  "
            f"{summary.mean_docs_per_second:13.1f}  "
            f"{latency_text}"
        )

    fastest = max(summaries, key=lambda item: item.mean_docs_per_second)
    best_quality = max(summaries, key=lambda item: (item.mean_ndcg, item.mean_mrr))
    near_best = [
        item
        for item in summaries
        if item.mean_ndcg >= best_quality.mean_ndcg - 0.002 and item.mean_mrr >= best_quality.mean_mrr - 0.002
    ]
    balanced = max(near_best, key=lambda item: item.mean_docs_per_second)
    print()
    print(f"Fastest:      {fastest.model}")
    print(f"Best quality: {best_quality.model}")
    print(f"Balanced:     {balanced.model} (within 0.002 nDCG/MRR of best quality, highest throughput)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=None, help=f"Single CrossEncoder model id. Default: {DEFAULT_MODEL}")
    parser.add_argument("--models", nargs="+", default=None, help="One or more CrossEncoder model ids to compare.")
    parser.add_argument(
        "--suite",
        action="store_true",
        help="Compare the built-in MiniLM reranker suite: L2, L4, L6, and L12.",
    )
    parser.add_argument("--device", default=None, help="Optional torch device, e.g. cpu, cuda, mps.")
    parser.add_argument("--repeats", type=int, default=3, help="Rerank repetitions per query and candidate size.")
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=list(DEFAULT_SIZES),
        help="Candidate counts to benchmark. Default: 10 50 100",
    )
    return parser.parse_args()


def _selected_models(args: argparse.Namespace) -> list[str]:
    if args.suite:
        return list(DEFAULT_MODEL_SUITE)
    if args.models is not None:
        return list(args.models)
    if args.model is not None:
        return [str(args.model)]
    return [DEFAULT_MODEL]


def main() -> None:
    args = parse_args()
    max_size = max(args.sizes)
    if args.repeats < 1:
        raise SystemExit("--repeats must be >= 1")
    if max_size < 1:
        raise SystemExit("--sizes must contain positive integers")

    entries = _expanded_entries(max_size)
    query_cases = _queries()
    summaries: list[ModelSummary] = []

    for model_name in _selected_models(args):
        print(f"Loading reranker: {model_name}")
        t0 = time.perf_counter()
        model = _load_cross_encoder(model_name, args.device)
        load_seconds = time.perf_counter() - t0
        print(f"Loaded in {load_seconds:.1f}s")

        # Warm up tokenizer/model kernels once so the measured loop is mostly rerank latency.
        _score_pairs(model, query_cases[0].query, _candidates(query_cases[0], entries, min(4, max_size)))

        results = [
            _run_size(model, entries, query_cases, size=size, repeats=args.repeats)
            for size in args.sizes
        ]
        summaries.append(ModelSummary(model=model_name, load_seconds=load_seconds, results=results))
        _print_results(results)

    _print_model_comparison(summaries)
    print()
    print("Quality is measured against hand-labeled relevant catalog entries using nDCG@10 and MRR@10.")
    print("Latency is per query candidate rerank after model load and one warm-up call.")


if __name__ == "__main__":
    main()
