"""Retrieval eval gate — recall@K against per-namespace golden queries.

Every plugin that ships a bundle ships a golden query suite (JSONL) and
a recall threshold. The publish flow runs the suite against the
freshly-built bundle; below threshold blocks the upload (caller's choice
of exit code in the CLI).

Wire format (one query per line)::

    {"query": "unemployment rate", "expected_codes": ["UNRATE"], "k": 5}
    {"query": "consumer price index", "expected_codes": ["CPI", "CPILFESL"]}

Fields:

- ``query`` (required): natural-language string passed to ``catalog.search``.
- ``expected_codes`` (required, non-empty): list of catalog codes that
  *must* appear in the top-K results for the query to count as a hit.
- ``k`` (optional, default ``5``): per-query top-K override.
- ``min_recall`` (optional): per-query floor (e.g., ``1.0`` for "exact
  match required"). The CLI's ``--min-recall`` is the aggregate floor;
  this is the per-query gate.

Output ``EvalResult.recall`` is ``|expected ∩ top_k| / |expected|`` —
the standard "did we retrieve every gold doc" recall, not precision. We
treat each expected_code equally; weighted recall is out of v1 scope.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from parsimony.catalog.models import SeriesMatch


class _SearchableCatalog(Protocol):
    """Minimal contract: anything with ``await search(...)`` returning
    :class:`SeriesMatch`. Both :class:`~parsimony.catalog.catalog.Catalog`
    and :class:`~parsimony.stores.hf_bundle.HFBundleCatalogStore` satisfy it.
    """

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
        query_embedding: list[float] | None = None,
    ) -> list[SeriesMatch]: ...


@dataclass(frozen=True)
class EvalQuery:
    """One golden query: text + the codes it MUST surface in top-K."""

    query: str
    expected_codes: tuple[str, ...]
    k: int = 5
    min_recall: float | None = None

    def __post_init__(self) -> None:
        if not self.query:
            raise ValueError("EvalQuery.query must be non-empty")
        if not self.expected_codes:
            raise ValueError("EvalQuery.expected_codes must be non-empty")
        if self.k <= 0:
            raise ValueError(f"EvalQuery.k must be positive, got {self.k}")
        if self.min_recall is not None and not (0.0 <= self.min_recall <= 1.0):
            raise ValueError(
                f"EvalQuery.min_recall must be in [0,1], got {self.min_recall}"
            )


@dataclass(frozen=True)
class EvalResult:
    """One query's outcome: which expected codes were retrieved."""

    query: str
    expected_codes: tuple[str, ...]
    top_codes: tuple[str, ...]
    recall: float

    @property
    def passed(self) -> bool:
        """``True`` iff at least one expected code surfaced in top-K.

        Use :func:`aggregate_recall` and a threshold for the actual gate;
        this is the per-query "any hit at all" indicator for the JSONL output.
        """
        return self.recall > 0.0


def load_queries(path: Path | str) -> list[EvalQuery]:
    """Parse a JSONL file of golden queries.

    Tolerant of blank lines and ``#``-prefixed comment lines (lines whose
    first non-whitespace character is ``#``); strict on every other line —
    a malformed entry raises rather than silently dropping a query.
    """
    p = Path(path)
    raw = p.read_text(encoding="utf-8")
    out: list[EvalQuery] = []
    for line_no, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{p}:{line_no}: invalid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"{p}:{line_no}: top-level value must be an object")
        try:
            query = EvalQuery(
                query=str(payload["query"]),
                expected_codes=tuple(str(c) for c in payload["expected_codes"]),
                k=int(payload.get("k", 5)),
                min_recall=(
                    float(payload["min_recall"]) if "min_recall" in payload else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{p}:{line_no}: invalid query: {exc}") from exc
        out.append(query)
    return out


async def run_eval(
    catalog: _SearchableCatalog,
    queries: Sequence[EvalQuery],
    *,
    namespace: str,
) -> list[EvalResult]:
    """Run every query against ``catalog`` restricted to ``namespace``.

    Each query uses its own ``k``; results preserve query order. Catalog
    failures (any exception) bubble up unchanged — the eval is meant to
    surface retrieval regressions, not catch unrelated outages.
    """
    out: list[EvalResult] = []
    for q in queries:
        matches = await catalog.search(q.query, limit=q.k, namespaces=[namespace])
        top_codes = tuple(m.code for m in matches)
        expected_set = set(q.expected_codes)
        hits = expected_set & set(top_codes)
        recall = len(hits) / len(expected_set)
        out.append(
            EvalResult(
                query=q.query,
                expected_codes=q.expected_codes,
                top_codes=top_codes,
                recall=recall,
            )
        )
    return out


def aggregate_recall(results: Sequence[EvalResult]) -> float:
    """Mean recall across ``results``. Empty input → ``0.0`` (defensive)."""
    if not results:
        return 0.0
    return sum(r.recall for r in results) / len(results)


def per_query_failures(
    results: Sequence[EvalResult],
    queries: Sequence[EvalQuery],
) -> list[EvalResult]:
    """Return the subset of ``results`` whose recall < query.min_recall.

    Queries without an explicit ``min_recall`` are not subject to a
    per-query floor — only the aggregate threshold (decided by the CLI).
    The two sequences must be in lockstep order (the same shape produced
    by :func:`run_eval`).
    """
    if len(results) != len(queries):
        raise ValueError(
            f"results ({len(results)}) and queries ({len(queries)}) length mismatch"
        )
    out: list[EvalResult] = []
    for r, q in zip(results, queries, strict=True):
        if q.min_recall is not None and r.recall < q.min_recall:
            out.append(r)
    return out


def format_summary(
    results: Sequence[EvalResult],
    *,
    namespace: str,
    threshold: float,
) -> dict[str, Any]:
    """Build a flat dict suitable for ``--json`` output and exit-code logic."""
    aggregate = aggregate_recall(results)
    return {
        "namespace": namespace,
        "queries": len(results),
        "recall_mean": round(aggregate, 4),
        "threshold": threshold,
        "passed": aggregate >= threshold,
        "results": [
            {
                "query": r.query,
                "expected_codes": list(r.expected_codes),
                "top_codes": list(r.top_codes),
                "recall": round(r.recall, 4),
            }
            for r in results
        ],
    }


__all__ = [
    "EvalQuery",
    "EvalResult",
    "aggregate_recall",
    "format_summary",
    "load_queries",
    "per_query_failures",
    "run_eval",
]
