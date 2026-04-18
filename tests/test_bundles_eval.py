"""Unit tests for :mod:`parsimony.bundles.eval` (recall@K gate)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parsimony.bundles.eval import (
    EvalQuery,
    EvalResult,
    aggregate_recall,
    format_summary,
    load_queries,
    per_query_failures,
    run_eval,
)
from parsimony.catalog.models import SeriesMatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _match(code: str, similarity: float = 0.9, namespace: str = "snb") -> SeriesMatch:
    return SeriesMatch(
        namespace=namespace,
        code=code,
        title=f"{code} title",
        similarity=similarity,
    )


class _StubCatalog:
    """Returns canned matches keyed by query string."""

    def __init__(self, responses: dict[str, list[SeriesMatch]]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, int, list[str] | None]] = []

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: list[str] | None = None,
        query_embedding: list[float] | None = None,
    ) -> list[SeriesMatch]:
        self.calls.append((query, limit, namespaces))
        return self._responses.get(query, [])[:limit]


# ---------------------------------------------------------------------------
# EvalQuery validation
# ---------------------------------------------------------------------------


class TestEvalQuery:
    def test_minimal_construction(self) -> None:
        q = EvalQuery(query="foo", expected_codes=("X",))
        assert q.query == "foo"
        assert q.expected_codes == ("X",)
        assert q.k == 5
        assert q.min_recall is None

    def test_empty_query_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            EvalQuery(query="", expected_codes=("X",))

    def test_empty_expected_codes_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            EvalQuery(query="foo", expected_codes=())

    def test_non_positive_k_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            EvalQuery(query="foo", expected_codes=("X",), k=0)

    def test_min_recall_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"\[0,1\]"):
            EvalQuery(query="foo", expected_codes=("X",), min_recall=1.5)


# ---------------------------------------------------------------------------
# load_queries
# ---------------------------------------------------------------------------


class TestLoadQueries:
    def test_jsonl_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "q.jsonl"
        path.write_text(
            json.dumps({"query": "a", "expected_codes": ["X"], "k": 3}) + "\n"
            + json.dumps({"query": "b", "expected_codes": ["Y", "Z"]}) + "\n",
            encoding="utf-8",
        )
        qs = load_queries(path)
        assert len(qs) == 2
        assert qs[0].query == "a"
        assert qs[0].k == 3
        assert qs[1].expected_codes == ("Y", "Z")
        assert qs[1].k == 5  # default

    def test_skips_blank_and_comment_lines(self, tmp_path: Path) -> None:
        path = tmp_path / "q.jsonl"
        path.write_text(
            "# header comment\n\n"
            + json.dumps({"query": "a", "expected_codes": ["X"]}) + "\n"
            + "  # indented comment\n",
            encoding="utf-8",
        )
        qs = load_queries(path)
        assert len(qs) == 1

    def test_invalid_json_raises_with_line_no(self, tmp_path: Path) -> None:
        path = tmp_path / "q.jsonl"
        path.write_text(
            json.dumps({"query": "a", "expected_codes": ["X"]}) + "\n"
            + "{not json}\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match=":2:"):
            load_queries(path)

    def test_missing_required_field_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "q.jsonl"
        path.write_text(json.dumps({"query": "a"}) + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match="invalid query"):
            load_queries(path)


# ---------------------------------------------------------------------------
# run_eval + aggregate_recall
# ---------------------------------------------------------------------------


class TestRunEval:
    @pytest.mark.asyncio
    async def test_perfect_recall(self) -> None:
        catalog = _StubCatalog({"q1": [_match("X"), _match("Y"), _match("Z")]})
        results = await run_eval(
            catalog,
            [EvalQuery(query="q1", expected_codes=("X", "Y"), k=3)],
            namespace="snb",
        )
        assert len(results) == 1
        assert results[0].recall == 1.0
        # Catalog received the right call shape.
        assert catalog.calls == [("q1", 3, ["snb"])]

    @pytest.mark.asyncio
    async def test_partial_recall(self) -> None:
        catalog = _StubCatalog({"q1": [_match("X"), _match("Z")]})
        results = await run_eval(
            catalog,
            [EvalQuery(query="q1", expected_codes=("X", "Y"), k=2)],
            namespace="snb",
        )
        assert results[0].recall == 0.5

    @pytest.mark.asyncio
    async def test_zero_recall(self) -> None:
        catalog = _StubCatalog({"q1": [_match("Z")]})
        results = await run_eval(
            catalog,
            [EvalQuery(query="q1", expected_codes=("X",), k=5)],
            namespace="snb",
        )
        assert results[0].recall == 0.0
        assert results[0].passed is False

    @pytest.mark.asyncio
    async def test_per_query_k_respected(self) -> None:
        # Catalog returns 5 matches; k=2 must truncate via the limit param.
        catalog = _StubCatalog({"q1": [_match(c) for c in ["A", "B", "C", "D", "E"]]})
        await run_eval(
            catalog, [EvalQuery(query="q1", expected_codes=("A",), k=2)], namespace="snb"
        )
        assert catalog.calls[0] == ("q1", 2, ["snb"])


# ---------------------------------------------------------------------------
# aggregate_recall + per_query_failures + format_summary
# ---------------------------------------------------------------------------


class TestAggregates:
    def test_aggregate_recall_mean(self) -> None:
        results = [
            EvalResult("q1", ("X",), ("X",), 1.0),
            EvalResult("q2", ("Y",), (), 0.0),
            EvalResult("q3", ("Z", "W"), ("Z",), 0.5),
        ]
        assert aggregate_recall(results) == pytest.approx(0.5)

    def test_aggregate_recall_empty(self) -> None:
        assert aggregate_recall([]) == 0.0

    def test_per_query_failures_uses_min_recall(self) -> None:
        queries = [
            EvalQuery(query="q1", expected_codes=("X",), min_recall=1.0),
            EvalQuery(query="q2", expected_codes=("Y",)),  # no floor
        ]
        results = [
            EvalResult("q1", ("X",), (), 0.0),  # floor violated
            EvalResult("q2", ("Y",), (), 0.0),  # no floor → not flagged
        ]
        failed = per_query_failures(results, queries)
        assert len(failed) == 1
        assert failed[0].query == "q1"

    def test_per_query_failures_length_mismatch(self) -> None:
        with pytest.raises(ValueError, match="length mismatch"):
            per_query_failures([], [EvalQuery(query="q", expected_codes=("X",))])

    def test_format_summary_passed_flag(self) -> None:
        results = [EvalResult("q1", ("X",), ("X",), 1.0)]
        summary = format_summary(results, namespace="snb", threshold=0.8)
        assert summary["passed"] is True
        assert summary["recall_mean"] == 1.0
        assert summary["queries"] == 1

    def test_format_summary_below_threshold(self) -> None:
        results = [EvalResult("q1", ("X",), (), 0.0)]
        summary = format_summary(results, namespace="snb", threshold=0.8)
        assert summary["passed"] is False
