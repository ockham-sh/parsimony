"""``iter_rows`` and ``multi_field_search``: one scan, both layouts, same answer.

The fixtures build the *same* population in both catalog layouts — one
row-indexed over entities, one value-indexed over a parquet mirror of those
entities — so a parity assertion compares only what the layouts do differently,
never a different corpus.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity, F
from parsimony.catalog.models import CatalogMatch, UnknownIndexedFieldError
from parsimony.errors import InvalidParameterError

ROWS: list[dict[str, str]] = [
    {"key": "A", "title": "Alpha monthly Germany", "FREQ_label": "Monthly", "REF_AREA_label": "Germany"},
    {"key": "B", "title": "Beta annual France", "FREQ_label": "Annual", "REF_AREA_label": "France"},
    {"key": "C", "title": "Gamma monthly France", "FREQ_label": "Monthly", "REF_AREA_label": "France"},
    {"key": "D", "title": "Delta annual Germany", "FREQ_label": "Annual", "REF_AREA_label": "Germany"},
    {"key": "E", "title": "Epsilon quarterly Italy", "FREQ_label": "Quarterly", "REF_AREA_label": "Italy"},
]

FACETS = {"FREQ_label": 1.0, "REF_AREA_label": 1.0}


def _entities() -> list[Entity]:
    return [
        Entity(
            namespace="demo",
            code=row["key"],
            title=row["title"],
            metadata={"FREQ_label": row["FREQ_label"], "REF_AREA_label": row["REF_AREA_label"]},
        )
        for row in ROWS
    ]


def _indexes() -> dict[str, BM25Index]:
    return {"title": BM25Index(), "FREQ_label": BM25Index(), "REF_AREA_label": BM25Index()}


def _memory_catalog() -> Catalog:
    catalog = Catalog("demo", indexes=_indexes())
    catalog.set_entities(_entities())
    catalog.build()
    return catalog


def _parquet_catalog(tmp_path: Path, rows: list[dict[str, Any]] | None = None) -> Catalog:
    from parsimony.catalog.contracts import CatalogBackendConfig

    parquet = tmp_path / "rows.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows if rows is not None else ROWS), parquet)
    catalog = _memory_catalog()
    catalog.attach_parquet_rows(
        parquet,
        config=CatalogBackendConfig(
            kind="parquet",
            rows_path="rows.parquet",
            namespace="demo",
            code_column="key",
            title_column="title",
        ),
    )
    return catalog


class _DatasetSpy:
    """Counts scans so the one-pass guarantee is asserted, not assumed."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.scanner_calls = 0

    @property
    def schema(self) -> Any:
        return self._delegate.schema

    def scanner(self, **kwargs: Any) -> Any:
        self.scanner_calls += 1
        return self._delegate.scanner(**kwargs)


def _spy_on(catalog: Catalog) -> _DatasetSpy:
    backend = catalog._backend  # noqa: SLF001
    assert backend is not None
    spy = _DatasetSpy(backend._dataset)  # noqa: SLF001
    backend._dataset = spy  # type: ignore[assignment]  # noqa: SLF001
    return spy


# ---------------------------------------------------------------------------
# iter_rows
# ---------------------------------------------------------------------------


def test_iter_rows_yields_native_rows_in_each_layout(tmp_path: Path) -> None:
    memory = list(_memory_catalog().iter_rows(filter={"REF_AREA_label": "Germany"}))
    assert {row["code"] for row in memory} == {"A", "D"}
    assert memory[0]["title"] == "Alpha monthly Germany"

    parquet = list(_parquet_catalog(tmp_path).iter_rows(filter={"REF_AREA_label": "Germany"}))
    assert {row["key"] for row in parquet} == {"A", "D"}


def test_iter_rows_projection_returns_requested_logical_names(tmp_path: Path) -> None:
    """``code`` is a logical name in both layouts, whatever column carries it."""
    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        rows = list(catalog.iter_rows(filter={"FREQ_label": "Quarterly"}, columns=["code", "title"]))
        assert rows == [{"code": "E", "title": "Epsilon quarterly Italy"}]


def test_iter_rows_selects_identical_row_sets_across_layouts(tmp_path: Path) -> None:
    predicate = F("FREQ_label").is_in(["Monthly", "Annual"]) & F("REF_AREA_label").eq("France")
    memory = list(_memory_catalog().iter_rows(filter=predicate, columns=["code"]))
    parquet = list(_parquet_catalog(tmp_path).iter_rows(filter=predicate, columns=["code"]))
    assert sorted(row["code"] for row in memory) == ["B", "C"]
    assert memory == parquet


def test_iter_rows_without_filter_returns_every_row(tmp_path: Path) -> None:
    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        assert len(list(catalog.iter_rows(columns=["code"]))) == len(ROWS)


def test_iter_rows_scans_parquet_once(tmp_path: Path) -> None:
    catalog = _parquet_catalog(tmp_path)
    spy = _spy_on(catalog)
    list(catalog.iter_rows(filter={"REF_AREA_label": "Germany"}))
    assert spy.scanner_calls == 1


def test_iter_rows_rejects_unknown_field_in_both_layouts(tmp_path: Path) -> None:
    with pytest.raises(InvalidParameterError, match="Unknown filter field"):
        list(_memory_catalog().iter_rows(filter={"nope": "x"}))
    with pytest.raises(InvalidParameterError, match="Unknown filter column"):
        list(_parquet_catalog(tmp_path).iter_rows(filter={"nope": "x"}))


def test_iter_rows_validates_eagerly() -> None:
    """A bad filter raises at the call, not on first iteration."""
    with pytest.raises(InvalidParameterError):
        _memory_catalog().iter_rows(filter={"REF_AREA_label": []})


def test_iter_rows_rejects_empty_projection() -> None:
    with pytest.raises(InvalidParameterError, match="at least one column"):
        _memory_catalog().iter_rows(columns=[])


# ---------------------------------------------------------------------------
# multi_field_search
# ---------------------------------------------------------------------------


def test_multi_field_search_prefers_rows_agreeing_on_more_facets(tmp_path: Path) -> None:
    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        matches = catalog.multi_field_search("monthly germany", fields=FACETS, limit=5)
        assert matches[0].code == "A", "the row matching both facets must win"
        assert {match.code for match in matches[1:]} <= {"C", "D"}
        assert all(match.score > 0.0 for match in matches)


def test_multi_field_search_ranks_identically_across_layouts(tmp_path: Path) -> None:
    memory = _memory_catalog().multi_field_search("monthly france", fields=FACETS, limit=5)
    parquet = _parquet_catalog(tmp_path).multi_field_search("monthly france", fields=FACETS, limit=5)
    assert [match.code for match in memory] == [match.code for match in parquet]
    assert [round(match.score, 12) for match in memory] == [round(match.score, 12) for match in parquet]


def test_multi_field_search_preserves_the_full_match_payload(tmp_path: Path) -> None:
    """Projecting for speed must not cost title, metadata, or identity."""
    memory = _memory_catalog().multi_field_search("monthly germany", fields=FACETS, limit=1)[0]
    parquet = _parquet_catalog(tmp_path).multi_field_search("monthly germany", fields=FACETS, limit=1)[0]
    for match in (memory, parquet):
        assert isinstance(match, CatalogMatch)
        assert (match.namespace, match.code) == ("demo", "A")
        assert match.title == "Alpha monthly Germany"
        assert match.metadata["FREQ_label"] == "Monthly"
        assert match.metadata["REF_AREA_label"] == "Germany"
        assert match.search_detail is not None
        assert {c.kind for f in match.search_detail.fields for c in f.components} == {"bm25"}


def test_multi_field_search_scans_candidate_rows_once(tmp_path: Path) -> None:
    catalog = _parquet_catalog(tmp_path)
    spy = _spy_on(catalog)
    catalog.multi_field_search("monthly germany", fields=FACETS, limit=5)
    assert spy.scanner_calls == 1, "candidates pool by distinct value, so one row scan serves every field"


def test_multi_field_search_with_field_links_still_scans_once(tmp_path: Path) -> None:
    """Value scoring must not go through the link-resolving public helper."""
    catalog = _parquet_catalog(tmp_path)
    catalog._field_links = {"FREQ_label": "key", "REF_AREA_label": "key"}  # noqa: SLF001
    spy = _spy_on(catalog)
    catalog.multi_field_search("monthly germany", fields=FACETS, limit=5)
    assert spy.scanner_calls == 1


def test_multi_field_search_ands_the_caller_filter(tmp_path: Path) -> None:
    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        matches = catalog.multi_field_search(
            "monthly germany", fields=FACETS, filter={"REF_AREA_label": "France"}, limit=5
        )
        assert [match.code for match in matches] == ["C"]


def test_multi_field_search_honours_limit_by_rank(tmp_path: Path) -> None:
    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        full = catalog.multi_field_search("monthly germany", fields=FACETS, limit=5)
        assert len(full) > 2
        capped = catalog.multi_field_search("monthly germany", fields=FACETS, limit=2)
        assert [match.code for match in capped] == [match.code for match in full[:2]]


def test_multi_field_search_orders_ties_by_namespace_then_code(tmp_path: Path) -> None:
    """Equal scores must not depend on scan order."""
    catalog = _memory_catalog()
    matches = catalog.multi_field_search("annual", fields={"FREQ_label": 1.0}, limit=5)
    tied = [match.code for match in matches if match.score == matches[0].score]
    assert tied == sorted(tied)


def test_multi_field_search_keeps_equal_codes_in_different_namespaces(tmp_path: Path) -> None:
    rows = [
        {"namespace": "left", "key": "SHARED", "title": "Alpha monthly Germany", "REF_AREA_label": "Germany"},
        {"namespace": "right", "key": "SHARED", "title": "Beta monthly Germany", "REF_AREA_label": "Germany"},
    ]
    catalog = _parquet_catalog(tmp_path, rows=rows)
    matches = catalog.multi_field_search("germany", fields={"REF_AREA_label": 1.0}, limit=5)
    assert [(match.namespace, match.code) for match in matches] == [("left", "SHARED"), ("right", "SHARED")]


def test_multi_field_search_weights_shift_the_ranking(tmp_path: Path) -> None:
    catalog = _parquet_catalog(tmp_path)
    area_led = catalog.multi_field_search("monthly italy", fields={"FREQ_label": 0.1, "REF_AREA_label": 10.0}, limit=1)
    freq_led = catalog.multi_field_search("monthly italy", fields={"FREQ_label": 10.0, "REF_AREA_label": 0.1}, limit=1)
    assert area_led[0].code == "E"
    assert freq_led[0].code in {"A", "C"}


def test_multi_field_search_returns_empty_when_nothing_scores(tmp_path: Path) -> None:
    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        assert catalog.multi_field_search("zzzz nonexistent", fields=FACETS, limit=5) == []


def test_multi_field_search_survives_a_field_with_no_positive_scores(tmp_path: Path) -> None:
    """One silent field must not zero the whole surface."""
    catalog = _parquet_catalog(tmp_path)
    matches = catalog.multi_field_search("germany", fields=FACETS, limit=5)
    assert {match.code for match in matches} == {"A", "D"}


def test_multi_field_search_candidate_values_caps_the_value_table(tmp_path: Path) -> None:
    catalog = _parquet_catalog(tmp_path)
    wide = catalog.multi_field_search("monthly germany", fields=FACETS, candidate_values=50, limit=5)
    narrow = catalog.multi_field_search("monthly germany", fields=FACETS, candidate_values=1, limit=5)
    assert len(narrow) <= len(wide)
    assert narrow[0].code == wide[0].code


def test_multi_field_search_scores_are_finite(tmp_path: Path) -> None:
    import math

    for catalog in (_memory_catalog(), _parquet_catalog(tmp_path)):
        for match in catalog.multi_field_search("monthly germany", fields=FACETS, limit=5):
            assert math.isfinite(match.score)


@pytest.mark.parametrize("weight", [0.0, -1.0, float("nan"), float("inf")])
def test_multi_field_search_rejects_non_positive_finite_weights(weight: float) -> None:
    with pytest.raises(InvalidParameterError, match="positive finite number"):
        _memory_catalog().multi_field_search("monthly", fields={"FREQ_label": weight}, limit=5)


def test_multi_field_search_rejects_unknown_field() -> None:
    with pytest.raises(UnknownIndexedFieldError):
        _memory_catalog().multi_field_search("monthly", fields={"nope": 1.0}, limit=5)


@pytest.mark.parametrize("query", ["", "   "])
def test_multi_field_search_requires_a_query(query: str) -> None:
    with pytest.raises(InvalidParameterError, match="non-empty query"):
        _memory_catalog().multi_field_search(query, fields=FACETS, limit=5)


@pytest.mark.parametrize("query", ["", "   "])
def test_search_rejects_blank_query_without_filter(query: str) -> None:
    """Blank query is not a filter-only read — that path requires omitting query."""
    with pytest.raises(InvalidParameterError, match="query= and/or filter="):
        _memory_catalog().search(query, limit=5)


def test_search_blank_query_with_filter_enumerates() -> None:
    matches = _memory_catalog().search("", filter={"FREQ_label": "Monthly"}, limit=10)
    assert {m.code for m in matches} == {"A", "C"}


def test_multi_field_search_requires_a_field() -> None:
    with pytest.raises(InvalidParameterError, match="at least one field weight"):
        _memory_catalog().multi_field_search("monthly", fields={}, limit=5)


@pytest.mark.parametrize("kwargs", [{"limit": 0}, {"candidate_values": 0}])
def test_multi_field_search_rejects_non_positive_bounds(kwargs: dict[str, int]) -> None:
    with pytest.raises(InvalidParameterError, match="at least 1"):
        _memory_catalog().multi_field_search("monthly", fields=FACETS, **kwargs)  # type: ignore[arg-type]
