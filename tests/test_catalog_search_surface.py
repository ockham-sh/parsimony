"""Coverage-ranked search + multi-field surface (``fields=``) — issue #69.

The ranking contract under test: results order by (coverage desc, fuzzy score
desc), where *coverage* is the fraction of the query's tokens consumed by the
union of the row's fully-consumed field values. Consequences:

* An exact value hit is coverage 1.0 and ranks FIRST, while fuzzy near-misses
  stay visible below it (the ANR/AVR false-friend class: the agent must see the
  neighbor to know it exists). No sentinel scores — ``score`` stays honest.
* Value-level term repetition cannot outrank the consumed value ("Current
  account, Current transfers" repeats "current" but gates to coverage 0).
* ``Catalog.search(query, fields=[...])`` unions consumed tokens across the
  named fields, so a verbose query decomposes over dimension labels without a
  rebuild.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog.contracts import CatalogBackendConfig
from parsimony.catalog.indexes import HybridIndex
from parsimony.catalog.models import UnknownIndexedFieldError
from parsimony.errors import InvalidParameterError

# The BOP shape: an aggregate whose ITEM label is exactly the natural query,
# buried behind children that repeat the parent phrase in their titles and
# labels; plus the ANR/AVR false-friend pair on ITEM labels.
_ROWS = [
    {
        "key": "AGG",
        "title": "Balance of payments, Current account, Total",
        "ITEM_label": "Current account",
        "REF_AREA_label": "Euro area",
    },
    {
        "key": "GOODS",
        "title": "Balance of payments, Current account, Goods",
        "ITEM_label": "Current account - Goods",
        "REF_AREA_label": "Euro area",
    },
    {
        "key": "SERV",
        "title": "Balance of payments, Current account, Services",
        "ITEM_label": "Current account - Services",
        "REF_AREA_label": "Euro area",
    },
    {
        "key": "TRANSF",
        "title": "Balance of payments, Current account, Current transfers",
        "ITEM_label": "Current account, Current transfers",
        "REF_AREA_label": "Euro area",
    },
    {
        "key": "ANR",
        "title": "HICP inflation, Annual rate of change, Germany",
        "ITEM_label": "Annual rate of change",
        "REF_AREA_label": "Germany",
    },
    {
        "key": "AVR",
        "title": "HICP inflation, Annual average rate of change, Germany",
        "ITEM_label": "Annual average rate of change",
        "REF_AREA_label": "Germany",
    },
]


def _entities() -> list[Entity]:
    return [
        Entity(
            namespace="demo",
            code=row["key"],
            title=row["title"],
            metadata={"ITEM_label": row["ITEM_label"], "REF_AREA_label": row["REF_AREA_label"]},
        )
        for row in _ROWS
    ]


def _build_catalog(tmp_path: Path, *, hybrid_item_index: bool = False) -> Catalog:
    parquet = tmp_path / "rows.parquet"
    pq.write_table(pa.Table.from_pylist(_ROWS), parquet)
    item_index = HybridIndex(components=[BM25Index()]) if hybrid_item_index else BM25Index()
    catalog = Catalog(
        "demo",
        indexes={
            "title": BM25Index(),
            "ITEM_label": item_index,
            "REF_AREA_label": BM25Index(),
        },
        default_field="title",
    )
    catalog.set_entities(_entities())
    catalog.build()
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


def _build_memory_catalog() -> Catalog:
    catalog = Catalog(
        "demo",
        indexes={
            "title": BM25Index(),
            "ITEM_label": BM25Index(),
        },
        default_field="title",
    )
    catalog.set_entities(_entities())
    catalog.build()
    return catalog


class TestCoverageRanksExactFirstWithoutSuppression:
    def test_exact_hit_is_coverage_one_and_keeps_near_misses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Annual rate of change", fields="ITEM_label", limit=10)
            codes = [m.code for m in matches]
            assert codes[0] == "ANR"
            assert matches[0].coverage == 1.0
            # The false friend must be visible below the exact hit, not suppressed.
            assert "AVR" in codes[1:]
            avr = next(m for m in matches if m.code == "AVR")
            assert avr.coverage < 1.0

    def test_term_repetition_cannot_outrank_the_consumed_value(self) -> None:
        """ "Current account, Current transfers" repeats query tokens but gates to 0."""
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Current account", fields="ITEM_label", limit=10)
            codes = [m.code for m in matches]
            assert codes[0] == "AGG"
            assert matches[0].coverage == 1.0
            assert "TRANSF" in codes[1:]

    def test_limit_still_bounds_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Annual rate of change", fields="ITEM_label", limit=1)
            assert [m.code for m in matches] == ["ANR"]

    def test_scores_stay_honest_no_sentinel_magnitudes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Current account", fields=["title", "ITEM_label"], limit=10)
            assert matches
            assert all(m.score < 1_000.0 for m in matches)
            assert all(0.0 <= m.coverage <= 1.0 for m in matches)

    def test_hybrid_index_composes_coverage_and_fuzzy_band(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp), hybrid_item_index=True)
            matches = catalog.search("Current account", fields="ITEM_label", limit=10)
            codes = [m.code for m in matches]
            assert codes[0] == "AGG"
            assert matches[0].coverage == 1.0
            assert {"GOODS", "SERV"} <= set(codes[1:])
            assert all(m.coverage < 1.0 for m in matches[1:])

    def test_dsl_clause_resolves_with_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("ITEM_label: Current account", limit=10)
            assert matches[0].code == "AGG"
            assert matches[0].coverage == 1.0


class TestMultiFieldSearchSurface:
    def test_fields_fuses_surfaces_and_consumed_label_pins_rank_one(self) -> None:
        """The bare-query BOP repro: composed titles bury the aggregate, but the
        aggregate's ITEM label is fully consumed — the fused surface must surface it."""
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Current account", fields=["title", "ITEM_label"], limit=10)
            codes = [m.code for m in matches]
            assert codes[0] == "AGG"
            assert matches[0].coverage == 1.0
            # Title-only candidates still participate in the fused ranking.
            assert {"GOODS", "SERV"} <= set(codes[1:])

    def test_coverage_unions_tokens_across_fields(self) -> None:
        """A verbose query decomposes: ITEM consumes "current account", REF_AREA
        consumes "euro area" — the union grades the aggregate above its children."""
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search(
                "current account euro area",
                fields=["title", "ITEM_label", "REF_AREA_label"],
                limit=10,
            )
            assert matches[0].code == "AGG"
            assert matches[0].coverage == 1.0
            goods = next(m for m in matches if m.code == "GOODS")
            assert goods.coverage == 0.5  # only "euro area" consumed
            anr = next((m for m in matches if m.code == "ANR"), None)
            assert anr is None or anr.coverage == 0.0

    def test_fields_deduplicates_rows_matched_via_several_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Current account", fields=["title", "ITEM_label"], limit=10)
            codes = [m.code for m in matches]
            assert len(codes) == len(set(codes))

    def test_fields_composes_with_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search(
                "annual rate",
                fields=["title", "ITEM_label"],
                filter={"REF_AREA_label": ["Germany"]},
                limit=10,
            )
            assert {m.code for m in matches} <= {"ANR", "AVR"}
            assert matches

    def test_fields_works_on_memory_backend(self) -> None:
        catalog = _build_memory_catalog()
        matches = catalog.search("Current account", fields=["title", "ITEM_label"], limit=10)
        assert [m.code for m in matches][0] == "AGG"
        assert matches[0].coverage == 1.0

    def test_fields_query_is_literal_not_dsl(self) -> None:
        """With a declared surface the query is text, never structured DSL."""
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("ITEM_label: Current account", fields=["title"], limit=10)
            # A DSL interpretation would resolve the exact ITEM_label hit to
            # coverage 1.0; the literal text must not.
            assert all(m.coverage < 1.0 for m in matches)

    def test_fields_requires_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            with pytest.raises(InvalidParameterError, match="fields="):
                catalog.search(fields=["title"], filter={"REF_AREA_label": ["Germany"]})

    def test_fields_accepts_a_single_string(self) -> None:
        """A bare string is a one-field surface — same results as a one-item list."""
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            via_str = catalog.search("Current account", fields="ITEM_label", limit=10)
            via_list = catalog.search("Current account", fields=["ITEM_label"], limit=10)
            assert [m.code for m in via_str] == [m.code for m in via_list]
            assert via_str[0].code == "AGG"

    def test_fields_must_not_be_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            with pytest.raises(InvalidParameterError, match="at least one"):
                catalog.search("x", fields=[])

    def test_fields_unknown_field_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            with pytest.raises(UnknownIndexedFieldError, match="NOPE"):
                catalog.search("x", fields=["title", "NOPE"])


class TestSearchValuesCoverage:
    def test_exact_value_ranks_first_with_coverage_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            values = catalog.search_values("Annual rate of change", "ITEM_label", limit=5)
            assert values[0].value == "Annual rate of change"
            assert values[0].coverage == 1.0
            texts = [v.value for v in values]
            assert "Annual average rate of change" in texts[1:]
