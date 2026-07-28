"""Single-field ``search()`` and value exactness, on the SDMX-shaped fixture.

Weighted multi-field composition has its own module
(``test_catalog_composable_search.py``); what is pinned here is the narrower
contract callers reach first:

* ``search(query, field=...)`` scores exactly one index and returns full matches.
* ``search_values`` marks the value the query literally names as ``exact`` and
  ranks it first, while keeping near-misses visible below it — the ANR/AVR
  false-friend class, where an agent must see the neighbour to know it exists.
* Nothing tiers rows behind the caller's back: a row's rank comes from its score
  and its identity, so a caller that wants a domain fact ranked above relevance
  must say so itself.
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
    catalog = Catalog("demo", indexes={"title": BM25Index(), "ITEM_label": BM25Index()})
    catalog.set_entities(_entities())
    catalog.build()
    return catalog


class TestSingleFieldSearch:
    def test_field_scores_one_index_and_keeps_near_misses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Annual rate of change", field="ITEM_label", limit=10)
            codes = [m.code for m in matches]
            assert codes[0] == "ANR"
            # The false friend must be visible below the top hit, not suppressed.
            assert "AVR" in codes[1:]

    def test_matches_carry_the_full_row_payload(self) -> None:
        """Ranking is not allowed to cost the caller its data.

        A match must arrive complete — identity, title and the row's metadata —
        because the projection that makes one scan cheap is exactly where fields
        get silently dropped.
        """
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            match = catalog.search("Current account", field="ITEM_label", limit=1)[0]
            assert (match.namespace, match.code) == ("demo", "AGG")
            assert match.title == "Balance of payments, Current account, Total"
            assert match.metadata["ITEM_label"] == "Current account"
            assert match.metadata["REF_AREA_label"] == "Euro area"

    def test_limit_bounds_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Annual rate of change", field="ITEM_label", limit=1)
            assert [m.code for m in matches] == ["ANR"]

    def test_scores_stay_honest_no_sentinel_magnitudes(self) -> None:
        """One field weighs 1.0, so a score is a normalized similarity in [0, 1].

        No magic magnitude may be used to force an ordering — a pinned row would
        show up here as a score far outside the normalized band.
        """
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("Current account", field="ITEM_label", limit=10)
            assert matches
            assert all(0.0 < m.score <= 1.0 for m in matches)

    def test_hybrid_index_search_ranks_and_labels_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp), hybrid_item_index=True)
            matches = catalog.search("Current account", field="ITEM_label", limit=10)
            codes = [m.code for m in matches]
            assert {"AGG", "GOODS", "SERV"} <= set(codes)
            assert all(m.search_detail is not None for m in matches)
            assert all(
                {c.kind for f in m.search_detail.fields for c in f.components} <= {"bm25", "vector"} for m in matches
            )

    def test_default_field_is_used_when_none_is_named(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            assert catalog.search("Goods", limit=10)[0].code == "GOODS"

    def test_works_identically_on_the_memory_layout(self) -> None:
        catalog = _build_memory_catalog()
        matches = catalog.search("Current account", field="ITEM_label", limit=10)
        assert matches[0].code == "AGG"

    def test_composes_with_a_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            matches = catalog.search("annual rate", field="ITEM_label", filter={"REF_AREA_label": "Germany"}, limit=10)
            assert matches
            assert {m.code for m in matches} <= {"ANR", "AVR"}

    def test_unknown_field_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            with pytest.raises(UnknownIndexedFieldError, match="NOPE"):
                catalog.search("x", field="NOPE")


class TestSearchValuesExactness:
    def test_exact_value_ranks_first_and_is_marked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            values = catalog.search_values("Annual rate of change", "ITEM_label", limit=5)
            assert values[0].value == "Annual rate of change"
            assert values[0].exact is True
            texts = [v.value for v in values]
            assert "Annual average rate of change" in texts[1:]
            assert all(v.exact is False for v in values[1:])

    def test_exactness_is_case_and_whitespace_insensitive_only(self) -> None:
        """Equality, nothing softer.

        Case and surrounding whitespace are spelling, not content, so they must not
        defeat an exact hit. A value that adds a token, however, names a different
        concept and must stay a guess — grading how nearly it matched would put a
        fact's authority behind a fuzzy result.
        """
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _build_catalog(Path(tmp))
            loose = catalog.search_values("  annual RATE of change ", "ITEM_label", limit=5)
            assert loose[0].value == "Annual rate of change"
            assert loose[0].exact is True

            partial = catalog.search_values("Annual rate", "ITEM_label", limit=5)
            assert all(v.exact is False for v in partial)
