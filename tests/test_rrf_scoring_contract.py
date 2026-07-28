"""Additional coverage for hierarchical RRF scoring and scalar-field contracts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity, HybridIndex, VectorIndex
from parsimony.catalog.indexes import IndexBuildContext, QueryContext, search_index_values
from parsimony.embedder import EmbedderInfo


class _CountingEmbedder:
    def __init__(self) -> None:
        self.query_calls = 0
        self._info = EmbedderInfo(model="count", dim=2, normalize=False, package="test")

    def info(self) -> EmbedderInfo:
        return self._info

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if texts == ["shared query"]:
            self.query_calls += 1
            return [[1.0, 0.0]]
        return [[0.0, 1.0] if "other" in text else [1.0, 0.0] for text in texts]


def test_query_context_embeds_once_across_shared_embedder_fields() -> None:
    embedder = _CountingEmbedder()
    entries = [
        Entity(namespace="ns", code="A", title="alpha", metadata={"label": "alpha"}),
        Entity(namespace="ns", code="B", title="other", metadata={"label": "other"}),
    ]
    catalog = Catalog(
        "demo",
        indexes={
            "title": VectorIndex(embedder=embedder),
            "label": VectorIndex(embedder=embedder),
        },
    )
    catalog.set_entities(entries)
    catalog.build()

    matches = catalog.multi_field_search("shared query", fields={"title": 1.0, "label": 1.0}, limit=5)
    assert matches
    assert matches[0].score == pytest.approx(1.0)
    assert embedder.query_calls == 1


def test_search_values_and_multi_field_share_relevance_order() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(
        [
            Entity(namespace="ns", code="A", title="gross domestic product", metadata={}),
            Entity(namespace="ns", code="B", title="domestic product", metadata={}),
            Entity(namespace="ns", code="C", title="unrelated", metadata={}),
        ]
    )
    catalog.build()

    values = catalog.search_values("domestic product", "title", limit=10)
    rows = catalog.multi_field_search("domestic product", fields={"title": 1.0}, limit=10)
    assert values[0].score == pytest.approx(1.0)
    assert rows[0].score == pytest.approx(1.0)
    assert values[0].value == rows[0].title


def test_exact_tokenless_value_reports_relevance_one() -> None:
    catalog = Catalog("demo", indexes={"code": BM25Index()})
    catalog.set_entities(
        [
            Entity(namespace="ns", code="-", title="dash", metadata={}),
            Entity(namespace="ns", code="M", title="monthly", metadata={}),
        ]
    )
    catalog.build()

    values = catalog.search_values("-", "code", limit=5)
    assert values[0].value == "-"
    assert values[0].exact is True
    assert values[0].score == pytest.approx(1.0)


def test_exact_survives_candidate_values_of_one() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(
        [
            Entity(namespace="ns", code="A", title="exact title", metadata={}),
            Entity(namespace="ns", code="B", title="noise one", metadata={}),
            Entity(namespace="ns", code="C", title="noise two", metadata={}),
        ]
    )
    catalog.build()
    rows = catalog.multi_field_search("exact title", fields={"title": 1.0}, limit=5, candidate_values=1)
    assert [row.code for row in rows] == ["A"]
    assert rows[0].score == pytest.approx(1.0)


def test_single_component_hybrid_scores_lexically() -> None:
    entries = [
        Entity(namespace="ns", code="A", title="Germany GDP", metadata={}),
        Entity(namespace="ns", code="B", title="France CPI", metadata={}),
    ]
    hybrid = HybridIndex(components=[BM25Index()])
    hybrid.build(entries, ctx=IndexBuildContext(field="title", vector_cache={}))
    scored = search_index_values(hybrid, QueryContext(query="Germany"), limit=5)
    assert scored[0].text == "Germany GDP"
    assert {c.kind for c in scored[0].components} == {"bm25"}
    assert scored[0].relevance == pytest.approx(1.0)


def test_default_indexes_skip_nested_metadata_keys() -> None:
    catalog = Catalog("demo")  # indexes=None → default policy
    catalog.set_entities(
        [
            Entity(
                namespace="ns",
                code="A",
                title="alpha",
                metadata={"freq": "M", "tags": ["a", "b"]},
            )
        ]
    )
    catalog.build()
    assert "freq" in catalog._indexes
    assert "tags" not in catalog._indexes
    assert catalog.search("alpha", limit=5)


def test_default_indexes_skip_bool_metadata_keys() -> None:
    catalog = Catalog("demo")
    catalog.set_entities(
        [
            Entity(
                namespace="ns",
                code="A",
                title="alpha",
                metadata={"freq": "M", "seasonal": True},
            )
        ]
    )
    catalog.build()
    assert "freq" in catalog._indexes
    assert "seasonal" not in catalog._indexes
    assert {m.code for m in catalog.search(filter={"seasonal": True})} == {"A"}


def test_nested_indexed_metadata_fails_at_build() -> None:
    catalog = Catalog("demo", indexes={"tags": BM25Index()})
    catalog.set_entities([Entity(namespace="ns", code="A", title="t", metadata={"tags": ["energy", "climate"]})])
    with pytest.raises(ValueError, match="must be scalar"):
        catalog.build()


def test_nested_parquet_indexed_column_fails_at_attach(tmp_path: Path) -> None:
    rows_path = tmp_path / "rows.parquet"
    table = pa.table(
        {
            "code": ["A"],
            "title": ["alpha"],
            "tags": pa.array([["energy", "climate"]], type=pa.list_(pa.string())),
        }
    )
    pq.write_table(table, rows_path)

    catalog = Catalog("demo", indexes={"tags": BM25Index()})
    catalog.set_entities([Entity(namespace="ns", code="energy", title="energy", metadata={})])
    catalog.build()

    from parsimony.catalog.contracts import CatalogBackendConfig

    with pytest.raises(ValueError, match="must be scalar"):
        catalog.attach_parquet_rows(rows_path, config=CatalogBackendConfig())


def test_runtime_only_index_save_fails_clearly(tmp_path: Path) -> None:
    class _RuntimeOnly:
        kind = "custom"

        @property
        def values(self):
            return []

        def build(self, entries, *, ctx):
            return None

        def score_values(self, ctx, *, limit):
            return {}

    catalog = Catalog("demo", indexes={"title": _RuntimeOnly()})  # type: ignore[arg-type]
    catalog.set_entities([Entity(namespace="ns", code="A", title="alpha", metadata={})])
    # Skip build — save path should reject unknown kinds.
    catalog._dirty = False
    with pytest.raises(TypeError, match="runtime-only"):
        catalog._write_indexes(tmp_path / "indexes")


def test_cross_field_agreement_outranks_single_field_hit() -> None:
    """RRF favors agreement: matching two fields beats a one-field landslide."""
    catalog = Catalog(
        "demo",
        indexes={"title": BM25Index(), "description": BM25Index()},
    )
    catalog.set_entities(
        [
            Entity(
                namespace="ns",
                code="ONE",
                title="gross domestic product germany quarterly",
                metadata={"description": "unrelated filler text"},
            ),
            Entity(
                namespace="ns",
                code="BOTH",
                title="gross domestic product",
                metadata={"description": "germany quarterly"},
            ),
            Entity(namespace="ns", code="F1", title="noise a", metadata={"description": "noise"}),
            Entity(namespace="ns", code="F2", title="noise b", metadata={"description": "noise"}),
            Entity(namespace="ns", code="F3", title="noise c", metadata={"description": "noise"}),
        ]
    )
    catalog.build()
    matches = catalog.multi_field_search(
        "gross domestic product germany quarterly",
        fields={"title": 1.0, "description": 1.0},
        limit=5,
    )
    assert matches[0].code == "BOTH"
    assert matches[0].score == pytest.approx(1.0)
    assert all(0.0 < m.score <= 1.0 for m in matches)


def test_heavier_field_weight_can_override_agreement() -> None:
    catalog = Catalog(
        "demo",
        indexes={"title": BM25Index(), "description": BM25Index()},
    )
    catalog.set_entities(
        [
            Entity(
                namespace="ns",
                code="TITLE",
                title="gross domestic product germany",
                metadata={"description": "noise"},
            ),
            Entity(
                namespace="ns",
                code="BOTH",
                title="product",
                metadata={"description": "gross domestic germany"},
            ),
            Entity(namespace="ns", code="F1", title="a", metadata={"description": "b"}),
            Entity(namespace="ns", code="F2", title="c", metadata={"description": "d"}),
        ]
    )
    catalog.build()
    matches = catalog.multi_field_search(
        "gross domestic product germany",
        fields={"title": 3.0, "description": 1.0},
        limit=5,
    )
    assert matches[0].code == "TITLE"
    assert matches[0].score == pytest.approx(1.0)
