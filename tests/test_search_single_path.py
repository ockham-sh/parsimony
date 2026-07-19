"""Behavioural pins for the single-path search engine.

Covers the regimes the rewrite introduced:

* Legacy snapshots keep loading — the frozen ``fusion`` key in hybrid meta is
  ignored, fusion is computed natively at query time.
* Single-field surfaces: only full consumption (coverage 1.0) reorders; partial
  containment is reported but does not rank.
* Multi-field surfaces: lexical-first with semantic void-fill — BM25 rules a
  field whenever it has any positive; the vector only fills a fully-abstaining
  field.
* ``matched`` records which component surfaced a row: "lexical", "semantic",
  "both", or ``None`` for filter-only reads.
* Tie-aware unweighted RRF over per-component value scores.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parsimony.catalog import BM25Index, Catalog, Entity, VectorIndex
from parsimony.catalog.indexes import (
    RRF_K,
    HybridIndex,
    IndexBuildContext,
    _rrf_value_scores,
    embed_query_vectors,
    search_index_values,
)
from parsimony.embedder import EmbedderInfo


class _StubEmbedder:
    """Deterministic embedder over a fixed text -> vector map (unknown -> axis 2)."""

    DIM = 3

    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self._vectors = vectors

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [list(self._vectors.get(text, [0.0, 0.0, 1.0])) for text in texts]

    def embed_query(self, query: str) -> list[float]:
        (vector,) = self.embed_texts([query])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub", dim=self.DIM, normalize=True, package="test")


# ---------------------------------------------------------------------------
# a. Legacy snapshot load: the frozen fusion key is ignored on load
# ---------------------------------------------------------------------------


def test_hybrid_load_ignores_legacy_fusion_spec(tmp_path: Path) -> None:
    vectors = {
        "GDP of Germany": [1.0, 0.0, 0.0],
        "CPI of France": [0.0, 1.0, 0.0],
        "Germany GDP": [0.97, 0.05, 0.0],
    }
    entries = [
        Entity(namespace="ns", code="A", title="GDP of Germany", metadata={}),
        Entity(namespace="ns", code="B", title="CPI of France", metadata={}),
    ]
    hybrid = HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder(vectors))])
    hybrid.build(entries, ctx=IndexBuildContext(field="title", vector_cache={}))

    path = tmp_path / "title"
    hybrid.save(path)

    # Rewrite the meta with a pre-0.0.2-style fusion spec; load must ignore it.
    meta_path = path / "meta.json"
    raw = json.loads(meta_path.read_text())
    raw["fusion"] = {"kind": "z_score_fusion", "weights": {"bm25": 0.5, "vector": 1.0}}
    meta_path.write_text(json.dumps(raw))

    loaded = HybridIndex.load(path)
    stub = _StubEmbedder(vectors)
    for component in loaded._components.values():
        if isinstance(component, VectorIndex):
            component._embedder = stub
            component._embedder_info = stub.info()

    query_vectors = embed_query_vectors("Germany GDP", [loaded])
    scored = search_index_values(loaded, "Germany GDP", limit=5, query_vectors=query_vectors)

    assert scored, "value scoring must work after loading a legacy snapshot"
    assert scored[0][0] == "GDP of Germany"


# ---------------------------------------------------------------------------
# b. Single-field surface: only coverage 1.0 reorders; partial is reported only
# ---------------------------------------------------------------------------


def _single_field_catalog() -> Catalog:
    entries = [
        # Full consumption of the query -> coverage 1.0, must pin rank 1.
        Entity(namespace="demo", code="EXACT", title="gross domestic product annual", metadata={}),
        # A leftover token ("extra") -> coverage 0, but a high fuzzy score.
        Entity(namespace="demo", code="HIGH", title="gross domestic product extra", metadata={}),
        # A short subset of the query -> partial coverage, low fuzzy score.
        Entity(namespace="demo", code="PARTIAL", title="annual", metadata={}),
    ]
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(entries)
    catalog.build()
    return catalog


def test_single_field_partial_coverage_does_not_reorder() -> None:
    catalog = _single_field_catalog()
    matches = catalog.search("gross domestic product annual", fields="title", limit=10)
    order = [m.code for m in matches]

    # Exact consumption pins rank 1 over a higher-scoring fuzzy row.
    assert order[0] == "EXACT"
    assert matches[0].coverage == 1.0

    # A partially-consumed but lower-scoring row must NOT outrank the higher
    # fuzzy row: single-field ordering below coverage 1.0 is score-only.
    assert order.index("HIGH") < order.index("PARTIAL")

    # Partial coverage is reported raw, not banded to zero and not to one.
    partial = next(m for m in matches if m.code == "PARTIAL")
    assert partial.coverage == 0.25
    high = next(m for m in matches if m.code == "HIGH")
    assert high.coverage == 0.0
    assert high.score > partial.score


# ---------------------------------------------------------------------------
# c. Multi-field surface: semantic void-fill, and BM25 wins when it has evidence
# ---------------------------------------------------------------------------


def _void_fill_catalog() -> Catalog:
    vectors = {
        "less than 25 years": [1.0, 0.0, 0.0],
        "young people": [1.0, 0.0, 0.0],  # query embeds onto the same axis
        "beta measure": [0.0, 1.0, 0.0],
        "alpha metric": [0.0, 0.0, 1.0],
        "alpha": [0.0, 1.0, 0.0],  # query embeds near the WRONG label ("beta measure")
    }
    entries = [
        Entity(
            namespace="demo", code="YOUTH", title="Population age bracket", metadata={"label": "less than 25 years"}
        ),
        Entity(namespace="demo", code="X", title="First metric series", metadata={"label": "alpha metric"}),
        Entity(namespace="demo", code="Y", title="Second measure series", metadata={"label": "beta measure"}),
    ]
    catalog = Catalog(
        "demo",
        indexes={
            "title": BM25Index(),
            "label": HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder(vectors))]),
        },
    )
    catalog.set_entities(entries)
    catalog.build()
    return catalog


def test_multi_field_void_fill_surfaces_semantic_neighbour() -> None:
    """BM25 abstains on the label; the vector's neighbour still surfaces the row."""
    catalog = _void_fill_catalog()
    matches = catalog.search("young people", fields=["title", "label"], limit=10)
    codes = [m.code for m in matches]

    assert "YOUTH" in codes
    youth = next(m for m in matches if m.code == "YOUTH")
    assert youth.matched == "semantic"


def test_multi_field_vector_does_not_perturb_lexical_ranking() -> None:
    """When BM25 has a positive on the field, the vector must not reorder it.

    The stub embeds the query near the "wrong" label ("beta measure"), but BM25
    ranks the token-matching label ("alpha metric"); the void-fill regime keeps
    the vector out entirely, so BM25's preference wins and the vector's favourite
    never surfaces via the label.
    """
    catalog = _void_fill_catalog()
    matches = catalog.search("alpha", fields=["title", "label"], limit=10)
    codes = [m.code for m in matches]

    assert "X" in codes
    x = next(m for m in matches if m.code == "X")
    assert x.matched == "lexical"
    # The vector's favourite label never surfaced — BM25 held the field.
    assert "Y" not in codes


# ---------------------------------------------------------------------------
# d. matched semantics: lexical / both / None
# ---------------------------------------------------------------------------


def _both_entries() -> list[Entity]:
    return [
        Entity(namespace="demo", code="GDP", title="gross domestic product", metadata={}),
        Entity(namespace="demo", code="CPI", title="consumer price index", metadata={}),
    ]


def test_bm25_only_search_marks_every_match_lexical() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(_both_entries())
    catalog.build()

    matches = catalog.search("gross domestic product", fields="title", limit=10)
    assert matches
    assert all(m.matched == "lexical" for m in matches)


def test_hybrid_single_field_agreement_marks_match_both() -> None:
    vectors = {"gross domestic product": [1.0, 0.0, 0.0], "consumer price index": [0.0, 1.0, 0.0]}
    catalog = Catalog(
        "demo",
        indexes={"title": HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder(vectors))])},
    )
    catalog.set_entities(_both_entries())
    catalog.build()

    matches = catalog.search("gross domestic product", fields="title", limit=10)
    assert matches[0].code == "GDP"
    assert matches[0].matched == "both"


def test_filter_only_search_has_no_matched_evidence() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(_both_entries())
    catalog.build()

    matches = catalog.search(filter={"code": ["GDP"]}, limit=10)
    assert [m.code for m in matches] == ["GDP"]
    assert matches[0].matched is None


# ---------------------------------------------------------------------------
# e. Tie-aware RRF over per-component value scores
# ---------------------------------------------------------------------------


def test_rrf_ties_share_competition_rank_and_vector_breaks_them() -> None:
    # bm25 ties vids 1 and 2 at score 2.0 (competition rank 1); vid 3 at 1.0
    # takes rank 3 (not 2) because the tie occupies both leading positions.
    bm25_only = _rrf_value_scores({"bm25": {1: 2.0, 2: 2.0, 3: 1.0}})
    assert bm25_only[1] == bm25_only[2]
    assert bm25_only[1] == pytest.approx(1.0 / (RRF_K + 1))
    assert bm25_only[3] == pytest.approx(1.0 / (RRF_K + 3))
    assert bm25_only[3] < bm25_only[1]

    # Adding the vector component (which ranks vid 2 above vid 1) breaks the tie
    # in vid 2's favour overall, while vid 3 stays last.
    fused = _rrf_value_scores({"bm25": {1: 2.0, 2: 2.0, 3: 1.0}, "vector": {2: 0.9, 1: 0.5}})
    order = sorted(fused, key=lambda vid: -fused[vid])
    assert order == [2, 1, 3]
    assert fused[2] > fused[1] > fused[3]
