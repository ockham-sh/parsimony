"""Behavioural pins for the single-path search engine.

Covers the regimes the rewrite introduced:

* Legacy snapshots keep loading — the frozen ``fusion`` key in hybrid meta is
  ignored, fusion is computed natively at query time.
* Ranking is score-only and deterministic: nothing is pinned above relevance
  behind the caller's back, and equal scores fall back to identity.
* One fusion regime per field, whatever the caller composes on top: BM25
  positives and the vector's top-k fuse under RRF, so a field carries its own
  semantic recall and does not change behaviour with the surface's arity.
* ``search_detail`` preserves component/field evidence for inspection; filter-only
  reads leave ``score`` and ``search_detail`` as ``None``.
* Tie-aware unweighted RRF over per-component value scores.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parsimony.catalog import BM25Index, Catalog, Entity, VectorIndex
from parsimony.catalog.indexes import (
    HybridIndex,
    IndexBuildContext,
    QueryContext,
    search_index_values,
)
from parsimony.catalog.models import CatalogMatch
from parsimony.embedder import EmbedderInfo
from parsimony.indexes import RRF_K, rrf


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


def _component_kinds(match: CatalogMatch) -> set[str]:
    assert match.search_detail is not None
    return {c.kind for field in match.search_detail.fields for c in field.components}


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

    scored = search_index_values(loaded, QueryContext(query="Germany GDP"), limit=5)

    assert scored, "value scoring must work after loading a legacy snapshot"
    assert scored[0].text == "GDP of Germany"


# ---------------------------------------------------------------------------
# b. Ranking is score-only: no hidden tier above relevance
# ---------------------------------------------------------------------------


def _single_field_catalog() -> Catalog:
    entries = [
        # The query, exactly.
        Entity(namespace="demo", code="EXACT", title="gross domestic product annual", metadata={}),
        # A leftover token ("extra") — a different concept, but a strong fuzzy score.
        Entity(namespace="demo", code="HIGH", title="gross domestic product extra", metadata={}),
        # A short subset of the query: overlaps, but explains little of it.
        Entity(namespace="demo", code="PARTIAL", title="annual", metadata={}),
    ]
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(entries)
    catalog.build()
    return catalog


def test_ranking_is_score_only_with_no_hidden_tier() -> None:
    """Relevance alone orders rows, and it is enough for the obvious case.

    The row that *is* the query still wins here, on score. What must not happen is
    a tier doing that work invisibly: partial overlap gets no promotion, so the
    weakly-overlapping row stays below the strongly-scoring one, and every score
    stays inside the normalized band a single weighted field can produce.
    """
    catalog = _single_field_catalog()
    matches = catalog.search("gross domestic product annual", field="title", limit=10)
    order = [m.code for m in matches]

    assert order[0] == "EXACT"
    assert order.index("HIGH") < order.index("PARTIAL")
    assert all(0.0 < m.score <= 1.0 for m in matches)
    assert all(m.search_detail is not None for m in matches)


def test_equal_scores_fall_back_to_identity_order() -> None:
    """Ties break on (namespace, code), so a result page never shuffles run to run."""
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(
        [
            Entity(namespace="demo", code="B", title="identical text", metadata={}),
            Entity(namespace="demo", code="A", title="identical text", metadata={}),
        ]
    )
    catalog.build()

    matches = catalog.search("identical text", field="title", limit=10)

    assert [m.score for m in matches] == [matches[0].score] * 2
    assert [m.code for m in matches] == ["A", "B"]


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


def test_hybrid_field_surfaces_a_semantic_neighbour_bm25_cannot_reach() -> None:
    """The recall this regime exists for: "young people" -> "less than 25 years".

    The label shares no token with the query, so BM25 finds nothing; the row is
    reachable only because the vector component participates in every hybrid
    field's scoring. ``search_detail`` records vector-only component evidence.
    """
    catalog = _void_fill_catalog()
    matches = catalog.multi_field_search("young people", fields={"title": 1.0, "label": 1.0}, limit=10)
    codes = [m.code for m in matches]

    assert "YOUTH" in codes
    youth = next(m for m in matches if m.code == "YOUTH")
    assert _component_kinds(youth) == {"vector"}


def test_lexical_and_semantic_candidates_both_surface_and_are_labelled() -> None:
    """A vector-only neighbour is reported beside a lexical hit, never in place of it.

    The stub embeds "alpha" near the *wrong* label ("beta measure") while BM25
    matches the token-sharing one ("alpha metric"). Both rows come back, each
    with component evidence for how it was found. Their tie is broken by identity,
    so the page is stable.
    """
    catalog = _void_fill_catalog()
    matches = catalog.multi_field_search("alpha", fields={"title": 1.0, "label": 1.0}, limit=10)

    by_code = {m.code: m for m in matches}
    assert {"X", "Y"} <= set(by_code)
    assert "bm25" in _component_kinds(by_code["X"])
    assert _component_kinds(by_code["Y"]) == {"vector"}
    assert [m.code for m in matches] == ["X", "Y"]


# ---------------------------------------------------------------------------
# d. search_detail: component evidence / None on filter-only
# ---------------------------------------------------------------------------


def _both_entries() -> list[Entity]:
    return [
        Entity(namespace="demo", code="GDP", title="gross domestic product", metadata={}),
        Entity(namespace="demo", code="CPI", title="consumer price index", metadata={}),
    ]


def test_bm25_only_search_records_bm25_component_evidence() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(_both_entries())
    catalog.build()

    matches = catalog.search("gross domestic product", field="title", limit=10)
    assert matches
    assert all(_component_kinds(m) == {"bm25"} for m in matches)
    top = matches[0]
    assert top.search_detail is not None
    assert top.search_detail.candidate_limit >= 1
    field = top.search_detail.fields[0]
    assert field.field == "title"
    assert field.components[0].raw_score > 0
    assert field.components[0].rank >= 1


def test_hybrid_single_field_agreement_records_both_components() -> None:
    vectors = {"gross domestic product": [1.0, 0.0, 0.0], "consumer price index": [0.0, 1.0, 0.0]}
    catalog = Catalog(
        "demo",
        indexes={"title": HybridIndex(components=[BM25Index(), VectorIndex(embedder=_StubEmbedder(vectors))])},
    )
    catalog.set_entities(_both_entries())
    catalog.build()

    matches = catalog.search("gross domestic product", field="title", limit=10)
    assert matches[0].code == "GDP"
    assert _component_kinds(matches[0]) == {"bm25", "vector"}


def test_filter_only_search_has_no_search_detail() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities(_both_entries())
    catalog.build()

    matches = catalog.search(filter={"code": ["GDP"]}, limit=10)
    assert [m.code for m in matches] == ["GDP"]
    assert matches[0].score is None
    assert matches[0].search_detail is None


# ---------------------------------------------------------------------------
# e. Tie-aware RRF over per-component value scores
# ---------------------------------------------------------------------------


def test_rrf_ties_share_competition_rank_and_vector_breaks_them() -> None:
    # bm25 ties vids 1 and 2 at score 2.0 (competition rank 1); vid 3 at 1.0
    # takes rank 3 (not 2) because the tie occupies both leading positions.
    bm25_only = rrf({"bm25": {1: 2.0, 2: 2.0, 3: 1.0}})
    assert bm25_only[1] == bm25_only[2]
    assert bm25_only[1] == pytest.approx(1.0)
    assert bm25_only[3] == pytest.approx((1.0 / (RRF_K + 3)) / (1.0 / (RRF_K + 1)))
    assert bm25_only[3] < bm25_only[1]

    # Adding the vector component (which ranks vid 2 above vid 1) breaks the tie
    # in vid 2's favour overall, while vid 3 stays last.
    fused = rrf({"bm25": {1: 2.0, 2: 2.0, 3: 1.0}, "vector": {2: 0.9, 1: 0.5}})
    order = sorted(fused, key=lambda vid: -fused[vid])
    assert order == [2, 1, 3]
    assert fused[2] == pytest.approx(1.0)
    assert fused[2] > fused[1] > fused[3]
