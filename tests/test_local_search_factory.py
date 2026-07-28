"""Tests for the shared ``make_local_search_connector`` factory.

The factory backs the 10 in-memory catalog search connectors (bde, bdf, boc, …).
These cover the filter/enumeration surface added for F4: a free-text ``query`` is a
ranked shortlist (capped small), while a pure ``filter`` read enumerates the whole
matching slice from the cached catalog into a variable (capped at ENUMERATION_LIMIT).
"""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog.search import ENUMERATION_LIMIT, RANKED_LIMIT, make_local_search_connector
from parsimony.errors import EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputSpec


def _build_catalog() -> Catalog:
    catalog = Catalog(
        name="demo",
        indexes={"title": BM25Index(), "description": BM25Index()},
    )
    catalog.set_entities(
        [
            Entity(namespace="ns", code="CP0000_DE", title="HICP Germany", metadata={"area": "DE"}),
            Entity(namespace="ns", code="CP0000_FR", title="HICP France", metadata={"area": "FR"}),
            Entity(
                namespace="ns",
                code="UNEMP_DE",
                title="Unemployment Germany",
                metadata={"area": "DE", "description": "Harmonised jobless rate benchmark."},
            ),
        ]
    )
    catalog.build()
    return catalog


_DEMO_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="area", role=ColumnRole.METADATA),
    ]
)


@pytest.fixture
def demo_search(monkeypatch, tmp_path):
    """A factory connector over a small in-memory catalog, cache isolated to tmp_path."""
    monkeypatch.setenv("PARSIMONY_CACHE_DIR", str(tmp_path))
    return make_local_search_connector(
        provider="demo",
        default_url="file:///unused",
        catalog_url_env_var="DEMO_CATALOG_URL",
        tags=["demo"],
        description="Search the demo catalog.",
        build_catalog=_build_catalog,
        output=_DEMO_SEARCH_OUTPUT,
    )


def test_factory_connector_declares_no_requires(demo_search) -> None:
    """Local catalog search needs no credentials — nothing to declare."""
    assert demo_search.requires == ()


def test_query_ranked_search(demo_search) -> None:
    df = demo_search(query="Germany", limit=5).frame
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # A ranked query re-ranks but does not exclude: France may still appear.
    assert "CP0000_DE" in set(df["code"])


def test_filter_only_enumerates_slice(demo_search) -> None:
    """Pure filter read returns every matching row, no query."""
    df = demo_search(filter={"area": "DE"}, limit=ENUMERATION_LIMIT).frame
    assert set(df["code"]) == {"CP0000_DE", "UNEMP_DE"}


def test_filter_excludes_nonmatching(demo_search) -> None:
    """Filter is an exact AND constraint that drops non-matching rows, unlike query."""
    df = demo_search(query="HICP", filter={"area": "FR"}, limit=10).frame
    assert set(df["code"]) == {"CP0000_FR"}


def test_ranked_query_rejects_enumeration_limit(demo_search) -> None:
    with pytest.raises(InvalidParameterError, match="ranked shortlist"):
        demo_search(query="Germany", limit=RANKED_LIMIT + 1)


def test_requires_query_or_filter(demo_search) -> None:
    with pytest.raises(InvalidParameterError, match="requires query="):
        demo_search()


def test_empty_query_is_treated_as_filter_only(demo_search) -> None:
    """An empty/whitespace query with a filter must not be a ranked search."""
    df = demo_search(query="   ", filter={"area": "FR"}, limit=ENUMERATION_LIMIT).frame
    assert set(df["code"]) == {"CP0000_FR"}


def test_no_match_raises_empty(demo_search) -> None:
    with pytest.raises(EmptyDataError):
        demo_search(filter={"area": "ZZ"}, limit=10)


def test_description_evidence_is_searched(demo_search) -> None:
    """The factory surface is title + description: query words that appear only in a
    row's description surface it on lexical evidence."""
    from parsimony.catalog import SearchDetail

    df = demo_search(query="jobless benchmark", limit=5).frame
    assert df.iloc[0]["code"] == "UNEMP_DE"
    detail = SearchDetail.model_validate_json(df.iloc[0]["search_detail"])
    assert {c.kind for f in detail.fields for c in f.components} == {"bm25"}


def test_non_default_surface_declaration(monkeypatch, tmp_path) -> None:
    """A connector can declare a narrower surface; the declaration is honored and stated."""
    monkeypatch.setenv("PARSIMONY_CACHE_DIR", str(tmp_path))
    desc_only = make_local_search_connector(
        provider="demo2",
        default_url="file:///unused",
        catalog_url_env_var="DEMO2_CATALOG_URL",
        tags=["demo"],
        description="Search the demo catalog.",
        build_catalog=_build_catalog,
        ranking_fields={"description": 1.0},
    )
    df = desc_only(query="jobless benchmark", limit=5).frame
    assert df.iloc[0]["code"] == "UNEMP_DE"
    # Titles are OFF this connector's declared surface: a title-only term finds nothing.
    with pytest.raises(EmptyDataError):
        desc_only(query="HICP", limit=5)
    assert "description field." in desc_only.description


@pytest.fixture
def weighted_search(monkeypatch, tmp_path):
    """The opt-in weighted recipe: connector-declared fields and weights."""
    monkeypatch.setenv("PARSIMONY_CACHE_DIR", str(tmp_path))
    return make_local_search_connector(
        provider="demo3",
        default_url="file:///unused",
        catalog_url_env_var="DEMO3_CATALOG_URL",
        tags=["demo"],
        description="Search the demo catalog.",
        build_catalog=_build_catalog,
        output=_DEMO_SEARCH_OUTPUT,
        ranking_fields={"title": 1.0, "description": 0.5},
    )


def test_weighted_recipe_reports_score_and_search_detail_only(weighted_search) -> None:
    """Weighted ranking states its policy in the weights, so it reports no extra fact."""
    df = weighted_search(query="hicp germany", limit=5).frame
    assert list(df.columns)[-2:] == ["score", "search_detail"]
    assert "coverage" not in df.columns
    assert df.iloc[0]["code"] == "CP0000_DE"


def test_weighted_recipe_declares_its_surface_in_the_output_spec(weighted_search) -> None:
    assert [column.name for column in weighted_search.output_spec.columns[-2:]] == ["score", "search_detail"]
    assert "title, description fields." in weighted_search.description


def test_weighted_recipe_accepts_membership_filters(weighted_search) -> None:
    df = weighted_search(query="germany", filter={"area": ["DE", "FR"]}, limit=10).frame
    assert set(df["code"]) <= {"CP0000_DE", "UNEMP_DE", "CP0000_FR"}
    assert "CP0000_DE" in set(df["code"])


def test_weighted_recipe_filter_only_read_still_enumerates(weighted_search) -> None:
    df = weighted_search(filter={"area": "DE"}, limit=ENUMERATION_LIMIT).frame
    assert set(df["code"]) == {"CP0000_DE", "UNEMP_DE"}
    assert list(df.columns)[-2:] == ["score", "search_detail"]


def test_every_factory_page_ends_with_the_same_ranking_pair(demo_search) -> None:
    """One ranking schema across every provider — and no third column to explain.

    A filter-only read ranks nothing, so ``search_detail`` is null there rather than
    carrying fabricated evidence.
    """
    from parsimony.catalog import SearchDetail

    ranked = demo_search(query="hicp germany", limit=5).frame
    assert list(ranked.columns)[-2:] == ["score", "search_detail"]
    assert ranked.iloc[0]["code"] == "CP0000_DE"
    detail = SearchDetail.model_validate_json(ranked.iloc[0]["search_detail"])
    assert {c.kind for f in detail.fields for c in f.components} == {"bm25"}

    enumerated = demo_search(filter={"area": "DE"}, limit=ENUMERATION_LIMIT).frame
    assert list(enumerated.columns)[-2:] == ["score", "search_detail"]
    assert enumerated["search_detail"].isna().all()
    assert enumerated["score"].isna().all()


def test_output_spec_metadata_columns_are_projected(demo_search) -> None:
    """METADATA roles on the output spec are the projection source (anti-drift)."""
    df = demo_search(query="Germany", limit=5).frame
    assert "area" in df.columns
    assert set(df.loc[df["code"] == "CP0000_DE", "area"]) == {"DE"}
    assert any(c.name == "area" for c in demo_search.output_spec.columns)
