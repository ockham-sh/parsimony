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


def _build_catalog() -> Catalog:
    catalog = Catalog(
        name="demo",
        indexes={"title": BM25Index()},
        default_field="title",
    )
    catalog.set_entities(
        [
            Entity(namespace="ns", code="CP0000_DE", title="HICP Germany", metadata={"area": "DE"}),
            Entity(namespace="ns", code="CP0000_FR", title="HICP France", metadata={"area": "FR"}),
            Entity(namespace="ns", code="UNEMP_DE", title="Unemployment Germany", metadata={"area": "DE"}),
        ]
    )
    catalog.build()
    return catalog


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
        metadata_columns=["area"],
    )


def test_query_ranked_search(demo_search) -> None:
    df = demo_search(query="Germany", limit=5).data
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # A ranked query re-ranks but does not exclude: France may still appear.
    assert "CP0000_DE" in set(df["code"])


def test_filter_only_enumerates_slice(demo_search) -> None:
    """Pure filter read returns every matching row, no query."""
    df = demo_search(filter={"area": "DE"}, limit=ENUMERATION_LIMIT).data
    assert set(df["code"]) == {"CP0000_DE", "UNEMP_DE"}


def test_filter_excludes_nonmatching(demo_search) -> None:
    """Filter is an exact AND constraint that drops non-matching rows, unlike query."""
    df = demo_search(query="HICP", filter={"area": "FR"}, limit=10).data
    assert set(df["code"]) == {"CP0000_FR"}


def test_ranked_query_rejects_enumeration_limit(demo_search) -> None:
    with pytest.raises(InvalidParameterError, match="ranked shortlist"):
        demo_search(query="Germany", limit=RANKED_LIMIT + 1)


def test_requires_query_or_filter(demo_search) -> None:
    with pytest.raises(InvalidParameterError, match="requires query="):
        demo_search()


def test_empty_query_is_treated_as_filter_only(demo_search) -> None:
    """An empty/whitespace query with a filter must not be a ranked search."""
    df = demo_search(query="   ", filter={"area": "FR"}, limit=ENUMERATION_LIMIT).data
    assert set(df["code"]) == {"CP0000_FR"}


def test_no_match_raises_empty(demo_search) -> None:
    with pytest.raises(EmptyDataError):
        demo_search(filter={"area": "ZZ"}, limit=10)
