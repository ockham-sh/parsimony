"""SearchDetail evidence and uncategorized ranking columns."""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity, SearchDetail
from parsimony.catalog.search import RANKING_COLUMNS, SCORE_COLUMN, SEARCH_DETAIL_COLUMN, search_hits_dataframe
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result


def test_search_detail_json_round_trip_on_factory_projection() -> None:
    catalog = Catalog("demo", indexes={"title": BM25Index()})
    catalog.set_entities([Entity(namespace="demo", code="A", title="gross domestic product", metadata={"area": "US"})])
    catalog.build()
    matches = catalog.search("gross domestic product", field="title", limit=5)
    assert matches[0].search_detail is not None

    columns = (
        Column(name="code", role=ColumnRole.KEY, namespace="demo"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="area", role=ColumnRole.METADATA),
    )
    df = search_hits_dataframe(matches, columns=columns)
    assert list(df.columns)[-2:] == ["score", "search_detail"]
    detail = SearchDetail.model_validate_json(df.iloc[0]["search_detail"])
    assert detail.candidate_limit >= 1
    assert detail.fields[0].field == "title"
    assert detail.fields[0].components[0].kind == "bm25"
    assert detail.fields[0].components[0].rank >= 1


def test_ranking_columns_are_uncategorized_and_hide_detail_from_llm() -> None:
    assert SCORE_COLUMN.role is None
    assert SEARCH_DETAIL_COLUMN.role is None
    assert SEARCH_DETAIL_COLUMN.exclude_from_llm_view is True
    assert RANKING_COLUMNS == (SCORE_COLUMN, SEARCH_DETAIL_COLUMN)

    frame = pd.DataFrame(
        [
            {
                "code": "A",
                "title": "Alpha",
                "score": 1.0,
                "search_detail": SearchDetail(candidate_limit=50, fields=[]).model_dump_json(),
            }
        ]
    )
    spec = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="demo"),
            Column(name="title", role=ColumnRole.TITLE),
            *RANKING_COLUMNS,
        ]
    )
    result = Result(raw=frame, provenance=Provenance(source="test", source_description="t"), output_spec=spec)
    # Uncategorized columns are not entity METADATA/DATA.
    entities = result.entities
    assert list(entities.values())[0].metadata == {}
    # No DATA-role columns → empty per-entity frames.
    assert all(frame.empty for frame in result.data.values())

    preview = result.to_llm()
    assert "score" in preview
    assert "search_detail" not in preview
    assert "candidate_limit" not in preview


def test_role_none_allows_exclude_from_llm_view() -> None:
    Column(name="diag", role=None, exclude_from_llm_view=True)
    with pytest.raises(ValueError, match="exclude_from_llm_view"):
        Column(name="x", role=ColumnRole.DATA, exclude_from_llm_view=True)


def test_arrow_round_trip_preserves_null_role() -> None:
    frame = pd.DataFrame([{"code": "A", "score": 1.0, "search_detail": None}])
    spec = OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="demo"),
            *RANKING_COLUMNS,
        ]
    )
    result = Result(raw=frame, provenance=Provenance(source="test", source_description="t"), output_spec=spec)
    restored = Result.from_arrow(result.to_arrow())
    assert restored.output_spec is not None
    by_name = {c.name: c for c in restored.output_spec.columns}
    assert by_name["score"].role is None
    assert by_name["search_detail"].role is None
    assert by_name["search_detail"].exclude_from_llm_view is True
