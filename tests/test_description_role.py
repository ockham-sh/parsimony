"""ColumnRole.DESCRIPTION end-to-end: schema accept → entries_from_result → search.

Also covers :attr:`SeriesMatch.bm25_rank` / :attr:`SeriesMatch.dense_rank`
since the same search-path fixture exercises both.
"""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.catalog import Catalog, entries_from_result
from parsimony.connector import _validate_loader_output
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


def _result_with_description(df: pd.DataFrame) -> Result:
    config = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="test_ns"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="definition", role=ColumnRole.DESCRIPTION),
            Column(name="unit", role=ColumnRole.METADATA),
        ]
    )
    return Result(data=df, provenance=Provenance(source="test"), output_schema=config)


def test_entries_from_result_populates_description() -> None:
    df = pd.DataFrame(
        {
            "code": ["A.1", "B.2"],
            "title": ["Series A", "Series B"],
            "definition": ["All outstanding debt held by the public.", "Intragovernmental holdings."],
            "unit": ["USD", "USD"],
        }
    )
    entries = entries_from_result(_result_with_description(df))

    by_code = {e.code: e for e in entries}
    assert by_code["A.1"].description == "All outstanding debt held by the public."
    assert by_code["B.2"].description == "Intragovernmental holdings."


def test_description_feeds_semantic_text() -> None:
    """SeriesEntry.semantic_text() should concatenate title + description once
    the column role populates the field — so the embedder indexes it."""
    df = pd.DataFrame(
        {
            "code": ["A.1"],
            "title": ["Series A"],
            "definition": ["Rich definitional text that the embedder should see."],
            "unit": ["USD"],
        }
    )
    (entry,) = entries_from_result(_result_with_description(df))
    text = entry.semantic_text()
    assert "Series A" in text
    assert "Rich definitional text" in text


def test_description_column_empty_string_yields_none() -> None:
    df = pd.DataFrame(
        {
            "code": ["A.1"],
            "title": ["Series A"],
            "definition": [""],
            "unit": ["USD"],
        }
    )
    (entry,) = entries_from_result(_result_with_description(df))
    assert entry.description is None


def test_description_column_missing_is_ok() -> None:
    """Schemas without a DESCRIPTION column still work; description stays None."""
    df = pd.DataFrame({"code": ["A.1"], "title": ["Series A"]})
    config = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="test_ns"),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )
    table = Result(data=df, provenance=Provenance(source="t"), output_schema=config)
    (entry,) = entries_from_result(table)
    assert entry.description is None


def test_multiple_description_columns_rejected() -> None:
    df = pd.DataFrame({"code": ["A.1"], "title": ["A"], "d1": ["x"], "d2": ["y"]})
    config = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="n"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="d1", role=ColumnRole.DESCRIPTION),
            Column(name="d2", role=ColumnRole.DESCRIPTION),
        ]
    )
    table = Result(data=df, provenance=Provenance(source="t"), output_schema=config)
    with pytest.raises(ValueError, match="at most one DESCRIPTION column"):
        entries_from_result(table)


def test_loader_rejects_description_column() -> None:
    """Loader outputs are per-row time series; DESCRIPTION is a catalog-only concern."""
    config = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="n"),
            Column(name="value", role=ColumnRole.DATA),
            Column(name="definition", role=ColumnRole.DESCRIPTION),
        ]
    )
    with pytest.raises(ValueError, match="Loader output must not include DESCRIPTION"):
        _validate_loader_output(config)


def test_description_column_cannot_be_excluded_from_llm_view() -> None:
    with pytest.raises(ValueError, match="exclude_from_llm_view is not allowed for description"):
        Column(name="definition", role=ColumnRole.DESCRIPTION, exclude_from_llm_view=True)


@pytest.mark.asyncio
async def test_description_text_boosts_bm25_recall() -> None:
    """An entry whose title lacks a word but whose description contains it
    should still surface via BM25 because keyword_text concatenates
    description into the searchable corpus."""
    import hashlib

    from parsimony.embedder import EmbedderInfo

    class _StubEmbedder:
        DIM = 8

        @property
        def dimension(self) -> int:
            return self.DIM

        async def embed_texts(self, texts: list[str]) -> list[list[float]]:
            out: list[list[float]] = []
            for t in texts:
                d = hashlib.sha256(t.encode("utf-8")).digest()
                raw = [d[i] / 255.0 for i in range(self.DIM)]
                n = sum(x * x for x in raw) ** 0.5 or 1.0
                out.append([x / n for x in raw])
            return out

        async def embed_query(self, query: str) -> list[float]:
            (v,) = await self.embed_texts([query])
            return v

        def info(self) -> EmbedderInfo:
            return EmbedderInfo(model="stub/hash-sha256", dim=self.DIM, normalize=True, package="test-stub")

    # BM25 IDF is ``log((N-n+0.5)/(n+0.5))`` — with a 2-doc corpus and a term
    # appearing in exactly 1 doc, IDF = log(1.5/1.5) = 0 and BM25 assigns score
    # 0. Padding the corpus with unrelated docs forces the rare term to have
    # non-zero IDF, which is the realistic production regime anyway.
    df = pd.DataFrame(
        {
            "code": [f"ROW.{i}" for i in range(10)] + ["A.1"],
            "title": [f"Filler series {i}" for i in range(10)] + ["First series"],
            "definition": [f"Some padding text {i}." for i in range(10)]
            + ["Mentions renewable wind energy production."],
            "unit": ["USD"] * 10 + ["MWh"],
        }
    )
    table = _result_with_description(df)

    cat = Catalog(name="test_ns", embedder=_StubEmbedder())
    await cat.add_from_result(table)
    hits = await cat.search("renewable wind energy", limit=2)
    assert hits, "expected at least one hit"
    assert hits[0].code == "A.1", f"description-bearing entry should rank first; got {hits[0].code}"

    # SeriesMatch exposes per-retriever ranks for debugging.
    top = hits[0]
    # BM25 returned this hit at rank 0 (sole entry matching "renewable wind energy").
    assert top.bm25_rank == 0
    # The stub embedder ranks somewhere — may or may not be rank 0. Either way,
    # it should be a non-negative integer or None.
    assert top.dense_rank is None or (isinstance(top.dense_rank, int) and top.dense_rank >= 0)
