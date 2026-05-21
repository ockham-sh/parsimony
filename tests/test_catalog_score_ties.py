import pandas as pd
import pytest

from parsimony.catalog import BM25Index, CatalogEntry, VectorIndex
from parsimony.embedder import EmbedderInfo
from parsimony.ranking import Ranking, RankingSet, ranking_from_scores


class _LiteralEmbedder:
    def __init__(self) -> None:
        self.embedded_text_batches: list[list[str]] = []

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="literal", dim=2, normalize=False, package="test")

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.embedded_text_batches.append(list(texts))
        return [self._vector(text) for text in texts]

    async def embed_query(self, query: str) -> list[float]:
        return self._vector(query)

    def _vector(self, text: str) -> list[float]:
        if "alpha" in text or "beta" in text or "query" in text:
            return [1.0, 0.0]
        if "rare" in text:
            return [0.0, 1.0]
        return [0.5, 0.5]


def _entries() -> list[CatalogEntry]:
    return [
        CatalogEntry(namespace="test", code="A", title="a", metadata={"label": "alpha"}),
        CatalogEntry(namespace="test", code="B", title="b", metadata={"label": "beta"}),
        CatalogEntry(namespace="test", code="C", title="c", metadata={"label": "rare gamma"}),
        CatalogEntry(namespace="test", code="D", title="d", metadata={"label": "other gamma"}),
    ]


class _FixedRanker:
    def __init__(self, code: str) -> None:
        self.code = code

    def __call__(self, rankings: RankingSet, *, limit: int) -> Ranking:
        del rankings, limit
        return Ranking(pd.DataFrame([{"namespace": "test", "code": self.code, "rank": 0, "score": 1.0}]))


@pytest.mark.asyncio
async def test_vector_index_assigns_same_rank_to_equal_scores() -> None:
    index = VectorIndex("label_vector", field="label", embedder=_LiteralEmbedder())
    await index.build(_entries())

    ranking = await index.ranking("query", limit=1)
    table = ranking.to_table().sort_values("code").reset_index(drop=True)

    assert list(table["code"]) == ["A", "B"]
    assert list(table["rank"]) == [0, 0]
    assert table.loc[0, "score"] == table.loc[1, "score"]


@pytest.mark.asyncio
async def test_vector_index_embeds_duplicate_field_text_once_per_build() -> None:
    embedder = _LiteralEmbedder()
    index = VectorIndex("label_vector", field="label", embedder=embedder)
    await index.build(
        [
            CatalogEntry(namespace="test", code="A", title="a", metadata={"label": "alpha"}),
            CatalogEntry(namespace="test", code="B", title="b", metadata={"label": "alpha"}),
            CatalogEntry(namespace="test", code="C", title="c", metadata={"label": "rare gamma"}),
        ]
    )

    assert embedder.embedded_text_batches == [["alpha", "rare gamma"]]


@pytest.mark.asyncio
async def test_bm25_index_assigns_same_rank_to_equal_scores() -> None:
    index = BM25Index("label_bm25", field="label")
    await index.build(
        [
            CatalogEntry(
                namespace="test",
                code="A",
                title="a",
                metadata={"label": "instantaneous forward rate 1 year"},
            ),
            CatalogEntry(
                namespace="test",
                code="B",
                title="b",
                metadata={"label": "instantaneous forward rate 2 year"},
            ),
            CatalogEntry(namespace="test", code="C", title="c", metadata={"label": "unrelated label"}),
            CatalogEntry(namespace="test", code="D", title="d", metadata={"label": "other unrelated"}),
            CatalogEntry(namespace="test", code="E", title="e", metadata={"label": "different words"}),
        ]
    )

    ranking = await index.ranking("instantaneous forward rate one year", limit=1)
    table = ranking.to_table().sort_values("code").reset_index(drop=True)

    assert list(table["code"]) == ["A", "B"]
    assert list(table["rank"]) == [0, 0]
    assert table.loc[0, "score"] == table.loc[1, "score"]


def test_ranking_from_scores_uses_competition_rank_and_keeps_boundary_group() -> None:
    ranking = ranking_from_scores(
        [
            ("n", "A", 10.0),
            ("n", "B", 10.0),
            ("n", "C", 9.0),
            ("n", "D", 8.0),
            ("n", "E", 8.0),
            ("n", "F", 8.0),
            ("n", "G", 7.0),
        ],
        limit=4,
    )

    table = ranking.to_table()

    assert list(table["code"]) == ["A", "B", "C", "D", "E", "F"]
    assert list(table["rank"]) == [0, 0, 2, 3, 3, 3]
    assert list(table["score"]) == [10.0, 10.0, 9.0, 8.0, 8.0, 8.0]
