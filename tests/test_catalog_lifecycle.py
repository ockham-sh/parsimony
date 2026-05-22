from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    CatalogEntry,
    HybridIndex,
    VectorIndex,
)
from parsimony.embedder import EmbedderInfo
from parsimony.result import Column, ColumnRole, OutputConfig


def _entries() -> list[CatalogEntry]:
    return [
        CatalogEntry(namespace="series", code="A", title="alpha title"),
        CatalogEntry(namespace="series", code="B", title="beta title"),
        CatalogEntry(namespace="series", code="C", title="gamma title"),
    ]


def _enumeration_schema(*, namespace: str | None = "series") -> OutputConfig:
    return OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace=namespace),
            Column(name="title", role=ColumnRole.TITLE),
        ]
    )


def _enumeration_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A", "B", "C"],
            "title": ["alpha title", "beta title", "gamma title"],
        }
    )


class _StubEmbedder:
    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] if "alpha" in text else [0.0, 1.0] for text in texts]

    async def embed_query(self, query: str) -> list[float]:
        return [1.0, 0.0] if "alpha" in query else [0.0, 1.0]

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub", dim=2, normalize=True, package="test")


async def test_catalog_build_entries_static_indexes_and_ranker() -> None:
    catalog = Catalog("artifact", indexes=[BM25Index("title_bm25", field="title")])
    catalog.set_entries(_entries())

    await catalog.build()

    hits, _ = await catalog.search("alpha", limit=1)
    assert hits[0].code == "A"


async def test_catalog_build_result_uses_key_namespace() -> None:
    catalog = Catalog("artifact", indexes=[BM25Index("title_bm25", field="title")])
    catalog.set_entries(_enumeration_schema(namespace="series").build_entries(_enumeration_df()))

    await catalog.build()

    assert {entry.namespace for entry in catalog.entries} == {"series"}


def test_build_entries_requires_key_namespace() -> None:
    with pytest.raises(ValueError, match="KEY column must declare namespace"):
        _enumeration_schema(namespace=None).build_entries(_enumeration_df())


async def test_catalog_mutation_methods_require_rebuild(tmp_path: Path) -> None:
    catalog = Catalog("artifact", indexes=[BM25Index("title_bm25", field="title")])
    catalog.set_entries(_entries())
    await catalog.build()

    catalog.set_entries([CatalogEntry(namespace="series", code="D", title="delta title")])
    with pytest.raises(ValueError, match="built before it can be searched"):
        await catalog.search("delta", limit=1)
    await catalog.build()

    catalog.set_indexes([BM25Index("code_bm25", field="code")])
    catalog = Catalog("artifact", indexes=[BM25Index("code_bm25", field="code")], default_field="code")
    catalog.set_entries(_entries())
    with pytest.raises(ValueError, match="built before it can be saved"):
        await catalog.save(f"file://{tmp_path}/artifact")
    await catalog.build()


async def test_catalog_must_be_built_before_search_or_push(tmp_path: Path) -> None:
    catalog = Catalog("artifact", indexes=[BM25Index("title_bm25", field="title")])
    catalog.set_entries(_entries())

    with pytest.raises(ValueError, match="built before it can be searched"):
        await catalog.search("alpha", limit=1)
    with pytest.raises(ValueError, match="built before it can be saved"):
        await catalog.save(f"file://{tmp_path}/artifact")


async def test_sparse_metadata_indexes_ignore_missing_or_empty_values() -> None:
    catalog = Catalog(
        "artifact",
        indexes=[
            HybridIndex(
                "description_hybrid",
                "description",
                indexes=[
                    BM25Index("description_bm25", field="description"),
                    VectorIndex("description_vector", field="description", embedder=_StubEmbedder()),
                ],
            )
        ],
        default_field="description",
    )
    catalog.set_entries(
        [
            CatalogEntry(namespace="series", code="A", title="alpha", metadata={"description": "alpha signal"}),
            CatalogEntry(namespace="series", code="B", title="beta", metadata={}),
            CatalogEntry(namespace="series", code="C", title="gamma", metadata={"description": ""}),
        ]
    )

    await catalog.build()
    hits, _ = await catalog.search("alpha", limit=5)

    assert [hit.code for hit in hits] == ["A"]


async def test_empty_sparse_index_builds_and_returns_no_ranking() -> None:
    catalog = Catalog(
        "artifact", indexes=[BM25Index("description_bm25", field="description")], default_field="description"
    )
    catalog.set_entries([CatalogEntry(namespace="series", code="A", title="alpha")])

    await catalog.build()
    hits, _ = await catalog.search("alpha", limit=5)

    assert list(hits) == []
