from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from parsimony.catalog import (
    BM25Index,
    Catalog,
    Entity,
    HybridIndex,
    VectorIndex,
)
from parsimony.embedder import EmbedderInfo
from parsimony.result import Column, ColumnRole, OutputConfig


def _entries() -> list[Entity]:
    return [
        Entity(namespace="series", code="A", title="alpha title"),
        Entity(namespace="series", code="B", title="beta title"),
        Entity(namespace="series", code="C", title="gamma title"),
    ]


def test_save_coerces_non_json_native_metadata(tmp_path: Path) -> None:
    # A datetime/Decimal metadata value must not crash the whole snapshot save;
    # it serializes to its string form (default=str) and round-trips as a string.
    import datetime as _dt
    from decimal import Decimal

    catalog = Catalog(name="meta", indexes={"title": BM25Index()})
    catalog.set_entities(
        [
            Entity(
                namespace="meta",
                code="A",
                title="alpha",
                metadata={"updated": _dt.date(2026, 6, 11), "ratio": Decimal("1.5")},
            )
        ]
    )
    catalog.build()
    catalog.save(f"file://{tmp_path}/snap")

    loaded = Catalog.load(f"file://{tmp_path}/snap")
    md = loaded.entities[0].metadata
    assert md["updated"] == "2026-06-11"
    assert md["ratio"] == "1.5"


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
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] if "alpha" in text else [0.0, 1.0] for text in texts]

    def embed_query(self, query: str) -> list[float]:
        return [1.0, 0.0] if "alpha" in query else [0.0, 1.0]

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub", dim=2, normalize=True, package="test")


def test_catalog_build_entities_static_indexes_and_ranker() -> None:
    catalog = Catalog("artifact", indexes={"title": BM25Index()})
    catalog.set_entities(_entries())

    catalog.build()

    hits = catalog.search("alpha", limit=1)
    assert hits[0].code == "A"


def test_catalog_build_result_uses_key_namespace() -> None:
    catalog = Catalog("artifact", indexes={"title": BM25Index()})
    catalog.set_entities(_enumeration_schema(namespace="series").build_entities(_enumeration_df()))

    catalog.build()

    assert {entry.namespace for entry in catalog.entities} == {"series"}


def test_build_entities_requires_key_namespace() -> None:
    with pytest.raises(ValueError, match="KEY column must declare namespace"):
        _enumeration_schema(namespace=None).build_entities(_enumeration_df())


def test_catalog_mutation_methods_require_rebuild(tmp_path: Path) -> None:
    catalog = Catalog("artifact", indexes={"title": BM25Index()})
    catalog.set_entities(_entries())
    catalog.build()

    catalog.set_entities([Entity(namespace="series", code="D", title="delta title")])
    with pytest.raises(ValueError, match="catalog.build\\(\\)"):
        catalog.search("delta", limit=1)
    catalog.build()

    catalog.set_indexes({"code": BM25Index()})
    catalog = Catalog("artifact", indexes={"code": BM25Index()}, default_field="code")
    catalog.set_entities(_entries())
    with pytest.raises(ValueError, match="catalog.build\\(\\)"):
        catalog.save(f"file://{tmp_path}/artifact")
    catalog.build()


def test_catalog_must_be_built_before_search_or_push(tmp_path: Path) -> None:
    catalog = Catalog("artifact", indexes={"title": BM25Index()})
    catalog.set_entities(_entries())

    with pytest.raises(ValueError, match="catalog.build\\(\\)"):
        catalog.search("alpha", limit=1)
    with pytest.raises(ValueError, match="catalog.build\\(\\)"):
        catalog.save(f"file://{tmp_path}/artifact")


def test_sparse_metadata_indexes_ignore_missing_or_empty_values() -> None:
    catalog = Catalog(
        "artifact",
        indexes={
            "description": HybridIndex(
                components=[
                    BM25Index(),
                    VectorIndex(embedder=_StubEmbedder()),
                ],
            )
        },
        default_field="description",
    )
    catalog.set_entities(
        [
            Entity(namespace="series", code="A", title="alpha", metadata={"description": "alpha signal"}),
            Entity(namespace="series", code="B", title="beta", metadata={}),
            Entity(namespace="series", code="C", title="gamma", metadata={"description": ""}),
        ]
    )

    catalog.build()
    hits = catalog.search("alpha", limit=5)

    assert [hit.code for hit in hits] == ["A"]


def test_empty_sparse_index_builds_and_returns_no_ranking() -> None:
    catalog = Catalog("artifact", indexes={"description": BM25Index()}, default_field="description")
    catalog.set_entities([Entity(namespace="series", code="A", title="alpha")])

    catalog.build()
    hits = catalog.search("alpha", limit=5)

    assert list(hits) == []
