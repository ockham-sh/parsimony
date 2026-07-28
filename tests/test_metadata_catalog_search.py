"""Metadata fields are ordinary catalog fields selected by index configuration."""

from __future__ import annotations

import hashlib

import pandas as pd

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.embedder import EmbedderInfo
from parsimony.result import Column, ColumnRole, OutputSpec, Result


class _StubEmbedder:
    DIM = 8

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            out.append([x / norm for x in raw])
        return out

    def embed_query(self, query: str) -> list[float]:
        (vector,) = self.embed_texts([query])
        return vector

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub/hash-sha256", dim=self.DIM, normalize=True, package="test-stub")


def _enumeration_schema() -> OutputSpec:
    return OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="test_ns"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="description", role=ColumnRole.METADATA),
            Column(name="unit", role=ColumnRole.METADATA),
        ]
    )


def test_entities_keeps_description_as_metadata() -> None:
    df = pd.DataFrame(
        {
            "code": ["A.1", "B.2"],
            "title": ["Series A", "Series B"],
            "description": ["All outstanding debt held by the public.", "Intragovernmental holdings."],
            "unit": ["USD", "USD"],
        }
    )
    entries = Result(raw=df, output_spec=_enumeration_schema()).entities.values()

    by_code = {entry.code: entry for entry in entries}
    assert by_code["A.1"].metadata["description"] == "All outstanding debt held by the public."
    assert by_code["B.2"].metadata["unit"] == "USD"
    assert "description" not in Entity.model_fields


def test_metadata_is_searchable_only_when_index_targets_it() -> None:
    df = pd.DataFrame(
        {
            "code": [f"ROW.{i}" for i in range(10)] + ["A.1"],
            "title": [f"Filler series {i}" for i in range(10)] + ["First series"],
            "description": [f"Some padding text {i}." for i in range(10)]
            + ["Mentions renewable wind energy production."],
            "unit": ["USD"] * 10 + ["MWh"],
        }
    )
    entries = Result(raw=df, output_spec=_enumeration_schema()).entities.values()

    title_only = Catalog(name="test_ns")
    title_only.set_entities(entries)
    title_only.build()
    title_hits = title_only.search("renewable wind energy", limit=2)
    assert not title_hits or title_hits[0].code != "A.1"

    description_indexed = Catalog(
        name="test_ns",
        indexes={"description": BM25Index()},
    )
    description_indexed.set_entities(entries)
    description_indexed.build()
    hits = description_indexed.search("renewable wind energy", field="description", limit=2)
    assert hits[0].code == "A.1"
    assert hits[0].score > 0
