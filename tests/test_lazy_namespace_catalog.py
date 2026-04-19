"""Tests for LazyNamespaceCatalog: auto-populate via bundle loader or enumerator."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony import Catalog, SeriesEntry
from parsimony._standard.embedder import EmbeddingProvider
from parsimony.bundles.lazy_catalog import (
    LazyNamespaceCatalog,
    _find_enumerator,
    _template_to_regex,
)
from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.connector import Connectors, enumerator
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result


class _FakeEmbedder(EmbeddingProvider):
    """Deterministic 4-dim embedder for tests — no torch, no network."""

    _DIM = 4

    @property
    def dimension(self) -> int:
        return self._DIM

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        # Same vector for every text so FAISS builds a valid IP index.
        return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

    async def embed_query(self, query: str) -> list[float]:
        return [1.0, 0.0, 0.0, 0.0]

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="fake", dim=self._DIM, normalize=True, package="tests")


def _new_catalog(name: str) -> Catalog:
    return Catalog(name, embedder=_FakeEmbedder())


# ---------------------------------------------------------------------------
# _template_to_regex
# ---------------------------------------------------------------------------


def test_template_to_regex_escapes_literals_and_captures_placeholders() -> None:
    pattern = _template_to_regex("sdmx_series_{agency}_{dataset_id}")
    match = pattern.match("sdmx_series_ecb_yc")
    assert match is not None
    assert match.groupdict() == {"agency": "ecb", "dataset_id": "yc"}


def test_template_to_regex_adjacent_placeholders_are_non_greedy() -> None:
    pattern = _template_to_regex("{a}_{b}")
    match = pattern.match("x_y")
    assert match is not None
    assert match.groupdict() == {"a": "x", "b": "y"}


# ---------------------------------------------------------------------------
# _find_enumerator
# ---------------------------------------------------------------------------


class _NoParams(BaseModel):
    pass


class _SeriesParams(BaseModel):
    agency: str
    dataset_id: str


_DATASETS_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="sdmx_datasets"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

_SERIES_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_{agency}_{dataset_id}"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
    ]
)


@enumerator(output=_DATASETS_OUTPUT)
async def _enumerate_datasets(params: _NoParams) -> Result:
    """List SDMX datasets."""
    return Result.from_dataframe(
        pd.DataFrame({"code": ["a"], "title": ["A"]}),
        Provenance(source="sdmx"),
    )


@enumerator(output=_SERIES_OUTPUT)
async def _enumerate_series(params: _SeriesParams) -> Result:
    """List series for (agency, dataset_id)."""
    return Result.from_dataframe(
        pd.DataFrame(
            {
                "code": [f"{params.agency}_{params.dataset_id}_001"],
                "title": ["Series"],
                "agency": [params.agency],
                "dataset_id": [params.dataset_id],
            }
        ),
        Provenance(source="sdmx"),
    )


def test_find_enumerator_matches_static_namespace() -> None:
    match = _find_enumerator(Connectors([_enumerate_datasets]), "sdmx_datasets")
    assert match is not None
    connector, params = match
    assert connector is _enumerate_datasets
    assert params == {}


def test_find_enumerator_returns_none_for_unknown_namespace() -> None:
    assert _find_enumerator(Connectors([_enumerate_datasets]), "other_namespace") is None


def test_find_enumerator_reverse_resolves_template_namespace() -> None:
    match = _find_enumerator(Connectors([_enumerate_series]), "sdmx_series_ecb_yc")
    assert match is not None
    connector, params = match
    assert connector is _enumerate_series
    assert params == {"agency": "ecb", "dataset_id": "yc"}


# ---------------------------------------------------------------------------
# LazyNamespaceCatalog: construction
# ---------------------------------------------------------------------------


def test_requires_at_least_one_source() -> None:
    base = _new_catalog("test")
    with pytest.raises(ValueError, match="connectors|bundle_loader"):
        LazyNamespaceCatalog(base)


def test_exposes_base_name_and_entries() -> None:
    base = _new_catalog("demo")
    wrapped = LazyNamespaceCatalog(base, connectors=Connectors([]))
    assert wrapped.name == "demo"
    assert wrapped.entries == []


# ---------------------------------------------------------------------------
# Bundle loader path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bundle_loader_populates_cold_namespace() -> None:
    """When the bundle loader returns a catalog, entries land in the base."""

    async def loader(ns: str) -> Catalog | None:
        remote = _new_catalog(ns)
        await remote.upsert([SeriesEntry(namespace=ns, code="X1", title="Example")])
        return remote

    base = _new_catalog("multi")
    wrapped = LazyNamespaceCatalog(base, bundle_loader=loader)
    await wrapped._ensure_namespace("example")
    assert "example" in await base.list_namespaces()
    assert await base.get("example", "X1") is not None


@pytest.mark.asyncio
async def test_bundle_loader_returning_none_falls_through(monkeypatch: Any) -> None:
    async def loader(ns: str) -> Catalog | None:
        return None

    base = _new_catalog("multi")
    wrapped = LazyNamespaceCatalog(base, bundle_loader=loader)
    # No connectors either — the namespace is marked attempted without being populated.
    await wrapped._ensure_namespace("missing")
    assert "missing" not in await base.list_namespaces()
    assert "missing" in wrapped._attempted


@pytest.mark.asyncio
async def test_attempted_cache_prevents_re_probe(monkeypatch: Any) -> None:
    call_count = {"n": 0}

    async def loader(ns: str) -> Catalog | None:
        call_count["n"] += 1
        return None

    base = _new_catalog("multi")
    wrapped = LazyNamespaceCatalog(base, bundle_loader=loader)
    await wrapped._ensure_namespace("missing")
    await wrapped._ensure_namespace("missing")
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_invalidate_clears_cache() -> None:
    async def loader(ns: str) -> Catalog | None:
        return None

    base = _new_catalog("multi")
    wrapped = LazyNamespaceCatalog(base, bundle_loader=loader)
    await wrapped._ensure_namespace("x")
    assert "x" in wrapped._attempted
    wrapped.invalidate("x")
    assert "x" not in wrapped._attempted
    wrapped.invalidate()
    assert not wrapped._attempted
