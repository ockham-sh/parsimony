"""Tests for BaseCatalog orchestration: index_result, ingest, dedupe, dry-run."""

from __future__ import annotations

import builtins
from typing import Any

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.catalog.catalog import BaseCatalog, _entries_from_table_result
from parsimony.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    catalog_key,
    code_token,
)
from parsimony.connector import Connectors, enumerator
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    SemanticTableResult,
)


class _AutoIndexFetchParams(BaseModel):
    q: str = "x"


def test_code_token_normalizes() -> None:
    assert code_token("ECB-YC") == "ecb_yc"
    assert code_token("  SR.3M  ") == "sr_3m"


class _RecordingCatalog(BaseCatalog):
    """In-memory catalog that records every upsert; minimal subclass for orchestration tests."""

    def __init__(self, existing: set[tuple[str, str]] | None = None, *, name: str = "test") -> None:
        self.name = name
        self.existing: set[tuple[str, str]] = set(existing or set())
        self.upsert_batches: list[list[SeriesEntry]] = []

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        self.upsert_batches.append(list(entries))
        for e in entries:
            self.existing.add(catalog_key(e.namespace, e.code))

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        return None

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        return {k for k in keys if catalog_key(k[0], k[1]) in self.existing}

    async def delete(self, namespace: str, code: str) -> None:
        self.existing.discard(catalog_key(namespace, code))

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
    ) -> builtins.list[SeriesMatch]:
        return []

    async def list_namespaces(self) -> builtins.list[str]:
        return []

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        return [], 0


@pytest.mark.asyncio
async def test_fred_enumerate_requires_bound_api_key() -> None:
    from parsimony.connectors.fred import FredEnumerateParams, enumerate_fred_release

    with pytest.raises(TypeError, match="unbound dependencies"):
        await enumerate_fred_release(FredEnumerateParams(release_id=1))


@pytest.mark.asyncio
async def test_ingest_inserts_new_entries() -> None:
    catalog = _RecordingCatalog()
    entries = [SeriesEntry(namespace="fred", code="GDPC1", title="GDP")]
    result = await catalog.ingest(entries)
    assert isinstance(result, IndexResult)
    assert (result.total, result.indexed, result.skipped, result.errors) == (1, 1, 0, 0)
    assert len(catalog.upsert_batches) == 1


@pytest.mark.asyncio
async def test_ingest_skips_existing_keys() -> None:
    catalog = _RecordingCatalog(existing={catalog_key("fred", "GDPC1")})
    entries = [SeriesEntry(namespace="fred", code="GDPC1", title="GDP")]
    result = await catalog.ingest(entries)
    assert (result.indexed, result.skipped) == (0, 1)
    assert catalog.upsert_batches == []


@pytest.mark.asyncio
async def test_ingest_force_upserts_existing_keys() -> None:
    catalog = _RecordingCatalog(existing={catalog_key("fred", "GDPC1")})
    entries = [SeriesEntry(namespace="fred", code="GDPC1", title="GDP")]
    result = await catalog.ingest(entries, force=True)
    assert (result.indexed, result.skipped) == (1, 0)
    assert len(catalog.upsert_batches) == 1


@pytest.mark.asyncio
async def test_index_result_requires_key_namespace() -> None:
    catalog = _RecordingCatalog()
    df = pd.DataFrame({"k": ["x"], "t": ["T"]})
    table = SemanticTableResult(
        data=df,
        provenance=Provenance(source="x"),
        output_schema=OutputConfig(
            columns=[
                Column(name="k", role=ColumnRole.KEY),
                Column(name="t", role=ColumnRole.TITLE),
            ]
        ),
    )
    with pytest.raises(ValueError, match="namespace"):
        await catalog.index_result(table)


def _dry_run_sample_table() -> SemanticTableResult:
    return SemanticTableResult(
        data=pd.DataFrame({"code_col": ["A", "B"], "title_col": ["One", "Two"]}),
        provenance=Provenance(source="dry_test"),
        output_schema=OutputConfig(
            columns=[
                Column(name="code_col", role=ColumnRole.KEY, namespace="test_ns"),
                Column(name="title_col", role=ColumnRole.TITLE),
            ]
        ),
    )


@pytest.mark.asyncio
async def test_index_result_dry_run_matches_live_dedupe_counts() -> None:
    table = _dry_run_sample_table()
    catalog = _RecordingCatalog()
    dry = await catalog.index_result(table, dry_run=True)
    live = await catalog.index_result(table)
    assert dry.total == live.total == 2
    assert dry.indexed == live.indexed == 2
    assert dry.skipped == live.skipped == 0
    assert len(catalog.upsert_batches) == 1


@pytest.mark.asyncio
async def test_index_result_dry_run_after_ingest_skips_all() -> None:
    table = _dry_run_sample_table()
    catalog = _RecordingCatalog()
    await catalog.index_result(table)
    dry = await catalog.index_result(table, dry_run=True)
    assert (dry.total, dry.indexed, dry.skipped) == (2, 0, 2)


@pytest.mark.asyncio
async def test_index_result_dry_run_force_counts_all_indexed() -> None:
    table = _dry_run_sample_table()
    catalog = _RecordingCatalog()
    await catalog.index_result(table)
    dry = await catalog.index_result(table, dry_run=True, force=True)
    assert (dry.indexed, dry.skipped) == (2, 0)


@pytest.mark.asyncio
async def test_index_result_dry_run_does_not_upsert() -> None:
    table = _dry_run_sample_table()
    catalog = _RecordingCatalog()
    await catalog.index_result(table, dry_run=True)
    assert catalog.upsert_batches == []


@pytest.mark.asyncio
async def test_index_result_as_callback() -> None:
    @enumerator(
        output=OutputConfig(
            columns=[
                Column(name="code_col", role=ColumnRole.KEY, namespace="test_ns"),
                Column(name="title_col", role=ColumnRole.TITLE),
            ],
        ),
    )
    async def fetch_multi(_p: _AutoIndexFetchParams) -> pd.DataFrame:
        """Multi-row fetch for auto-index test."""
        return pd.DataFrame({"code_col": ["A", "B", "A"], "title_col": ["One", "Two", "One"]})

    catalog = _RecordingCatalog()
    wired = Connectors([fetch_multi.with_callback(catalog.index_result)])
    await wired["fetch_multi"](q="x")
    assert len(catalog.upsert_batches) == 1
    batch = catalog.upsert_batches[0]
    assert len(batch) == 2
    by_code = {e.code: e for e in batch}
    assert by_code["A"].title == "One"
    assert by_code["B"].title == "Two"
    assert by_code["A"].namespace == "test_ns"


@pytest.mark.asyncio
async def test_fred_enumerate_connector_produces_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_enumerate(http: Any, release_id: int, *, page_size: int):
        assert release_id == 51
        return [
            {"id": "GDPC1", "title": "Real GDP", "notes": "Billions"},
            {"id": "UNRATE", "title": "Unemployment Rate", "notes": None},
        ]

    monkeypatch.setattr("parsimony.connectors.fred._enumerate_release_series", _fake_enumerate)

    from parsimony.connectors.fred import FredEnumerateParams, enumerate_fred_release

    conn = enumerate_fred_release.bind_deps(api_key="dummy")
    res = await conn(FredEnumerateParams(release_id=51))
    entries = _entries_from_table_result(res, extra_tags=["release_51"])

    assert len(entries) == 2
    assert sorted(e.code for e in entries) == ["GDPC1", "UNRATE"]
    assert entries[0].title == "Real GDP"


@pytest.mark.asyncio
async def test_fred_enumerate_entries_have_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_enumerate(http: Any, release_id: int, *, page_size: int) -> list[dict[str, Any]]:
        return [
            {"id": "GDPC1", "title": "Real GDP", "frequency_short": "Q", "units_short": "Bil. of $"},
            {"id": "UNRATE", "title": "Unemployment Rate", "frequency_short": "M", "units_short": "%"},
        ]

    monkeypatch.setattr("parsimony.connectors.fred._enumerate_release_series", _fake_enumerate)

    from parsimony.connectors.fred import FredEnumerateParams, enumerate_fred_release

    conn = enumerate_fred_release.bind_deps(api_key="dummy")
    res = await conn(FredEnumerateParams(release_id=51))
    entries = _entries_from_table_result(res)

    by_code = {e.code: e for e in entries}
    assert by_code["GDPC1"].metadata["frequency_short"] == "Q"
    assert by_code["GDPC1"].metadata["units_short"] == "Bil. of $"
    assert by_code["GDPC1"].metadata["release_id"] == 51
    assert by_code["UNRATE"].metadata["frequency_short"] == "M"
