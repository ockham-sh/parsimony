"""Tests for DataStore, load_result, and InMemoryDataStore."""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, loader
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    SemanticTableResult,
)
from parsimony.stores.data_store import DataStore, LoadResult, _data_from_table_result
from parsimony.stores.memory_data import InMemoryDataStore


class _Params(BaseModel):
    q: str = "x"


LOAD_SCHEMA = OutputConfig(
    columns=[
        Column(name="code_col", role=ColumnRole.KEY, namespace="test_ns"),
        Column(name="obs", role=ColumnRole.DATA),
    ]
)


@loader(output=LOAD_SCHEMA)
async def demo_loader(_p: _Params) -> pd.DataFrame:
    """Load test observations."""
    return pd.DataFrame({"code_col": ["A"], "obs": [1.0]})


def test_data_from_table_result_extracts_data_columns_only() -> None:
    table = SemanticTableResult(
        data=pd.DataFrame({"code_col": ["X"], "obs": [42.0], "extra": ["z"]}),
        provenance=Provenance(source="t"),
        output_schema=LOAD_SCHEMA,
    )
    rows = _data_from_table_result(table)
    assert len(rows) == 1
    ns, code, df = rows[0]
    assert ns == "test_ns"
    assert code == "X"
    assert list(df.columns) == ["obs"]
    assert df["obs"].iloc[0] == 42.0


def test_data_from_table_result_groups_by_key() -> None:
    table = SemanticTableResult(
        data=pd.DataFrame(
            {
                "code_col": ["A", "B", "A"],
                "obs": [1.0, 2.0, 3.0],
            }
        ),
        provenance=Provenance(source="t"),
        output_schema=LOAD_SCHEMA,
    )
    rows = _data_from_table_result(table)
    assert len(rows) == 2
    by_code = {code: df for _, code, df in rows}
    assert len(by_code["A"]) == 2
    assert len(by_code["B"]) == 1
    assert list(by_code["A"].columns) == ["obs"]


def test_data_from_table_result_requires_key_namespace() -> None:
    table = SemanticTableResult(
        data=pd.DataFrame({"code_col": ["a"], "obs": [1.0]}),
        provenance=Provenance(source="t"),
        output_schema=OutputConfig(
            columns=[
                Column(name="code_col", role=ColumnRole.KEY),
                Column(name="obs", role=ColumnRole.DATA),
            ]
        ),
    )
    with pytest.raises(ValueError, match="namespace"):
        _data_from_table_result(table)


@pytest.mark.asyncio
async def test_load_result_skips_existing_keys() -> None:
    store = InMemoryDataStore()
    await store.upsert("test_ns", "A", pd.DataFrame({"obs": [0.0]}))

    table = SemanticTableResult(
        data=pd.DataFrame({"code_col": ["A", "B"], "obs": [1.0, 2.0]}),
        provenance=Provenance(source="t"),
        output_schema=LOAD_SCHEMA,
    )
    r = await store.load_result(table, force=False)
    assert r.total == 2
    assert r.loaded == 1
    assert r.skipped == 1
    b = await store.get("test_ns", "B")
    assert b is not None and b["obs"].iloc[0] == 2.0
    a = await store.get("test_ns", "A")
    assert a is not None and a["obs"].iloc[0] == 0.0


@pytest.mark.asyncio
async def test_load_result_force_upserts_existing() -> None:
    store = InMemoryDataStore()
    await store.upsert("test_ns", "A", pd.DataFrame({"obs": [0.0]}))

    table = SemanticTableResult(
        data=pd.DataFrame({"code_col": ["A"], "obs": [9.0]}),
        provenance=Provenance(source="t"),
        output_schema=LOAD_SCHEMA,
    )
    r = await store.load_result(table, force=True)
    assert r.total == 1
    assert r.loaded == 1
    assert r.skipped == 0
    a = await store.get("test_ns", "A")
    assert a is not None and a["obs"].iloc[0] == 9.0


@pytest.mark.asyncio
async def test_load_result_as_callback() -> None:
    store = InMemoryDataStore()
    wired = demo_loader.with_callback(store.load_result)
    await wired(q="x")
    df = await store.get("test_ns", "A")
    assert df is not None
    assert list(df.columns) == ["obs"]
    assert df["obs"].iloc[0] == 1.0


@pytest.mark.asyncio
async def test_load_result_via_connectors_with_callback() -> None:
    store = InMemoryDataStore()
    c = Connectors([demo_loader.with_callback(store.load_result)])
    await c["demo_loader"](q="x")
    df = await store.get("test_ns", "A")
    assert df is not None


@pytest.mark.asyncio
async def test_in_memory_data_store_crud() -> None:
    store = InMemoryDataStore()
    df = pd.DataFrame({"x": [1, 2]})
    await store.upsert("ns", "c1", df)
    assert await store.exists([("ns", "c1")]) == {("ns", "c1")}
    got = await store.get("ns", "c1")
    assert got is not None
    pd.testing.assert_frame_equal(got.reset_index(drop=True), df.reset_index(drop=True))
    await store.delete("ns", "c1")
    assert await store.get("ns", "c1") is None


def test_in_memory_data_store_is_data_store() -> None:
    store = InMemoryDataStore()
    assert isinstance(store, DataStore)


def test_load_result_model() -> None:
    r = LoadResult(total=2, loaded=1, skipped=1, errors=0)
    assert r.total == 2
