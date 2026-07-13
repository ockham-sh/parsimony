"""Tests for InMemoryDataStore and load_result."""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.connector import Connectors, loader
from parsimony.entity import EntityRef
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result
from parsimony.stores import InMemoryDataStore, LoadResult

LOAD_SCHEMA = OutputSpec(
    columns=[
        Column(name="code_col", role=ColumnRole.KEY, namespace="test_ns"),
        Column(name="obs", role=ColumnRole.DATA),
    ]
)


@loader(output=LOAD_SCHEMA)
def demo_loader(q: str = "x") -> pd.DataFrame:
    """Load test observations."""
    return pd.DataFrame({"code_col": ["A"], "obs": [1.0]})


def test_result_data_extracts_data_columns_only() -> None:
    table = Result(
        raw=pd.DataFrame({"code_col": ["X"], "obs": [42.0], "extra": ["z"]}),
        provenance=Provenance(source="t", source_description="t"),
        output_spec=LOAD_SCHEMA,
    )
    data = table.data
    assert len(data) == 1
    frame = data[EntityRef("test_ns", "X")]
    assert list(frame.columns) == ["obs"]
    assert frame["obs"].iloc[0] == 42.0


def test_result_data_groups_by_key() -> None:
    table = Result(
        raw=pd.DataFrame(
            {
                "code_col": ["A", "B", "A"],
                "obs": [1.0, 2.0, 3.0],
            }
        ),
        provenance=Provenance(source="t", source_description="t"),
        output_spec=LOAD_SCHEMA,
    )
    data = table.data
    assert len(data) == 2
    assert len(data[EntityRef("test_ns", "A")]) == 2
    assert len(data[EntityRef("test_ns", "B")]) == 1
    assert list(data[EntityRef("test_ns", "A")].columns) == ["obs"]
    assert data.keys() == table.entities.keys()


def test_loader_requires_key_namespace() -> None:
    unnamespaced = OutputSpec(
        columns=[
            Column(name="code_col", role=ColumnRole.KEY),
            Column(name="obs", role=ColumnRole.DATA),
        ]
    )
    with pytest.raises(ValueError, match="non-empty namespace"):
        loader(output=unnamespaced)(pd.DataFrame)


def test_load_result_skips_existing_keys() -> None:
    store = InMemoryDataStore()
    store.upsert("test_ns", "A", pd.DataFrame({"obs": [0.0]}))

    table = Result(
        raw=pd.DataFrame({"code_col": ["A", "B"], "obs": [1.0, 2.0]}),
        provenance=Provenance(source="t", source_description="t"),
        output_spec=LOAD_SCHEMA,
    )
    r = store.load_result(table, force=False)
    assert r.total == 2
    assert r.loaded == 1
    assert r.skipped == 1
    b = store.get("test_ns", "B")
    assert b is not None and b["obs"].iloc[0] == 2.0
    a = store.get("test_ns", "A")
    assert a is not None and a["obs"].iloc[0] == 0.0


def test_load_result_force_upserts_existing() -> None:
    store = InMemoryDataStore()
    store.upsert("test_ns", "A", pd.DataFrame({"obs": [0.0]}))

    table = Result(
        raw=pd.DataFrame({"code_col": ["A"], "obs": [9.0]}),
        provenance=Provenance(source="t", source_description="t"),
        output_spec=LOAD_SCHEMA,
    )
    r = store.load_result(table, force=True)
    assert r.total == 1
    assert r.loaded == 1
    assert r.skipped == 0
    a = store.get("test_ns", "A")
    assert a is not None and a["obs"].iloc[0] == 9.0


def test_load_result_directly() -> None:
    """Userland pattern: call store.load_result(result) after the connector returns."""
    store = InMemoryDataStore()
    result = demo_loader(q="x")
    store.load_result(result)
    df = store.get("test_ns", "A")
    assert df is not None
    assert list(df.columns) == ["obs"]
    assert df["obs"].iloc[0] == 1.0


def test_load_result_via_connectors() -> None:
    store = InMemoryDataStore()
    c = Connectors([demo_loader])
    result = c["demo_loader"](q="x")
    store.load_result(result)
    df = store.get("test_ns", "A")
    assert df is not None


def test_data_store_crud() -> None:
    store = InMemoryDataStore()
    df = pd.DataFrame({"x": [1, 2]})
    store.upsert("ns", "c1", df)
    assert store.exists([("ns", "c1")]) == {("ns", "c1")}
    got = store.get("ns", "c1")
    assert got is not None
    pd.testing.assert_frame_equal(got.reset_index(drop=True), df.reset_index(drop=True))
    store.delete("ns", "c1")
    assert store.get("ns", "c1") is None


def test_load_result_model() -> None:
    r = LoadResult(total=2, loaded=1, skipped=1, errors=0)
    assert r.total == 2
