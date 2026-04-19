"""Tests for the Parquet <-> SeriesEntry round-trip transforms."""

from __future__ import annotations

import pyarrow as pa
import pytest

from parsimony.bundles.errors import BundleError
from parsimony.bundles.format import ENTRIES_PARQUET_SCHEMA
from parsimony.catalog.arrow_adapters import (
    arrow_rows_to_entries,
    arrow_table_to_entries,
    entries_to_arrow_table,
)
from parsimony.catalog.models import SeriesEntry


def _entries(n: int) -> list[SeriesEntry]:
    return [
        SeriesEntry(
            namespace="snb",
            code=f"cube_{i}",
            title=f"Series {i}",
            description=f"Desc {i}" if i % 2 == 0 else None,
            tags=["macro", "ch"],
            metadata={"freq": "M", "index": i},
            properties={"prop": f"p{i}"},
        )
        for i in range(n)
    ]


class TestRoundTrip:
    def test_round_trip_preserves_fields(self):
        original = _entries(3)
        table = entries_to_arrow_table(original)
        recovered = arrow_table_to_entries(table, namespace="snb")

        assert len(recovered) == len(original)
        for a, b in zip(original, recovered, strict=True):
            assert a.namespace == b.namespace
            assert a.code == b.code
            assert a.title == b.title
            assert a.description == b.description
            assert list(a.tags) == list(b.tags)
            assert a.metadata == b.metadata
            assert a.properties == b.properties

    def test_row_id_is_dense(self):
        table = entries_to_arrow_table(_entries(5))
        ids = table.column("row_id").to_pylist()
        assert ids == [0, 1, 2, 3, 4]

    def test_schema_matches_contract(self):
        table = entries_to_arrow_table(_entries(2))
        assert table.schema == ENTRIES_PARQUET_SCHEMA


class TestAlignmentGuard:
    def test_strict_rejects_gap_in_row_ids(self):
        base = entries_to_arrow_table(_entries(3))
        # Flip row_id ordering by sorting descending and reassigning.
        tampered = base.drop(["row_id"]).append_column(
            "row_id",
            pa.array([0, 2, 4], type=pa.int64()),
        )
        with pytest.raises(BundleError):
            arrow_table_to_entries(tampered, namespace="snb")

    def test_strict_rejects_namespace_mismatch(self):
        base = entries_to_arrow_table(_entries(2))
        with pytest.raises(BundleError, match="namespace"):
            arrow_table_to_entries(base, namespace="other")

    def test_arrow_rows_allows_arbitrary_row_ids(self):
        table = entries_to_arrow_table(_entries(4))
        subset = table.take(pa.array([2, 0, 3], type=pa.int64()))
        # arrow_rows_to_entries skips alignment validation for point-lookup paths.
        recovered = arrow_rows_to_entries(subset)
        codes = [e.code for e in recovered]
        assert codes == ["cube_2", "cube_0", "cube_3"]


class TestSchemaValidation:
    def test_missing_column_raises(self):
        table = pa.table({"namespace": ["snb"], "code": ["c"]})
        with pytest.raises(BundleError, match="missing required columns"):
            arrow_table_to_entries(table, namespace="snb")

    def test_wrong_column_type_raises(self):
        base = entries_to_arrow_table(_entries(1))
        # Replace row_id with a string column of the same name.
        tampered = base.drop(["row_id"]).append_column(
            "row_id",
            pa.array(["0"], type=pa.string()),
        )
        with pytest.raises(BundleError, match="type"):
            arrow_table_to_entries(tampered, namespace="snb")
