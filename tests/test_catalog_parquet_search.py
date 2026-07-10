"""Tests for parquet-backed catalog search."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog.backends import ParquetRowBackend
from parsimony.catalog.contracts import CatalogBackendConfig
from parsimony.catalog.models import UnknownIndexedFieldError
from parsimony.errors import InvalidParameterError


def _build_parquet_catalog(tmp_path: Path) -> Catalog:
    rows = [
        {
            "key": "A",
            "title": "Alpha monthly Germany",
            "FREQ_code": "M",
            "FREQ_label": "Monthly",
            "REF_AREA_code": "DE",
            "REF_AREA_label": "Germany",
        },
        {
            "key": "B",
            "title": "Beta annual France",
            "FREQ_code": "A",
            "FREQ_label": "Annual",
            "REF_AREA_code": "FR",
            "REF_AREA_label": "France",
        },
    ]
    parquet = tmp_path / "rows.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), parquet)

    entities = [
        Entity(namespace="demo", code="FREQ:M", title="Monthly", metadata={"FREQ_label": "Monthly", "FREQ_code": "M"}),
        Entity(namespace="demo", code="FREQ:A", title="Annual", metadata={"FREQ_label": "Annual", "FREQ_code": "A"}),
        Entity(
            namespace="demo",
            code="REF:DE",
            title="Germany",
            metadata={"REF_AREA_label": "Germany", "REF_AREA_code": "DE"},
        ),  # noqa: E501
        Entity(
            namespace="demo",
            code="REF:FR",
            title="France",
            metadata={"REF_AREA_label": "France", "REF_AREA_code": "FR"},
        ),  # noqa: E501
    ]
    catalog = Catalog(
        "demo",
        indexes={
            "title": BM25Index(),
            "FREQ_label": BM25Index(),
            "REF_AREA_label": BM25Index(),
        },
        default_field="title",
        field_links={"FREQ_label": "FREQ_code", "REF_AREA_label": "REF_AREA_code"},
    )
    catalog.set_entities(entities)
    catalog.build()
    backend = CatalogBackendConfig(
        kind="parquet",
        rows_path="rows.parquet",
        namespace="demo",
        code_column="key",
        title_column="title",
    )
    catalog.attach_parquet_rows(parquet, config=backend)
    return catalog


class _DatasetSpy:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.scanner_calls = 0

    @property
    def schema(self) -> Any:
        return self._delegate.schema

    def scanner(self, **kwargs: Any) -> Any:
        self.scanner_calls += 1
        return self._delegate.scanner(**kwargs)

    def to_table(self, **kwargs: Any) -> Any:
        raise AssertionError("ParquetRowBackend.iter_rows must stream via scanner batches")


def test_parquet_filter_only() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = _build_parquet_catalog(Path(tmp))
        matches = catalog.search(filter={"FREQ_code": ["M"]}, limit=10)
        assert {match.code for match in matches} == {"A"}


def test_parquet_filter_accepts_logical_code_alias() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = _build_parquet_catalog(Path(tmp))
        matches = catalog.search(filter={"code": ["A"]}, limit=10)
        assert [match.code for match in matches] == ["A"]


def test_parquet_filter_rejects_unknown_column() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = _build_parquet_catalog(Path(tmp))
        with pytest.raises(InvalidParameterError, match="Unknown parquet filter column"):
            catalog.search(filter={"missing": ["A"]}, limit=10)


def test_parquet_iter_rows_streams_scanner_batches() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        rows = [
            {"key": "A", "title": "Alpha", "FREQ_code": "M"},
            {"key": "B", "title": "Beta", "FREQ_code": "A"},
        ]
        parquet = Path(tmp) / "rows.parquet"
        pq.write_table(pa.Table.from_pylist(rows), parquet)
        backend = ParquetRowBackend(
            parquet,
            config=CatalogBackendConfig(kind="parquet", code_column="key", title_column="title"),
        )
        spy = _DatasetSpy(backend._dataset)  # noqa: SLF001
        backend._dataset = spy  # type: ignore[assignment]  # noqa: SLF001

        streamed = list(backend.iter_rows(filter_spec={"FREQ_code": ["M"]}, columns=["key", "FREQ_code"]))

        assert spy.scanner_calls == 1
        assert streamed == [{"key": "A", "FREQ_code": "M"}]


def test_parquet_field_search_with_filter() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = _build_parquet_catalog(Path(tmp))
        matches = catalog.search(
            query="Germany",
            fields="REF_AREA_label",
            filter={"FREQ_code": ["M"]},
            limit=10,
        )
        assert len(matches) == 1
        assert matches[0].code == "A"


def test_parquet_search_values() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = _build_parquet_catalog(Path(tmp))
        values = catalog.search_values("Germany", field="REF_AREA_label", limit=5)
        assert values
        assert values[0].value == "Germany"
        assert values[0].linked_value == "DE"


def test_search_values_unknown_field_uses_catalog_error() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = _build_parquet_catalog(Path(tmp))
        with pytest.raises(UnknownIndexedFieldError):
            catalog.search_values("Germany", field="missing", limit=5)


def test_parquet_save_load_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        catalog = _build_parquet_catalog(root / "src")
        catalog._save_to_path(root / "snapshot")  # noqa: SLF001
        loaded = Catalog.load(f"file://{root / 'snapshot'}")
        matches = loaded.search(filter={"REF_AREA_code": ["FR"]}, limit=5)
        assert len(matches) == 1
        assert matches[0].code == "B"
