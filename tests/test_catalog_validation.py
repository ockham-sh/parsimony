"""Tests for catalog snapshot validation and manifest contract digests."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog.storage import META_FILENAME, CatalogMeta
from parsimony.catalog.validation import (
    CatalogValidationError,
    compute_manifest_contract_sha256,
    validate_catalog_snapshot,
)


def _write_memory_catalog(target: Path) -> None:
    catalog = Catalog(name="demo", indexes={"title": BM25Index()})
    catalog.set_entities([Entity(namespace="demo", code="A", title="alpha")])
    catalog.build()
    catalog.save(f"file://{target}")


def _mirror_as_hf_snapshot(real: Path, snap: Path) -> None:
    """Mirror *real* into *snap* the way ``huggingface_hub.snapshot_download`` does:
    real directories, but every file a symlink pointing OUTSIDE *snap* (into the
    blob cache). Used to prove validation tolerates symlinked snapshot files.
    """
    for src in sorted(real.rglob("*")):
        dst = snap / src.relative_to(real)
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.symlink_to(src)


def test_validate_catalog_snapshot_memory_backend(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    _write_memory_catalog(bundle)
    meta = validate_catalog_snapshot(bundle)
    assert meta.name == "demo"
    assert meta.build.manifest_contract_sha256


def test_validate_catalog_snapshot_accepts_hf_symlinked_files(tmp_path: Path) -> None:
    """Regression: snapshot_download stores files as symlinks into a blob cache,
    so a legitimate rows file resolves outside the snapshot dir. Validation must
    not reject it as escaping the catalog directory."""
    real = tmp_path / "blobs"
    _write_memory_catalog(real)
    snap = tmp_path / "snapshot"
    _mirror_as_hf_snapshot(real, snap)
    assert (snap / "entries.parquet").is_symlink()
    meta = validate_catalog_snapshot(snap)
    assert meta.name == "demo"


def test_unknown_backend_kind_rejected(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    _write_memory_catalog(bundle)
    raw = json.loads((bundle / META_FILENAME).read_text())
    raw["backend"]["kind"] = "weird"
    (bundle / META_FILENAME).write_text(json.dumps(raw))
    with pytest.raises(ValidationError):
        validate_catalog_snapshot(bundle)


def test_manifest_contract_digest_mismatch(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    _write_memory_catalog(bundle)
    raw = json.loads((bundle / META_FILENAME).read_text())
    raw["backend"]["code_column"] = "wrong"
    raw["build"]["manifest_contract_sha256"] = "deadbeef"
    (bundle / META_FILENAME).write_text(json.dumps(raw))
    with pytest.raises(CatalogValidationError, match="manifest contract digest"):
        validate_catalog_snapshot(bundle)


def test_parquet_backend_missing_field_link_column(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "indexes" / "title").mkdir(parents=True)
    pq.write_table(
        pa.table({"key": ["k1"], "title": ["t1"]}),
        bundle / "rows.parquet",
    )
    meta_obj = CatalogMeta.model_validate(
        {
            "schema_version": 1,
            "name": "series",
            "namespaces": ["series"],
            "entry_count": 1,
            "index_fields": {"title": "bm25"},
            "default_field": "title",
            "backend": {
                "kind": "parquet",
                "rows_filename": "rows.parquet",
                "namespace": "series",
                "code_column": "key",
                "title_column": "title",
                "field_links": {"geo_label": "geo_code"},
            },
            "build": {"content_sha256": "", "manifest_contract_sha256": ""},
        }
    )
    raw = json.loads(meta_obj.model_dump_json())
    raw["build"]["manifest_contract_sha256"] = compute_manifest_contract_sha256(meta_obj)
    (bundle / META_FILENAME).write_text(json.dumps(raw))
    with pytest.raises(CatalogValidationError, match="field_links"):
        validate_catalog_snapshot(bundle)


def test_catalog_load_runs_validation(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    _write_memory_catalog(bundle)
    (bundle / "indexes" / "extra").mkdir()
    with pytest.raises(CatalogValidationError, match="Unexpected index directory"):
        Catalog.load(f"file://{bundle}")


def test_bm25_index_accepts_values_that_tokenize_empty(tmp_path: Path) -> None:
    catalog = Catalog(name="demo", indexes={"label": BM25Index()})
    catalog.set_entities([Entity(namespace="demo", code="A", title="alpha", metadata={"label": "-"})])
    catalog.build()
    catalog.save(f"file://{tmp_path / 'bundle'}")
    loaded = Catalog.load(f"file://{tmp_path / 'bundle'}")

    assert loaded.search("label: -", limit=5)[0].code == "A"
