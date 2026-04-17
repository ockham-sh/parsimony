"""Tests for the bundle wire contract (parsimony/stores/hf_bundle/format.py).

These tests don't touch FAISS or HuggingFace — they only exercise the
Pydantic manifest schema and the Parquet schema constants. They run on
every CI environment.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from parsimony.stores.hf_bundle.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    ENTRIES_PARQUET_SCHEMA,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    BundleManifest,
    hf_repo_id,
)

_VALID_MODEL_REV = "c9745ed1d9f207416be6d2e6f8de32d1f16199bf"
_VALID_SHA256 = "a" * 64


def _valid_manifest_kwargs(**overrides):
    base = dict(
        namespace="fred",
        built_at=datetime.now(UTC),
        entry_count=10,
        embedding_model="sentence-transformers/all-MiniLM-L6-v2",
        embedding_model_revision=_VALID_MODEL_REV,
        embedding_dim=384,
        entries_sha256=_VALID_SHA256,
        index_sha256=_VALID_SHA256,
    )
    base.update(overrides)
    return base


class TestBundleFilenames:
    def test_filenames_constant(self):
        assert frozenset({ENTRIES_FILENAME, INDEX_FILENAME, MANIFEST_FILENAME}) == BUNDLE_FILENAMES

    def test_hf_repo_id_template(self):
        assert hf_repo_id("snb") == "parsimony-dev/snb"


class TestParquetSchema:
    def test_has_row_id_int64(self):
        field = ENTRIES_PARQUET_SCHEMA.field("row_id")
        assert str(field.type) == "int64"
        assert not field.nullable

    def test_tags_is_list_of_string(self):
        field = ENTRIES_PARQUET_SCHEMA.field("tags")
        assert str(field.type) == "list<item: string>"

    def test_metadata_properties_are_strings(self):
        # JSON-serialized for schema stability across heterogeneous dicts.
        assert str(ENTRIES_PARQUET_SCHEMA.field("metadata").type) == "string"
        assert str(ENTRIES_PARQUET_SCHEMA.field("properties").type) == "string"


class TestBundleManifest:
    def test_valid_minimum(self):
        m = BundleManifest(**_valid_manifest_kwargs())
        assert m.namespace == "fred"
        assert m.embedding_dim == 384

    def test_rejects_extra_fields(self):
        with pytest.raises(ValidationError):
            BundleManifest(**_valid_manifest_kwargs(bogus_field="yes"))

    def test_rejects_disallowed_model_prefix(self):
        with pytest.raises(ValidationError, match="not under an allowed prefix"):
            BundleManifest(**_valid_manifest_kwargs(embedding_model="evil/model"))

    def test_accepts_parsimony_dev_prefix(self):
        m = BundleManifest(**_valid_manifest_kwargs(embedding_model="parsimony-dev/custom-embedder"))
        assert m.embedding_model == "parsimony-dev/custom-embedder"

    def test_rejects_short_model_revision(self):
        with pytest.raises(ValidationError, match="40-char commit SHA"):
            BundleManifest(**_valid_manifest_kwargs(embedding_model_revision="abc"))

    def test_rejects_invalid_sha256(self):
        with pytest.raises(ValidationError, match="64 lowercase hex"):
            BundleManifest(**_valid_manifest_kwargs(entries_sha256="notahex"))

    def test_rejects_naive_datetime(self):
        with pytest.raises(ValidationError, match="timezone-aware"):
            BundleManifest(**_valid_manifest_kwargs(built_at=datetime(2026, 1, 1)))

    def test_rejects_negative_entry_count(self):
        with pytest.raises(ValidationError):
            BundleManifest(**_valid_manifest_kwargs(entry_count=-1))

    def test_rejects_zero_dim(self):
        with pytest.raises(ValidationError):
            BundleManifest(**_valid_manifest_kwargs(embedding_dim=0))

    def test_frozen(self):
        m = BundleManifest(**_valid_manifest_kwargs())
        with pytest.raises(ValidationError):
            m.namespace = "other"  # type: ignore[misc]
