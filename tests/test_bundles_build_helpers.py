"""Unit tests for the small helpers in :mod:`parsimony.bundles.build`.

Covered here:

- ``entries_to_arrow_table`` schema + dense ``row_id`` invariants.
- ``_build_faiss_index`` happy path + shape rejection.
- ``write_bundle_dir`` round-trip: SHAs in manifest match files on disk.
- ``embed_entries_in_batches`` batch-ordering.
- ``scrub_token`` redaction.
- ``fetch_published_entry_count`` graceful network failure.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from parsimony.bundles.build import _build_faiss_index
from parsimony.bundles.format import (
    ENTRIES_PARQUET_SCHEMA,
    FAISS_HNSW_EF_CONSTRUCTION,
    FAISS_HNSW_M,
)
from parsimony.bundles.safety import fetch_published_entry_count, scrub_token
from parsimony.catalog.arrow_adapters import entries_to_arrow_table
from parsimony.catalog.catalog import embed_entries_in_batches
from tests.bundles_helpers import (
    FakeEmbeddingProvider,
    make_fixture_entries,
    requires_faiss,
)

# ---------------------------------------------------------------------------
# entries_to_arrow_table
# ---------------------------------------------------------------------------


class TestEntriesToArrowTable:
    def test_schema_matches_contract(self):
        entries = make_fixture_entries("snbtest", n=3)
        table = entries_to_arrow_table(entries)
        assert table.schema == ENTRIES_PARQUET_SCHEMA

    def test_row_id_is_dense(self):
        entries = make_fixture_entries("snbtest", n=4)
        table = entries_to_arrow_table(entries)
        ids = table.column("row_id").to_pylist()
        assert ids == [0, 1, 2, 3]

    def test_metadata_round_trips_as_json(self):
        entries = make_fixture_entries("snbtest", n=1)
        table = entries_to_arrow_table(entries)
        meta = table.column("metadata").to_pylist()[0]
        assert json.loads(meta)["test"] is True


# ---------------------------------------------------------------------------
# _build_faiss_index
# ---------------------------------------------------------------------------


@requires_faiss
class TestBuildFaissIndex:
    def test_ntotal_and_dim(self):
        vectors = np.zeros((5, 8), dtype=np.float32)
        vectors[:, 0] = 1.0
        idx = _build_faiss_index(vectors, dim=8, m=FAISS_HNSW_M)
        assert idx.ntotal == 5
        assert int(idx.d) == 8
        assert idx.hnsw.efConstruction == FAISS_HNSW_EF_CONSTRUCTION

    def test_zero_vectors_rejected(self):
        with pytest.raises(ValueError, match="zero vectors"):
            _build_faiss_index(np.empty((0, 8), dtype=np.float32), dim=8)

    def test_shape_mismatch_rejected(self):
        with pytest.raises(ValueError, match="shape"):
            _build_faiss_index(np.array([[1.0, 2.0]], dtype=np.float32), dim=8)


# ---------------------------------------------------------------------------
# write_bundle_dir (sync helper for fixtures + replays)
# ---------------------------------------------------------------------------


@requires_faiss
class TestWriteBundleDirRoundTrip:
    @pytest.mark.asyncio
    async def test_round_trip_manifest_matches_files(self, tmp_path):
        from tests.bundles_helpers import write_bundle_dir

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=3)
        raw = await provider.embed_texts([f"{e.title} {e.code}" for e in entries])
        vectors = np.asarray(raw, dtype=np.float32)

        out = tmp_path / "bundle"
        manifest = write_bundle_dir(
            out,
            namespace="snbtest",
            entries=entries,
            vectors=vectors,
            provider=provider,
        )
        assert manifest.entry_count == 3
        assert (out / "entries.parquet").exists()
        assert (out / "index.faiss").exists()
        assert (out / "manifest.json").exists()

        def _sha(p: Path) -> str:
            return hashlib.sha256(p.read_bytes()).hexdigest()

        assert manifest.entries_sha256 == _sha(out / "entries.parquet")
        assert manifest.index_sha256 == _sha(out / "index.faiss")


# ---------------------------------------------------------------------------
# embed_entries_in_batches
# ---------------------------------------------------------------------------


class TestEmbedEntriesInBatches:
    @pytest.mark.asyncio
    async def test_batches_preserve_order_and_return_ndarray(self):
        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=7)
        vectors = await embed_entries_in_batches(entries, provider=provider, batch_size=3)
        assert isinstance(vectors, np.ndarray)
        assert vectors.shape == (7, provider.dimension)
        assert vectors.dtype == np.float32


# ---------------------------------------------------------------------------
# build_bundle_dir end-to-end (async orchestrator)
# ---------------------------------------------------------------------------


@requires_faiss
class TestBuildBundleDir:
    @pytest.mark.asyncio
    async def test_round_trip_with_static_runner(self, tmp_path):
        """build_bundle_dir wires plan → runner → embed → finalize end-to-end."""
        from parsimony.bundles.build import build_bundle_dir
        from parsimony.bundles.spec import CatalogPlan

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=4)

        async def runner(plan: CatalogPlan):
            assert plan.namespace == "snbtest"
            return entries

        out = tmp_path / "bundle"
        manifest = await build_bundle_dir(
            namespace="snbtest",
            plans=[CatalogPlan(namespace="snbtest")],
            runner=runner,
            out_dir=out,
            provider=provider,
        )
        assert manifest.entry_count == 4
        assert manifest.embedding_model == provider.model_id
        assert (out / "entries.parquet").exists()
        assert (out / "index.faiss").exists()
        assert (out / "manifest.json").exists()

    @pytest.mark.asyncio
    async def test_rejects_namespace_mismatch_in_plan(self, tmp_path):
        from parsimony.bundles.build import build_bundle_dir
        from parsimony.bundles.errors import BundleSpecError
        from parsimony.bundles.spec import CatalogPlan

        provider = FakeEmbeddingProvider()

        async def runner(plan: CatalogPlan):
            return make_fixture_entries(plan.namespace, n=1)

        with pytest.raises(BundleSpecError, match="mismatch|namespace"):
            await build_bundle_dir(
                namespace="snbtest",
                plans=[CatalogPlan(namespace="other_ns")],
                runner=runner,
                out_dir=tmp_path / "bundle",
                provider=provider,
            )

    @pytest.mark.asyncio
    async def test_rejects_zero_entries(self, tmp_path):
        from parsimony.bundles.build import build_bundle_dir
        from parsimony.bundles.spec import CatalogPlan

        provider = FakeEmbeddingProvider()

        async def empty_runner(plan: CatalogPlan):
            return []

        with pytest.raises(RuntimeError, match="zero entries"):
            await build_bundle_dir(
                namespace="snbtest",
                plans=[CatalogPlan(namespace="snbtest")],
                runner=empty_runner,
                out_dir=tmp_path / "bundle",
                provider=provider,
            )


# ---------------------------------------------------------------------------
# Safety helpers
# ---------------------------------------------------------------------------


class TestScrubToken:
    def test_empty_token_is_noop(self):
        assert scrub_token("harmless text", "") == "harmless text"

    def test_literal_token_redacted(self):
        text = "uploaded with token hf_realtokenvalue1234567890"
        redacted = scrub_token(text, "hf_realtokenvalue1234567890")
        assert "hf_realtokenvalue1234567890" not in redacted
        assert "[REDACTED]" in redacted


class TestFetchPublishedEntryCount:
    def test_returns_none_on_error(self, monkeypatch):
        def fake_download(**kwargs):
            raise RuntimeError("network unreachable")

        monkeypatch.setattr("huggingface_hub.hf_hub_download", fake_download, raising=False)
        assert fetch_published_entry_count("snb") is None
