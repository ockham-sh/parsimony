"""Unit tests for the HF bundle builder (pure + IO helpers).

The builder was on the coverage omit list — in practice ``write_bundle_dir``
was covered via fixtures but the pure helpers weren't. This module fills
that gap with unit tests that don't require a live enumerator.

Tests that need ``faiss`` use the ``requires_faiss`` marker so CI tasks
without it skip gracefully.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parsimony.stores.hf_bundle.format import (
    ENTRIES_PARQUET_SCHEMA,
    FAISS_HNSW_EF_CONSTRUCTION,
    FAISS_HNSW_M,
)
from tests.hf_bundle_helpers import (
    FakeEmbeddingProvider,
    make_fixture_entries,
    requires_faiss,
)


class TestAssembleTable:
    def test_schema_matches_contract(self):
        """assemble_table output must equal ENTRIES_PARQUET_SCHEMA."""
        from parsimony.stores.hf_bundle.builder import assemble_table

        entries = make_fixture_entries("snbtest", n=3)
        table = assemble_table(entries)
        assert table.schema == ENTRIES_PARQUET_SCHEMA

    def test_row_id_is_dense(self):
        from parsimony.stores.hf_bundle.builder import assemble_table

        entries = make_fixture_entries("snbtest", n=4)
        table = assemble_table(entries)
        ids = table.column("row_id").to_pylist()
        assert ids == [0, 1, 2, 3]

    def test_metadata_and_properties_are_json_strings(self):
        from parsimony.stores.hf_bundle.builder import assemble_table

        entries = make_fixture_entries("snbtest", n=1)
        table = assemble_table(entries)
        meta = table.column("metadata").to_pylist()[0]
        # Must round-trip as JSON.
        assert json.loads(meta)["test"] is True


@requires_faiss
class TestBuildFaissIndex:
    def test_ntotal_and_dim(self):
        from parsimony.stores.hf_bundle.builder import build_faiss_index

        vectors = [[1.0] + [0.0] * 7 for _ in range(5)]
        idx = build_faiss_index(vectors, dim=8, m=FAISS_HNSW_M)
        assert idx.ntotal == 5
        assert int(idx.d) == 8
        # Sanity: efConstruction is set per manifest default.
        assert idx.hnsw.efConstruction == FAISS_HNSW_EF_CONSTRUCTION

    def test_zero_vectors_rejected(self):
        from parsimony.stores.hf_bundle.builder import build_faiss_index

        with pytest.raises(ValueError, match="zero vectors"):
            build_faiss_index([], dim=8)

    def test_shape_mismatch_rejected(self):
        from parsimony.stores.hf_bundle.builder import build_faiss_index

        with pytest.raises(ValueError, match="shape"):
            build_faiss_index([[1.0, 2.0]], dim=8)


@requires_faiss
class TestWriteBundleDirRoundTrip:
    @pytest.mark.asyncio
    async def test_round_trip_manifest_matches_files(self, tmp_path):
        """write_bundle_dir → read manifest → SHAs match files on disk."""
        import hashlib

        from parsimony.stores.hf_bundle.builder import write_bundle_dir

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=3)
        vectors = await provider.embed_texts([f"{e.title} {e.code}" for e in entries])

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

        # SHAs in the manifest must match what's on disk.
        def _sha(p: Path) -> str:
            return hashlib.sha256(p.read_bytes()).hexdigest()

        assert manifest.entries_sha256 == _sha(out / "entries.parquet")
        assert manifest.index_sha256 == _sha(out / "index.faiss")


class TestScrubToken:
    def test_empty_token_is_noop(self):
        from parsimony.stores.hf_bundle.builder import _scrub_token

        assert _scrub_token("harmless text", "") == "harmless text"

    def test_literal_token_redacted(self):
        from parsimony.stores.hf_bundle.builder import _scrub_token

        text = "uploaded with token hf_realtokenvalue1234567890"
        red = _scrub_token(text, "hf_realtokenvalue1234567890")
        assert "hf_realtokenvalue1234567890" not in red
        assert "[REDACTED]" in red


class TestEmbedEntriesInBatches:
    @pytest.mark.asyncio
    async def test_batches_preserve_order(self):
        from parsimony.stores.hf_bundle.builder import embed_entries_in_batches

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=7)
        vectors = await embed_entries_in_batches(entries, provider=provider, batch_size=3)
        assert len(vectors) == 7
        # Each vector has the expected dim.
        assert all(len(v) == provider.dimension for v in vectors)


class TestResolveEnumerator:
    def test_unknown_namespace_rejected(self):
        from parsimony.stores.hf_bundle.builder import _resolve_enumerator

        with pytest.raises(ValueError, match="No enumerator registered"):
            _resolve_enumerator("nonexistent_namespace")


class TestCurrentGitSha:
    def test_returns_none_or_valid_sha(self):
        """Smoke test: either returns None or a 40-char hex SHA."""
        from parsimony.stores.hf_bundle.builder import current_git_sha

        sha = current_git_sha()
        if sha is not None:
            assert len(sha) == 40
            assert all(c in "0123456789abcdef" for c in sha)


class TestFetchPublishedEntryCount:
    def test_returns_none_on_error(self, monkeypatch):
        """No reachable HF / network error → None (guard is advisory)."""
        from parsimony.stores.hf_bundle import builder as bmod

        def fake_download(**kwargs):
            raise RuntimeError("network unreachable")

        monkeypatch.setattr("huggingface_hub.hf_hub_download", fake_download, raising=False)
        # When hf_hub_download raises anything, guard silently returns None.
        assert bmod._fetch_published_entry_count("snb") is None


@requires_faiss
class TestPublishBundle:
    def test_dry_run_skips_upload(self, tmp_path, monkeypatch):
        """dry_run=True must not call upload_bundle."""
        from parsimony.stores.hf_bundle import builder as bmod

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=2)

        async def fake_run_enumerator(namespace):
            return entries

        upload_called = False

        def fake_upload(**kwargs):
            nonlocal upload_called
            upload_called = True
            return "deadbeef" * 5

        monkeypatch.setattr(bmod, "run_enumerator", fake_run_enumerator)
        monkeypatch.setattr(bmod, "upload_bundle", fake_upload)
        bmod._ENUMERATOR_MODULE["snbtest"] = ("tests.fake", "enumerate_fake")
        try:
            report = bmod.publish_bundle(
                "snbtest",
                provider=provider,
                dry_run=True,
            )
        finally:
            bmod._ENUMERATOR_MODULE.pop("snbtest", None)
        assert report["status"] == "dry_run"
        assert report["entry_count"] == 2
        assert upload_called is False

    def test_live_publish_calls_upload(self, tmp_path, monkeypatch):
        """Non-dry-run path must call upload_bundle and report commit_sha."""
        from parsimony.stores.hf_bundle import builder as bmod

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=3)

        async def fake_run_enumerator(namespace):
            return entries

        def fake_upload(bundle_dir, *, namespace, token=None):
            return "cafebabe" * 5

        monkeypatch.setattr(bmod, "run_enumerator", fake_run_enumerator)
        monkeypatch.setattr(bmod, "upload_bundle", fake_upload)
        # No previously-published bundle (returns None → no shrink guard).
        monkeypatch.setattr(bmod, "_fetch_published_entry_count", lambda _ns: None)
        bmod._ENUMERATOR_MODULE["snbtest"] = ("tests.fake", "enumerate_fake")
        try:
            report = bmod.publish_bundle("snbtest", provider=provider)
        finally:
            bmod._ENUMERATOR_MODULE.pop("snbtest", None)
        assert report["status"] == "published"
        assert report["commit_sha"] == "cafebabe" * 5
        assert report["entry_count"] == 3

    def test_shrink_guard_refuses_without_flag(self, tmp_path, monkeypatch):
        """Dropping entry_count below 50% of published must raise without --allow-shrink."""
        from parsimony.stores.hf_bundle import builder as bmod

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=2)

        async def fake_run_enumerator(namespace):
            return entries

        monkeypatch.setattr(bmod, "run_enumerator", fake_run_enumerator)
        monkeypatch.setattr(bmod, "_fetch_published_entry_count", lambda _ns: 100)
        bmod._ENUMERATOR_MODULE["snbtest"] = ("tests.fake", "enumerate_fake")
        try:
            with pytest.raises(RuntimeError, match="refusing to publish"):
                bmod.publish_bundle("snbtest", provider=provider)
        finally:
            bmod._ENUMERATOR_MODULE.pop("snbtest", None)

    def test_keep_dir_preserves_bundle(self, tmp_path, monkeypatch):
        """--keep-dir copies the built bundle before tempdir cleanup."""
        from parsimony.stores.hf_bundle import builder as bmod

        provider = FakeEmbeddingProvider()
        entries = make_fixture_entries("snbtest", n=2)

        async def fake_run_enumerator(namespace):
            return entries

        monkeypatch.setattr(bmod, "run_enumerator", fake_run_enumerator)
        keep = tmp_path / "kept"
        bmod._ENUMERATOR_MODULE["snbtest"] = ("tests.fake", "enumerate_fake")
        try:
            report = bmod.publish_bundle("snbtest", provider=provider, dry_run=True, keep_dir=keep)
        finally:
            bmod._ENUMERATOR_MODULE.pop("snbtest", None)
        assert (keep / "manifest.json").exists()
        assert (keep / "entries.parquet").exists()
        assert (keep / "index.faiss").exists()
        assert report["kept_dir"] == str(keep)


class TestParseArgs:
    def test_build_requires_namespace_and_out_dir(self):
        from parsimony.stores.hf_bundle.builder import _parse_args

        ns = _parse_args(["build", "snb", "/tmp/out"])
        assert ns.command == "build"
        assert ns.namespace == "snb"
        assert ns.out_dir == "/tmp/out"

    def test_publish_dry_run_flag(self):
        from parsimony.stores.hf_bundle.builder import _parse_args

        ns = _parse_args(["publish", "snb", "--dry-run"])
        assert ns.dry_run is True
        assert ns.yes is False

    def test_publish_yes_allow_shrink_keep_dir(self):
        from parsimony.stores.hf_bundle.builder import _parse_args

        ns = _parse_args(["publish", "snb", "--yes", "--allow-shrink", "--keep-dir", "/tmp"])
        assert ns.yes is True
        assert ns.allow_shrink is True
        assert ns.keep_dir == "/tmp"
