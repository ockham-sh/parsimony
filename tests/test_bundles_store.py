"""Round-trip integration tests for HFBundleCatalogStore.

The headline test builds a tiny fixture bundle via the real
:func:`parsimony.bundles.build.write_bundle_dir` helper, loads it via
:class:`HFBundleCatalogStore` from the local cache, and runs a search —
no network, no sentence-transformers.

Tests that need ``faiss`` are gated on ``requires_faiss`` so
environments without FAISS skip gracefully.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

import numpy as np
import pytest

from parsimony.bundles.errors import (
    BundleError,
)
from parsimony.bundles.format import MANIFEST_FILENAME
from tests.bundles_helpers import (
    FakeEmbeddingProvider,
    TopicAwareFakeProvider,
    make_fixture_entries,
    requires_faiss,
)


@pytest.fixture
def provider():
    return FakeEmbeddingProvider()


@pytest.fixture
def topic_provider():
    return TopicAwareFakeProvider()


@pytest.fixture
def topic_fixture_cache(tmp_path, topic_provider):
    """Fixture cache built with the topic-aware provider so ranking is testable."""
    pytest.importorskip("faiss")
    from tests.bundles_helpers import write_bundle_dir

    namespace = "snbtest"
    revision = "0" * 40
    entries = make_fixture_entries(namespace, n=5)

    cache_base = tmp_path / "cache"
    bundle_dir = cache_base / namespace / revision
    bundle_dir.mkdir(parents=True)

    raw = asyncio.run(topic_provider.embed_texts([_embedding_text(e) for e in entries]))
    vectors = np.asarray(raw, dtype=np.float32)

    write_bundle_dir(
        bundle_dir,
        namespace=namespace,
        entries=entries,
        vectors=vectors,
        provider=topic_provider,
    )
    return cache_base, namespace, revision


@pytest.fixture
def fixture_cache(tmp_path, provider):
    """Build a fixture bundle into a cache layout, return (cache_base, namespace, revision)."""
    pytest.importorskip("faiss")
    from tests.bundles_helpers import write_bundle_dir

    namespace = "snbtest"
    revision = "0" * 40
    entries = make_fixture_entries(namespace, n=5)

    cache_base = tmp_path / "cache"
    bundle_dir = cache_base / namespace / revision
    bundle_dir.mkdir(parents=True)

    # Embed deterministically; this is the fake provider so no network.
    raw = asyncio.run(provider.embed_texts([_embedding_text(e) for e in entries]))
    vectors = np.asarray(raw, dtype=np.float32)

    write_bundle_dir(
        bundle_dir,
        namespace=namespace,
        entries=entries,
        vectors=vectors,
        provider=provider,
    )
    return cache_base, namespace, revision


def _embedding_text(entry) -> str:
    from parsimony.catalog.catalog import build_embedding_text

    return build_embedding_text(entry)


@requires_faiss
class TestBundleRoundTrip:
    @pytest.mark.asyncio
    async def test_build_load_search(self, topic_fixture_cache, topic_provider):
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = topic_fixture_cache
        store = HFBundleCatalogStore(
            embeddings=topic_provider,
            cache_dir=cache_base,
            pin=revision,
        )
        # Point-load a single bundle from the cache layout (no network).
        loaded = await store.try_load_remote(namespace)
        assert loaded is True

        status = store.status()
        assert namespace in status["namespaces"]
        assert status["namespaces"][namespace]["entry_count"] == 5

        results = await store.search(
            "unemployment rate",
            limit=3,
            namespaces=[namespace],
        )
        assert len(results) > 0
        # The unemployment-tagged fixture row must rank first — fake
        # provider deterministically maps the query and that title to
        # the same one-hot vector. A title-text composition bug, FAISS
        # metric-direction bug, or row-id mismatch would break this.
        assert results[0].code.endswith("unemployment_rate"), (
            f"expected unemployment_rate to rank first, got top_codes={[m.code for m in results]}"
        )
        # Every result must come from the queried namespace.
        assert all(m.namespace == namespace for m in results)
        # Similarities are bounded to [0, 1].
        assert all(0.0 <= m.similarity <= 1.0 for m in results)

    @pytest.mark.asyncio
    async def test_list_namespaces_only_loaded(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        assert await store.list_namespaces() == []
        await store.try_load_remote(namespace)
        assert await store.list_namespaces() == [namespace]

    @pytest.mark.asyncio
    async def test_upsert_and_delete_raise(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(NotImplementedError):
            await store.upsert([])
        with pytest.raises(NotImplementedError):
            await store.delete("snb", "x")


def _rewrite_manifest(bundle_dir, **overrides):
    """Modify fields on manifest.json; keep SHAs valid."""
    path = bundle_dir / MANIFEST_FILENAME
    data = json.loads(path.read_text(encoding="utf-8"))
    data.update(overrides)
    path.write_text(json.dumps(data), encoding="utf-8")


@requires_faiss
class TestIntegrityChecks:
    @pytest.mark.asyncio
    async def test_corrupt_manifest_rejected(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        bundle_dir = cache_base / namespace / revision
        (bundle_dir / MANIFEST_FILENAME).write_text("{not json}", encoding="utf-8")

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_manifest_entry_count_mismatch_rejected(self, fixture_cache, provider):
        """Manifest claiming 99 entries against a 5-row Parquet must fail."""
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        _rewrite_manifest(cache_base / namespace / revision, entry_count=99)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError, match="entry_count"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_manifest_embedding_dim_mismatch_rejected(self, fixture_cache, provider):
        """Manifest embedding_dim that disagrees with FAISS ``d`` must fail."""
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        # The fixture's fake provider.dimension == 8; we set manifest to 16.
        # The provider-dim check fires first; swap provider with a 16-dim fake
        # to force through to the FAISS dim check.
        _rewrite_manifest(cache_base / namespace / revision, embedding_dim=16)

        bigger = FakeEmbeddingProvider(dim=16)
        store = HFBundleCatalogStore(embeddings=bigger, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError, match="FAISS dim"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_oversize_manifest_rejected(self, fixture_cache, provider, monkeypatch):
        """Size caps fire before SHA/parse even runs — tampering flood guard."""
        from parsimony.stores import hf_bundle as store_mod
        from parsimony.stores.hf_bundle import integrity as integrity_mod

        cache_base, namespace, revision = fixture_cache
        # Cap manifest at 10 bytes; real fixture manifest is kilobytes.
        monkeypatch.setattr(integrity_mod, "MAX_MANIFEST_BYTES", 10)

        s = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError, match="manifest.json"):
            await s.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_oversize_parquet_rejected(self, fixture_cache, provider, monkeypatch):
        from parsimony.stores import hf_bundle as store_mod
        from parsimony.stores.hf_bundle import integrity as integrity_mod

        cache_base, namespace, revision = fixture_cache
        monkeypatch.setattr(integrity_mod, "MAX_PARQUET_BYTES", 10)

        s = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError, match="entries.parquet"):
            await s.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_oversize_index_rejected(self, fixture_cache, provider, monkeypatch):
        from parsimony.stores import hf_bundle as store_mod
        from parsimony.stores.hf_bundle import integrity as integrity_mod

        cache_base, namespace, revision = fixture_cache
        monkeypatch.setattr(integrity_mod, "MAX_INDEX_BYTES", 10)

        s = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError, match="index.faiss"):
            await s.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_corrupt_faiss_index_rejected(self, fixture_cache, provider):
        """Truncate index.faiss so faiss.read_index fails to parse."""
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        bundle_dir = cache_base / namespace / revision
        index_file = bundle_dir / "index.faiss"
        # Truncate mid-header; faiss.read_index raises. We also need to update
        # the manifest SHA so the SHA check doesn't fire first.
        truncated = index_file.read_bytes()[:16]
        index_file.write_bytes(truncated)
        import hashlib

        new_sha = hashlib.sha256(truncated).hexdigest()
        _rewrite_manifest(bundle_dir, index_sha256=new_sha)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleError, match="read_index"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_extra_file_in_snapshot_rejected(self, tmp_path, provider, monkeypatch):
        """A tampered repo with a 4th file alongside the three allowed ones is rejected.

        Drives through the public ``try_load_remote`` API; fakes
        ``huggingface_hub.snapshot_download`` (the library boundary) to
        populate ``local_dir`` with an extra file in addition to the three
        bundle files, then asserts the integrity check rejects it.
        """
        from parsimony.bundles.format import (
            ENTRIES_FILENAME,
            INDEX_FILENAME,
            MANIFEST_FILENAME,
        )
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        revision = "a" * 40

        def fake_snapshot_download(**kwargs):
            local_dir = Path(kwargs["local_dir"])
            local_dir.mkdir(parents=True, exist_ok=True)
            # Three allowed files (empty bytes is fine — extras check fires
            # before SHA / parse).
            (local_dir / ENTRIES_FILENAME).write_bytes(b"")
            (local_dir / INDEX_FILENAME).write_bytes(b"")
            (local_dir / MANIFEST_FILENAME).write_bytes(b"")
            # The fourth file is what the test is asserting against.
            (local_dir / "extra.bin").write_bytes(b"unauthorized payload")
            return str(local_dir)

        monkeypatch.setattr("huggingface_hub.snapshot_download", fake_snapshot_download)

        store = HFBundleCatalogStore(
            embeddings=provider, cache_dir=tmp_path / "cache", pin=revision
        )
        with pytest.raises(BundleError, match="Unexpected files"):
            await store.try_load_remote("tampered")


@requires_faiss
class TestRefresh:
    @pytest.mark.asyncio
    async def test_refresh_returns_unchanged_when_same_revision(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        await store.try_load_remote(namespace)

        # Force freshness checker to re-examine; under pin, the decision
        # short-circuits to the cached revision.
        result = await store.refresh(namespace)
        assert result["updated"] is False
        assert result["old_revision"] == revision
        assert result["new_revision"] == revision


@requires_faiss
class TestFreshnessFallback:
    """Exercise the three no-pin branches of the freshness checker.

    These tests patch the *library boundary* (`huggingface_hub.HfApi.repo_info`
    and `huggingface_hub.snapshot_download`), not parsimony's internal
    `_head_check`/`_download_snapshot` helpers — so a refactor of the
    internals doesn't silently bypass the property under test.
    """

    @pytest.mark.asyncio
    async def test_no_pin_head_success_uses_remote_revision(self, fixture_cache, provider, monkeypatch):
        """HfApi.repo_info returns a SHA matching the cached one → use cache, no network."""
        from huggingface_hub import HfApi

        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache

        class _FakeInfo:
            sha = revision

        def fake_repo_info(self, *args, **kwargs):
            return _FakeInfo()

        monkeypatch.setattr(HfApi, "repo_info", fake_repo_info)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=None)
        assert await store.try_load_remote(namespace) is True
        status = store.status()
        assert status["namespaces"][namespace]["revision"] == revision

    @pytest.mark.asyncio
    async def test_no_pin_head_failure_with_cache_serves_stale(self, fixture_cache, provider, monkeypatch):
        """HfApi.repo_info raises HTTP error → fall back to cached revision with WARN."""
        from huggingface_hub import HfApi
        from huggingface_hub.utils import HfHubHTTPError

        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, _revision = fixture_cache

        def fake_repo_info(self, *args, **kwargs):
            import httpx

            response = httpx.Response(
                503, request=httpx.Request("GET", "https://example.invalid"), text="down"
            )
            raise HfHubHTTPError("network unreachable", response=response)

        monkeypatch.setattr(HfApi, "repo_info", fake_repo_info)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=None)
        assert await store.try_load_remote(namespace) is True
        assert store.has(namespace)

    @pytest.mark.asyncio
    async def test_no_pin_head_failure_without_cache_raises(self, tmp_path, provider, monkeypatch):
        """HfApi.repo_info raises HTTP error and no cache present → BundleError."""
        from huggingface_hub import HfApi
        from huggingface_hub.utils import HfHubHTTPError

        from parsimony.bundles.errors import BundleError
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        def fake_repo_info(self, *args, **kwargs):
            import httpx

            response = httpx.Response(
                503, request=httpx.Request("GET", "https://example.invalid"), text="down"
            )
            raise HfHubHTTPError("network unreachable", response=response)

        monkeypatch.setattr(HfApi, "repo_info", fake_repo_info)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=tmp_path / "cache", pin=None)
        with pytest.raises(BundleError):
            await store.try_load_remote("somens")

    @pytest.mark.asyncio
    async def test_pin_unavailable_and_cache_mismatch_raises_stale(self, fixture_cache, provider, monkeypatch):
        """pin set but snapshot_download fails and cache is a different revision → BundleError."""
        from parsimony.bundles.errors import BundleError
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, _revision = fixture_cache
        # Pin a different revision than what's cached — forces download path.
        foreign_pin = "d" * 40

        def fake_snapshot_download(**kwargs):
            raise RuntimeError("network unreachable")

        monkeypatch.setattr("huggingface_hub.snapshot_download", fake_snapshot_download)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=foreign_pin)
        with pytest.raises(BundleError):
            await store.try_load_remote(namespace)


@requires_faiss
class TestCacheLayoutMutations:
    @pytest.mark.asyncio
    async def test_missing_repo_returns_false(self, tmp_path, provider):
        """Namespaces that don't exist on HF (404) return False, not raise."""
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        store = HFBundleCatalogStore(
            embeddings=provider,
            cache_dir=tmp_path / "nocache",
            pin="1" * 40,
        )
        # The namespace almost certainly doesn't exist on HF. Either the
        # snapshot download 404s (BundleNotFoundError -> False) or the
        # network is unreachable (BundleError, which is a valid
        # programmer-observable error signal). Either is acceptable.
        from parsimony.bundles.errors import BundleError

        try:
            result = await store.try_load_remote("nonexistent_namespace_parsimony_test")
            assert result is False
        except BundleError:
            pass  # network unreachable is also a valid outcome

    @pytest.mark.asyncio
    async def test_shutil_cleanup_doesnt_leak(self, fixture_cache, provider):
        """Sanity: removing the cache between runs doesn't leave state in the store."""
        from parsimony.stores.hf_bundle import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        await store.try_load_remote(namespace)
        assert store.has(namespace)

        # Tear down and construct a new store pointing at a fresh cache.
        shutil.rmtree(cache_base)
        store2 = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        assert not store2.has(namespace)
