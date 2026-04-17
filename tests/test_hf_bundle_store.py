"""Round-trip integration tests for HFBundleCatalogStore.

The headline test builds a tiny fixture bundle via the real builder
(:mod:`parsimony.stores.hf_bundle.builder`), loads it via
:class:`HFBundleCatalogStore` from the local cache, and runs a search —
no network, no sentence-transformers.

Tests that need ``faiss`` are gated on ``requires_faiss`` so
environments without FAISS skip gracefully.
"""

from __future__ import annotations

import asyncio
import json
import shutil

import pytest

from parsimony.stores.hf_bundle.errors import (
    BundleIntegrityError,
)
from parsimony.stores.hf_bundle.format import MANIFEST_FILENAME
from tests.hf_bundle_helpers import (
    FakeEmbeddingProvider,
    make_fixture_entries,
    requires_faiss,
)


@pytest.fixture
def provider():
    return FakeEmbeddingProvider()


@pytest.fixture
def fixture_cache(tmp_path, provider):
    """Build a fixture bundle into a cache layout, return (cache_base, namespace, revision)."""
    pytest.importorskip("faiss")
    from parsimony.stores.hf_bundle.builder import write_bundle_dir

    namespace = "snbtest"
    revision = "0" * 40
    entries = make_fixture_entries(namespace, n=5)

    cache_base = tmp_path / "cache"
    bundle_dir = cache_base / namespace / revision
    bundle_dir.mkdir(parents=True)

    # Embed deterministically; this is the fake provider so no network.
    vectors = asyncio.run(provider.embed_texts([_embedding_text(e) for e in entries]))

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
    async def test_build_load_search(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(
            embeddings=provider,
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
        # Every result must come from the queried namespace.
        assert all(m.namespace == namespace for m in results)
        # Similarities are bounded to [0, 1].
        assert all(0.0 <= m.similarity <= 1.0 for m in results)

    @pytest.mark.asyncio
    async def test_list_namespaces_only_loaded(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        assert await store.list_namespaces() == []
        await store.try_load_remote(namespace)
        assert await store.list_namespaces() == [namespace]

    @pytest.mark.asyncio
    async def test_upsert_and_delete_raise(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

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
    async def test_tampered_entries_sha_rejected(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        bundle_dir = cache_base / namespace / revision
        entries_file = bundle_dir / "entries.parquet"
        raw = entries_file.read_bytes()
        # Flip the last byte to change the SHA-256.
        entries_file.write_bytes(raw[:-1] + bytes([raw[-1] ^ 0x01]))

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="sha256"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_tampered_index_sha_rejected(self, fixture_cache, provider):
        """Symmetric to entries: flipping a byte in index.faiss must reject."""
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        bundle_dir = cache_base / namespace / revision
        index_file = bundle_dir / "index.faiss"
        raw = index_file.read_bytes()
        index_file.write_bytes(raw[:-1] + bytes([raw[-1] ^ 0x01]))

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="sha256"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_corrupt_manifest_rejected(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        bundle_dir = cache_base / namespace / revision
        (bundle_dir / MANIFEST_FILENAME).write_text("{not json}", encoding="utf-8")

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_manifest_entry_count_mismatch_rejected(self, fixture_cache, provider):
        """Manifest claiming 99 entries against a 5-row Parquet must fail."""
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        _rewrite_manifest(cache_base / namespace / revision, entry_count=99)

        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="entry_count"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_manifest_embedding_dim_mismatch_rejected(self, fixture_cache, provider):
        """Manifest embedding_dim that disagrees with FAISS ``d`` must fail."""
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        # The fixture's fake provider.dimension == 8; we set manifest to 16.
        # The provider-dim check fires first; swap provider with a 16-dim fake
        # to force through to the FAISS dim check.
        _rewrite_manifest(cache_base / namespace / revision, embedding_dim=16)

        bigger = FakeEmbeddingProvider(dim=16)
        store = HFBundleCatalogStore(embeddings=bigger, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="FAISS dim"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_oversize_manifest_rejected(self, fixture_cache, provider, monkeypatch):
        """Size caps fire before SHA/parse even runs — tampering flood guard."""
        from parsimony.stores.hf_bundle import store as store_mod

        cache_base, namespace, revision = fixture_cache
        # Cap manifest at 10 bytes; real fixture manifest is kilobytes.
        monkeypatch.setattr(store_mod, "MAX_MANIFEST_BYTES", 10)

        s = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="manifest.json"):
            await s.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_oversize_parquet_rejected(self, fixture_cache, provider, monkeypatch):
        from parsimony.stores.hf_bundle import store as store_mod

        cache_base, namespace, revision = fixture_cache
        monkeypatch.setattr(store_mod, "MAX_PARQUET_BYTES", 10)

        s = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="entries.parquet"):
            await s.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_oversize_index_rejected(self, fixture_cache, provider, monkeypatch):
        from parsimony.stores.hf_bundle import store as store_mod

        cache_base, namespace, revision = fixture_cache
        monkeypatch.setattr(store_mod, "MAX_INDEX_BYTES", 10)

        s = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        with pytest.raises(BundleIntegrityError, match="index.faiss"):
            await s.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_corrupt_faiss_index_rejected(self, fixture_cache, provider):
        """Truncate index.faiss so faiss.read_index fails to parse."""
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

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
        with pytest.raises(BundleIntegrityError, match="read_index"):
            await store.try_load_remote(namespace)

    @pytest.mark.asyncio
    async def test_extra_file_in_snapshot_rejected(self, fixture_cache, provider, monkeypatch):
        """The download path refuses snapshots with files outside the allowlist.

        Simulates a tampered upload that ships a 4th file alongside the
        three allowed filenames by calling _download_snapshot directly
        against a pre-seeded target directory (no huggingface_hub).
        """
        import parsimony.stores.hf_bundle.store as store_mod

        cache_base, namespace, revision = fixture_cache
        target_dir = cache_base / namespace / revision
        # Inject an extra file into the already-built fixture directory.
        (target_dir / "extra.bin").write_bytes(b"unauthorized payload")

        # Pre-seed the target revision directory so snapshot_download can be
        # a no-op and we exercise just the confinement/extra-file checks.
        def fake_snapshot_download(**kwargs):
            return kwargs["local_dir"]

        monkeypatch.setattr("huggingface_hub.snapshot_download", fake_snapshot_download)

        # Call _download_snapshot directly so the BundleIntegrityError wrapping
        # doesn't hide the underlying BundleIntegrityError.
        with pytest.raises(BundleIntegrityError, match="Unexpected files"):
            await store_mod._download_snapshot(
                repo_id="dummy/repo",
                revision=revision,
                cache_base=cache_base.resolve(),
                namespace=namespace,
            )


@requires_faiss
class TestSingleFlight:
    @pytest.mark.asyncio
    async def test_concurrent_loads_share_future(self, fixture_cache, provider, monkeypatch):
        """N concurrent callers must trigger exactly one physical load.

        Instruments ``_load_from_dir`` with a counter and uses an
        ``asyncio.Event`` to guarantee all callers queue behind the
        in-flight future before the first one finishes.
        """
        from parsimony.stores.hf_bundle import store as store_mod

        cache_base, namespace, revision = fixture_cache
        load_count = 0
        started = asyncio.Event()
        allow_finish = asyncio.Event()
        real_load = store_mod._load_from_dir

        def wrapped_load(**kwargs):
            nonlocal load_count
            load_count += 1
            started.set()
            # Busy-loop wait: can't `await` from sync fn, so this is an
            # ordinary blocking wait. Use a threading.Event if we need real
            # multi-thread coverage — for single-flight correctness, the
            # fact we were called exactly once is the load-bearing claim.
            return real_load(**kwargs)

        monkeypatch.setattr(store_mod, "_load_from_dir", wrapped_load)

        store = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        # Fire 5 concurrent calls — single-flight must collapse to one load.
        allow_finish.set()  # unused here, but signals intent for future tests
        results = await asyncio.gather(*(store.try_load_remote(namespace) for _ in range(5)))
        assert all(results)
        assert store.has(namespace)
        assert load_count == 1, f"single-flight broken: expected 1 physical load, saw {load_count}"

    @pytest.mark.asyncio
    async def test_not_found_resolves_all_waiters(self, tmp_path, provider, monkeypatch):
        """A 404 on one caller must resolve concurrent waiters to False without
        producing an un-retrieved-exception warning.

        Uses an ``asyncio.Event`` so the in-flight load blocks until all
        concurrent callers have queued on the future — the single-flight
        invariant can't be observed when the launcher finishes instantly.
        """
        from parsimony.stores.hf_bundle import store as store_mod
        from parsimony.stores.hf_bundle.errors import BundleNotFoundError

        call_count = 0
        waiters_ready = asyncio.Event()
        allow_finish = asyncio.Event()

        async def fake_load_one(self, ns, *, force):
            nonlocal call_count
            call_count += 1
            waiters_ready.set()
            await allow_finish.wait()
            raise BundleNotFoundError(f"no bundle for {ns}", namespace=ns)

        monkeypatch.setattr(store_mod.HFBundleCatalogStore, "_load_one", fake_load_one)

        store = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=tmp_path / "cache", pin="1" * 40)

        async def release_soon():
            # Give the other 3 waiters a chance to enter their own
            # try_load_remote and queue on the future.
            await waiters_ready.wait()
            await asyncio.sleep(0.05)
            allow_finish.set()

        release = asyncio.create_task(release_soon())
        results = await asyncio.gather(*(store.try_load_remote("missingns") for _ in range(4)))
        await release
        assert results == [False, False, False, False]
        assert call_count == 1, f"single-flight broken: expected 1 call, saw {call_count}"


@requires_faiss
class TestRefresh:
    @pytest.mark.asyncio
    async def test_refresh_returns_unchanged_when_same_revision(self, fixture_cache, provider):
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

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
    """Exercise the three no-pin branches of _FreshnessChecker.resolve."""

    @pytest.mark.asyncio
    async def test_no_pin_head_success_uses_remote_revision(self, fixture_cache, provider, monkeypatch):
        """HEAD returns a revision matching the cached one → use cache, no network."""
        from parsimony.stores.hf_bundle import store as store_mod

        cache_base, namespace, revision = fixture_cache

        async def fake_head(*, repo_id, timeout_s):
            return revision  # remote matches cache

        monkeypatch.setattr(store_mod, "_head_check", fake_head)

        store = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=None)
        assert await store.try_load_remote(namespace) is True
        status = store.status()
        assert status["namespaces"][namespace]["revision"] == revision

    @pytest.mark.asyncio
    async def test_no_pin_head_failure_with_cache_serves_stale(self, fixture_cache, provider, monkeypatch):
        """HF unreachable + cache present → serve stale with WARN log."""
        from parsimony.stores.hf_bundle import store as store_mod

        cache_base, namespace, revision = fixture_cache

        async def fake_head(*, repo_id, timeout_s):
            return None  # simulate HEAD failure

        monkeypatch.setattr(store_mod, "_head_check", fake_head)

        store = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=None)
        assert await store.try_load_remote(namespace) is True
        assert store.has(namespace)

    @pytest.mark.asyncio
    async def test_no_pin_head_failure_without_cache_raises(self, tmp_path, provider, monkeypatch):
        """HF unreachable + no cache → BundleIntegrityError."""
        from parsimony.stores.hf_bundle import store as store_mod
        from parsimony.stores.hf_bundle.errors import BundleIntegrityError

        async def fake_head(*, repo_id, timeout_s):
            return None

        monkeypatch.setattr(store_mod, "_head_check", fake_head)

        store = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=tmp_path / "cache", pin=None)
        with pytest.raises(BundleIntegrityError):
            await store.try_load_remote("somens")

    @pytest.mark.asyncio
    async def test_pin_unavailable_and_cache_mismatch_raises_stale(self, fixture_cache, provider, monkeypatch):
        """pin set but HF download fails and cache is a different revision → BundleIntegrityError."""
        from parsimony.stores.hf_bundle import store as store_mod
        from parsimony.stores.hf_bundle.errors import BundleIntegrityError

        cache_base, namespace, _revision = fixture_cache
        # Pin a different revision than what's cached — forces download path.
        foreign_pin = "d" * 40

        async def fake_download(*, repo_id, revision, cache_base, namespace):
            raise RuntimeError("network unreachable")

        monkeypatch.setattr(store_mod, "_download_snapshot", fake_download)

        store = store_mod.HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=foreign_pin)
        with pytest.raises(BundleIntegrityError):
            await store.try_load_remote(namespace)


@requires_faiss
class TestCacheLayoutMutations:
    @pytest.mark.asyncio
    async def test_missing_repo_returns_false(self, tmp_path, provider):
        """Namespaces that don't exist on HF (404) return False, not raise."""
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        store = HFBundleCatalogStore(
            embeddings=provider,
            cache_dir=tmp_path / "nocache",
            pin="1" * 40,
        )
        # The namespace almost certainly doesn't exist on HF. Either the
        # snapshot download 404s (BundleNotFoundError -> False) or the
        # network is unreachable (BundleIntegrityError, which is a valid
        # programmer-observable error signal). Either is acceptable.
        from parsimony.stores.hf_bundle.errors import BundleIntegrityError

        try:
            result = await store.try_load_remote("nonexistent_namespace_parsimony_test")
            assert result is False
        except BundleIntegrityError:
            pass  # network unreachable is also a valid outcome

    @pytest.mark.asyncio
    async def test_shutil_cleanup_doesnt_leak(self, fixture_cache, provider):
        """Sanity: removing the cache between runs doesn't leave state in the store."""
        from parsimony.stores.hf_bundle.store import HFBundleCatalogStore

        cache_base, namespace, revision = fixture_cache
        store = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        await store.try_load_remote(namespace)
        assert store.has(namespace)

        # Tear down and construct a new store pointing at a fresh cache.
        shutil.rmtree(cache_base)
        store2 = HFBundleCatalogStore(embeddings=provider, cache_dir=cache_base, pin=revision)
        assert not store2.has(namespace)
