"""Regression tests for ``download_hf_subpath`` (the scoped hf:// catalog fetch).

The fetch helpers live in :mod:`parsimony.catalog.remote`. A sub-catalog inside a
large monorepo must be listed and downloaded by sub-tree only — ``snapshot_download``
enumerates the *entire* repo tree before ``allow_patterns`` filters downloads, so on a
17k-file repo it stalls for minutes just to fetch a handful of files. These tests pin
the scoped-listing behaviour, revision threading, and the offline fallback to a cached
snapshot.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import huggingface_hub
import huggingface_hub.hf_api
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog import remote as catalog_remote
from parsimony.errors import CatalogNotFoundError


def _write_catalog(target: Path) -> None:
    catalog = Catalog(name="ecb", indexes={"title": BM25Index()})
    catalog.set_entities([Entity(namespace="ecb", code="EXR", title="exchange rates")])
    catalog.build()
    catalog.save(f"file://{target}")


def _backdate_built_at(snapshot_dir: Path, *, age_s: float) -> None:
    """Rewrite a saved snapshot's ``meta.json`` ``build.built_at`` to look *age_s* seconds old."""
    meta_path = snapshot_dir / "meta.json"
    data = json.loads(meta_path.read_text())
    data["build"]["built_at"] = (datetime.now(UTC) - timedelta(seconds=age_s)).isoformat()
    meta_path.write_text(json.dumps(data))


FAKE_COMMIT_SHA = "a" * 40


class _FakeRepoFile:
    """Stand-in for ``huggingface_hub.hf_api.RepoFile`` (a blob entry)."""

    def __init__(self, path: str, size: int = 0) -> None:
        self.path = path
        self.size = size


class _FakeRef:
    """Stand-in for a branch/tag entry in ``list_repo_refs``."""

    def __init__(self, name: str, target_commit: str) -> None:
        self.name = name
        self.target_commit = target_commit


def _install_subpath_fakes(monkeypatch: pytest.MonkeyPatch, sub: str, snap: Path) -> dict[str, object]:
    """Fake the Hugging Face listing/download so a sub-path load runs offline.

    Files are read straight from ``<snap>/<filename>``. Returns the dict the fakes
    record their call args into so callers can assert scope and revision threading.
    """
    target = snap / sub
    repo_files = [f"{sub}/{p.relative_to(target).as_posix()}" for p in target.rglob("*") if p.is_file()]
    recorded: dict[str, object] = {}

    class FakeHfApi:
        def list_repo_refs(self, *, repo_id, repo_type=None):
            recorded["refs_repo_id"] = repo_id
            recorded["refs_calls"] = int(recorded.get("refs_calls", 0)) + 1
            return SimpleNamespace(
                branches=[_FakeRef("main", FAKE_COMMIT_SHA)],
                tags=[_FakeRef("v1", "b" * 40)],
            )

        def list_repo_tree(self, *, repo_id, path_in_repo=None, recursive=False, repo_type=None, revision=None):
            recorded["repo_id"] = repo_id
            recorded["path_in_repo"] = path_in_repo
            recorded["recursive"] = recursive
            recorded["repo_type"] = repo_type
            recorded["revision"] = revision
            return [_FakeRepoFile(f, size=len((snap / f).read_bytes())) for f in repo_files]

    def fake_download(*, repo_id, filename, repo_type, revision, cache_dir):
        recorded["download_revision"] = revision
        return str(snap / filename)

    def boom_snapshot(*args, **kwargs):
        raise AssertionError("snapshot_download must not be called for a sub-path load")

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeHfApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_download)
    monkeypatch.setattr(huggingface_hub, "snapshot_download", boom_snapshot)
    monkeypatch.setattr(huggingface_hub.hf_api, "RepoFile", _FakeRepoFile)
    return recorded


def test_load_hf_subpath_lists_only_subtree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Loading ``hf://org/repo/sub`` lists only ``sub`` and never snapshot_downloads the repo."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)
    # A cache miss here would emit the first-pull INFO; pin it so the assertions
    # below are about scoping, not logging (logging has its own tests).
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)

    catalog = Catalog.load(f"hf://parsimony-dev/sdmx/{sub}")

    assert recorded["path_in_repo"] == sub  # scoped listing, not the whole repo
    assert recorded["recursive"] is True
    assert recorded["repo_type"] == "dataset"
    matches = catalog.search("exchange", limit=5)
    assert any(m.code == "EXR" for m in matches)


def test_download_hf_subpath_returns_scoped_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The shared helper returns the catalog *directory* (consumed by connectors), scoped to sub."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)

    target = catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache")

    assert recorded["path_in_repo"] == sub  # scoped listing, not the whole repo
    assert target == snap / sub
    assert (target / "meta.json").is_file()


def test_download_hf_subpath_nested_sub(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A two-component sub resolves to the correct nested dir (pins the parents[] arithmetic)."""
    sub = "nested/bundle"
    snap = tmp_path / "snap"
    _write_catalog(snap / "nested" / "bundle")
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)

    target = catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache")

    assert recorded["path_in_repo"] == sub
    assert target == snap / "nested" / "bundle"
    assert (target / "meta.json").is_file()


def test_load_hf_subpath_threads_revision(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A pinned ``@<rev>`` reaches both the listing and every per-file download."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)

    Catalog.load(f"hf://parsimony-dev/sdmx@deadbeef/{sub}")

    assert recorded["revision"] == "deadbeef"  # listing scoped to the pin
    assert recorded["download_revision"] == "deadbeef"  # each blob fetched at the pin


def test_branch_revision_is_pinned_to_a_commit_sha(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """An unpinned load resolves ``main`` to a sha once, then uses it everywhere.

    ``hf_hub_download`` only skips its per-file HEAD when the revision is a commit
    sha, so resolving the branch once is what lets a warm bundle re-resolve with no
    network at all. Listing at the same sha also stops a mid-fetch republish from
    assembling a bundle out of two different commits.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)

    catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache")

    assert recorded["refs_calls"] == 1  # resolved once, not per file
    assert recorded["revision"] == FAKE_COMMIT_SHA
    assert recorded["download_revision"] == FAKE_COMMIT_SHA


def test_sha_revision_skips_the_ref_lookup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A revision that is already a commit sha is immutable — nothing to resolve."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)
    sha = "c" * 40

    catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, revision=sha, cache_dir=tmp_path / "hfcache")

    assert "refs_calls" not in recorded  # no ref round-trip at all
    assert recorded["revision"] == sha
    assert recorded["download_revision"] == sha


def test_downloads_run_concurrently_and_preserve_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Files are fetched in parallel, and results stay in listing order.

    Order is load-bearing: the snapshot directory is derived by pairing the first
    result's path with the first requested filename.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)

    file_count = sum(1 for p in (snap / sub).rglob("*") if p.is_file())
    # Every file must be in flight at once before any of them may finish. Under a
    # serial loop the first call blocks forever and the test fails on the timeout;
    # under the thread pool all parties arrive and it releases immediately.
    barrier = threading.Barrier(min(file_count, catalog_remote._DEFAULT_DOWNLOAD_WORKERS), timeout=10)
    real_download = huggingface_hub.hf_hub_download

    def concurrent_download(*, repo_id, filename, repo_type, revision, cache_dir):
        barrier.wait()
        return real_download(
            repo_id=repo_id, filename=filename, repo_type=repo_type, revision=revision, cache_dir=cache_dir
        )

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", concurrent_download)

    target = catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache")

    assert (target / "meta.json").is_file()  # snapshot dir derived correctly from ordered results


@pytest.mark.parametrize("value", ["1", "ON", "yes", "true"])
def test_hf_transfer_forces_serial_downloads(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """``hf_transfer`` parallelises a single file itself, so we must not stack a pool on it.

    Driven through the environment rather than ``huggingface_hub.constants``: the
    constant is absent at both ends of the supported version range, so asserting
    against it passed locally and failed in CI on nothing but resolver luck. The
    env var is the knob a user actually sets, in the spellings hub accepts.
    """
    monkeypatch.setenv("HF_HUB_ENABLE_HF_TRANSFER", value)
    assert catalog_remote._download_workers() == 1


@pytest.mark.parametrize("value", ["", "0", "off", "no"])
def test_downloads_stay_concurrent_without_hf_transfer(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """Anything hub does not read as true leaves the thread pool at full width."""
    monkeypatch.setenv("HF_HUB_ENABLE_HF_TRANSFER", value)
    assert catalog_remote._download_workers() > 1


def test_downloads_stay_concurrent_when_hf_transfer_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """The common case: the variable is not in the environment at all."""
    monkeypatch.delenv("HF_HUB_ENABLE_HF_TRANSFER", raising=False)
    assert catalog_remote._download_workers() > 1


def test_fetch_logs_start_and_completion_pair(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A fetch brackets itself with a pre-download line and a completion line.

    The order is the point: the size has to land *before* the wait for a caller
    to judge whether a stall is proportionate, and a start with no matching
    finish is what marks a long wait as progress rather than a hang.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        Catalog.load(f"hf://parsimony-dev/sdmx/{sub}")

    messages = [r.getMessage() for r in caplog.records]
    started = [m for m in messages if m.startswith("Downloading ")]
    finished = [m for m in messages if m.startswith("Downloaded catalog")]
    assert len(started) == 1, messages
    assert len(finished) == 1, messages
    assert f"hf://parsimony-dev/sdmx/{sub}" in started[0]
    assert "MB) for catalog" in started[0]  # size known before the wait, not after
    assert messages.index(started[0]) < messages.index(finished[0])


def test_cold_pull_announces_itself_before_the_repo_listing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A cold resolve logs before listing the repo, not just before downloading.

    The listing is a network round-trip that can stall for minutes on a large
    repo, and it happens *before* the file count and size are knowable. Without
    a line ahead of it, the whole stall is silent and the first output arrives
    only once the slow part is already over.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        Catalog.load(f"hf://parsimony-dev/sdmx/{sub}")

    messages = [r.getMessage() for r in caplog.records]
    resolving = [m for m in messages if m.startswith("Resolving catalog")]
    assert len(resolving) == 1, messages
    # Ahead of the download line, which cannot be emitted until listing returns.
    assert messages.index(resolving[0]) < min(i for i, m in enumerate(messages) if m.startswith("Downloading "))


def test_fetch_logs_even_when_meta_json_is_already_cached(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A cached ``meta.json`` must not silence a real bundle download.

    ``meta.json`` is one file of many in a bundle, so gating the log on its
    presence hid exactly the largest fetches: a republish (the old commit's
    ``meta.json`` is still cached while every file re-downloads), an interrupted
    first pull, and any post-TTL revalidation.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: snap / sub / "meta.json")

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache")

    assert any(m.getMessage().startswith("Downloading ") for m in caplog.records)


def test_revalidate_ttl_hit_is_silent_and_makes_no_hub_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Within the revalidate window a repeat resolution logs nothing and skips the Hub.

    This — not the presence of ``meta.json`` — is what keeps a warm path quiet.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    recorded = _install_subpath_fakes(monkeypatch, sub, snap)
    cache_dir = tmp_path / "hfcache"

    catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir)
    calls_after_first = recorded["refs_calls"]

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir)

    assert recorded["refs_calls"] == calls_after_first  # no second Hub round-trip
    assert not caplog.records


def test_load_hf_subpath_offline_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When the listing can't reach Hugging Face, fall back to the cached snapshot."""
    sub = "sdmx_datasets_ecb"
    cached_target = tmp_path / "cache_snapshot" / sub
    _write_catalog(cached_target)

    class BoomApi:
        # Resolving the ref is the first network call the fetch makes, so an
        # offline run fails here rather than at the listing.
        def list_repo_refs(self, **kwargs):
            raise OSError("network unreachable")

        def list_repo_tree(self, **kwargs):
            raise OSError("network unreachable")

    monkeypatch.setattr(huggingface_hub, "HfApi", BoomApi)
    # The fallback resolves the cached meta.json and returns its parent directory.
    monkeypatch.setattr(
        catalog_remote,
        "_cached_meta_path",
        lambda root, sub, **kwargs: cached_target / "meta.json",
    )

    catalog = Catalog.load(f"hf://parsimony-dev/sdmx/{sub}")
    matches = catalog.search("exchange", limit=5)
    assert any(m.code == "EXR" for m in matches)


def test_load_hf_subpath_no_cache_reraises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Listing failure with no cached snapshot surfaces the original error."""

    class BoomApi:
        # Resolving the ref is the first network call the fetch makes, so an
        # offline run fails here rather than at the listing.
        def list_repo_refs(self, **kwargs):
            raise OSError("network unreachable")

        def list_repo_tree(self, **kwargs):
            raise OSError("network unreachable")

    monkeypatch.setattr(huggingface_hub, "HfApi", BoomApi)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)

    with pytest.raises(OSError, match="network unreachable"):
        Catalog.load("hf://parsimony-dev/sdmx/missing")


# ---------------------------------------------------------------------------
# Freshness policy: max_age_s (offline fallback) + revalidate_ttl_s (online)
# ---------------------------------------------------------------------------


def test_download_hf_subpath_offline_fallback_raises_past_max_age(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Past ``max_age_s``, a stale cached snapshot is refused rather than served."""
    sub = "sdmx_datasets_ecb"
    cached_target = tmp_path / "cache_snapshot" / sub
    _write_catalog(cached_target)
    _backdate_built_at(cached_target, age_s=3600)

    class BoomApi:
        # Resolving the ref is the first network call the fetch makes, so an
        # offline run fails here rather than at the listing.
        def list_repo_refs(self, **kwargs):
            raise OSError("network unreachable")

        def list_repo_tree(self, **kwargs):
            raise OSError("network unreachable")

    monkeypatch.setattr(huggingface_hub, "HfApi", BoomApi)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: cached_target / "meta.json")

    with pytest.raises(CatalogNotFoundError, match="exceeding max_age_s"):
        catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache", max_age_s=60)


def test_download_hf_subpath_offline_fallback_within_max_age_serves_and_warns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Within ``max_age_s`` (or with no policy set), the cached snapshot is served and its age is logged."""
    sub = "sdmx_datasets_ecb"
    cached_target = tmp_path / "cache_snapshot" / sub
    _write_catalog(cached_target)
    _backdate_built_at(cached_target, age_s=120)

    class BoomApi:
        # Resolving the ref is the first network call the fetch makes, so an
        # offline run fails here rather than at the listing.
        def list_repo_refs(self, **kwargs):
            raise OSError("network unreachable")

        def list_repo_tree(self, **kwargs):
            raise OSError("network unreachable")

    monkeypatch.setattr(huggingface_hub, "HfApi", BoomApi)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: cached_target / "meta.json")

    with caplog.at_level(logging.WARNING, logger="parsimony.catalog.remote"):
        target = catalog_remote.download_hf_subpath(
            "parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache", max_age_s=3600
        )

    assert target == cached_target
    assert any("120s old" in r.getMessage() for r in caplog.records)


def test_download_hf_subpath_revalidate_ttl_skips_second_listing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A second call within ``revalidate_ttl_s`` reuses the snapshot dir without re-listing the Hub."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)
    list_calls = {"n": 0}
    fake_cls = huggingface_hub.HfApi
    original_list_repo_tree = fake_cls.list_repo_tree

    def counting_list_repo_tree(self, **kwargs):
        list_calls["n"] += 1
        return original_list_repo_tree(self, **kwargs)

    monkeypatch.setattr(fake_cls, "list_repo_tree", counting_list_repo_tree)

    cache_dir = tmp_path / "hfcache"
    first = catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir)
    second = catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir)

    assert first == second == snap / sub
    assert list_calls["n"] == 1  # second call served from the revalidation-TTL shortcut


def test_download_hf_subpath_revalidate_ttl_none_always_relists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``revalidate_ttl_s=None`` disables the shortcut — every call re-lists the Hub."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)
    list_calls = {"n": 0}
    fake_cls = huggingface_hub.HfApi
    original_list_repo_tree = fake_cls.list_repo_tree

    def counting_list_repo_tree(self, **kwargs):
        list_calls["n"] += 1
        return original_list_repo_tree(self, **kwargs)

    monkeypatch.setattr(fake_cls, "list_repo_tree", counting_list_repo_tree)

    cache_dir = tmp_path / "hfcache"
    catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir, revalidate_ttl_s=None)
    catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir, revalidate_ttl_s=None)

    assert list_calls["n"] == 2


def test_recent_snapshot_dir_round_trip(tmp_path: Path) -> None:
    """``_remember_snapshot_dir`` + ``_recent_snapshot_dir`` round-trip within the TTL window."""
    recent = catalog_remote._recent_snapshot_dir
    snap = tmp_path / "snap"
    snap.mkdir()
    cache_dir = tmp_path / "hfcache"

    assert recent("org/repo", "sub", None, cache_dir=cache_dir, revalidate_ttl_s=60) is None

    catalog_remote._remember_snapshot_dir("org/repo", "sub", None, snap, cache_dir=cache_dir)

    assert recent("org/repo", "sub", None, cache_dir=cache_dir, revalidate_ttl_s=60) == snap
    # Disabling the shortcut always misses, regardless of freshness.
    assert recent("org/repo", "sub", None, cache_dir=cache_dir, revalidate_ttl_s=None) is None


def test_recent_snapshot_dir_missing_dir_falls_through(tmp_path: Path) -> None:
    """A remembered snapshot dir that no longer exists on disk (e.g. cache cleared) is not trusted."""
    cache_dir = tmp_path / "hfcache"
    catalog_remote._remember_snapshot_dir("org/repo", "sub", None, tmp_path / "gone", cache_dir=cache_dir)

    assert (
        catalog_remote._recent_snapshot_dir("org/repo", "sub", None, cache_dir=cache_dir, revalidate_ttl_s=60) is None
    )


def test_snapshot_age_s_reads_built_at(tmp_path: Path) -> None:
    """Age is computed from the snapshot's ``build.built_at``, not the file's mtime."""
    target = tmp_path / "snap"
    _write_catalog(target)
    _backdate_built_at(target, age_s=120)

    age = catalog_remote._snapshot_age_s(target)

    assert age is not None
    assert 115 <= age <= 130


def test_snapshot_age_s_missing_meta_returns_none(tmp_path: Path) -> None:
    """No ``meta.json`` (or a corrupt one) is a soft failure, not an exception."""
    assert catalog_remote._snapshot_age_s(tmp_path / "nothing-here") is None


def test_revalidation_that_downloads_nothing_stays_silent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Past the TTL the Hub is re-listed, but an all-cached pass must not claim a download.

    This is the honesty case: the revalidation still costs a listing round-trip,
    so something *does* happen — but nothing transfers, and a log line saying
    otherwise would teach a reader to distrust the ones that matter.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)
    cache_dir = tmp_path / "hfcache"
    # Every file already on disk, so there is nothing left to fetch — including
    # meta.json, which is what marks this a revalidation rather than a cold pull.
    monkeypatch.setattr(catalog_remote, "_uncached_files", lambda root, files, **kwargs: [])
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: snap / sub / "meta.json")

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        # revalidate_ttl_s=None forces the listing rather than the TTL shortcut.
        catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=cache_dir, revalidate_ttl_s=None)

    assert not caplog.records


def test_cache_probe_uses_the_pinned_sha_not_the_branch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The cached-snapshot probe must ask at the commit sha the files were fetched at.

    Downloading at a sha does not write the repo's ``refs/<branch>`` pointer, so
    a probe by branch name cannot see a snapshot this function itself wrote. It
    returns None forever, which silently disables the offline stale-cache
    fallback — the code reports the Hub as unreachable with a perfectly good
    copy on disk.
    """
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)

    seen: list[object] = []
    real = catalog_remote._cached_meta_path

    def spy(root: str, sub: str, **kwargs: object) -> object:
        seen.append(kwargs.get("revision"))
        return real(root, sub, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(catalog_remote, "_cached_meta_path", spy)
    catalog_remote.download_hf_subpath("parsimony-dev/sdmx", sub, cache_dir=tmp_path / "hfcache")

    assert seen, "expected the cache to be probed"
    assert all(rev == FAKE_COMMIT_SHA for rev in seen), seen
