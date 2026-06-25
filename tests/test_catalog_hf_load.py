"""Regression tests for ``download_hf_subpath`` (the scoped hf:// catalog fetch).

The fetch helpers live in :mod:`parsimony.catalog.remote`. A sub-catalog inside a
large monorepo must be listed and downloaded by sub-tree only — ``snapshot_download``
enumerates the *entire* repo tree before ``allow_patterns`` filters downloads, so on a
17k-file repo it stalls for minutes just to fetch a handful of files. These tests pin
the scoped-listing behaviour, revision threading, and the offline fallback to a cached
snapshot.
"""

from __future__ import annotations

import logging
from pathlib import Path

import huggingface_hub
import huggingface_hub.hf_api
import pytest

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog import remote as catalog_remote


def _write_catalog(target: Path) -> None:
    catalog = Catalog(name="ecb", indexes={"title": BM25Index()})
    catalog.set_entities([Entity(namespace="ecb", code="EXR", title="exchange rates")])
    catalog.build()
    catalog.save(f"file://{target}")


class _FakeRepoFile:
    """Stand-in for ``huggingface_hub.hf_api.RepoFile`` (a blob entry)."""

    def __init__(self, path: str) -> None:
        self.path = path


def _install_subpath_fakes(monkeypatch: pytest.MonkeyPatch, sub: str, snap: Path) -> dict[str, object]:
    """Fake the Hugging Face listing/download so a sub-path load runs offline.

    Files are read straight from ``<snap>/<filename>``. Returns the dict the fakes
    record their call args into so callers can assert scope and revision threading.
    """
    target = snap / sub
    repo_files = [f"{sub}/{p.relative_to(target).as_posix()}" for p in target.rglob("*") if p.is_file()]
    recorded: dict[str, object] = {}

    class FakeHfApi:
        def list_repo_tree(self, *, repo_id, path_in_repo=None, recursive=False, repo_type=None, revision=None):
            recorded["repo_id"] = repo_id
            recorded["path_in_repo"] = path_in_repo
            recorded["recursive"] = recursive
            recorded["repo_type"] = repo_type
            recorded["revision"] = revision
            return [_FakeRepoFile(f) for f in repo_files]

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


def test_load_hf_subpath_logs_first_pull(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A cache miss logs a one-line first-pull signal for the human watching logs."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)  # not cached

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        Catalog.load(f"hf://parsimony-dev/sdmx/{sub}")

    assert any("first load" in r.getMessage() for r in caplog.records)


def test_load_hf_subpath_cache_hit_is_silent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A warm (already-cached) load emits no first-pull noise."""
    sub = "sdmx_datasets_ecb"
    snap = tmp_path / "snap"
    _write_catalog(snap / sub)
    _install_subpath_fakes(monkeypatch, sub, snap)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: snap / sub / "meta.json")

    with caplog.at_level(logging.INFO, logger="parsimony.catalog.remote"):
        Catalog.load(f"hf://parsimony-dev/sdmx/{sub}")

    assert not any("first load" in r.getMessage() for r in caplog.records)


def test_load_hf_subpath_offline_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When the listing can't reach Hugging Face, fall back to the cached snapshot."""
    sub = "sdmx_datasets_ecb"
    cached_target = tmp_path / "cache_snapshot" / sub
    _write_catalog(cached_target)

    class BoomApi:
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
        def list_repo_tree(self, **kwargs):
            raise OSError("network unreachable")

    monkeypatch.setattr(huggingface_hub, "HfApi", BoomApi)
    monkeypatch.setattr(catalog_remote, "_cached_meta_path", lambda root, sub, **kwargs: None)

    with pytest.raises(OSError, match="network unreachable"):
        Catalog.load("hf://parsimony-dev/sdmx/missing")
