"""Tests for :mod:`parsimony.cache` — public path helpers, internal
hardening primitives, and :class:`TTLDiskCache`.

The autouse ``_pin_parsimony_cache_dir`` fixture in ``conftest.py``
pins ``PARSIMONY_CACHE_DIR`` to a per-test tmp dir, so these tests
can call the helpers freely without touching the real user cache.
"""

from __future__ import annotations

import os
import stat
import time
from pathlib import Path

import pytest

from parsimony import cache
from parsimony.cache import (
    _PARSIMONY_CACHE_ENV,
    TTLDiskCache,
    _resolve_root,
    _safe_mkdir,
    _sanitize_subkey,
)

# ---------------------------------------------------------------------------
# _resolve_root — pure path resolution
# ---------------------------------------------------------------------------


def test_resolve_root_honors_env_var(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "explicit"
    monkeypatch.setenv(_PARSIMONY_CACHE_ENV, str(target))
    assert _resolve_root() == target


def test_resolve_root_expands_tilde(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_PARSIMONY_CACHE_ENV, "~/custom-cache")
    resolved = _resolve_root()
    assert "~" not in str(resolved)
    assert resolved.is_absolute()


def test_resolve_root_falls_back_to_platformdirs(monkeypatch: pytest.MonkeyPatch) -> None:
    from platformdirs import user_cache_dir

    monkeypatch.delenv(_PARSIMONY_CACHE_ENV, raising=False)
    assert _resolve_root() == Path(user_cache_dir("parsimony"))


# ---------------------------------------------------------------------------
# _safe_mkdir — world/group-writable rejection + 0o700 perms
# ---------------------------------------------------------------------------


def test_safe_mkdir_creates_missing_directory(tmp_path: Path) -> None:
    target = tmp_path / "fresh" / "nested"
    _safe_mkdir(target)
    assert target.is_dir()


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only mode bits")
def test_safe_mkdir_chmods_to_0700(tmp_path: Path) -> None:
    target = tmp_path / "private"
    _safe_mkdir(target)
    mode = stat.S_IMODE(target.stat().st_mode)
    assert mode == 0o700


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only mode bits")
def test_safe_mkdir_rejects_world_writable_existing_dir(tmp_path: Path) -> None:
    hostile = tmp_path / "hostile"
    hostile.mkdir()
    os.chmod(hostile, 0o777)
    with pytest.raises(RuntimeError, match="world-writable or group-writable"):
        _safe_mkdir(hostile)


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only mode bits")
def test_safe_mkdir_rejects_group_writable_existing_dir(tmp_path: Path) -> None:
    hostile = tmp_path / "shared"
    hostile.mkdir()
    os.chmod(hostile, 0o770)
    with pytest.raises(RuntimeError, match="world-writable or group-writable"):
        _safe_mkdir(hostile)


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only mode bits")
def test_safe_mkdir_rejects_world_writable_ancestor(tmp_path: Path) -> None:
    """If the immediate ancestor of a missing dir is world-writable, refuse."""
    parent = tmp_path / "shared"
    parent.mkdir()
    os.chmod(parent, 0o777)
    target = parent / "child"
    with pytest.raises(RuntimeError, match="world-writable or group-writable"):
        _safe_mkdir(target)


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only mode bits")
def test_safe_mkdir_accepts_sticky_world_writable_ancestor(tmp_path: Path) -> None:
    """Sticky bit (canonical: ``/tmp``) makes world-writable safe to traverse.

    Sticky restricts rename/unlink to file owner, so a world-writable sticky
    parent cannot be used to swap our cache subtree out from under us.
    """
    parent = tmp_path / "tmp-like"
    parent.mkdir()
    os.chmod(parent, 0o1777)  # sticky + rwxrwxrwx — exactly /tmp's mode
    target = parent / "child"
    _safe_mkdir(target)  # must not raise
    assert target.is_dir()


def test_safe_mkdir_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "idempotent"
    _safe_mkdir(target)
    _safe_mkdir(target)
    _safe_mkdir(target)
    assert target.is_dir()


# ---------------------------------------------------------------------------
# _sanitize_subkey — boundary validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "valid",
    [
        "sdmx",
        "fmp",
        "sentence-transformers__all-MiniLM-L6-v2",
        "model-name-abc12345",
        "_internal",
        "v1.2.3",
        "model.v2",
    ],
)
def test_sanitize_subkey_accepts(valid: str) -> None:
    assert _sanitize_subkey(valid) == valid


@pytest.mark.parametrize(
    "hostile",
    [
        "",
        "..",
        ".hidden",
        "../etc/passwd",
        "foo/bar",
        "foo\\bar",
        "foo bar",
        "foo;rm",
        "foo$",
        "foo\nbar",
    ],
)
def test_sanitize_subkey_rejects(hostile: str) -> None:
    with pytest.raises(ValueError):
        _sanitize_subkey(hostile)


# ---------------------------------------------------------------------------
# Public path helpers — directory layout
# ---------------------------------------------------------------------------


def test_root_returns_pinned_cache_dir(_pin_parsimony_cache_dir: Path) -> None:
    assert cache.root() == _pin_parsimony_cache_dir


def test_root_creates_dir(_pin_parsimony_cache_dir: Path) -> None:
    cache.root()
    assert _pin_parsimony_cache_dir.is_dir()


def test_catalogs_dir_layout(_pin_parsimony_cache_dir: Path) -> None:
    p = cache.catalogs_dir()
    assert p == _pin_parsimony_cache_dir / "catalogs"
    assert p.is_dir()


def test_models_dir_parent_form(_pin_parsimony_cache_dir: Path) -> None:
    p = cache.models_dir()
    assert p == _pin_parsimony_cache_dir / "models"
    assert p.is_dir()


def test_models_dir_per_slug(_pin_parsimony_cache_dir: Path) -> None:
    slug = "sentence-transformers__all-MiniLM-L6-v2-12345678"
    p = cache.models_dir(slug)
    assert p == _pin_parsimony_cache_dir / "models" / slug
    assert p.is_dir()


def test_embeddings_dir_per_slug(_pin_parsimony_cache_dir: Path) -> None:
    slug = "stub-12345678"
    p = cache.embeddings_dir(slug)
    assert p == _pin_parsimony_cache_dir / "embeddings" / slug
    assert p.is_dir()


def test_connectors_dir_per_provider(_pin_parsimony_cache_dir: Path) -> None:
    p = cache.connectors_dir("sdmx")
    assert p == _pin_parsimony_cache_dir / "connectors" / "sdmx"
    assert p.is_dir()


def test_helpers_reject_path_traversal(_pin_parsimony_cache_dir: Path) -> None:
    with pytest.raises(ValueError):
        cache.embeddings_dir("../escape")
    with pytest.raises(ValueError):
        cache.connectors_dir("..")
    with pytest.raises(ValueError):
        cache.models_dir("foo/bar")


# ---------------------------------------------------------------------------
# info() — introspection
# ---------------------------------------------------------------------------


def test_info_reports_zero_for_empty_root(_pin_parsimony_cache_dir: Path) -> None:
    report = cache.info()
    assert report["root"] == str(_pin_parsimony_cache_dir)
    assert set(report["subdirs"]) == {"catalogs", "models", "embeddings", "connectors"}
    for entry in report["subdirs"].values():
        assert entry["exists"] is False
        assert entry["size_bytes"] == 0
        assert entry["files"] == 0


def test_info_does_not_create_missing_dirs(_pin_parsimony_cache_dir: Path) -> None:
    cache.info()
    # info() must be read-only — it doesn't create the four subdirs.
    assert not (_pin_parsimony_cache_dir / "catalogs").exists()
    assert not (_pin_parsimony_cache_dir / "models").exists()
    assert not (_pin_parsimony_cache_dir / "embeddings").exists()
    assert not (_pin_parsimony_cache_dir / "connectors").exists()


def test_info_after_seeding(_pin_parsimony_cache_dir: Path) -> None:
    p = cache.embeddings_dir("seeded-12345678")
    (p / "fragments.parquet").write_bytes(b"x" * 1024)
    (p / "meta.json").write_text("{}")

    report = cache.info()
    embeddings = report["subdirs"]["embeddings"]
    assert embeddings["exists"] is True
    assert embeddings["files"] == 2
    assert embeddings["size_bytes"] == 1024 + 2  # parquet + "{}"

    # Other subdirs are still empty.
    assert report["subdirs"]["catalogs"]["files"] == 0
    assert report["subdirs"]["models"]["files"] == 0
    assert report["subdirs"]["connectors"]["files"] == 0


# ---------------------------------------------------------------------------
# clear() — selective and full removal
# ---------------------------------------------------------------------------


def test_clear_specific_subdir_only_removes_that_one(_pin_parsimony_cache_dir: Path) -> None:
    cache.embeddings_dir("victim").joinpath("vec.parquet").write_bytes(b"x")
    cache.connectors_dir("survivor").joinpath("listing.json").write_text("[]")

    cache.clear(subdir="embeddings")

    assert not (_pin_parsimony_cache_dir / "embeddings").exists()
    assert (_pin_parsimony_cache_dir / "connectors" / "survivor" / "listing.json").exists()


def test_clear_no_arg_wipes_all_named_subdirs(_pin_parsimony_cache_dir: Path) -> None:
    cache.embeddings_dir("a").joinpath("a.bin").write_bytes(b"x")
    cache.connectors_dir("b").joinpath("b.json").write_text("[]")
    cache.models_dir("c").joinpath("c.onnx").write_bytes(b"x")
    cache.catalogs_dir().joinpath("d.txt").write_text("x")

    cache.clear()

    for name in ("catalogs", "models", "embeddings", "connectors"):
        assert not (_pin_parsimony_cache_dir / name).exists(), name


def test_clear_unknown_subdir_raises(_pin_parsimony_cache_dir: Path) -> None:
    with pytest.raises(ValueError, match="unknown cache subdir"):
        cache.clear(subdir="bogus")


def test_clear_is_idempotent_when_subdir_missing(_pin_parsimony_cache_dir: Path) -> None:
    # No subdirs exist yet — clear must be a no-op (no FileNotFoundError).
    cache.clear()
    cache.clear(subdir="embeddings")


# ---------------------------------------------------------------------------
# TTLDiskCache — JSON KV with per-call TTL
# ---------------------------------------------------------------------------


def test_ttl_get_returns_none_when_missing(tmp_path: Path) -> None:
    c = TTLDiskCache(tmp_path)
    assert c.get("nope", max_age_s=3600) is None


def test_ttl_round_trip_within_ttl(tmp_path: Path) -> None:
    c = TTLDiskCache(tmp_path)
    payload = {"agency": "ESTAT", "n": 7661}
    c.put("datasets-ESTAT", payload)
    assert c.get("datasets-ESTAT", max_age_s=3600) == payload


def test_ttl_returns_none_when_stale(tmp_path: Path) -> None:
    c = TTLDiskCache(tmp_path)
    c.put("k", [1, 2, 3])

    # Backdate the file mtime by two hours; one-hour TTL should reject it.
    [path] = list(tmp_path.glob("*.json"))
    old = time.time() - 7200
    os.utime(path, (old, old))

    assert c.get("k", max_age_s=3600) is None


def test_ttl_returns_none_on_corrupt_json(tmp_path: Path) -> None:
    c = TTLDiskCache(tmp_path)
    c.put("k", {"v": 1})
    [path] = list(tmp_path.glob("*.json"))
    path.write_text("{not valid json")

    assert c.get("k", max_age_s=3600) is None


def test_ttl_put_leaves_no_tmp_files(tmp_path: Path) -> None:
    c = TTLDiskCache(tmp_path)
    c.put("k", {"v": 1})
    assert list(tmp_path.glob("*.tmp")) == []


def test_ttl_unsafe_key_chars_round_trip(tmp_path: Path) -> None:
    """Keys with ``:`` / ``/`` / spaces are sanitized and disambiguated.

    ``"a:b"`` and ``"a/b"`` both sanitize to ``"a_b"`` but must NOT
    collide on disk — the hash suffix prevents cross-talk.
    """
    c = TTLDiskCache(tmp_path)
    c.put("datasets:ESTAT", [1, 2])
    c.put("datasets/ESTAT", [3, 4])

    assert c.get("datasets:ESTAT", max_age_s=3600) == [1, 2]
    assert c.get("datasets/ESTAT", max_age_s=3600) == [3, 4]
