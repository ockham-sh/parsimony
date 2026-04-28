"""Disk-backed cache primitives + global cache-path helpers.

The kernel's cache lives at the user cache root :func:`root` —
``platformdirs.user_cache_dir("parsimony")`` by default, overridable
via ``PARSIMONY_CACHE_DIR``. Four named subdirectories carry distinct
classes of artefact:

* :func:`catalogs_dir`    — HuggingFace catalog snapshots downloaded
  for search. Read-side: terminal, mcp post-refactor.
* :func:`models_dir`      — embedder model artefacts (ONNX, …).
  Read+write: query time and publish time.
* :func:`embeddings_dir`  — :class:`~parsimony.embedder.FragmentEmbeddingCache`
  parquet shards. Write-side: publish.
* :func:`connectors_dir`  — connector-owned scratch (opt-in per
  connector; replaces the SDMX-vocabulary ``dataflows/`` and
  ``portals/`` of earlier drafts).

For ad-hoc small JSON payloads keyed by string, use :class:`TTLDiskCache`.
For large binary or vector payloads, build a purpose-specific cache —
this primitive is sized for small JSON only.
"""

from __future__ import annotations

__all__ = [
    "TTLDiskCache",
    "catalogs_dir",
    "clear",
    "connectors_dir",
    "embeddings_dir",
    "info",
    "models_dir",
    "root",
]

import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import stat
import time
from pathlib import Path
from typing import Any

from platformdirs import user_cache_dir

logger = logging.getLogger(__name__)

_PARSIMONY_CACHE_ENV = "PARSIMONY_CACHE_DIR"

# Permissive enough for HF-style slugs (``sentence-transformers__all-MiniLM-L6-v2-abc12345``)
# and provider names (``sdmx``, ``fmp``). Forbids leading ``.`` so ``..`` and
# ``.hidden`` can't be used as path components.
_VALID_SUBKEY = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_\-.]*$")

# Named subdirectories under the cache root. Keep in lockstep with the
# helper functions below — :func:`info` and :func:`clear` iterate this.
_SUBDIRS: tuple[str, ...] = ("catalogs", "models", "embeddings", "connectors")


# ---------------------------------------------------------------------------
# Internal: path resolution + safety hardening
# ---------------------------------------------------------------------------


def _resolve_root() -> Path:
    """Return the cache root path. Pure resolution — no I/O, no mkdir.

    Honors ``PARSIMONY_CACHE_DIR`` (with ``~`` expansion) when set;
    otherwise delegates to ``platformdirs.user_cache_dir("parsimony")``
    so the layout is platform-correct (``~/.cache/parsimony`` on Linux,
    ``~/Library/Caches/parsimony`` on macOS,
    ``%LOCALAPPDATA%/parsimony/Cache`` on Windows).
    """
    override = os.environ.get(_PARSIMONY_CACHE_ENV)
    if override:
        return Path(override).expanduser()
    return Path(user_cache_dir("parsimony"))


def _safe_mkdir(path: Path) -> None:
    """Ensure *path* exists with safe perms; refuse a world/group-writable tree.

    On POSIX, an existing ancestor with ``g+w`` or ``o+w`` is rejected — that
    is the classic cache-poisoning vector. The directory is then created (if
    missing) and chmod'd to ``0o700``. On Windows the writable-bits check is
    skipped because ``Path.stat().st_mode`` does not reliably reflect ACLs.
    """
    _ensure_safe(path)
    path.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        # Best-effort. The world-writable check above is the actual safety gate.
        with contextlib.suppress(OSError):
            os.chmod(path, 0o700)


def _ensure_safe(path: Path) -> None:
    if os.name != "posix":
        return
    try:
        st = path.stat()
    except FileNotFoundError:
        parent = path.parent
        if parent != path and parent.exists():
            _ensure_safe(parent)
        return
    if not stat.S_ISDIR(st.st_mode):
        raise RuntimeError(f"Cache path {path} exists and is not a directory")
    # World/group-writable directories are a poisoning vector EXCEPT when
    # the sticky bit is set (canonical example: ``/tmp``). Sticky restricts
    # rename/unlink to the file's owner, so an attacker cannot replace
    # our cache subtree.
    writable_by_others = bool(st.st_mode & (stat.S_IWGRP | stat.S_IWOTH))
    sticky = bool(st.st_mode & stat.S_ISVTX)
    if writable_by_others and not sticky:
        raise RuntimeError(
            f"Refusing to use world-writable or group-writable cache dir {path}; "
            f"pick a user-private directory or unset {_PARSIMONY_CACHE_ENV}."
        )


def _sanitize_subkey(name: str) -> str:
    """Validate a per-subdir key (slug, provider name) against path-traversal.

    Allowed: ASCII letters, digits, ``_``, ``-``, ``.``; must not start
    with ``.``. ``..`` and anything containing ``/`` or shell metacharacters
    is rejected. Raises :class:`ValueError` so callers fail fast at the
    API boundary.
    """
    if not name:
        raise ValueError("cache subkey must be non-empty")
    if not _VALID_SUBKEY.fullmatch(name):
        raise ValueError(f"invalid cache subkey {name!r}: must match [A-Za-z0-9_][A-Za-z0-9_\\-.]*")
    return name


# ---------------------------------------------------------------------------
# Public path helpers
# ---------------------------------------------------------------------------


def root() -> Path:
    """Return the cache root, ensured to exist with safe perms."""
    r = _resolve_root()
    _safe_mkdir(r)
    return r


def catalogs_dir(provider: str | None = None) -> Path:
    """Return ``$ROOT/catalogs/[<provider>/]`` — HuggingFace catalog snapshots.

    With *provider* set, returns the per-provider staging directory used
    by publish drivers in ``parsimony-connectors/packages/<provider>/scripts/``.
    Each ``<provider>/`` mirrors ``hf://ockham/<provider>`` 1:1: namespace
    directories sit directly under it (``<provider>/<namespace>/...``)
    so ``hf upload ockham/<provider> <provider>/`` is a no-transform push.

    With *provider* omitted, returns the parent — useful for callers that
    iterate every provider (cache info, integration tests).
    """
    p = root() / "catalogs"
    if provider is not None:
        p = p / _sanitize_subkey(provider)
    _safe_mkdir(p)
    return p


def models_dir(slug: str | None = None) -> Path:
    """Return ``$ROOT/models/[<slug>/]`` — embedder model artefacts.

    With *slug* omitted, returns the parent for callers that own an
    internal layout (e.g. :class:`~parsimony.embedder.OnnxEmbedder`
    nests ``<slug>/<variant>`` underneath).
    """
    p = root() / "models"
    if slug is not None:
        p = p / _sanitize_subkey(slug)
    _safe_mkdir(p)
    return p


def embeddings_dir(slug: str | None = None) -> Path:
    """Return ``$ROOT/embeddings/[<slug>/]`` — fragment vector shards.

    *slug* identifies an embedder (model + dim + normalize) so that
    embedders with different identities never share a cache file.
    """
    p = root() / "embeddings"
    if slug is not None:
        p = p / _sanitize_subkey(slug)
    _safe_mkdir(p)
    return p


def connectors_dir(provider: str | None = None) -> Path:
    """Return ``$ROOT/connectors/[<provider>/]`` — connector-owned scratch."""
    p = root() / "connectors"
    if provider is not None:
        p = p / _sanitize_subkey(provider)
    _safe_mkdir(p)
    return p


# ---------------------------------------------------------------------------
# Introspection / maintenance
# ---------------------------------------------------------------------------


def info() -> dict[str, Any]:
    """Return cache occupancy as a JSON-shaped dict for the CLI / operators.

    Walks each named subdirectory once. Does not create missing dirs —
    a non-existent subdir reports ``exists=False, size_bytes=0, files=0``.
    """
    r = _resolve_root()
    out: dict[str, Any] = {"root": str(r), "subdirs": {}}
    for name in _SUBDIRS:
        p = r / name
        if not p.exists():
            out["subdirs"][name] = {
                "path": str(p),
                "size_bytes": 0,
                "files": 0,
                "exists": False,
            }
            continue
        size = 0
        files = 0
        for dirpath, _, filenames in os.walk(p):
            for fname in filenames:
                fp = Path(dirpath) / fname
                try:
                    size += fp.stat().st_size
                    files += 1
                except OSError:
                    pass
        out["subdirs"][name] = {
            "path": str(p),
            "size_bytes": size,
            "files": files,
            "exists": True,
        }
    return out


def clear(subdir: str | None = None) -> None:
    """Remove a named subdir, or all named subdirs when *subdir* is ``None``.

    Skeleton dirs are not recreated — the next call to a helper will.
    Unknown *subdir* names raise :class:`ValueError`.
    """
    r = _resolve_root()
    if subdir is not None:
        if subdir not in _SUBDIRS:
            raise ValueError(f"unknown cache subdir {subdir!r}; expected one of {_SUBDIRS}")
        target = r / subdir
        if target.exists():
            shutil.rmtree(target)
        return
    for name in _SUBDIRS:
        target = r / name
        if target.exists():
            shutil.rmtree(target)


# ---------------------------------------------------------------------------
# TTLDiskCache — JSON KV with per-call TTL
# ---------------------------------------------------------------------------

_VALID_KEY = re.compile(r"^[A-Za-z0-9_\-]+$")


def _safe_filename(key: str) -> str:
    """Return a filesystem-safe filename for *key*.

    Keys that already match ``[A-Za-z0-9_\\-]+`` map to ``{key}.json``
    (human-readable). Keys with unsafe characters are sanitized and
    suffixed with a short hash to disambiguate keys that would otherwise
    collide after sanitization (e.g. ``"a:b"`` and ``"a/b"``).
    """
    if _VALID_KEY.fullmatch(key):
        return f"{key}.json"
    sanitized = re.sub(r"[^A-Za-z0-9_\-]", "_", key)[:60]
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]
    return f"{sanitized}_{digest}.json"


class TTLDiskCache:
    """JSON-backed key/value cache with per-get TTL.

    Storage is one file per key under *root*. Writes are atomic
    (``tmp + os.replace``) so a crashed writer never leaves a corrupt
    cache file visible to a concurrent reader.

    The TTL is **caller-specified at every** :meth:`get` — the cache
    itself stores no expiry metadata, just the value's mtime. This keeps
    the on-disk format trivial and lets a single cache directory serve
    callers with different freshness needs.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    def get(self, key: str, *, max_age_s: float) -> Any | None:
        """Return cached value if present and younger than *max_age_s*.

        Returns ``None`` (without raising) on missing file, stale TTL,
        or corrupt JSON — callers fall through to the live computation.
        """
        path = self._path_for(key)
        try:
            stat_result = path.stat()
        except FileNotFoundError:
            return None
        age = time.time() - stat_result.st_mtime
        if age > max_age_s:
            return None
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("TTLDiskCache: ignoring unreadable %s: %s", path, exc)
            return None

    def put(self, key: str, value: Any) -> None:
        """Persist *value* under *key* atomically. *value* must be JSON-serializable."""
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / (path.name + ".tmp")
        tmp.write_text(json.dumps(value, separators=(",", ":")))
        os.replace(tmp, path)

    def _path_for(self, key: str) -> Path:
        return self._root / _safe_filename(key)
