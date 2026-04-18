"""Filesystem layout for cached bundles.

Cache layout::

    <cache_base>/<namespace>/<commit_sha>/
        manifest.json
        entries.parquet
        index.faiss

One cached revision per namespace is kept; a fresh download replaces any
older sibling. The commit SHA is the directory name — no sidecar file.
"""

from __future__ import annotations

import contextlib
import re
import shutil
from pathlib import Path
from typing import Final

from parsimony.bundles.format import BUNDLE_FILENAMES

# 40-char lowercase hex SHA — the commit pin shape.
_SHA40_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")


def _bundle_dir(cache_base: Path, namespace: str, revision: str) -> Path:
    return cache_base / namespace / revision


def _bundle_files_present(bundle_dir: Path) -> bool:
    return all((bundle_dir / name).exists() for name in BUNDLE_FILENAMES)


def _scan_cached_revision(cache_base: Path, namespace: str) -> str | None:
    """Return the cached revision for a namespace, or None.

    Scans ``cache_base/<namespace>/`` for a subdirectory whose name matches
    a 40-char SHA and whose three bundle files are present. If multiple
    match, picks the most recently modified — siblings get cleaned up on the
    next successful load.
    """
    ns_dir = cache_base / namespace
    if not ns_dir.is_dir():
        return None
    candidates: list[tuple[float, str]] = []
    for child in ns_dir.iterdir():
        if not child.is_dir() or not _SHA40_RE.match(child.name):
            continue
        if not _bundle_files_present(child):
            continue
        candidates.append((child.stat().st_mtime, child.name))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _cleanup_old_revisions(cache_base: Path, namespace: str, *, keep: str) -> None:
    """Remove sibling revision directories, keeping only ``keep``."""
    ns_dir = cache_base / namespace
    if not ns_dir.is_dir():
        return
    for child in ns_dir.iterdir():
        if not child.is_dir() or child.name == keep:
            continue
        if not _SHA40_RE.match(child.name):
            continue
        with contextlib.suppress(OSError):
            shutil.rmtree(child)
