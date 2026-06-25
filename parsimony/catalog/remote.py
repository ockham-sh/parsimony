"""Remote catalog transport: resolve, download, and upload Hugging Face snapshots.

This module owns every interaction with ``huggingface_hub`` so the core
:class:`~parsimony.catalog.catalog.Catalog` stays free of remote-fetch plumbing.
``resolve_catalog_dir`` maps any catalog URL to a local directory; the loader in
:mod:`parsimony.catalog.catalog` reads that directory. ``_save_hf`` is the upload
counterpart used by :meth:`Catalog.save`.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from parsimony.catalog.storage import META_FILENAME
from parsimony.catalog.urls import REPO_TYPE, parse_catalog_url
from parsimony.errors import CatalogNotFoundError

if TYPE_CHECKING:
    from parsimony.catalog.catalog import Catalog

logger = logging.getLogger(__name__)


def resolve_catalog_dir(url: str | Path, *, cache_dir: Path | None = None) -> Path:
    """Resolve a catalog URL to the local directory that holds its snapshot.

    The single place that maps a catalog URL to a loadable directory, for every
    scheme. ``file://`` (or a bare path) resolves to the directory itself; an
    ``hf://`` URL downloads the snapshot — sub-path scoped (see
    :func:`download_hf_subpath`) — into the local cache and returns that directory.

    This is the directory :meth:`Catalog._load_from_path` reads. Use it directly
    when you need the catalog's files (e.g. to scan a sibling parquet) rather than
    a loaded :class:`Catalog`; use :meth:`Catalog.load` when you want the catalog.
    Resolution is a pure mapping — a returned ``file://`` directory is not required
    to exist; the loader validates it.
    """
    parsed = parse_catalog_url(url)
    if parsed.scheme == "file":
        return Path(parsed.root) / parsed.sub if parsed.sub else Path(parsed.root)
    if parsed.scheme == "hf":
        if parsed.sub:
            return download_hf_subpath(parsed.root, parsed.sub, revision=parsed.revision, cache_dir=cache_dir)
        return _download_hf_repo(parsed.root, revision=parsed.revision, cache_dir=cache_dir)
    raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")


def _download_hf_repo(root: str, *, revision: str | None, cache_dir: Path | None) -> Path:
    """Download a whole-repo catalog (no sub-path) and return its local directory.

    A small repo lists cheaply, so ``snapshot_download`` is fine here — the
    sub-path scoping in :func:`download_hf_subpath` only matters inside a monorepo.
    """
    if cache_dir is None:
        from parsimony import cache

        cache_dir = cache.catalogs_dir()

    from huggingface_hub import snapshot_download

    _log_first_pull(root, "", revision=revision, cache_dir=cache_dir)
    return Path(snapshot_download(repo_id=root, repo_type=REPO_TYPE, revision=revision, cache_dir=cache_dir))


def download_hf_subpath(root: str, sub: str, *, revision: str | None = None, cache_dir: Path | None = None) -> Path:
    """Download only ``sub/`` from a Hugging Face dataset repo; return its local dir.

    Path-scoped on purpose: lists and downloads just the sub-tree, never
    enumerating the whole repo. ``snapshot_download`` always lists the *entire*
    tree before ``allow_patterns`` filters downloads, so a sub-catalog living in
    a monorepo of thousands of files stalls for minutes just to fetch a handful —
    use this instead of ``snapshot_download(..., allow_patterns=[f"{sub}/*"])``.

    The listing needs the network, so when Hugging Face is unreachable we fall
    back to the already-cached snapshot on disk — preserving ``snapshot_download``'s
    offline behaviour for a catalog that has been loaded before. The returned
    directory is a loadable catalog bundle (the dir that holds ``meta.json``).
    """
    if cache_dir is None:
        from parsimony import cache

        cache_dir = cache.catalogs_dir()

    from huggingface_hub import HfApi, hf_hub_download
    from huggingface_hub.errors import HfHubHTTPError
    from huggingface_hub.hf_api import RepoFile

    try:
        entries = list(
            HfApi().list_repo_tree(
                repo_id=root,
                path_in_repo=sub,
                recursive=True,
                repo_type=REPO_TYPE,
                revision=revision,
            )
        )
    except (OSError, HfHubHTTPError) as exc:
        meta = _cached_meta_path(root, sub, revision=revision, cache_dir=cache_dir)
        if meta is not None:
            logger.warning(
                "Hugging Face unreachable for hf://%s/%s (%s); using cached snapshot %s",
                root,
                sub,
                type(exc).__name__,
                meta.parent,
            )
            return meta.parent
        raise

    files = [entry.path for entry in entries if isinstance(entry, RepoFile)]
    if not files:
        raise CatalogNotFoundError(f"Catalog bundle not present at hf://{root}/{sub}")

    _log_first_pull(root, sub, revision=revision, cache_dir=cache_dir)
    snapshot_dir: Path | None = None
    for filename in files:
        downloaded = Path(
            hf_hub_download(
                repo_id=root,
                filename=filename,
                repo_type=REPO_TYPE,
                revision=revision,
                cache_dir=cache_dir,
            )
        )
        if snapshot_dir is None:
            # downloaded == <snapshot_dir>/<filename>; strip filename's components.
            snapshot_dir = downloaded.parents[len(PurePosixPath(filename).parts) - 1]
    assert snapshot_dir is not None  # files is non-empty
    return snapshot_dir / sub


def _cached_meta_path(root: str, sub: str, *, revision: str | None, cache_dir: Path) -> Path | None:
    """Local path of the catalog's cached ``meta.json``, or ``None`` if not cached."""
    from huggingface_hub import try_to_load_from_cache

    filename = f"{sub}/{META_FILENAME}" if sub else META_FILENAME
    cached = try_to_load_from_cache(
        repo_id=root,
        filename=filename,
        cache_dir=str(cache_dir),
        repo_type=REPO_TYPE,
        revision=revision,
    )
    if isinstance(cached, str) and Path(cached).is_file():
        return Path(cached)
    return None


def _log_first_pull(root: str, sub: str, *, revision: str | None, cache_dir: Path) -> None:
    """Emit one INFO line the first time a catalog is fetched (not on a cache hit).

    The fetch can take seconds to a minute. On a headless/server log — where the
    Hugging Face download progress bars do not render — this is the only signal
    that the stall is a one-time download; later loads hit the local cache and
    stay silent. The agent never sees this (connector logging does not reach it):
    its expectation is set by the static SKILL guidance instead.
    """
    if _cached_meta_path(root, sub, revision=revision, cache_dir=cache_dir) is not None:
        return
    label = f"hf://{root}/{sub}" if sub else f"hf://{root}"
    logger.info("Fetching catalog %s from Hugging Face (first load; cached locally after this)", label)


def _save_hf(catalog: Catalog, root: str, sub: str, *, builder: str | None = None) -> None:
    from huggingface_hub import HfApi

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "snapshot"
        catalog._save_to_path(staging, builder=builder)
        api = HfApi()
        api.create_repo(repo_id=root, repo_type=REPO_TYPE, exist_ok=True)
        api.upload_folder(
            folder_path=str(staging),
            repo_id=root,
            repo_type=REPO_TYPE,
            path_in_repo=sub or None,
        )
