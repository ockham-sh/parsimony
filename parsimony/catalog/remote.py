"""Remote catalog transport: resolve, download, and upload Hugging Face snapshots.

This module owns every interaction with ``huggingface_hub`` so the core
:class:`~parsimony.catalog.catalog.Catalog` stays free of remote-fetch plumbing.
``resolve_catalog_dir`` maps any catalog URL to a local directory; the loader in
:mod:`parsimony.catalog.catalog` reads that directory. ``_upload_hf`` is the
upload counterpart: :meth:`Catalog.save` serializes itself to a staging
directory and hands that path here. This module works in paths, never in
``Catalog`` objects, so it carries no dependency back on ``catalog.py``.

Freshness policy: the ``hf://`` path balances two failure modes on every call —
paying a live network round-trip to Hugging Face just to confirm nothing changed,
and (when the Hub is unreachable) silently serving a snapshot that may be
arbitrarily old. ``revalidate_ttl_s`` bounds the first (skip re-listing the Hub
within the window); ``max_age_s`` bounds the second (refuse a cached snapshot
older than the window instead of serving it past a caller-declared staleness
tolerance). Both default to the old, unbounded behaviour when passed ``None``.
"""

from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from parsimony.catalog.storage import META_FILENAME, read_meta
from parsimony.catalog.urls import REPO_TYPE, parse_catalog_url
from parsimony.errors import CatalogNotFoundError

logger = logging.getLogger(__name__)

# Catalog bundles are many small files, so a cold fetch is dominated by per-file
# round-trips rather than bandwidth. 8 matches ``snapshot_download``'s own default
# — enough to hide the latency, conservative enough not to trip Hub rate limits.
_DEFAULT_DOWNLOAD_WORKERS = 8

# Mirrors huggingface_hub's own truthiness set for its boolean env vars.
_TRUE_VALUES = {"1", "ON", "YES", "TRUE"}

# Mirrors the condition ``hf_hub_download`` itself uses to decide whether it may
# skip its HEAD request and return the cached pointer path directly. Spelled out
# here rather than imported: the constant lives in ``huggingface_hub.file_download``
# (not the public ``constants``) and has been renamed across the versions this
# package supports, so depending on it would be a portability trap for a regex
# that is just "a 40-char git sha".
_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

# Online path: how long a successful Hub listing is trusted before the next
# call re-lists rather than reusing the local snapshot dir outright. Published
# catalog snapshots are typically rebuilt on a schedule (not continuously), so
# re-checking the Hub on every single call is a paid round-trip for no benefit
# most of the time. Pass ``revalidate_ttl_s=None`` (or ``0``) to always revalidate.
_DEFAULT_REVALIDATE_TTL_S: float = 300.0


def resolve_catalog_dir(
    url: str | Path,
    *,
    cache_dir: Path | None = None,
    max_age_s: float | None = None,
    revalidate_ttl_s: float | None = _DEFAULT_REVALIDATE_TTL_S,
) -> Path:
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

    ``max_age_s`` and ``revalidate_ttl_s`` only affect the ``hf://`` path — see
    module docstring. Both are ignored for ``file://``.
    """
    parsed = parse_catalog_url(url)
    if parsed.scheme == "file":
        return Path(parsed.root) / parsed.sub if parsed.sub else Path(parsed.root)
    if parsed.scheme == "hf":
        if parsed.sub:
            return download_hf_subpath(
                parsed.root,
                parsed.sub,
                revision=parsed.revision,
                cache_dir=cache_dir,
                max_age_s=max_age_s,
                revalidate_ttl_s=revalidate_ttl_s,
            )
        return _download_hf_repo(
            parsed.root,
            revision=parsed.revision,
            cache_dir=cache_dir,
            max_age_s=max_age_s,
            revalidate_ttl_s=revalidate_ttl_s,
        )
    raise ValueError(f"Unsupported catalog URL scheme {parsed.scheme!r}. Supported: ['file', 'hf']")


def _download_hf_repo(
    root: str,
    *,
    revision: str | None,
    cache_dir: Path | None,
    max_age_s: float | None = None,
    revalidate_ttl_s: float | None = _DEFAULT_REVALIDATE_TTL_S,
) -> Path:
    """Download a whole-repo catalog (no sub-path) and return its local directory.

    A small repo lists cheaply, so ``snapshot_download`` is fine here — the
    sub-path scoping in :func:`download_hf_subpath` only matters inside a monorepo.
    Shares :func:`download_hf_subpath`'s revalidation-TTL shortcut and offline
    max-age policy (both keyed with ``sub=""``, since a whole-repo catalog's
    ``meta.json`` lives at the repo root).
    """
    if cache_dir is None:
        from parsimony import cache

        cache_dir = cache.catalogs_dir()

    recent = _recent_snapshot_dir(root, "", revision, cache_dir=cache_dir, revalidate_ttl_s=revalidate_ttl_s)
    if recent is not None:
        return recent

    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import HfHubHTTPError

    label = _label(root, "")
    # Whole-repo fetches go through ``snapshot_download``, which resolves and
    # skips already-cached files internally — so unlike the sub-path branch there
    # is no cheap way to know up front whether anything will transfer. Word it as
    # a resolution rather than claiming a download, and let the elapsed time on
    # the closing line distinguish a cache hit from a real fetch.
    logger.info("Resolving catalog %s against Hugging Face", label)
    started = time.monotonic()
    try:
        downloaded = snapshot_download(repo_id=root, repo_type=REPO_TYPE, revision=revision, cache_dir=cache_dir)
        snapshot_dir = Path(downloaded)
    except (OSError, HfHubHTTPError) as exc:
        meta = _cached_meta_path(root, "", revision=revision, cache_dir=cache_dir)
        if meta is not None:
            _warn_or_raise_stale(root, "", meta.parent, exc, max_age_s=max_age_s)
            return meta.parent
        raise

    logger.info("Resolved catalog %s in %.1fs", label, time.monotonic() - started)
    _remember_snapshot_dir(root, "", revision, snapshot_dir, cache_dir=cache_dir)
    return snapshot_dir


def download_hf_subpath(
    root: str,
    sub: str,
    *,
    revision: str | None = None,
    cache_dir: Path | None = None,
    max_age_s: float | None = None,
    revalidate_ttl_s: float | None = _DEFAULT_REVALIDATE_TTL_S,
) -> Path:
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

    ``revalidate_ttl_s`` skips the listing call entirely (Hub *and* offline-fallback
    logic both) when a snapshot for this ``(root, sub, revision)`` was already
    resolved within the window and still exists on disk — this is what keeps a
    catalog that rarely changes from paying a live Hub round-trip on every call.
    Pass ``None`` or ``0`` to always revalidate (the old behaviour).

    A fetch that actually transfers something brackets itself with two ``INFO``
    lines on ``parsimony.catalog.remote``: one before the download saying how many
    files and how many MB are coming, and one after with the elapsed time. The pair
    is deliberate — the size has to arrive *before* the wait for a caller to judge
    whether a stall is proportionate, and a start with no matching finish is what
    marks a long wait as progress rather than a hang. A revalidation that downloads
    nothing stays silent, so the lines never claim work that did not happen. A
    library configures no handlers, so a consumer that wants them opts in.

    ``max_age_s`` bounds the offline-fallback path only: when the Hub is
    unreachable and a cached snapshot exists, its age (the cached ``meta.json``'s
    ``build.built_at``, not local download time) is always logged; past
    ``max_age_s`` it raises :class:`CatalogNotFoundError` instead of serving the
    stale snapshot. ``None`` (default) preserves the old behaviour of serving any
    cached snapshot regardless of age.
    """
    if cache_dir is None:
        from parsimony import cache

        cache_dir = cache.catalogs_dir()

    recent = _recent_snapshot_dir(root, sub, revision, cache_dir=cache_dir, revalidate_ttl_s=revalidate_ttl_s)
    if recent is not None:
        return recent

    from huggingface_hub import HfApi, hf_hub_download
    from huggingface_hub.errors import HfHubHTTPError
    from huggingface_hub.hf_api import RepoFile

    label = _label(root, sub)
    started = time.monotonic()

    api = HfApi()
    cached_meta: Path | None = None
    try:
        # Resolve a branch/None revision to its commit sha *once*, then list and
        # download at that pinned commit. Two payoffs: ``hf_hub_download`` skips
        # its per-file HEAD entirely for already-cached files (it only takes that
        # shortcut when the revision is a commit hash), and the listing and the
        # downloads observe the same commit — a republish mid-fetch can no longer
        # assemble a bundle from two different commits.
        pinned = _resolve_commit(api, root, revision)

        # Probe the cache at the *pinned* sha, never at the branch name.
        # Downloading at a sha does not write the repo's ``refs/<branch>``
        # pointer, so a snapshot this function fetched is invisible to a lookup
        # by branch — which would silently disable the stale-cache fallback below.
        cached_meta = _cached_meta_path(root, sub, revision=pinned, cache_dir=cache_dir)

        # The listing is the one part of a cold resolve that can stall for minutes
        # with nothing yet to report (a large monorepo), so it needs a line
        # *before* it — a completion line cannot answer "am I stuck". Past the
        # revalidation TTL this same code runs with the snapshot already on disk
        # and usually downloads nothing, and announcing a fetch there would claim
        # work that never happens. Nothing cached is the honest discriminator: it
        # means every byte is still to come.
        if cached_meta is None:
            logger.info("Resolving catalog %s against Hugging Face", label)
        else:
            logger.debug("Revalidating catalog %s against Hugging Face", label)

        entries = list(
            api.list_repo_tree(
                repo_id=root,
                path_in_repo=sub,
                recursive=True,
                repo_type=REPO_TYPE,
                revision=pinned,
            )
        )
    except (OSError, HfHubHTTPError) as exc:
        # The sha may be unknown here (the failure may be the ref lookup itself),
        # so fall back to the caller's revision to find *something* cached.
        if cached_meta is None:
            cached_meta = _cached_meta_path(root, sub, revision=revision, cache_dir=cache_dir)
        if cached_meta is not None:
            _warn_or_raise_stale(root, sub, cached_meta.parent, exc, max_age_s=max_age_s)
            return cached_meta.parent
        raise

    repo_files = [entry for entry in entries if isinstance(entry, RepoFile)]
    if not repo_files:
        raise CatalogNotFoundError(f"Catalog bundle not present at hf://{root}/{sub}")
    files = [entry.path for entry in repo_files]

    # Announced *before* the wait, not after: the size of what is about to be
    # transferred is what lets a caller judge whether a stall is proportionate,
    # and it is only knowable once the listing is in. An empty list means every
    # file is already cached — a revalidation, not a download — so nothing is
    # logged and the pass stays silent.
    pending = set(_uncached_files(root, files, revision=pinned, cache_dir=cache_dir))
    if pending:
        logger.info(
            "Downloading %d of %d files (%.1f MB) for catalog %s",
            len(pending),
            len(files),
            sum(entry.size for entry in repo_files if entry.path in pending) / 1e6,
            label,
        )

    def fetch(filename: str) -> Path:
        return Path(
            hf_hub_download(
                repo_id=root,
                filename=filename,
                repo_type=REPO_TYPE,
                revision=pinned,
                cache_dir=cache_dir,
            )
        )

    # A catalog bundle is many small files (a meta.json, a parquet, and one index
    # directory per dimension — ~100 files totalling ~1 MB is typical), so the
    # fetch is bound by per-file round-trips, not bandwidth. Downloading them
    # concurrently is what makes a cold resolution take seconds instead of a
    # minute. This mirrors ``snapshot_download``, which fans the same
    # ``hf_hub_download`` calls out over a thread pool of the same default width;
    # concurrent access is safe because that function takes a per-file lock.
    # A width of 1 (the ``hf_transfer`` case) is just a serial loop, so there is
    # no separate branch for it. Order is preserved, which the next line relies on.
    with ThreadPoolExecutor(max_workers=_download_workers()) as pool:
        downloaded = list(pool.map(fetch, files))

    # downloaded[i] == <snapshot_dir>/<files[i]>; strip the filename's components.
    snapshot_dir = downloaded[0].parents[len(PurePosixPath(files[0]).parts) - 1]
    if pending:
        # Closes the interval the line above opened: a start with no matching
        # finish is still running, which is what tells a captured log that a long
        # wait is progress rather than a hang.
        logger.info("Downloaded catalog %s in %.1fs", label, time.monotonic() - started)
    result = snapshot_dir / sub
    _remember_snapshot_dir(root, sub, revision, result, cache_dir=cache_dir)
    return result


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


def _snapshot_age_s(snapshot_dir: Path) -> float | None:
    """Age, in seconds, of *snapshot_dir*'s ``meta.json`` ``build.built_at``.

    Returns ``None`` when ``meta.json`` is missing or unreadable. Deliberately
    reflects when the snapshot's *content* was built/published, not when this
    machine happened to download it — the two can differ by however long the
    snapshot sat on the Hub before being fetched, which is the number an
    operator deciding "is this too stale" actually cares about.
    """
    try:
        built_at = read_meta(snapshot_dir).build.built_at
    except (OSError, ValueError) as exc:
        logger.debug("Could not read %s for staleness check: %s", snapshot_dir / META_FILENAME, exc)
        return None
    if built_at.tzinfo is None:
        built_at = built_at.replace(tzinfo=UTC)
    return (datetime.now(UTC) - built_at).total_seconds()


def _warn_or_raise_stale(root: str, sub: str, snapshot_dir: Path, exc: Exception, *, max_age_s: float | None) -> None:
    """Log (or, past *max_age_s*, raise) about serving *snapshot_dir* offline.

    The age is always surfaced — via the raised message or the warning log —
    so an operator can tell a 5-minute-old snapshot from a month-old one, even
    when no ``max_age_s`` policy is configured.
    """
    age_s = _snapshot_age_s(snapshot_dir)
    age_desc = f"{age_s:.0f}s old" if age_s is not None else "of unknown age"
    label = f"hf://{root}/{sub}" if sub else f"hf://{root}"
    if max_age_s is not None and age_s is not None and age_s > max_age_s:
        raise CatalogNotFoundError(
            f"Cached snapshot for {label} is {age_desc}, exceeding max_age_s={max_age_s:.0f}s, and "
            f"Hugging Face is unreachable ({type(exc).__name__}: {exc})"
        ) from exc
    logger.warning(
        "Hugging Face unreachable for %s (%s); using cached snapshot %s (%s)",
        label,
        type(exc).__name__,
        snapshot_dir,
        age_desc,
    )


def _revalidate_key(root: str, sub: str, revision: str | None) -> str:
    import hashlib

    return hashlib.sha256(f"{root}\n{sub}\n{revision or ''}".encode()).hexdigest()


def _recent_snapshot_dir(
    root: str, sub: str, revision: str | None, *, cache_dir: Path, revalidate_ttl_s: float | None
) -> Path | None:
    """Return the last snapshot dir resolved for this ``(root, sub, revision)`` within *revalidate_ttl_s*.

    A hit means "skip the Hub call entirely" — the online listing and the
    offline-fallback path are both bypassed. ``None`` or ``0`` disables this
    (every call revalidates against the Hub, the pre-freshness-policy behaviour).
    Falls through to a full re-resolution when the remembered directory no
    longer exists on disk (e.g. the cache was cleared).
    """
    if not revalidate_ttl_s:
        return None
    from parsimony.cache import TTLDiskCache

    cache = TTLDiskCache(cache_dir / ".revalidate")
    cached = cache.get(_revalidate_key(root, sub, revision), max_age_s=revalidate_ttl_s)
    if not isinstance(cached, dict):
        return None
    snapshot_dir = Path(cached["snapshot_dir"])
    return snapshot_dir if snapshot_dir.is_dir() else None


def _remember_snapshot_dir(root: str, sub: str, revision: str | None, snapshot_dir: Path, *, cache_dir: Path) -> None:
    """Record a freshly-resolved snapshot dir so the next call can skip revalidation."""
    from parsimony.cache import TTLDiskCache

    cache = TTLDiskCache(cache_dir / ".revalidate")
    cache.put(_revalidate_key(root, sub, revision), {"snapshot_dir": str(snapshot_dir)})


def _label(root: str, sub: str) -> str:
    """Human-readable ``hf://`` label for logs and error messages."""
    return f"hf://{root}/{sub}" if sub else f"hf://{root}"


def _uncached_files(root: str, files: list[str], *, revision: str, cache_dir: Path) -> list[str]:
    """Which of *files* are not already on disk at *revision*.

    Local stat calls only — no network — so this is cheap enough to run before
    every fetch. It exists to keep the logs honest: past the revalidation TTL the
    Hub is re-listed even when nothing has changed, and without this the pass
    would announce a download that never happens.
    """
    from huggingface_hub import try_to_load_from_cache

    missing: list[str] = []
    for filename in files:
        cached = try_to_load_from_cache(
            repo_id=root,
            filename=filename,
            cache_dir=str(cache_dir),
            repo_type=REPO_TYPE,
            revision=revision,
        )
        if not (isinstance(cached, str) and Path(cached).is_file()):
            missing.append(filename)
    return missing


def _resolve_commit(api: Any, root: str, revision: str | None) -> str:
    """Resolve *revision* to a concrete commit sha, or return it if already one.

    A branch name (or ``None``, meaning ``main``) has to be re-resolved by the Hub
    on every single file request. A commit sha does not: ``hf_hub_download``
    short-circuits to the local pointer path without any network call when the
    file is already cached. Paying one ref lookup here to pin the whole fetch is
    what turns a warm re-resolution from ~100 sequential HEAD requests into zero.

    Tags and shas are passed through untouched — both are already immutable, so
    there is nothing to resolve.
    """
    if revision and _COMMIT_SHA_RE.match(revision):
        return revision
    refs = api.list_repo_refs(repo_id=root, repo_type=REPO_TYPE)
    wanted = revision or "main"
    for branch in refs.branches:
        if branch.name == wanted:
            return str(branch.target_commit)
    for tag in refs.tags:
        if tag.name == wanted:
            return str(tag.target_commit)
    # Not a known branch or tag: hand it back and let the Hub reject it, so the
    # error the caller sees comes from the request rather than from guessing here.
    return wanted


def _download_workers() -> int:
    """How many files to fetch concurrently.

    ``hf_transfer`` parallelises the transfer of a *single* file internally, so
    stacking a thread pool on top of it fights its own scheduler — ``huggingface_hub``
    drops to a serial loop for the same reason. Mirror that.

    Read from the environment rather than ``huggingface_hub.constants``: the
    constant is absent at both ends of the version range this package supports
    (added after 0.25, removed again in 1.x), so touching it raises
    ``AttributeError`` depending only on which version resolved. The env var is
    the documented user-facing knob and has been stable throughout — same trap,
    same answer, as the commit-sha regex above.
    """
    return 1 if os.environ.get("HF_HUB_ENABLE_HF_TRANSFER", "").upper() in _TRUE_VALUES else _DEFAULT_DOWNLOAD_WORKERS


def _upload_hf(local_dir: Path, root: str, sub: str) -> None:
    """Upload a serialized catalog snapshot directory to ``hf://root/sub``."""
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=root, repo_type=REPO_TYPE, exist_ok=True)
    api.upload_folder(
        folder_path=str(local_dir),
        repo_id=root,
        repo_type=REPO_TYPE,
        path_in_repo=sub or None,
    )
