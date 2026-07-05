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
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from parsimony.catalog.storage import META_FILENAME, read_meta
from parsimony.catalog.urls import REPO_TYPE, parse_catalog_url
from parsimony.errors import CatalogNotFoundError

logger = logging.getLogger(__name__)

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

    _log_first_pull(root, "", revision=revision, cache_dir=cache_dir)
    try:
        downloaded = snapshot_download(repo_id=root, repo_type=REPO_TYPE, revision=revision, cache_dir=cache_dir)
        snapshot_dir = Path(downloaded)
    except (OSError, HfHubHTTPError) as exc:
        meta = _cached_meta_path(root, "", revision=revision, cache_dir=cache_dir)
        if meta is not None:
            _warn_or_raise_stale(root, "", meta.parent, exc, max_age_s=max_age_s)
            return meta.parent
        raise

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
            _warn_or_raise_stale(root, sub, meta.parent, exc, max_age_s=max_age_s)
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
