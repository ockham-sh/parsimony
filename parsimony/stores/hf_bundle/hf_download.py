"""HuggingFace Hub I/O — freshness HEAD check, remote size pre-flight, snapshot download.

These are the only functions that touch the network. All access is anonymous
(``token=False``); the client store never reads HF token env vars.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from parsimony.bundles.errors import BundleError, BundleNotFoundError
from parsimony.bundles.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    MAX_INDEX_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_PARQUET_BYTES,
)
from parsimony.stores.hf_bundle.cache_layout import _SHA40_RE, _bundle_dir

logger = logging.getLogger(__name__)

# Per-operation timeouts.
_MODEL_INFO_TIMEOUT_S = 10.0
_SNAPSHOT_TIMEOUT_S = 300.0


_BUNDLE_FILE_CAPS: dict[str, int] = {
    MANIFEST_FILENAME: MAX_MANIFEST_BYTES,
    ENTRIES_FILENAME: MAX_PARQUET_BYTES,
    INDEX_FILENAME: MAX_INDEX_BYTES,
}


async def _remote_size_check(
    *,
    repo_id: str,
    revision: str,
    namespace: str,
    timeout_s: float,
) -> None:
    """Reject before bytes hit disk if any remote file exceeds its cap.

    Calls ``HfApi.repo_info(files_metadata=True)`` and inspects each bundle
    file's reported size. A file over the cap raises
    :class:`BundleError` so :func:`_download_snapshot` is never
    invoked. This is the **only** place that can stop a 10 TB-index DoS —
    the post-download check inside ``_load_from_dir`` runs after disk write.
    """
    from huggingface_hub import HfApi
    from huggingface_hub.utils import HfHubHTTPError

    def _call() -> Any | None:
        try:
            api = HfApi()
            return api.repo_info(
                repo_id=repo_id,
                repo_type="dataset",
                revision=revision,
                files_metadata=True,
            )
        except HfHubHTTPError as exc:
            logger.debug("remote_size_check http_error repo=%s exc=%s", repo_id, exc)
            return None

    try:
        async with asyncio.timeout(timeout_s):
            info = await asyncio.to_thread(_call)
    except TimeoutError:
        # The post-write size checks in _load_from_dir remain authoritative,
        # so a soft-fail here is acceptable for a pre-flight probe only.
        logger.warning("remote_size_check timeout repo=%s revision=%s", repo_id, revision)
        return

    if info is None:
        return

    siblings = getattr(info, "siblings", None) or []
    for sibling in siblings:
        name = getattr(sibling, "rfilename", None)
        size = getattr(sibling, "size", None)
        if name in _BUNDLE_FILE_CAPS and isinstance(size, int):
            cap = _BUNDLE_FILE_CAPS[name]
            if size > cap:
                raise BundleError(
                    f"remote {name} reports size {size} bytes, exceeds cap {cap} bytes",
                    namespace=namespace,
                    resource=f"{repo_id}@{revision}",
                )


async def _head_check(*, repo_id: str, timeout_s: float) -> str | None:
    """Return the remote commit SHA for ``repo_id``, or ``None`` on any failure."""
    from huggingface_hub import HfApi
    from huggingface_hub.utils import HfHubHTTPError

    def _call() -> str | None:
        try:
            api = HfApi()
            info = api.repo_info(repo_id=repo_id, repo_type="dataset", files_metadata=False)
        except HfHubHTTPError as exc:
            logger.debug("head_check http_error repo=%s exc=%s", repo_id, exc)
            return None
        sha = getattr(info, "sha", None)
        if not isinstance(sha, str) or not _SHA40_RE.match(sha):
            logger.debug("head_check invalid_sha repo=%s sha=%r", repo_id, sha)
            return None
        return sha

    try:
        async with asyncio.timeout(timeout_s):
            return await asyncio.to_thread(_call)
    except TimeoutError:
        logger.debug("head_check timeout repo=%s", repo_id)
        return None


async def _download_snapshot(
    *,
    repo_id: str,
    revision: str,
    cache_base: Path,
    namespace: str,
) -> Path:
    """Download the three bundle files into ``cache_base/<namespace>/<revision>/``."""
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import (
        EntryNotFoundError,
        RepositoryNotFoundError,
        RevisionNotFoundError,
    )

    target_dir = _bundle_dir(cache_base, namespace, revision)
    target_dir.mkdir(parents=True, exist_ok=True)

    # Remote size check BEFORE snapshot_download: rejects a tampered repo
    # where index.faiss has been replaced with a 10 TB file before any
    # bytes hit disk.
    await _remote_size_check(
        repo_id=repo_id,
        revision=revision,
        namespace=namespace,
        timeout_s=_MODEL_INFO_TIMEOUT_S,
    )

    def _call() -> str:
        # Anonymous access (token=False) and strict filename allowlist — the
        # only three files ever land on disk.
        return snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            revision=revision,
            local_dir=str(target_dir),
            allow_patterns=list(BUNDLE_FILENAMES),
            token=False,
        )

    t0 = time.monotonic()
    try:
        async with asyncio.timeout(_SNAPSHOT_TIMEOUT_S):
            downloaded = await asyncio.to_thread(_call)
    except TimeoutError as exc:
        raise BundleError(
            f"Snapshot download exceeded {_SNAPSHOT_TIMEOUT_S}s",
            namespace=namespace,
            resource=repo_id,
        ) from exc
    except RepositoryNotFoundError as exc:
        raise BundleNotFoundError(
            f"No HF repository {repo_id!r}",
            namespace=namespace,
            resource=repo_id,
        ) from exc
    except RevisionNotFoundError as exc:
        raise BundleNotFoundError(
            f"HF revision {revision!r} not found in {repo_id!r}",
            namespace=namespace,
            resource=f"{repo_id}@{revision}",
        ) from exc
    except EntryNotFoundError as exc:
        raise BundleNotFoundError(
            f"Expected file missing from HF repo {repo_id!r}: {exc}",
            namespace=namespace,
            resource=repo_id,
        ) from exc
    except Exception as exc:
        raise BundleError(
            f"snapshot_download failed for {repo_id!r}@{revision}: {exc}",
            namespace=namespace,
            resource=repo_id,
        ) from exc

    elapsed = time.monotonic() - t0
    logger.info(
        "catalog.snapshot_downloaded namespace=%s revision=%s duration_s=%.2f path=%s",
        namespace,
        revision,
        elapsed,
        downloaded,
    )

    # Path confinement: every file must resolve under cache_base.
    cache_base_resolved = cache_base.resolve()
    for name in BUNDLE_FILENAMES:
        path = (target_dir / name).resolve()
        try:
            path.relative_to(cache_base_resolved)
        except ValueError as exc:
            raise BundleError(
                f"Downloaded file {name!r} resolved outside cache dir: {path}",
                namespace=namespace,
                resource=str(path),
            ) from exc
        if not path.is_file():
            raise BundleError(
                f"Required file {name!r} missing from snapshot",
                namespace=namespace,
                resource=repo_id,
            )

    # Reject any extra files.
    extras = [p.name for p in target_dir.iterdir() if p.is_file() and p.name not in BUNDLE_FILENAMES]
    if extras:
        raise BundleError(
            f"Unexpected files in snapshot: {extras}",
            namespace=namespace,
            resource=repo_id,
        )

    return target_dir
