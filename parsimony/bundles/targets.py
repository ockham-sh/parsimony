"""HuggingFace Hub bundle target — uploads + retry/backoff + parallel control.

What this owns (only this module):

- :class:`HFBundleTarget` exposes :meth:`publish` which:

  1. Asserts the ``HF_TOKEN`` is non-empty (fast operator-error).
  2. Logs the ``whoami`` user/orgs at INFO before any state-changing call
     (token-scope assertion — fail fast on auth errors).
  3. Runs :func:`shrink_guard` against the published bundle so a transient
     enumerator outage can't atomically replace a 100 k-row bundle with a
     10-row bundle.
  4. Uploads via ``HfApi.upload_folder`` inside :func:`asyncio.to_thread`.
  5. Wraps the upload in a retry policy: 5 attempts, exponential backoff
     (1/2/4/8/16 s with jitter), honoring server-supplied ``Retry-After``
     when the header is present (integer seconds or HTTP-date). Retries
     429/5xx + connection/read timeouts. Auth (401/403) and not-found (404)
     are NOT retried — those are operator config bugs.
  6. Token-scrubs every error message via
     :func:`~parsimony.bundles.safety.scrub_token` before propagation.

Concurrency control: a single :class:`asyncio.Semaphore` instance per
:class:`HFBundleTarget` caps in-flight uploads (default 8, configurable
via ``PARSIMONY_PUBLISH_CONCURRENCY``). Build remains sequential —
parallelism is upload-only.

The semaphore is created lazily on first use inside a running event loop;
the module is safe to import without a loop.
"""

from __future__ import annotations

import asyncio
import email.utils
import logging
import os
import random
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from parsimony.bundles.format import BUNDLE_FILENAMES, hf_repo_id
from parsimony.bundles.safety import (
    format_exc_chain,
    scrub_token,
    shrink_guard,
)

logger = logging.getLogger(__name__)


_RETRY_BASE_DELAY_S: float = 1.0
_RETRY_CAP_DELAY_S: float = 16.0
_RETRY_MAX_ATTEMPTS: int = 5
# Status codes that warrant a retry. 401/403/404 are operator config bugs
# and DO NOT belong in this set.
_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
# Cap on Retry-After to prevent a hostile or buggy server stalling the
# pipeline indefinitely.
_RETRY_AFTER_CAP_S: float = 120.0

_DEFAULT_PUBLISH_CONCURRENCY: int = 8

# Anchored matcher for retryable status codes when only the message text is
# available. Word boundaries prevent "401" matching inside "5040…" or "/v1/500-…".
_STATUS_TEXT_RE: re.Pattern[str] = re.compile(
    r"\b(?:" + "|".join(str(c) for c in sorted(_RETRYABLE_STATUS_CODES)) + r")\b"
)


def _publish_concurrency_from_env() -> int:
    raw = os.environ.get("PARSIMONY_PUBLISH_CONCURRENCY")
    if raw and raw.isdigit():
        n = int(raw)
        if n >= 1:
            return n
    return _DEFAULT_PUBLISH_CONCURRENCY


def _read_token_from_env() -> str | None:
    return os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")


def _is_retryable(exc: BaseException) -> bool:
    """Decide whether *exc* came from a transient HF/HTTP failure.

    Imports are deferred so the read-side store doesn't pay the
    huggingface_hub cost on import.
    """
    try:
        from huggingface_hub.utils import HfHubHTTPError
    except ImportError:
        HfHubHTTPError = None  # type: ignore[assignment,misc]
    try:
        import requests
    except ImportError:
        requests = None  # type: ignore[assignment]

    if HfHubHTTPError is not None and isinstance(exc, HfHubHTTPError):
        response = getattr(exc, "response", None)
        code = getattr(response, "status_code", None) if response is not None else None
        if isinstance(code, int):
            return code in _RETRYABLE_STATUS_CODES
        # No parseable status — fall back to anchored regex on the message.
        return bool(_STATUS_TEXT_RE.search(str(exc)))

    return requests is not None and isinstance(
        exc, requests.ConnectionError | requests.ReadTimeout
    )


def _retry_after_seconds(exc: BaseException) -> float | None:
    """Extract a server-supplied ``Retry-After`` value from *exc*, if any.

    Accepts either RFC 7231 forms: a non-negative integer (seconds) or an
    HTTP-date. Capped at :data:`_RETRY_AFTER_CAP_S`.
    """
    response = getattr(exc, "response", None)
    if response is None:
        return None
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
        if seconds < 0:
            return None
        return min(_RETRY_AFTER_CAP_S, seconds)
    except (TypeError, ValueError):
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    delta = (parsed - datetime.now(UTC)).total_seconds()
    if delta <= 0:
        return 0.0
    return min(_RETRY_AFTER_CAP_S, delta)


def _backoff_delay(attempt: int) -> float:
    """Decorrelated exponential backoff: ``base * 2**(attempt-1)`` + jitter."""
    expo = _RETRY_BASE_DELAY_S * (2 ** (attempt - 1))
    capped = min(_RETRY_CAP_DELAY_S, expo)
    return float(capped * (0.5 + random.random() * 0.5))  # noqa: S311 — jitter, not crypto


class HFBundleTarget:
    """Catalog publish target backed by ``parsimony-dev/<namespace>`` on HF Hub."""

    def __init__(
        self,
        *,
        max_concurrency: int | None = None,
    ) -> None:
        self._max_concurrency = max_concurrency or _publish_concurrency_from_env()
        # Created lazily inside a running loop so import-time has no asyncio dep.
        self._upload_sem: asyncio.Semaphore | None = None

    def _get_sem(self) -> asyncio.Semaphore:
        if self._upload_sem is None:
            self._upload_sem = asyncio.Semaphore(self._max_concurrency)
        return self._upload_sem

    async def publish(
        self,
        bundle_dir: Path,
        *,
        namespace: str,
        allow_shrink: bool = False,
    ) -> str:
        """Atomically replace the published bundle for ``namespace``.

        Returns the HF commit SHA from ``upload_folder``. Raises:

        - :class:`RuntimeError` if no ``HF_TOKEN`` is set, if shrink_guard
          rejects the publish, if the token-scope probe surfaces an auth
          error, or if every retry attempt fails.
        - Other huggingface_hub errors only on non-retryable failures
          (404, 401/403, malformed repo).
        """
        token = _read_token_from_env()
        if not token:
            raise RuntimeError(
                "HF upload requires a token: set HF_TOKEN or HUGGING_FACE_HUB_TOKEN "
                "(the write-scoped token for parsimony-dev)"
            )

        # Refuse extra files in bundle_dir before *any* network call so a
        # misbehaving builder can't ship a junk file under the
        # ``allow_patterns`` filter via a future regression.
        extras = [
            p.name for p in bundle_dir.iterdir() if p.is_file() and p.name not in BUNDLE_FILENAMES
        ]
        if extras:
            raise RuntimeError(
                f"Refusing to upload extra files in bundle dir: {extras}"
            )

        await _assert_token_scope(token)

        # Read the manifest's entry_count to feed shrink_guard. Manifest
        # parse failures here are programmer errors (the build path just
        # wrote it) — let them propagate.
        from parsimony.bundles.format import MANIFEST_FILENAME, BundleManifest

        manifest_text = (bundle_dir / MANIFEST_FILENAME).read_text(encoding="utf-8")
        manifest = BundleManifest.model_validate_json(manifest_text)
        shrink_guard(
            namespace,
            fresh_entry_count=manifest.entry_count,
            allow_shrink=allow_shrink,
        )

        sem = self._get_sem()
        async with sem:
            return await self._upload_with_retry(
                bundle_dir=bundle_dir,
                namespace=namespace,
                token=token,
            )

    async def _upload_with_retry(
        self,
        *,
        bundle_dir: Path,
        namespace: str,
        token: str,
    ) -> str:
        attempts = 0
        last_exc: BaseException | None = None
        while attempts < _RETRY_MAX_ATTEMPTS:
            attempts += 1
            try:
                return await asyncio.to_thread(
                    _upload_blocking,
                    bundle_dir=bundle_dir,
                    namespace=namespace,
                    token=token,
                )
            except Exception as exc:
                last_exc = exc
                if not _is_retryable(exc) or attempts >= _RETRY_MAX_ATTEMPTS:
                    scrubbed = scrub_token(format_exc_chain(exc), token)
                    if attempts >= _RETRY_MAX_ATTEMPTS and _is_retryable(exc):
                        raise RuntimeError(
                            f"HF upload exhausted {_RETRY_MAX_ATTEMPTS} attempts: {scrubbed}"
                        ) from None
                    raise RuntimeError(scrubbed) from None
                delay = _retry_after_seconds(exc)
                if delay is None:
                    delay = _backoff_delay(attempts)
                logger.warning(
                    "hf_bundle.upload.retry namespace=%s attempt=%d/%d delay_s=%.2f cause=%s",
                    namespace,
                    attempts,
                    _RETRY_MAX_ATTEMPTS,
                    delay,
                    type(exc).__name__,
                )
                await asyncio.sleep(delay)
        # Defensive: the loop always either returns or raises above. The
        # ``assert`` keeps mypy happy — last_exc must be set after >=1 attempt.
        assert last_exc is not None
        raise RuntimeError(
            f"HF upload failed after {attempts} attempts: "
            f"{scrub_token(format_exc_chain(last_exc), token)}"
        )


def _upload_blocking(
    *,
    bundle_dir: Path,
    namespace: str,
    token: str,
) -> str:
    """Blocking upload — runs inside :func:`asyncio.to_thread`."""
    from huggingface_hub import HfApi

    repo_id = hf_repo_id(namespace)
    api = HfApi(token=token)
    api.create_repo(
        repo_id=repo_id,
        repo_type="dataset",
        exist_ok=True,
        private=False,
    )
    commit = api.upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(bundle_dir),
        allow_patterns=list(BUNDLE_FILENAMES),
        commit_message=f"publish catalog bundle for {namespace}",
    )
    return getattr(commit, "oid", None) or str(commit)


async def _assert_token_scope(token: str) -> None:
    """Token-scope assertion via ``HfApi.whoami``.

    Logs the resolved user/orgs at INFO so CI logs make it obvious which
    identity is publishing. Raises :class:`RuntimeError` on any
    HfHubHTTPError (auth or otherwise) — wrong-service tokens, blocked
    accounts, and similar config bugs surface here rather than after five
    upload retries. Pure transport errors (network, timeout) are logged at
    WARN and execution continues — the upload's own retry policy will
    surface those.
    """

    def _probe() -> dict[str, Any] | str:
        from huggingface_hub import HfApi
        from huggingface_hub.utils import HfHubHTTPError

        api = HfApi(token=token)
        try:
            return api.whoami(token=token)
        except HfHubHTTPError as exc:
            response = getattr(exc, "response", None)
            code = getattr(response, "status_code", None) if response is not None else None
            return f"AUTH_ERROR:{code if isinstance(code, int) else type(exc).__name__}"
        except Exception as exc:  # pure network etc.
            return f"WARN:{type(exc).__name__}"

    try:
        async with asyncio.timeout(10.0):
            result = await asyncio.to_thread(_probe)
    except TimeoutError:
        logger.warning("hf_bundle.whoami timeout — proceeding to upload retry path")
        return

    if isinstance(result, str):
        if result.startswith("AUTH_ERROR:"):
            code = result.removeprefix("AUTH_ERROR:")
            raise RuntimeError(
                f"HF token rejected by whoami ({code}); refusing to publish. "
                "Check the token scope in your CI secret store."
            )
        logger.warning("hf_bundle.whoami soft_fail signal=%s", result)
        return

    user = result.get("name") or result.get("email") or "unknown"
    orgs = [o.get("name") for o in result.get("orgs", []) if isinstance(o, dict)]
    logger.info("hf_bundle.whoami user=%s orgs=%s", user, sorted(o for o in orgs if o))


__all__ = [
    "HFBundleTarget",
]
