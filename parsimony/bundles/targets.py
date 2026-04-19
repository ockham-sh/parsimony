"""HuggingFace Hub bundle target — uploads + retry/backoff + parallel control.

What this owns:

- :class:`HFBundleTarget` exposes :meth:`publish` which:

  1. Asserts the ``HF_TOKEN`` is non-empty (fast operator-error).
  2. Logs the ``whoami`` user/orgs at INFO before any state-changing call
     (token-scope assertion — fail fast on auth errors).
  3. Runs :func:`shrink_guard` against the published bundle so a transient
     enumerator outage can't atomically replace a 100 k-row bundle with a
     10-row bundle.
  4. Uploads via ``HfApi.upload_folder`` inside :func:`asyncio.to_thread`.
  5. Wraps the upload with :mod:`tenacity` — 5 attempts, exponential
     backoff with jitter, retrying only on transient HTTP + connection
     errors. Auth (401/403) and not-found (404) are NOT retried — those
     are operator config bugs.
  6. Token-scrubs every error message via
     :func:`~parsimony.bundles.safety.scrub_token` before propagation.

Concurrency control: a single :class:`asyncio.Semaphore` instance per
:class:`HFBundleTarget` caps in-flight uploads (default 8, configurable
via ``PARSIMONY_PUBLISH_CONCURRENCY``). Build remains sequential —
parallelism is upload-only.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from parsimony.bundles.format import BUNDLE_FILENAMES, hf_repo_id
from parsimony.bundles.safety import (
    format_exc_chain,
    scrub_token,
    shrink_guard,
)

logger = logging.getLogger(__name__)

_RETRY_MAX_ATTEMPTS: int = 5
_RETRY_BASE_DELAY_S: float = 1.0
_RETRY_CAP_DELAY_S: float = 16.0

# Status codes that warrant a retry. 401/403/404 are operator config bugs.
_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

_DEFAULT_PUBLISH_CONCURRENCY: int = 8


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
    """True iff *exc* is a transient HF/HTTP failure worth retrying."""
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
        return isinstance(code, int) and code in _RETRYABLE_STATUS_CODES

    return requests is not None and isinstance(
        exc, requests.ConnectionError | requests.ReadTimeout
    )


def _operational_upload_exc_types() -> tuple[type[BaseException], ...]:
    """Operational exception classes from the upload path.

    These are the errors whose messages need token-scrubbing and
    RuntimeError wrapping (HF auth/http, network, filesystem). Programmer
    bugs — AttributeError, KeyError, TypeError, ImportError, … — MUST
    propagate untouched so misconfigured call sites crash loudly instead
    of being silently reported as a failed bundle.
    """
    types: list[type[BaseException]] = [OSError]
    try:
        from huggingface_hub.utils import HfHubHTTPError
        types.append(HfHubHTTPError)
    except ImportError:
        pass
    try:
        import requests
        types.append(requests.RequestException)
    except ImportError:
        pass
    return tuple(types)


class HFBundleTarget:
    """Catalog publish target backed by ``parsimony-dev/<namespace>`` on HF Hub."""

    def __init__(
        self,
        *,
        max_concurrency: int | None = None,
    ) -> None:
        self._max_concurrency = max_concurrency or _publish_concurrency_from_env()
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

        Returns the HF commit SHA from ``upload_folder``. Raises
        :class:`RuntimeError` on token / shrink-guard / retry-exhaustion
        failures; other huggingface_hub errors on non-retryable failures
        (404, 401/403, malformed repo).
        """
        token = _read_token_from_env()
        if not token:
            raise RuntimeError(
                "HF upload requires a token: set HF_TOKEN or HUGGING_FACE_HUB_TOKEN "
                "(the write-scoped token for parsimony-dev)"
            )

        extras = [
            p.name for p in bundle_dir.iterdir() if p.is_file() and p.name not in BUNDLE_FILENAMES
        ]
        if extras:
            raise RuntimeError(
                f"Refusing to upload extra files in bundle dir: {extras}"
            )

        await _assert_token_scope(token)

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
        retryer = AsyncRetrying(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(_RETRY_MAX_ATTEMPTS),
            wait=wait_exponential_jitter(
                initial=_RETRY_BASE_DELAY_S, max=_RETRY_CAP_DELAY_S
            ),
            reraise=False,
        )
        try:
            async for attempt in retryer:
                with attempt:
                    attempt_no = attempt.retry_state.attempt_number
                    if attempt_no > 1:
                        logger.warning(
                            "hf_bundle.upload.retry namespace=%s attempt=%d/%d",
                            namespace,
                            attempt_no,
                            _RETRY_MAX_ATTEMPTS,
                        )
                    return await asyncio.to_thread(
                        _upload_blocking,
                        bundle_dir=bundle_dir,
                        namespace=namespace,
                        token=token,
                    )
        except RetryError as exc:
            cause = exc.last_attempt.exception() if exc.last_attempt else exc
            scrubbed = scrub_token(format_exc_chain(cause or exc), token)
            raise RuntimeError(
                f"HF upload exhausted {_RETRY_MAX_ATTEMPTS} attempts: {scrubbed}"
            ) from None
        except _operational_upload_exc_types() as exc:
            # Non-retryable operational failure (HF auth/http, network, FS).
            # Programmer errors (AttributeError, KeyError, …) are NOT in this
            # tuple and propagate to the fan-out, which also excludes them
            # from its operational allowlist.
            scrubbed = scrub_token(format_exc_chain(exc), token)
            raise RuntimeError(scrubbed) from None
        # Unreachable — AsyncRetrying either returns from the with-block or raises.
        raise RuntimeError(f"HF upload failed for {namespace}: no attempts ran")


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
