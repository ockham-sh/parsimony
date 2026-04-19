"""Behavioral tests for the HF Hub bundle target (retry / auth / token hygiene).

The only network boundary stubbed is :func:`parsimony.bundles.targets._upload_blocking`
plus the ``huggingface_hub`` whoami probe and the anonymous shrink-guard
manifest fetch. ``_is_retryable`` runs unmocked so regressions in status
classification, scrub hygiene, or semaphore semantics are caught here.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from parsimony.bundles import targets as targets_module
from parsimony.bundles.format import (
    ENTRIES_FILENAME,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
)
from parsimony.bundles.targets import HFBundleTarget, _is_retryable

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_FAKE_TOKEN = "hf_fakefakefakefakefakefakefake12345"


def _write_bundle_dir(root: Path, *, namespace: str, entry_count: int = 100) -> Path:
    """Materialize a minimal three-file bundle dir for the target to operate on."""
    bundle = root / namespace
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / ENTRIES_FILENAME).write_bytes(b"\x00" * 16)
    (bundle / INDEX_FILENAME).write_bytes(b"\x00" * 16)
    manifest = {
        "namespace": namespace,
        "built_at": "2026-04-18T00:00:00+00:00",
        "entry_count": entry_count,
        "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
        "embedding_model_revision": "0" * 40,
        "embedding_dim": 384,
        "faiss_hnsw_ef_search": 64,
        "entries_sha256": "0" * 64,
        "index_sha256": "0" * 64,
        "builder_git_sha": None,
    }
    (bundle / MANIFEST_FILENAME).write_text(json.dumps(manifest))
    return bundle


@pytest.fixture
def fake_token(monkeypatch):
    monkeypatch.setenv("HF_TOKEN", _FAKE_TOKEN)


@pytest.fixture
def stubbed_whoami(monkeypatch):
    """Make the whoami probe succeed with a fixed identity."""

    async def fake(token: str) -> None:
        return None

    monkeypatch.setattr(targets_module, "_assert_token_scope", fake)


@pytest.fixture
def disabled_shrink_guard(monkeypatch):
    """Pretend nothing is published yet so shrink_guard is a pass-through."""
    monkeypatch.setattr(
        "parsimony.bundles.safety.fetch_published_entry_count",
        lambda namespace: None,
    )
    monkeypatch.setattr(
        targets_module,
        "shrink_guard",
        lambda namespace, *, fresh_entry_count, allow_shrink=False: {
            "previous_entry_count": None,
            "shrink_ratio": None,
        },
    )


# ---------------------------------------------------------------------------
# Helpers to build HfHubHTTPError-shaped exceptions without network
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, *, status_code: int | None = None, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.headers = headers or {}
        self.request = None  # huggingface_hub's HfHubHTTPError pulls this off

    def json(self):  # huggingface_hub may try to parse a JSON body
        return {}


def _make_hf_hub_http_error(
    *,
    status_code: int | None = None,
    headers: dict[str, str] | None = None,
    message: str = "boom",
):
    from huggingface_hub.utils import HfHubHTTPError

    response = _FakeResponse(status_code=status_code, headers=headers)
    exc = HfHubHTTPError(message, response=response)  # type: ignore[arg-type]
    return exc


# ---------------------------------------------------------------------------
# _is_retryable
# ---------------------------------------------------------------------------


class TestIsRetryable:
    @pytest.mark.parametrize("code", [429, 500, 502, 503, 504])
    def test_retryable_status_codes(self, code):
        exc = _make_hf_hub_http_error(status_code=code)
        assert _is_retryable(exc) is True

    @pytest.mark.parametrize("code", [400, 401, 403, 404, 422])
    def test_non_retryable_status_codes(self, code):
        exc = _make_hf_hub_http_error(status_code=code)
        assert _is_retryable(exc) is False

    def test_status_none_is_not_retryable(self):
        """Without a parseable status code, don't retry — message-regex fallback is noise."""
        exc = _make_hf_hub_http_error(status_code=None, message="upstream returned 502 bad gateway")
        assert _is_retryable(exc) is False

    def test_unrelated_exception_not_retryable(self):
        assert _is_retryable(ValueError("nothing to see here")) is False


# ---------------------------------------------------------------------------
# HFBundleTarget.publish — happy path + retry / scrub / semaphore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPublishHappyPath:
    async def test_returns_commit_sha_from_upload(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        bundle = _write_bundle_dir(tmp_path, namespace="snb")
        observed: dict[str, object] = {}

        def fake_upload(*, bundle_dir, namespace, token):
            observed["bundle_dir"] = bundle_dir
            observed["namespace"] = namespace
            observed["token"] = token
            return "deadbeef" * 5

        monkeypatch.setattr(targets_module, "_upload_blocking", fake_upload)
        target = HFBundleTarget()
        sha = await target.publish(bundle, namespace="snb")

        assert sha == "deadbeef" * 5
        assert observed["namespace"] == "snb"
        assert observed["token"] == _FAKE_TOKEN


@pytest.mark.asyncio
class TestPublishRetryPolicy:
    async def test_429_retries_then_succeeds(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        bundle = _write_bundle_dir(tmp_path, namespace="snb")

        async def fake_sleep(delay):
            return None

        monkeypatch.setattr(targets_module.asyncio, "sleep", fake_sleep)

        attempts = {"n": 0}

        def fake_upload(*, bundle_dir, namespace, token):
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise _make_hf_hub_http_error(status_code=429, headers={"Retry-After": "3"})
            return "f" * 40

        monkeypatch.setattr(targets_module, "_upload_blocking", fake_upload)
        target = HFBundleTarget()
        sha = await target.publish(bundle, namespace="snb")
        assert sha == "f" * 40
        assert attempts["n"] == 2

    async def test_401_is_not_retried(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        bundle = _write_bundle_dir(tmp_path, namespace="snb")
        attempts = {"n": 0}

        def fake_upload(*, bundle_dir, namespace, token):
            attempts["n"] += 1
            raise _make_hf_hub_http_error(status_code=401, message="bad token")

        monkeypatch.setattr(targets_module, "_upload_blocking", fake_upload)
        target = HFBundleTarget()
        with pytest.raises(RuntimeError):
            await target.publish(bundle, namespace="snb")
        assert attempts["n"] == 1

    async def test_403_is_not_retried(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        bundle = _write_bundle_dir(tmp_path, namespace="snb")
        attempts = {"n": 0}

        def fake_upload(*, bundle_dir, namespace, token):
            attempts["n"] += 1
            raise _make_hf_hub_http_error(status_code=403, message="forbidden")

        monkeypatch.setattr(targets_module, "_upload_blocking", fake_upload)
        target = HFBundleTarget()
        with pytest.raises(RuntimeError):
            await target.publish(bundle, namespace="snb")
        assert attempts["n"] == 1

    async def test_exhausted_retries_scrubs_token(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        bundle = _write_bundle_dir(tmp_path, namespace="snb")

        async def fake_sleep(delay):
            return None

        monkeypatch.setattr(targets_module.asyncio, "sleep", fake_sleep)

        def fake_upload(*, bundle_dir, namespace, token):
            raise _make_hf_hub_http_error(
                status_code=503,
                message=f"upstream gateway barfed; token={_FAKE_TOKEN} echoed",
            )

        monkeypatch.setattr(targets_module, "_upload_blocking", fake_upload)
        target = HFBundleTarget()
        with pytest.raises(RuntimeError) as excinfo:
            await target.publish(bundle, namespace="snb")
        assert "exhausted" in str(excinfo.value)
        assert _FAKE_TOKEN not in str(excinfo.value)


@pytest.mark.asyncio
class TestPublishConcurrencyCap:
    async def test_semaphore_caps_inflight_uploads(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        """Replace _upload_blocking with an async coroutine via a stub
        ``asyncio.to_thread`` so we observe the semaphore in a single event
        loop without bridging across threads.
        """
        bundle_a = _write_bundle_dir(tmp_path, namespace="ns_a")
        bundle_b = _write_bundle_dir(tmp_path, namespace="ns_b")
        bundle_c = _write_bundle_dir(tmp_path, namespace="ns_c")

        in_flight = 0
        max_seen = 0

        async def fake_to_thread(fn, *args, **kwargs):
            nonlocal in_flight, max_seen
            in_flight += 1
            max_seen = max(max_seen, in_flight)
            try:
                # Yield long enough for sibling tasks to compete for the sem.
                await asyncio.sleep(0.05)
                return "a" * 40
            finally:
                in_flight -= 1

        monkeypatch.setattr(targets_module.asyncio, "to_thread", fake_to_thread)

        target = HFBundleTarget(max_concurrency=2)
        await asyncio.gather(
            target.publish(bundle_a, namespace="ns_a"),
            target.publish(bundle_b, namespace="ns_b"),
            target.publish(bundle_c, namespace="ns_c"),
        )
        assert max_seen == 2


@pytest.mark.asyncio
class TestPublishEdgeCases:
    async def test_missing_token_raises_runtime_error(
        self, monkeypatch, tmp_path, stubbed_whoami, disabled_shrink_guard
    ):
        monkeypatch.delenv("HF_TOKEN", raising=False)
        monkeypatch.delenv("HUGGING_FACE_HUB_TOKEN", raising=False)
        bundle = _write_bundle_dir(tmp_path, namespace="snb")
        target = HFBundleTarget()
        with pytest.raises(RuntimeError, match="HF_TOKEN"):
            await target.publish(bundle, namespace="snb")

    async def test_extra_files_in_bundle_dir_are_refused(
        self, monkeypatch, tmp_path, fake_token, stubbed_whoami, disabled_shrink_guard
    ):
        bundle = _write_bundle_dir(tmp_path, namespace="snb")
        (bundle / "junk.txt").write_text("oops")
        target = HFBundleTarget()
        with pytest.raises(RuntimeError, match="extra files"):
            await target.publish(bundle, namespace="snb")
