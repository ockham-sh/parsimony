"""Client store code must never touch HF token env vars.

Tests assert the runtime *property* (no token leaves the client store, every
HF download passes ``token=False``) by capturing kwargs at the
``huggingface_hub.snapshot_download`` boundary — not by AST-grepping the
source. Refactors that change *how* the constraint is enforced (e.g.,
``HfApi(token=False)`` instead of ``snapshot_download(token=False)``) must
remain detectable by these tests.
"""

from __future__ import annotations

import re

import pytest

_HF_TOKEN_SENTINEL = "hf_TESTSENTINEL_should_never_be_passed_to_hub"


@pytest.mark.asyncio
async def test_snapshot_download_receives_token_false_and_no_sentinel(monkeypatch, tmp_path):
    """Run a try_load_remote and assert the captured snapshot_download kwargs:

    1. ``token`` is the literal ``False`` (anonymous access)
    2. The HF_TOKEN sentinel never appears in any captured kwarg value
    """
    from parsimony.bundles.errors import BundleIntegrityError, BundleNotFoundError
    from parsimony.stores.hf_bundle import HFBundleCatalogStore
    from tests.bundles_helpers import FakeEmbeddingProvider

    monkeypatch.setenv("HF_TOKEN", _HF_TOKEN_SENTINEL)
    monkeypatch.setenv("HUGGINGFACE_HUB_TOKEN", _HF_TOKEN_SENTINEL)

    captured_calls: list[dict] = []

    def fake_snapshot_download(**kwargs):
        captured_calls.append(kwargs)
        # Any exception is fine — we only assert on captured kwargs.
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr("huggingface_hub.snapshot_download", fake_snapshot_download)

    store = HFBundleCatalogStore(
        embeddings=FakeEmbeddingProvider(),
        cache_dir=tmp_path / "cache",
        pin="1" * 40,
    )

    # Any exception is fine here — we only care that snapshot_download was
    # invoked with the right kwargs. The precise wrapping varies.
    with pytest.raises((RuntimeError, BundleIntegrityError, BundleNotFoundError)):
        await store.try_load_remote("nonexistent_namespace")

    assert captured_calls, "snapshot_download must have been invoked"
    for call in captured_calls:
        assert call.get("token") is False, (
            f"snapshot_download must receive token=False (got {call.get('token')!r}); "
            "anonymous-only access is the client-store contract"
        )
        for k, v in call.items():
            if isinstance(v, str):
                assert _HF_TOKEN_SENTINEL not in v, (
                    f"HF_TOKEN sentinel leaked into snapshot_download kwarg {k!r}={v!r}"
                )


def test_redactor_masks_bearer_token_and_hf_prefix():
    from parsimony.bundles.safety import scrub_token

    raw = (
        "POST https://api.example.com with Authorization: Bearer hf_abc123def4567890ZZZZ "
        "and x-api-key: secret123456 failed"
    )
    redacted = scrub_token(raw, "")
    # Every token-shape marker must be gone.
    assert "hf_abc123def4567890ZZZZ" not in redacted
    assert not re.search(r"Bearer\s+\S", redacted, re.IGNORECASE)
    assert not re.search(r"Authorization:\s*\S", redacted, re.IGNORECASE)


def test_scrub_token_redacts_after_format_exc_chain():
    """End-to-end: a token surfaced via the cause chain must be redacted."""
    from parsimony.bundles.safety import format_exc_chain, scrub_token

    try:
        try:
            raise RuntimeError("inner with Bearer hf_secret1234567890ABCD xxx")
        except RuntimeError as inner:
            raise ValueError("outer wrap") from inner
    except ValueError as exc:
        text = format_exc_chain(exc)
        red = scrub_token(text, "")
        assert "hf_secret1234567890ABCD" not in red
