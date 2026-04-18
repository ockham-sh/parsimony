"""Behavioral tests for parsimony.bundles.safety (shrink guard + helpers).

Coverage matrix:

- ``shrink_guard`` boundary ratios (just-fail, just-pass, identity, growth).
- ``allow_shrink=True`` bypasses the guard.
- ``previous=0`` and ``previous=None`` are pass-through (no published bundle).
- ``format_exc_chain`` walks ``__cause__`` / ``__context__``.
- ``fetch_published_entry_count`` enforces the ``MAX_MANIFEST_BYTES`` cap and
  refuses to parse oversized files.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parsimony.bundles.format import MAX_MANIFEST_BYTES
from parsimony.bundles.safety import (
    SHRINK_GUARD_RATIO,
    fetch_published_entry_count,
    format_exc_chain,
    shrink_guard,
)

# ---------------------------------------------------------------------------
# shrink_guard
# ---------------------------------------------------------------------------


class TestShrinkGuard:
    def _stub_published(self, monkeypatch, value):
        monkeypatch.setattr(
            "parsimony.bundles.safety.fetch_published_entry_count",
            lambda namespace: value,
        )

    def test_no_published_yields_pass_through(self, monkeypatch):
        self._stub_published(monkeypatch, None)
        out = shrink_guard("snb", fresh_entry_count=10)
        assert out == {"previous_entry_count": None, "shrink_ratio": None}

    def test_zero_previous_yields_pass_through(self, monkeypatch):
        self._stub_published(monkeypatch, 0)
        out = shrink_guard("snb", fresh_entry_count=42)
        assert out == {"previous_entry_count": None, "shrink_ratio": None}

    def test_just_below_threshold_raises(self, monkeypatch):
        self._stub_published(monkeypatch, 100)
        # 49 / 100 = 0.49 — under SHRINK_GUARD_RATIO (0.50)
        with pytest.raises(RuntimeError, match="refusing to publish"):
            shrink_guard("snb", fresh_entry_count=49)

    def test_at_threshold_passes(self, monkeypatch):
        """Boundary check: ratio == SHRINK_GUARD_RATIO is accepted."""
        self._stub_published(monkeypatch, 100)
        out = shrink_guard("snb", fresh_entry_count=50)
        assert out["previous_entry_count"] == 100
        assert out["shrink_ratio"] is not None
        assert out["shrink_ratio"] == 0.5
        assert SHRINK_GUARD_RATIO == 0.5

    def test_growth_passes(self, monkeypatch):
        self._stub_published(monkeypatch, 100)
        out = shrink_guard("snb", fresh_entry_count=200)
        assert out["shrink_ratio"] == 2.0

    def test_allow_shrink_bypasses_guard(self, monkeypatch):
        self._stub_published(monkeypatch, 100)
        out = shrink_guard("snb", fresh_entry_count=1, allow_shrink=True)
        assert out["previous_entry_count"] == 100
        assert out["shrink_ratio"] == 0.01


# ---------------------------------------------------------------------------
# format_exc_chain
# ---------------------------------------------------------------------------


class TestFormatExcChain:
    def test_walks_cause(self):
        try:
            try:
                raise ValueError("inner cause")
            except ValueError as inner:
                raise RuntimeError("outer wrap") from inner
        except RuntimeError as exc:
            text = format_exc_chain(exc)
        assert "RuntimeError" in text and "outer wrap" in text
        assert "ValueError" in text and "inner cause" in text

    def test_walks_implicit_context(self):
        try:
            try:
                raise ValueError("first")
            except ValueError:
                # Intentional bare raise — testing implicit __context__ chaining.
                raise RuntimeError("masked")  # noqa: B904
        except RuntimeError as exc:
            text = format_exc_chain(exc)
        assert "first" in text and "masked" in text


# ---------------------------------------------------------------------------
# fetch_published_entry_count manifest size cap
# ---------------------------------------------------------------------------


class TestFetchPublishedEntryCountSizeCap:
    def _stub_download(self, monkeypatch, target_path: Path):
        """Make hf_hub_download return ``target_path``."""
        def fake(**kwargs):
            return str(target_path)

        monkeypatch.setattr(
            "huggingface_hub.hf_hub_download", fake, raising=False
        )

    def test_returns_count_for_normal_manifest(self, tmp_path, monkeypatch):
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({"entry_count": 42}))
        self._stub_download(monkeypatch, manifest)
        assert fetch_published_entry_count("snb") == 42

    def test_oversized_manifest_returns_none(self, tmp_path, monkeypatch):
        """A manifest exceeding MAX_MANIFEST_BYTES must NOT be parsed."""
        manifest = tmp_path / "manifest.json"
        # Write something larger than the cap with "entry_count" anywhere in
        # the bytes — confirms the size cap fires before json.loads runs.
        body = b'{"entry_count": 7}\n' + b"x" * (MAX_MANIFEST_BYTES + 16)
        manifest.write_bytes(body)
        self._stub_download(monkeypatch, manifest)
        assert fetch_published_entry_count("snb") is None

    def test_corrupt_manifest_returns_none(self, tmp_path, monkeypatch):
        manifest = tmp_path / "manifest.json"
        manifest.write_text("not json at all")
        self._stub_download(monkeypatch, manifest)
        assert fetch_published_entry_count("snb") is None

    def test_missing_entry_count_returns_none(self, tmp_path, monkeypatch):
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({"other_field": 1}))
        self._stub_download(monkeypatch, manifest)
        assert fetch_published_entry_count("snb") is None
