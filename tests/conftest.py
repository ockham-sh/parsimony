"""Shared test fixtures for parsimony test suite.

The new :mod:`parsimony.discover` is stateless — no cache — so the legacy
autouse fixture that cleared the per-process cache between tests is gone.

The :func:`_pin_parsimony_cache_dir` autouse fixture isolates the
:mod:`parsimony.cache` root to a per-test tmp dir so no test (whatever
kernel path it exercises) writes to the user's real cache.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _pin_parsimony_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Pin ``PARSIMONY_CACHE_DIR`` to a per-test tmp dir for every test.

    Returning the path lets a test that wants to inspect the cache root
    request the fixture by name; otherwise the autouse effect is enough.
    """
    cache_root = tmp_path / "parsimony_cache"
    monkeypatch.setenv("PARSIMONY_CACHE_DIR", str(cache_root))
    return cache_root
