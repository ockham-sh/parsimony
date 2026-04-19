"""Integration tests for build_connectors_from_env after discovery refactor.

The Phase 1.3 contract: the connector set returned by
``build_connectors_from_env`` must remain byte-identical to the pre-refactor
output while the registry delegates to ``parsimony.discovery``.
"""

from __future__ import annotations

from importlib.metadata import EntryPoint
from types import ModuleType
from typing import Any

import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector


class _TP(BaseModel):
    x: str = "y"


def _toy(name: str, **kwargs: Any):
    async def _fn(params: _TP, *, api_key: str) -> dict[str, Any]:
        return {"ok": True}

    _fn.__doc__ = "Fetch a toy observation with plenty of description length."
    _fn.__name__ = name
    return connector(**kwargs)(_fn)


def _make_module(
    path: str,
    connectors: Connectors,
    env_vars: dict[str, str] | None = None,
) -> ModuleType:
    mod = ModuleType(path)
    mod.CONNECTORS = connectors  # type: ignore[attr-defined]
    if env_vars is not None:
        mod.ENV_VARS = env_vars  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Baseline — empty discovery when no plugins are installed
# ---------------------------------------------------------------------------


def test_baseline_empty_when_no_plugins_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kernel-only install (no connector packages) yields an empty Connectors set.

    Post-Phase-3, the kernel ships no connectors of its own. Any test that
    needs connectors in the surface must install an entry-point plugin or
    monkeypatch one in.
    """
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery import build_connectors_from_env

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    c = build_connectors_from_env(env={})
    assert list(c.names()) == []


# ---------------------------------------------------------------------------
# Discovered providers are composed
# ---------------------------------------------------------------------------


def test_discovered_provider_appears_in_build_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin discovered via entry points should appear in build_connectors_from_env output.

    Monkeypatches entry-point discovery to return only the fake plugin, so the
    assertion set is deterministic (bundled providers are exercised in
    :func:`test_baseline_provider_set_unchanged` and
    :func:`test_connector_names_snapshot_matches_pre_refactor`).
    """
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery import build_connectors_from_env

    fake_module = _make_module(
        "pkg_fake_provider",
        Connectors([_toy("fake_fetch")]),
        env_vars={"api_key": "FAKE_API_KEY"},
    )

    ep = EntryPoint(name="fake", value="pkg_fake_provider", group="parsimony.providers")

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: fake_module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-fake", "0.1.0"))
    discovery._clear_cache()

    env = {"FAKE_API_KEY": "present"}
    c = build_connectors_from_env(env=env)
    names = set(c.names())

    assert "fake_fetch" in names


def test_discovered_provider_silently_skipped_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing env var for a discovered plugin silently skips it — no raise."""
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery import build_connectors_from_env

    fake_module = _make_module(
        "pkg_silent_skip",
        Connectors([_toy("silent_fetch")]),
        env_vars={"api_key": "SILENT_API_KEY"},
    )

    ep = EntryPoint(name="silent", value="pkg_silent_skip", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: fake_module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-silent", "0.1.0"))
    discovery._clear_cache()

    env = {"FRED_API_KEY": "test", "FMP_API_KEY": "test"}
    # SILENT_API_KEY absent: provider is skipped, no exception
    c = build_connectors_from_env(env=env)
    assert "silent_fetch" not in c.names()


def test_discovered_single_entry_no_duplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry-point pointing at a synthetic module composes cleanly without duplicates.

    Uses ``tests.fixtures.synth_provider`` because the kernel no longer
    contains provider-specific modules — every connector lives in its own
    ``parsimony_<name>`` package (freeze §6).
    """
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery import build_connectors_from_env

    ep = EntryPoint(
        name="synth",
        value="tests.fixtures.synth_provider",
        group="parsimony.providers",
    )

    import tests.fixtures.synth_provider as synth_module

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: synth_module)
    monkeypatch.setattr(
        discovery,
        "_distribution_for_entry_point",
        lambda _ep: ("parsimony-core", "0.1.0a1"),
    )
    discovery._clear_cache()

    c = build_connectors_from_env(env={})
    names = c.names()

    # Both connectors from synth_provider are present exactly once.
    assert names.count("synth_fetch") == 1
    assert names.count("enumerate_synth") == 1
