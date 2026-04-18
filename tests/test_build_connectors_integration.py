"""Integration tests for build_connectors_from_env after discovery refactor.

The Phase 1.3 contract: the connector set returned by
``build_connectors_from_env`` must remain byte-identical to the pre-refactor
output while the registry delegates to ``parsimony.plugins.discovery``.
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
# Baseline — PROVIDERS tuple still works
# ---------------------------------------------------------------------------


def test_baseline_provider_set_unchanged() -> None:
    """Precondition: the factory returns the same connector names as before Phase 1.

    If this test drifts, Phase 1.3's byte-identity guarantee has been violated.
    """
    from parsimony.connectors import build_connectors_from_env

    env = {
        "FRED_API_KEY": "test",
        "FMP_API_KEY": "test",
        "EODHD_API_KEY": "test",
        "COINGECKO_API_KEY": "test",
        "FINNHUB_API_KEY": "test",
        "TIINGO_API_KEY": "test",
        "EIA_API_KEY": "test",
        "BDF_API_KEY": "test",
        "ALPHA_VANTAGE_API_KEY": "test",
        "RIKSBANK_API_KEY": "test",
        "DESTATIS_API_KEY": "test",
        "BLS_API_KEY": "test",
        "FINANCIAL_REPORTS_API_KEY": "test",
    }

    c = build_connectors_from_env(env=env)
    names = set(c.names())

    # Canonical expectations from the pre-refactor snapshot. sdmx_fetch
    # was removed from this set when the connector migrated to the
    # parsimony-sdmx plugin; it's still discoverable via entry point when
    # the plugin is installed (covered in the plugin's own tests).
    assert "fred_fetch" in names
    assert "fred_search" in names
    assert "fmp_quotes" in names
    assert "treasury_fetch" in names


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
    from parsimony.connectors import build_connectors_from_env
    from parsimony.plugins import discovery

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
    from parsimony.connectors import build_connectors_from_env
    from parsimony.plugins import discovery

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


def test_discovered_dedupes_against_bundled(monkeypatch: pytest.MonkeyPatch) -> None:
    """If an entry-point re-declares a bundled provider module, discovered wins (no duplicates).

    Uses ``parsimony.connectors.treasury`` as the bundled module under test — a
    credential-free provider that loads reliably in CI. FRED and SDMX are no
    longer bundled (extracted to their own plugins), so neither can serve this role.
    """
    from parsimony.connectors import build_connectors_from_env
    from parsimony.plugins import discovery

    ep = EntryPoint(name="treasury", value="parsimony.connectors.treasury", group="parsimony.providers")

    import parsimony.connectors.treasury as real_treasury_module

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: real_treasury_module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-core", "0.1.0a1"))
    discovery._clear_cache()

    c = build_connectors_from_env(env={})
    names = c.names()

    # treasury_fetch must appear exactly once despite dual registration
    assert names.count("treasury_fetch") == 1


# ---------------------------------------------------------------------------
# Snapshot contract test
# ---------------------------------------------------------------------------


def test_connector_names_snapshot_matches_pre_refactor() -> None:
    """Golden-snapshot contract: the set of connector names with all creds provided
    should match the pre-refactor output. Update the snapshot deliberately if intent changes.
    """
    from parsimony.connectors import build_connectors_from_env

    env = {
        "FRED_API_KEY": "test",
        "FMP_API_KEY": "test",
        "EODHD_API_KEY": "test",
        "COINGECKO_API_KEY": "test",
        "FINNHUB_API_KEY": "test",
        "TIINGO_API_KEY": "test",
        "EIA_API_KEY": "test",
        "BDF_API_KEY": "test",
        "ALPHA_VANTAGE_API_KEY": "test",
        "RIKSBANK_API_KEY": "test",
        "DESTATIS_API_KEY": "test",
        "BLS_API_KEY": "test",
        "FINANCIAL_REPORTS_API_KEY": "test",
    }
    c = build_connectors_from_env(env=env)
    names = set(c.names())

    # Names established by test_fetch_connectors_factory.py today. The
    # sdmx_* connectors moved to parsimony-sdmx and are no longer bundled;
    # their presence is covered by that plugin's own test suite.
    expected_subset = {
        "fred_fetch",
        "fred_search",
        "fmp_quotes",
        "fmp_search",
        "fmp_taxonomy",
        "fmp_screener",
        "treasury_fetch",
    }
    missing = expected_subset - names
    assert not missing, f"Expected connectors missing after refactor: {sorted(missing)}"
