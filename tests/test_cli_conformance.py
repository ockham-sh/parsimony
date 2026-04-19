"""Tests for ``parsimony conformance verify`` CLI subcommand."""

from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

import pytest

from parsimony.cli.conformance import run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeMeta(dict[str, Any]):
    """Minimal PackageMetadata stand-in: supports ``"Name" in meta``, ``meta["Name"]``."""

    def __init__(self, name: str) -> None:
        super().__init__()
        self["Name"] = name

    def get_all(self, key: str) -> list[str]:
        return []


def _fake_distribution(
    *,
    name: str,
    version: str,
    entry_points: list[SimpleNamespace],
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=_FakeMeta(name),
        version=version,
        entry_points=entry_points,
    )


# ---------------------------------------------------------------------------
# Not-installed path
# ---------------------------------------------------------------------------


def test_not_installed_emits_json_and_exits_2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.metadata

    def _raise(_name: str) -> Any:
        raise importlib.metadata.PackageNotFoundError(_name)

    monkeypatch.setattr(
        "parsimony.cli.conformance.importlib.metadata.distribution", _raise
    )

    buf = io.StringIO()
    code = run(distribution_name="parsimony-missing", stream=buf)

    assert code == 2
    report = json.loads(buf.getvalue())
    assert report["distribution"] == "parsimony-missing"
    assert report["passed"] is False
    assert report["error"] == "not_installed"


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_passes_when_plugin_conforms(monkeypatch: pytest.MonkeyPatch) -> None:
    """A real conforming plugin (the synth fixture) exits 0."""
    import tests.fixtures.synth_provider as synth

    ep = SimpleNamespace(
        name="synth",
        value="tests.fixtures.synth_provider",
        group="parsimony.providers",
        load=lambda: synth,
    )
    dist = _fake_distribution(
        name="parsimony-synth",
        version="0.1.0",
        entry_points=[ep],
    )

    monkeypatch.setattr(
        "parsimony.cli.conformance.importlib.metadata.distribution",
        lambda _name: dist,
    )

    buf = io.StringIO()
    code = run(distribution_name="parsimony-synth", stream=buf)

    assert code == 0
    report = json.loads(buf.getvalue())
    assert report["passed"] is True
    assert report["distribution"] == "parsimony-synth"
    assert len(report["entry_points"]) == 1
    assert report["entry_points"][0]["status"] == "pass"


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


def test_fails_when_plugin_import_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom() -> Any:
        raise RuntimeError("plugin init failed")

    ep = SimpleNamespace(
        name="broken",
        value="parsimony_broken",
        group="parsimony.providers",
        load=_boom,
    )
    dist = _fake_distribution(
        name="parsimony-broken",
        version="0.0.1",
        entry_points=[ep],
    )

    monkeypatch.setattr(
        "parsimony.cli.conformance.importlib.metadata.distribution",
        lambda _name: dist,
    )

    buf = io.StringIO()
    code = run(distribution_name="parsimony-broken", stream=buf)

    assert code == 1
    report = json.loads(buf.getvalue())
    assert report["passed"] is False
    entry = report["entry_points"][0]
    assert entry["status"] == "fail"
    assert "import failed" in entry["reason"]


def test_fails_when_plugin_violates_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin whose module has no CONNECTORS export fails conformance."""
    from types import ModuleType

    empty = ModuleType("parsimony_empty")
    # Intentionally no CONNECTORS attribute → ConformanceError from assert_plugin_valid.

    ep = SimpleNamespace(
        name="empty",
        value="parsimony_empty",
        group="parsimony.providers",
        load=lambda: empty,
    )
    dist = _fake_distribution(
        name="parsimony-empty",
        version="0.0.1",
        entry_points=[ep],
    )

    monkeypatch.setattr(
        "parsimony.cli.conformance.importlib.metadata.distribution",
        lambda _name: dist,
    )

    buf = io.StringIO()
    code = run(distribution_name="parsimony-empty", stream=buf)

    assert code == 1
    report = json.loads(buf.getvalue())
    assert report["passed"] is False
    assert report["entry_points"][0]["status"] == "fail"


def test_no_entry_points_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A distribution with no parsimony.providers entry points cannot pass."""
    dist = _fake_distribution(
        name="parsimony-silent",
        version="0.0.1",
        entry_points=[],
    )

    monkeypatch.setattr(
        "parsimony.cli.conformance.importlib.metadata.distribution",
        lambda _name: dist,
    )

    buf = io.StringIO()
    code = run(distribution_name="parsimony-silent", stream=buf)

    # No entry points → not "pass" (nothing to verify).
    assert code == 1
    report = json.loads(buf.getvalue())
    assert report["passed"] is False
    assert report["entry_points"] == []


