"""Tests for ``parsimony list-plugins`` CLI subcommand."""

from __future__ import annotations

import json
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
        return {}

    _fn.__doc__ = "Fetch a toy observation with a plenty long description."
    _fn.__name__ = name
    return connector(**kwargs)(_fn)


def _make_module(path: str, **attrs: Any) -> ModuleType:
    mod = ModuleType(path)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


# ---------------------------------------------------------------------------
# run_list_plugins — programmatic entry point
# ---------------------------------------------------------------------------


def test_list_plugins_json_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli.list_plugins import run
    from parsimony.plugins import discovery

    mod = _make_module(
        "pkg_foo_cli",
        CONNECTORS=Connectors([_toy("foo_fetch")]),
        ENV_VARS={"api_key": "FOO_API_KEY"},
        PROVIDER_METADATA={"homepage": "https://example.com"},
    )
    ep = EntryPoint(name="foo", value="pkg_foo_cli", group="parsimony.providers")

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-foo", "0.1.0"))
    discovery._clear_cache()

    exit_code = run(json_output=True, env={"FOO_API_KEY": "present"})
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert isinstance(payload, list)
    assert len(payload) == 1
    entry = payload[0]
    assert entry["name"] == "foo"
    assert entry["module"] == "pkg_foo_cli"
    assert entry["distribution"] == "parsimony-foo"
    assert entry["version"] == "0.1.0"
    assert entry["connector_count"] == 1
    assert entry["env_vars_present"] == ["FOO_API_KEY"]
    assert entry["env_vars_missing"] == []
    assert entry["conformance"] == "pass"


def test_list_plugins_reports_missing_env_vars(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli.list_plugins import run
    from parsimony.plugins import discovery

    mod = _make_module(
        "pkg_missing_env",
        CONNECTORS=Connectors([_toy("x_fetch")]),
        ENV_VARS={"api_key": "MISSING_KEY"},
    )
    ep = EntryPoint(name="missing", value="pkg_missing_env", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-missing", "0.1.0"))
    discovery._clear_cache()

    exit_code = run(json_output=True, env={})  # MISSING_KEY absent
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload[0]["env_vars_present"] == []
    assert payload[0]["env_vars_missing"] == ["MISSING_KEY"]


def test_list_plugins_table_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli.list_plugins import run
    from parsimony.plugins import discovery

    mod = _make_module(
        "pkg_table_test",
        CONNECTORS=Connectors([_toy("table_fetch")]),
    )
    ep = EntryPoint(name="table", value="pkg_table_test", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-table", "0.1.0"))
    discovery._clear_cache()

    exit_code = run(json_output=False, env={})
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "table" in captured.out
    assert "pkg_table_test" in captured.out


def test_list_plugins_empty_when_no_providers(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli.list_plugins import run
    from parsimony.plugins import discovery

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    exit_code = run(json_output=False, env={})
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No parsimony plugins" in captured.out or "0 plugins" in captured.out


def test_list_plugins_reports_conformance_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli.list_plugins import run
    from parsimony.plugins import discovery

    # Broken: ENV_VARS key has no matching dep
    mod = _make_module(
        "pkg_broken_conformance",
        CONNECTORS=Connectors([_toy("ok_fetch")]),
        ENV_VARS={"ghost_dep": "GHOST_KEY"},
    )
    ep = EntryPoint(name="broken", value="pkg_broken_conformance", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-broken", "0.1.0"))
    discovery._clear_cache()

    exit_code = run(json_output=True, env={})
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload[0]["conformance"] == "fail"
    # non-zero exit signals failure for CI gating
    assert exit_code != 0


# ---------------------------------------------------------------------------
# Entry-point wiring via main()
# ---------------------------------------------------------------------------


def test_main_dispatches_to_list_plugins(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main
    from parsimony.plugins import discovery

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    exit_code = main(["list-plugins"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out  # some output emitted


def test_main_json_flag(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main
    from parsimony.plugins import discovery

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    exit_code = main(["list-plugins", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert json.loads(captured.out) == []
