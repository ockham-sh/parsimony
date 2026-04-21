"""Tests for the ``parsimony`` CLI (``list`` and ``publish`` verbs)."""

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
# list
# ---------------------------------------------------------------------------


def test_list_json_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    mod = _make_module(
        "pkg_foo_cli",
        CONNECTORS=Connectors([_toy("foo_fetch")]),
        ENV_VARS={"api_key": "FOO_API_KEY"},
    )
    ep = EntryPoint(name="foo", value="pkg_foo_cli", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-foo", "0.1.0"))
    monkeypatch.setenv("FOO_API_KEY", "present")
    discovery._clear_cache()

    exit_code = main(["list", "--json"])
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
    assert entry["catalogs"] == []


def test_list_reports_missing_env_vars(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    mod = _make_module(
        "pkg_missing_env",
        CONNECTORS=Connectors([_toy("x_fetch")]),
        ENV_VARS={"api_key": "MISSING_KEY"},
    )
    ep = EntryPoint(name="missing", value="pkg_missing_env", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-missing", "0.1.0"))
    monkeypatch.delenv("MISSING_KEY", raising=False)
    discovery._clear_cache()

    exit_code = main(["list", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload[0]["env_vars_present"] == []
    assert payload[0]["env_vars_missing"] == ["MISSING_KEY"]


def test_list_table_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    mod = _make_module(
        "pkg_table_test",
        CONNECTORS=Connectors([_toy("table_fetch")]),
    )
    ep = EntryPoint(name="table", value="pkg_table_test", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-table", "0.1.0"))
    discovery._clear_cache()

    exit_code = main(["list"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "table" in captured.out


def test_list_empty_when_no_providers(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    exit_code = main(["list"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No parsimony plugins" in captured.out or "0 plugins" in captured.out


def test_list_reports_conformance_pass_without_strict_flag(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    mod = _make_module(
        "pkg_bad_conformance",
        CONNECTORS=Connectors([_toy("ok_fetch")]),
        ENV_VARS={"ghost_dep": "GHOST_KEY"},  # ghost_dep is not a real connector dep
    )
    ep = EntryPoint(name="broken", value="pkg_bad_conformance", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-broken", "0.1.0"))
    discovery._clear_cache()

    exit_code = main(["list", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload[0]["conformance"] == "fail"
    assert exit_code == 0


def test_list_strict_exits_nonzero_on_conformance_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    mod = _make_module(
        "pkg_bad_conformance_strict",
        CONNECTORS=Connectors([_toy("ok_fetch")]),
        ENV_VARS={"ghost_dep": "GHOST_KEY"},
    )
    ep = EntryPoint(name="broken_strict", value="pkg_bad_conformance_strict", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-broken", "0.1.0"))
    discovery._clear_cache()

    exit_code = main(["list", "--json", "--strict"])
    assert exit_code == 1


def test_list_includes_static_catalog_namespaces(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    toy = _toy("fred_enumerate")
    mod = _make_module(
        "pkg_with_catalogs",
        CONNECTORS=Connectors([toy]),
        CATALOGS=[("fred", toy)],
    )
    ep = EntryPoint(name="fred", value="pkg_with_catalogs", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [ep])
    monkeypatch.setattr(discovery, "_import_module", lambda path: mod)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-fred", "0.1.0"))
    discovery._clear_cache()

    exit_code = main(["list", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload[0]["catalogs"] == ["fred"]


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


def test_publish_rejects_target_without_namespace_placeholder(capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main

    exit_code = main(["publish", "--provider", "fred", "--target", "file:///tmp/catalog"])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "{namespace}" in captured.err


def test_publish_unknown_provider(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    import parsimony.discovery as discovery
    from parsimony.cli import main

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    exit_code = main(["publish", "--provider", "bogus", "--target", "file:///tmp/{namespace}"])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "bogus" in captured.err
