"""Tests for the ``parsimony`` CLI (``list`` and ``publish`` verbs).

The new kernel surfaces ``parsimony.discover`` — these tests monkeypatch its
``iter_providers`` seam rather than reaching into a cache-backed discovery
module.
"""

from __future__ import annotations

import json
from types import ModuleType
from typing import Any

import pytest
from pydantic import BaseModel

from parsimony.connector import Connectors, connector
from parsimony.discover import Provider


class _TP(BaseModel):
    x: str = "y"


def _toy(name: str, *, env: dict[str, str] | None = None, **kwargs: Any):
    async def _fn(params: _TP, *, api_key: str) -> dict[str, Any]:
        return {}

    _fn.__doc__ = "Fetch a toy observation with a plenty long description."
    _fn.__name__ = name
    return connector(env=env, **kwargs)(_fn)


def _public_toy(name: str, **kwargs: Any):
    async def _fn(params: _TP) -> dict[str, Any]:
        return {}

    _fn.__doc__ = "Public fetch with no deps."
    _fn.__name__ = name
    return connector(**kwargs)(_fn)


def _make_module(path: str, **attrs: Any) -> ModuleType:
    mod = ModuleType(path)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


def _patch_providers(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module: ModuleType | None,
    provider: Provider | None,
) -> None:
    """Install a fake ``iter_providers`` + ``import_module`` pair.

    Patches both the kernel ``parsimony.discover`` exports AND the names bound
    in downstream modules (``cli``, ``publish``) that imported them at module
    load time.
    """
    from parsimony import cli as cli_mod
    from parsimony import publish as publish_mod

    providers = [provider] if provider is not None else []

    def _fake_iter() -> list[Provider]:
        return list(providers)

    import parsimony.discover as discover_mod

    monkeypatch.setattr(discover_mod, "iter_providers", lambda: iter(_fake_iter()))
    monkeypatch.setattr(cli_mod, "iter_providers", lambda: iter(_fake_iter()))
    monkeypatch.setattr(publish_mod, "iter_providers", lambda: iter(_fake_iter()))

    if module is not None:
        monkeypatch.setitem(__import__("sys").modules, module.__name__, module)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_json_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main

    mod = _make_module(
        "pkg_foo_cli",
        CONNECTORS=Connectors([_toy("foo_fetch", env={"api_key": "FOO_API_KEY"})]),
    )
    prov = Provider(
        name="foo",
        module_path="pkg_foo_cli",
        dist_name="parsimony-foo",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=mod, provider=prov)
    monkeypatch.setenv("FOO_API_KEY", "present")

    exit_code = main(["list", "--json", "--strict"])
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert isinstance(payload, dict)
    plugins = payload["plugins"]
    assert len(plugins) == 1
    entry = plugins[0]
    assert entry["name"] == "foo"
    assert entry["module"] == "pkg_foo_cli"
    assert entry["distribution"] == "parsimony-foo"
    assert entry["version"] == "0.1.0"
    assert entry["connector_count"] == 1
    assert entry["conformance"] == "pass"
    assert entry["catalogs"] == []
    assert payload["env_vars"] == ["FOO_API_KEY"]


def test_list_metadata_only_without_strict(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    # Module is intentionally NOT importable (no sys.modules entry). Without
    # --strict, metadata-only listing should still succeed.
    prov = Provider(
        name="ghost",
        module_path="pkg_never_imported_cli",
        dist_name="parsimony-ghost",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=None, provider=prov)

    exit_code = main(["list", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["plugins"][0]["conformance"] == "skipped"


def test_list_table_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main

    mod = _make_module(
        "pkg_table_test",
        CONNECTORS=Connectors([_public_toy("table_fetch")]),
    )
    prov = Provider(
        name="table",
        module_path="pkg_table_test",
        dist_name="parsimony-table",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=mod, provider=prov)

    exit_code = main(["list"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "table" in captured.out


def test_list_empty_when_no_providers(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main

    _patch_providers(monkeypatch, module=None, provider=None)

    exit_code = main(["list"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No parsimony plugins" in captured.out or "0 plugins" in captured.out


def test_list_reports_conformance_pass_without_strict_flag(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    # Without --strict, conformance is reported as "skipped" — no plugin imports.
    mod = _make_module(
        "pkg_bad_conformance",
        CONNECTORS=Connectors([_public_toy("ok_fetch")]),
    )
    prov = Provider(
        name="broken",
        module_path="pkg_bad_conformance",
        dist_name="parsimony-broken",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=mod, provider=prov)

    exit_code = main(["list", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["plugins"][0]["conformance"] == "skipped"
    assert exit_code == 0


def test_list_strict_exits_nonzero_on_conformance_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony.cli import main

    # A connector whose env_map key doesn't match any dep → conformance fail.
    bad = _toy("ok_fetch", env={"ghost_dep": "GHOST_KEY"})
    mod = _make_module(
        "pkg_bad_conformance_strict",
        CONNECTORS=Connectors([bad]),
    )
    prov = Provider(
        name="broken_strict",
        module_path="pkg_bad_conformance_strict",
        dist_name="parsimony-broken",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=mod, provider=prov)

    exit_code = main(["list", "--json", "--strict"])
    assert exit_code == 1


def test_list_includes_static_catalog_namespaces(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    toy = _public_toy("fred_enumerate")
    mod = _make_module(
        "pkg_with_catalogs",
        CONNECTORS=Connectors([toy]),
        CATALOGS=[("fred", toy)],
    )
    prov = Provider(
        name="fred",
        module_path="pkg_with_catalogs",
        dist_name="parsimony-fred",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=mod, provider=prov)

    exit_code = main(["list", "--json", "--strict"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["plugins"][0]["catalogs"] == ["fred"]


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
    from parsimony.cli import main

    _patch_providers(monkeypatch, module=None, provider=None)

    exit_code = main(["publish", "--provider", "bogus", "--target", "file:///tmp/{namespace}"])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "bogus" in captured.err
