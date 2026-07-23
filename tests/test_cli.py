"""Tests for the ``parsimony`` CLI (``list`` and ``cache`` verbs).

The new kernel surfaces ``parsimony.discover`` — these tests monkeypatch its
``iter_providers`` seam rather than reaching into a cache-backed discovery
module.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from types import ModuleType
from typing import Any

import pytest

from parsimony import cli
from parsimony.connector import Connectors, connector
from parsimony.discover import Provider
from parsimony.registry import ConnectorRegistry, InstallableConnector, RegistryError


def _toy(name: str, **kwargs: Any):
    def _fn(x: str = "y", api_key: str = "") -> dict[str, Any]:
        return {}

    _fn.__doc__ = "Fetch a toy observation with a plenty long description."
    _fn.__name__ = name
    kwargs.setdefault("secrets", ("api_key",))
    return connector(**kwargs)(_fn)


def _public_toy(name: str, **kwargs: Any):
    def _fn(x: str = "y") -> dict[str, Any]:
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

    Patches both the kernel ``parsimony.discover`` exports and the name bound
    in ``cli`` at module load time.
    """
    from parsimony import cli as cli_mod

    providers = [provider] if provider is not None else []

    def _fake_iter() -> list[Provider]:
        return list(providers)

    import parsimony.discover as discover_mod

    monkeypatch.setattr(discover_mod, "iter_providers", lambda: iter(_fake_iter()))
    monkeypatch.setattr(cli_mod, "iter_providers", lambda: iter(_fake_iter()))

    if module is not None:
        monkeypatch.setitem(__import__("sys").modules, module.__name__, module)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_json_output(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from parsimony.cli import main

    mod = _make_module(
        "pkg_foo_cli",
        CONNECTORS=Connectors([_toy("foo_fetch")]),
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
    assert "env_vars" not in payload


def test_list_metadata_only_without_strict(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
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


def test_list_skips_conformance_without_strict_flag(
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
    assert payload["plugins"][0]["connector_count"] == 1
    assert exit_code == 0


def test_list_strict_exits_nonzero_on_conformance_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony.cli import main

    mod = _make_module(
        "pkg_bad_conformance_strict",
        CONNECTORS=Connectors([]),
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


def test_list_does_not_report_provider_build_protocols(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    toy = _public_toy("fred_enumerate")
    mod = _make_module(
        "pkg_connector_only",
        CONNECTORS=Connectors([toy]),
    )
    prov = Provider(
        name="fred",
        module_path="pkg_connector_only",
        dist_name="parsimony-fred",
        version="0.1.0",
    )
    _patch_providers(monkeypatch, module=mod, provider=prov)

    exit_code = main(["list", "--json", "--strict"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert "catalogs" not in payload["plugins"][0]


# ---------------------------------------------------------------------------
# list --available
#
# The CLI is a thin consumer of parsimony.registry.list_available(); these
# tests only cover argument handling and rendering/delegation, not the
# fetch/fallback logic itself (see tests/test_registry.py for that).
# ---------------------------------------------------------------------------


def _fake_registry(*, source: str = "remote") -> ConnectorRegistry:
    return ConnectorRegistry(
        generated_at="2026-07-22",
        connectors=(
            InstallableConnector(
                package="parsimony-fred",
                provider="FRED (Federal Reserve Economic Data)",
                entry_point="fred",
                connector_count=2,
                keyless=False,
            ),
        ),
        source=source,  # type: ignore[arg-type]
    )


def test_list_available_json_delegates_to_registry(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    monkeypatch.setattr(cli, "list_available", lambda: _fake_registry(source="remote"))

    exit_code = main(["list", "--available", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["source"] == "remote"
    assert payload["generated_at"] == "2026-07-22"
    assert payload["connectors"] == [
        {
            "package": "parsimony-fred",
            "provider": "FRED (Federal Reserve Economic Data)",
            "entry_point": "fred",
            "connector_count": 2,
            "keyless": False,
        }
    ]


def test_list_available_table_shows_source_and_generated_at(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    monkeypatch.setattr(cli, "list_available", lambda: _fake_registry(source="bundled"))

    exit_code = main(["list", "--available"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "parsimony-fred" in captured.out
    assert "bundled registry" in captured.out
    assert "2026-07-22" in captured.out


def test_list_available_empty_registry_prints_hint(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    empty = ConnectorRegistry(generated_at="2026-07-22", connectors=(), source="remote")
    monkeypatch.setattr(cli, "list_available", lambda: empty)

    exit_code = main(["list", "--available"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No installable connectors" in captured.out


def test_list_available_registry_error_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from parsimony.cli import main

    def raise_registry_error() -> ConnectorRegistry:
        raise RegistryError(
            "the connector registry is unavailable",
            remote_error=RuntimeError("network down"),
            bundled_error=RuntimeError("bundle missing"),
        )

    monkeypatch.setattr(cli, "list_available", raise_registry_error)

    exit_code = main(["list", "--available"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "the connector registry is unavailable" in captured.err


def test_list_available_and_strict_are_mutually_exclusive() -> None:
    from parsimony.cli import main

    with pytest.raises(SystemExit) as excinfo:
        main(["list", "--available", "--strict"])
    assert excinfo.value.code == 2


def test_verbose_flag_surfaces_parsimony_logs(capsys: pytest.CaptureFixture[str]) -> None:
    """``-v`` attaches a handler so parsimony's own INFO records reach stderr.

    Without one the record is never even constructed (root defaults to WARNING),
    which is why a catalog download used to stall with no explanation at all.
    """
    cli._configure_logging(verbose=True)
    logging.getLogger("parsimony.catalog.remote").info("Fetching catalog hf://x/y")

    assert "Fetching catalog hf://x/y" in capsys.readouterr().err


def test_without_verbose_info_is_suppressed(capsys: pytest.CaptureFixture[str]) -> None:
    """The default stays quiet — verbosity is opt-in, not the CLI's house style."""
    cli._configure_logging(verbose=False)
    logging.getLogger("parsimony.catalog.remote").info("Fetching catalog hf://x/y")

    assert capsys.readouterr().err == ""


@pytest.fixture(autouse=True)
def _reset_parsimony_logger() -> Iterator[None]:
    """Undo handler/level changes so these tests don't leak into the rest of the run."""
    package_logger = logging.getLogger("parsimony")
    handlers, level = list(package_logger.handlers), package_logger.level
    yield
    package_logger.handlers = handlers
    package_logger.setLevel(level)
