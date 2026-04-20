"""Tests for :mod:`parsimony.discovery` — entry-point based plugin discovery."""

from importlib.metadata import EntryPoint
from types import ModuleType
from typing import Any

import pytest
from pydantic import BaseModel

from parsimony.connector import Connector, Connectors, connector


class _ToyParams(BaseModel):
    x: str = "y"


def _make_module(
    name: str,
    *,
    connectors: "Connectors | None" = None,
    env_vars: "dict[str, str] | None" = None,
    provider_metadata: "dict[str, Any] | None" = None,
) -> ModuleType:
    """Build an in-memory module exposing the plugin contract."""
    module = ModuleType(name)
    if connectors is not None:
        module.CONNECTORS = connectors  # type: ignore[attr-defined]
    if env_vars is not None:
        module.ENV_VARS = env_vars  # type: ignore[attr-defined]
    if provider_metadata is not None:
        module.PROVIDER_METADATA = provider_metadata  # type: ignore[attr-defined]
    return module


def _toy_connector(name: str = "toy_fetch", **kwargs: Any) -> Connector:
    """Build a minimal valid connector for tests."""

    async def _fn(params: _ToyParams) -> dict[str, Any]:
        return {"ok": True}

    _fn.__doc__ = kwargs.pop("doc", "Fetch a toy observation with plenty of description length.")
    _fn.__name__ = name
    return connector(**kwargs)(_fn)


# ---------------------------------------------------------------------------
# iter_entry_points
# ---------------------------------------------------------------------------


def test_iter_entry_points_returns_parsimony_providers_group(monkeypatch: pytest.MonkeyPatch) -> None:
    """iter_entry_points filters to group='parsimony.providers'."""
    from parsimony.discovery import _scan as discovery

    fake_eps = [
        EntryPoint(name="foo", value="pkg_foo", group="parsimony.providers"),
        EntryPoint(name="bar", value="pkg_bar", group="parsimony.providers"),
    ]

    def fake_entry_points(*, group: str) -> list[EntryPoint]:
        assert group == "parsimony.providers"
        return fake_eps

    monkeypatch.setattr(discovery, "_entry_points", fake_entry_points)
    discovery._clear_cache()

    result = list(discovery.iter_entry_points())
    names = [ep.name for ep in result]
    assert names == ["foo", "bar"]


def test_iter_entry_points_returns_empty_when_no_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony.discovery import _scan as discovery

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: [])
    discovery._clear_cache()

    assert list(discovery.iter_entry_points()) == []


# ---------------------------------------------------------------------------
# load_provider
# ---------------------------------------------------------------------------


def test_load_provider_returns_discovered_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """load_provider returns a DiscoveredProvider record with connectors, env_vars, and metadata."""
    from parsimony.discovery import _scan as discovery

    module = _make_module(
        "pkg_foo_test",
        connectors=Connectors([_toy_connector("foo_fetch")]),
        env_vars={"api_key": "FOO_API_KEY"},
        provider_metadata={"homepage": "https://example.com/foo"},
    )

    ep = EntryPoint(name="foo", value="pkg_foo_test", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_import_module", lambda path: module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-foo", "0.1.0"))

    provider = discovery.load_provider(ep)

    assert provider.name == "foo"
    assert provider.module_path == "pkg_foo_test"
    assert provider.distribution_name == "parsimony-foo"
    assert provider.version == "0.1.0"
    assert len(provider.connectors) == 1
    assert provider.env_vars == {"api_key": "FOO_API_KEY"}
    assert provider.provider_metadata == {"homepage": "https://example.com/foo"}


def test_load_provider_defaults_optional_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing ENV_VARS and PROVIDER_METADATA default to empty dicts, not errors."""
    from parsimony.discovery import _scan as discovery

    module = _make_module("pkg_bare", connectors=Connectors([_toy_connector("bare_fetch")]))

    ep = EntryPoint(name="bare", value="pkg_bare", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_import_module", lambda path: module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-bare", "0.0.1"))

    provider = discovery.load_provider(ep)

    assert provider.env_vars == {}
    assert provider.provider_metadata == {}


def test_load_provider_raises_when_connectors_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """load_provider raises PluginContractError if the module omits CONNECTORS."""
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery.errors import PluginContractError

    module = _make_module("pkg_broken")  # no CONNECTORS attribute

    ep = EntryPoint(name="broken", value="pkg_broken", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_import_module", lambda path: module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-broken", "0.1.0"))

    with pytest.raises(PluginContractError, match="CONNECTORS"):
        discovery.load_provider(ep)


def test_load_provider_raises_when_connectors_is_wrong_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """CONNECTORS must be a Connectors instance, not e.g. a list."""
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery.errors import PluginContractError

    module = _make_module("pkg_wrong")
    module.CONNECTORS = [_toy_connector("wrong_fetch")]  # type: ignore[attr-defined]

    ep = EntryPoint(name="wrong", value="pkg_wrong", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_import_module", lambda path: module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-wrong", "0.1.0"))

    with pytest.raises(PluginContractError, match="Connectors"):
        discovery.load_provider(ep)


def test_load_provider_propagates_import_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the target module raises on import, discovery surfaces it clearly."""
    from parsimony.discovery import _scan as discovery
    from parsimony.discovery.errors import PluginImportError

    def _boom(path: str) -> ModuleType:
        raise ImportError(f"cannot import {path}")

    ep = EntryPoint(name="bad", value="pkg_missing", group="parsimony.providers")
    monkeypatch.setattr(discovery, "_import_module", _boom)

    with pytest.raises(PluginImportError, match="pkg_missing"):
        discovery.load_provider(ep)


# ---------------------------------------------------------------------------
# discovered_providers
# ---------------------------------------------------------------------------


def test_discovered_providers_returns_list_of_records(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony.discovery import _scan as discovery

    module_a = _make_module("pkg_a", connectors=Connectors([_toy_connector("a_fetch")]))
    module_b = _make_module(
        "pkg_b",
        connectors=Connectors([_toy_connector("b_fetch")]),
        env_vars={"token": "B_TOKEN"},
    )

    eps = [
        EntryPoint(name="a", value="pkg_a", group="parsimony.providers"),
        EntryPoint(name="b", value="pkg_b", group="parsimony.providers"),
    ]
    modules = {"pkg_a": module_a, "pkg_b": module_b}

    monkeypatch.setattr(discovery, "_entry_points", lambda *, group: eps)
    monkeypatch.setattr(discovery, "_import_module", lambda path: modules[path])
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda ep: (f"parsimony-{ep.name}", "0.1.0"))
    discovery._clear_cache()

    providers = discovery.discovered_providers()

    assert [p.name for p in providers] == ["a", "b"]
    assert providers[1].env_vars == {"token": "B_TOKEN"}


def test_discovered_providers_caches_results(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second call should not re-invoke entry_points() or _import_module."""
    from parsimony.discovery import _scan as discovery

    module = _make_module("pkg_cached", connectors=Connectors([_toy_connector("cached_fetch")]))
    ep = EntryPoint(name="cached", value="pkg_cached", group="parsimony.providers")

    ep_calls = {"n": 0}
    import_calls = {"n": 0}

    def count_eps(*, group: str) -> list[EntryPoint]:
        ep_calls["n"] += 1
        return [ep]

    def count_imports(path: str) -> ModuleType:
        import_calls["n"] += 1
        return module

    monkeypatch.setattr(discovery, "_entry_points", count_eps)
    monkeypatch.setattr(discovery, "_import_module", count_imports)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-cached", "0.1.0"))
    discovery._clear_cache()

    first = discovery.discovered_providers()
    second = discovery.discovered_providers()

    assert first is second or first == second
    assert ep_calls["n"] == 1
    assert import_calls["n"] == 1


def test_clear_cache_forces_rediscovery(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony.discovery import _scan as discovery

    module = _make_module("pkg_reload", connectors=Connectors([_toy_connector("reload_fetch")]))
    ep = EntryPoint(name="reload", value="pkg_reload", group="parsimony.providers")

    ep_calls = {"n": 0}
    monkeypatch.setattr(
        discovery,
        "_entry_points",
        lambda *, group: ep_calls.update(n=ep_calls["n"] + 1) or [ep],
    )
    monkeypatch.setattr(discovery, "_import_module", lambda path: module)
    monkeypatch.setattr(discovery, "_distribution_for_entry_point", lambda _ep: ("parsimony-reload", "0.1.0"))
    discovery._clear_cache()

    discovery.discovered_providers()
    discovery._clear_cache()
    discovery.discovered_providers()

    assert ep_calls["n"] == 2
