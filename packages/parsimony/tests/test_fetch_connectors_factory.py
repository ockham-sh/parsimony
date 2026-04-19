"""Tests for :func:`parsimony.connectors.build_connectors_from_env` and the provider registry.

These tests cover only the bundled-in-core providers. Plugin-specific surface
checks (e.g. SDMX) live in the corresponding plugin packages.
"""

from __future__ import annotations


def test_factory_includes_bundled_surfaces() -> None:
    """The unified factory returns search, discovery, and fetch connectors."""
    env = {
        "FRED_API_KEY": "test",
        "FMP_API_KEY": "test",
    }
    from parsimony.connectors import build_connectors_from_env

    c = build_connectors_from_env(env=env)
    names = {x.name for x in c}

    assert "fred" in names
    assert "fmp_quotes" in names
    assert "fred_search" in names
    assert "fmp_search" in names
    assert "fmp_taxonomy" in names
    assert "fmp_screener" in names


def test_tool_tag_filter_matches_mcp_pattern() -> None:
    """Filtering by tags=['tool'] gives the MCP tool surface."""
    env = {
        "FRED_API_KEY": "test",
        "FMP_API_KEY": "test",
    }
    from parsimony.connectors import build_connectors_from_env

    all_connectors = build_connectors_from_env(env=env)
    tool_connectors = all_connectors.filter(tags=["tool"])
    tool_names = {x.name for x in tool_connectors}

    assert "fred_search" in tool_names
    assert "fmp_search" in tool_names
    assert "fmp_screener" in tool_names

    assert "fred" not in tool_names
    assert "fmp_quotes" not in tool_names


def test_lenient_skips_required_providers() -> None:
    """With lenient=True, missing required env vars don't raise."""
    from parsimony.connectors import build_connectors_from_env

    c = build_connectors_from_env(env={}, lenient=True)
    names = {x.name for x in c}

    assert "treasury" in names

    assert "fred" not in names
    assert "fmp_quotes" not in names


def test_registry_covers_bundled_provider_modules() -> None:
    """Every bundled provider module in the registry exports CONNECTORS."""
    import importlib

    from parsimony.connectors import PROVIDERS

    for spec in PROVIDERS:
        if spec.module is None:
            continue
        module = importlib.import_module(spec.module)
        assert hasattr(module, "CONNECTORS"), f"{spec.module} missing CONNECTORS"
