"""Tests for :func:`parsimony.connectors.build_connectors_from_env` and the provider registry."""

from __future__ import annotations


def test_factory_includes_all_surfaces() -> None:
    """The unified factory returns search, discovery, fetch, and enumerator connectors."""
    env = {
        "FRED_API_KEY": "test",
        "FMP_API_KEY": "test",
    }
    from parsimony.connectors import build_connectors_from_env

    c = build_connectors_from_env(env=env)
    names = {x.name for x in c}

    # Fetch connectors present
    assert "fred_fetch" in names
    assert "sdmx_fetch" in names
    assert "fmp_quotes" in names

    # Search/discovery connectors present (previously excluded by fetch factory)
    assert "fred_search" in names
    assert "sdmx_list_datasets" in names
    assert "sdmx_dsd" in names
    assert "sdmx_codelist" in names
    assert "sdmx_series_keys" in names
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

    # Tool-tagged connectors are included
    assert "fred_search" in tool_names
    assert "fmp_search" in tool_names
    assert "fmp_screener" in tool_names
    assert "sdmx_list_datasets" in tool_names

    # Non-tool connectors are excluded
    assert "fred_fetch" not in tool_names
    assert "sdmx_fetch" not in tool_names
    assert "fmp_quotes" not in tool_names


def test_lenient_skips_required_providers() -> None:
    """With lenient=True, missing required env vars don't raise."""
    from parsimony.connectors import build_connectors_from_env

    c = build_connectors_from_env(env={}, lenient=True)
    names = {x.name for x in c}

    # Public providers still present
    assert "sdmx_fetch" in names
    assert "treasury_fetch" in names

    # Required providers skipped (no API keys)
    assert "fred_fetch" not in names
    assert "fmp_quotes" not in names


def test_registry_covers_all_provider_modules() -> None:
    """Every provider module in the registry exports CONNECTORS."""
    import importlib

    from parsimony.connectors import PROVIDERS

    for spec in PROVIDERS:
        module = importlib.import_module(spec.module)
        assert hasattr(module, "CONNECTORS"), f"{spec.module} missing CONNECTORS"
