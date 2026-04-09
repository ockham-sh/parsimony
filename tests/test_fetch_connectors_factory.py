"""Tests for :func:`ockham.connectors.build_fetch_connectors_from_env`."""

from __future__ import annotations


def test_fetch_factory_excludes_discovery_and_screener() -> None:
    env = {
        "FRED_API_KEY": "test",
        "FMP_API_KEY": "test",
    }
    from ockham.connectors import build_fetch_connectors_from_env

    c = build_fetch_connectors_from_env(env=env)
    names = {x.name for x in c}
    assert "fred_fetch" in names
    assert "sdmx_fetch" in names
    assert "fred_search" not in names
    assert "sdmx_list_datasets" not in names
    assert "sdmx_dsd" not in names
    assert "sdmx_codelist" not in names
    assert "sdmx_series_keys" not in names
    assert "fmp_search" not in names
    assert "fmp_taxonomy" not in names
    assert "fmp_screener" not in names
    assert "fmp_quotes" in names
