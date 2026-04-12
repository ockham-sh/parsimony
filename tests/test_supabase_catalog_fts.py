"""Tests for Supabase catalog full-text search RPC wiring."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from parsimony.stores.supabase import SupabaseCatalogStore


@pytest.mark.asyncio
async def test_search_text_calls_search_series_fts_and_maps_rank() -> None:
    row = {
        "namespace": "fred",
        "code": "GDP",
        "title": "Gross Domestic Product",
        "tags": ["macro"],
        "metadata": {},
        "embedding": None,
        "observable_id": None,
    }
    rpc_result = MagicMock()
    rpc_result.data = [
        {
            "namespace": "fred",
            "code": "GDP",
            "title": "Gross Domestic Product",
            "rank": 0.42,
        }
    ]
    rpc_chain = MagicMock()
    rpc_chain.execute.return_value = rpc_result

    load_result = MagicMock()
    load_result.data = [row]

    table_chain = MagicMock()
    table_chain.select.return_value.eq.return_value.in_.return_value.execute.return_value = (
        load_result
    )

    client = MagicMock()
    client.rpc.return_value = rpc_chain
    client.table.return_value = table_chain

    store = SupabaseCatalogStore(client, vector_dim=768)
    matches = await store.search_text("gross domestic", 5, namespaces=None)

    client.rpc.assert_called_once()
    call_kw = client.rpc.call_args
    assert call_kw[0][0] == "search_series_fts"
    assert call_kw[0][1]["query_text"] == "gross domestic"
    assert call_kw[0][1]["match_count"] == 5
    assert "p_namespaces" not in call_kw[0][1]

    assert len(matches) == 1
    assert matches[0].namespace == "fred"
    assert matches[0].code == "GDP"
    assert matches[0].similarity == pytest.approx(0.42)


@pytest.mark.asyncio
async def test_search_text_passes_p_namespaces_when_set() -> None:
    rpc_result = MagicMock()
    rpc_result.data = []
    rpc_chain = MagicMock()
    rpc_chain.execute.return_value = rpc_result

    client = MagicMock()
    client.rpc.return_value = rpc_chain

    store = SupabaseCatalogStore(client, vector_dim=768)
    await store.search_text("x", 3, namespaces=["fred", "ecb"])

    payload = client.rpc.call_args[0][1]
    assert payload["p_namespaces"] == ["fred", "ecb"]
