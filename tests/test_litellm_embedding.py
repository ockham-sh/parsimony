"""Smoke tests for LiteLLMEmbeddingProvider.

Uses pytest.importorskip so these are silently skipped when litellm is not installed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

litellm = pytest.importorskip("litellm")

from parsimony.embeddings.litellm import (  # noqa: E402
    LiteLLMEmbeddingProvider,
    _normalize_embedding,
    _validate_embedding_response,
)

# --- Unit tests for helpers ---


def test_normalize_embedding_unit_vector() -> None:
    vec = [1.0, 0.0, 0.0]
    assert _normalize_embedding(vec) == [1.0, 0.0, 0.0]


def test_normalize_embedding_scales() -> None:
    vec = [3.0, 4.0]
    result = _normalize_embedding(vec)
    assert pytest.approx(result, abs=1e-9) == [0.6, 0.8]


def test_normalize_embedding_zero_vector() -> None:
    vec = [0.0, 0.0, 0.0]
    assert _normalize_embedding(vec) == [0.0, 0.0, 0.0]


def test_validate_embedding_response_dict_items() -> None:
    response = {
        "data": [
            {"embedding": [0.1, 0.2, 0.3]},
            {"embedding": [0.4, 0.5, 0.6]},
        ]
    }
    result = _validate_embedding_response(response, expected_count=2, expected_dim=3)
    assert len(result) == 2
    assert result[0] == [0.1, 0.2, 0.3]


def test_validate_embedding_response_pydantic_objects() -> None:
    """Simulate litellm Pydantic Embedding objects with attribute access."""

    class FakeEmbedding:
        def __init__(self, embedding: list[float]) -> None:
            self.embedding = embedding

    response = {"data": [FakeEmbedding([0.1, 0.2])]}
    result = _validate_embedding_response(response, expected_count=1, expected_dim=2)
    assert result == [[0.1, 0.2]]


def test_validate_embedding_response_missing_data() -> None:
    with pytest.raises(ValueError, match="missing 'data' key"):
        _validate_embedding_response({}, expected_count=1, expected_dim=3)


def test_validate_embedding_response_count_mismatch() -> None:
    response = {"data": [{"embedding": [0.1]}]}
    with pytest.raises(ValueError, match="returned 1 items, expected 2"):
        _validate_embedding_response(response, expected_count=2, expected_dim=1)


def test_validate_embedding_response_dim_mismatch() -> None:
    response = {"data": [{"embedding": [0.1, 0.2]}]}
    with pytest.raises(ValueError, match="dimension 2, expected 3"):
        _validate_embedding_response(response, expected_count=1, expected_dim=3)


# --- Integration-style tests with mocked litellm ---


@pytest.fixture
def provider() -> LiteLLMEmbeddingProvider:
    return LiteLLMEmbeddingProvider(model="text-embedding-004", dimension=3)


async def test_embed_texts_empty(provider: LiteLLMEmbeddingProvider) -> None:
    result = await provider.embed_texts([])
    assert result == []


async def test_embed_texts_single_batch(provider: LiteLLMEmbeddingProvider) -> None:
    mock_response = {
        "data": [
            {"embedding": [1.0, 0.0, 0.0]},
            {"embedding": [0.0, 1.0, 0.0]},
        ]
    }
    with patch("parsimony.embeddings.litellm.litellm") as mock_litellm:
        mock_litellm.aembedding = AsyncMock(return_value=mock_response)
        result = await provider.embed_texts(["hello", "world"])

    assert len(result) == 2
    # Should be normalized
    assert pytest.approx(result[0], abs=1e-9) == [1.0, 0.0, 0.0]


async def test_embed_query(provider: LiteLLMEmbeddingProvider) -> None:
    mock_response = {"data": [{"embedding": [0.6, 0.8, 0.0]}]}
    with patch("parsimony.embeddings.litellm.litellm") as mock_litellm:
        mock_litellm.aembedding = AsyncMock(return_value=mock_response)
        result = await provider.embed_query("test query")

    assert len(result) == 3
    # Should be normalized
    assert pytest.approx(sum(x * x for x in result), abs=1e-9) == 1.0


async def test_embed_texts_api_error(provider: LiteLLMEmbeddingProvider) -> None:
    with patch("parsimony.embeddings.litellm.litellm") as mock_litellm:
        mock_litellm.aembedding = AsyncMock(side_effect=RuntimeError("API down"))
        with pytest.raises(RuntimeError, match="Embedding call failed"):
            await provider.embed_texts(["hello"])
