"""Process-wide model cache: a loaded catalog must not reload the model per call.

Each catalog snapshot loaded from disk reconstructs its ``VectorIndex`` with a fresh
embedder, so without a shared model cache the same SentenceTransformer is loaded once per
loaded catalog. These tests fake ``sentence_transformers`` to assert one load per key.
"""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator

import pytest

from parsimony import embedder as embedder_mod


@pytest.fixture
def fake_sentence_transformers(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[str, str | None]]]:
    calls: list[tuple[str, str | None]] = []

    class FakeSentenceTransformer:
        def __init__(self, model_name: str, device: str | None = None, local_files_only: bool = False) -> None:
            calls.append((model_name, device))

    mod = types.ModuleType("sentence_transformers")
    mod.SentenceTransformer = FakeSentenceTransformer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", mod)
    monkeypatch.setattr(embedder_mod, "_MODEL_CACHE", {})
    yield calls


def test_shared_model_loaded_once_per_key(fake_sentence_transformers: list[tuple[str, str | None]]) -> None:
    first = embedder_mod._load_shared_model("m", None)
    second = embedder_mod._load_shared_model("m", None)
    assert first is second
    assert len(fake_sentence_transformers) == 1


def test_shared_model_distinct_per_key(fake_sentence_transformers: list[tuple[str, str | None]]) -> None:
    a = embedder_mod._load_shared_model("m", None)
    b = embedder_mod._load_shared_model("m2", None)
    assert a is not b
    assert len(fake_sentence_transformers) == 2


def test_two_embedders_share_one_model(fake_sentence_transformers: list[tuple[str, str | None]]) -> None:
    e1 = embedder_mod.SentenceTransformerEmbedder(model="m")
    e2 = embedder_mod.SentenceTransformerEmbedder(model="m")
    assert e1._get_model() is e2._get_model()
    assert len(fake_sentence_transformers) == 1
