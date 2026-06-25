"""Process-wide model cache: a loaded catalog must not reload the model per call.

Each catalog snapshot loaded from disk reconstructs its ``VectorIndex`` with a fresh
embedder, so without a shared model cache the same SentenceTransformer is loaded once per
loaded catalog. These tests fake ``sentence_transformers`` to assert one load per key.
"""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from pathlib import Path

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
    # Keep these cache tests offline: skip Hub resolution so the fake model name
    # is handed straight to FakeSentenceTransformer (no snapshot_download call).
    monkeypatch.setattr(embedder_mod, "_resolve_cached_model_dir", lambda model_name: None)
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


def test_load_sentence_transformer_resolves_to_local_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A cached model is resolved to a directory and constructed from that PATH.

    ``SentenceTransformer(repo_id, local_files_only=True)`` still fires blocking
    Hub HEAD requests; resolving via ``snapshot_download(local_files_only=True)``
    first and constructing from the returned dir keeps the warm load offline.
    """
    local_dir = tmp_path / "snapshot"
    local_dir.mkdir()
    st_args: list[str] = []
    snap_calls: list[tuple[str, bool]] = []

    class FakeSentenceTransformer:
        def __init__(self, model_name: str, device: str | None = None, local_files_only: bool = False) -> None:
            st_args.append(model_name)

    st_mod = types.ModuleType("sentence_transformers")
    st_mod.SentenceTransformer = FakeSentenceTransformer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", st_mod)

    def fake_snapshot_download(model_name: str, local_files_only: bool = False) -> str:
        snap_calls.append((model_name, local_files_only))
        return str(local_dir)

    hub_mod = types.ModuleType("huggingface_hub")
    hub_mod.snapshot_download = fake_snapshot_download  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "huggingface_hub", hub_mod)

    model = embedder_mod._load_sentence_transformer("sentence-transformers/all-MiniLM-L6-v2", None)

    assert isinstance(model, FakeSentenceTransformer)
    # Cache-first: resolved offline (local_files_only=True), never the bare repo id.
    assert snap_calls == [("sentence-transformers/all-MiniLM-L6-v2", True)]
    assert st_args == [str(local_dir)]
