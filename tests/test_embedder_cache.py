"""Process-wide model cache: a loaded catalog must not reload the model per call.

Each catalog snapshot loaded from disk reconstructs its ``VectorIndex`` with a fresh
embedder, so without a shared model cache the same SentenceTransformer is loaded once per
loaded catalog. These tests fake ``sentence_transformers`` to assert one load per key.
"""

from __future__ import annotations

import logging
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


def test_model_download_logs_start_and_completion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A genuine model cache miss brackets the ~90 MB download with a log pair.

    Without it this is the one remaining multi-second stall in a cold first search
    that produces no capturable signal at all.
    """
    local_dir = tmp_path / "model"
    local_dir.mkdir()
    calls: list[bool] = []

    def fake_snapshot_download(model_name: str, local_files_only: bool = False) -> str:
        calls.append(local_files_only)
        if local_files_only:
            raise OSError("not cached")  # force the miss path
        return str(local_dir)

    hub_mod = types.ModuleType("huggingface_hub")
    hub_mod.snapshot_download = fake_snapshot_download  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "huggingface_hub", hub_mod)

    with caplog.at_level(logging.INFO, logger="parsimony.embedder"):
        resolved = embedder_mod._resolve_cached_model_dir("sentence-transformers/all-MiniLM-L6-v2")

    assert resolved == str(local_dir)
    assert calls == [True, False]  # tried cache first, then downloaded
    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("Downloading embedding model") for m in messages), messages
    assert any(m.startswith("Downloaded embedding model") for m in messages), messages


def test_cached_model_resolution_is_silent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An already-cached model resolves offline and logs nothing."""
    local_dir = tmp_path / "model"
    local_dir.mkdir()

    hub_mod = types.ModuleType("huggingface_hub")
    hub_mod.snapshot_download = lambda model_name, local_files_only=False: str(local_dir)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "huggingface_hub", hub_mod)

    with caplog.at_level(logging.INFO, logger="parsimony.embedder"):
        embedder_mod._resolve_cached_model_dir("sentence-transformers/all-MiniLM-L6-v2")

    assert not caplog.records


def test_model_load_logs_start_and_completion_pair(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Materializing the weights is bracketed even when nothing is downloaded.

    Suppressing the ``Loading weights`` bar removed the only signal this path
    had, and it costs seconds on every fresh process with the model fully
    cached. Unlike the bar, these lines survive redirection off a tty.
    """
    local_dir = tmp_path / "model"
    local_dir.mkdir()

    st_mod = types.ModuleType("sentence_transformers")
    st_mod.SentenceTransformer = lambda path, device=None: object()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", st_mod)

    with caplog.at_level(logging.INFO, logger="parsimony.embedder"):
        embedder_mod._load_sentence_transformer(str(local_dir), None)

    messages = [r.getMessage() for r in caplog.records]
    assert [m for m in messages if m.startswith("Loading embedding model")], messages
    assert [m for m in messages if m.startswith("Loaded embedding model")], messages


def test_quiet_weight_loading_toggles_only_transformers_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bar is silenced via transformers' own flag, and hub download bars survive.

    Asserted against the flag rather than by scraping stderr on purpose: the
    ``Loading weights`` bar only exists on transformers 5.x, so a stderr-scraping
    test would pass vacuously on an older pin instead of catching a regression.

    The second half is the real trap. ``transformers.utils.logging.disable_progress_bar()``
    also calls huggingface_hub's global ``disable_progress_bars()`` — using it here
    would silence the catalog/model *download* bars, i.e. destroy the progress
    signal a human watching a slow first call depends on.
    """
    hf_logging = pytest.importorskip("transformers.utils.logging")
    from huggingface_hub.utils import are_progress_bars_disabled

    monkeypatch.setattr(hf_logging, "_tqdm_active", True, raising=False)
    assert not are_progress_bars_disabled()

    with embedder_mod._quiet_weight_loading():
        assert hf_logging._tqdm_active is False  # bar suppressed for the load
        assert not are_progress_bars_disabled()  # hub download bars untouched

    assert hf_logging._tqdm_active is True  # caller's setting restored
    assert not are_progress_bars_disabled()


def test_quiet_weight_loading_restores_a_disabled_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restores the prior value, rather than assuming the caller wanted bars on."""
    hf_logging = pytest.importorskip("transformers.utils.logging")
    monkeypatch.setattr(hf_logging, "_tqdm_active", False, raising=False)

    with embedder_mod._quiet_weight_loading():
        pass

    assert hf_logging._tqdm_active is False
