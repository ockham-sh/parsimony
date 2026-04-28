"""Tests for the ``parsimony cache`` CLI subcommand.

Each test invokes :func:`parsimony.cli.main` with a fresh ``argv`` and
captures stdout/stderr. The autouse ``_pin_parsimony_cache_dir`` fixture
in ``conftest.py`` already isolates the cache root per-test, so these
tests never touch the user's real cache.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parsimony.cli import main


def _seed(path: Path, *, n_bytes: int = 1024) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * n_bytes)


# ---------------------------------------------------------------------------
# parsimony cache path
# ---------------------------------------------------------------------------


def test_cache_path_prints_root(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["cache", "path"])
    out = capsys.readouterr().out.strip()
    assert rc == 0
    assert out == str(_pin_parsimony_cache_dir)


# ---------------------------------------------------------------------------
# parsimony cache info
# ---------------------------------------------------------------------------


def test_cache_info_table_when_empty(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["cache", "info"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "SUBDIR" in out and "FILES" in out and "PATH" in out
    for name in ("catalogs", "models", "embeddings", "connectors"):
        assert name in out
    assert f"root: {_pin_parsimony_cache_dir}" in out


def test_cache_info_table_after_seed(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "embeddings" / "foo" / "vec.bin", n_bytes=2048)

    rc = main(["cache", "info"])
    out = capsys.readouterr().out
    assert rc == 0
    # Embeddings line: 1 file, 2.0 KB. Other dirs still empty.
    embedding_line = [ln for ln in out.splitlines() if ln.startswith("embeddings")][0]
    assert "1" in embedding_line
    assert "2.0 KB" in embedding_line


def test_cache_info_json_emits_valid_json(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "embeddings" / "foo" / "vec.bin", n_bytes=512)
    rc = main(["cache", "info", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["root"] == str(_pin_parsimony_cache_dir)
    assert payload["subdirs"]["embeddings"]["files"] == 1
    assert payload["subdirs"]["embeddings"]["size_bytes"] == 512
    assert payload["subdirs"]["catalogs"]["exists"] is False


# ---------------------------------------------------------------------------
# parsimony cache clear
# ---------------------------------------------------------------------------


def test_cache_clear_yes_wipes_all_subdirs(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "embeddings" / "a" / "x.bin")
    _seed(_pin_parsimony_cache_dir / "connectors" / "b" / "y.json", n_bytes=8)

    rc = main(["cache", "clear", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Cleared all subdirs" in out
    assert not (_pin_parsimony_cache_dir / "embeddings").exists()
    assert not (_pin_parsimony_cache_dir / "connectors").exists()


def test_cache_clear_specific_subdir(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "embeddings" / "victim" / "x.bin")
    _seed(_pin_parsimony_cache_dir / "connectors" / "survivor" / "y.json", n_bytes=8)

    rc = main(["cache", "clear", "--subdir", "embeddings", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "subdir 'embeddings'" in out
    assert not (_pin_parsimony_cache_dir / "embeddings").exists()
    assert (_pin_parsimony_cache_dir / "connectors" / "survivor" / "y.json").exists()


def test_cache_clear_unknown_subdir_exits_2(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["cache", "clear", "--subdir", "bogus", "--yes"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "unknown cache subdir 'bogus'" in err


def test_cache_clear_empty_is_noop(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["cache", "clear", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Nothing to clear" in out


def test_cache_clear_prompt_aborts_on_no(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed(_pin_parsimony_cache_dir / "embeddings" / "foo" / "x.bin")
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    rc = main(["cache", "clear"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Aborted." in out
    assert (_pin_parsimony_cache_dir / "embeddings" / "foo" / "x.bin").exists()


def test_cache_clear_prompt_confirms_on_yes(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed(_pin_parsimony_cache_dir / "embeddings" / "foo" / "x.bin")
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    rc = main(["cache", "clear"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Cleared" in out
    assert not (_pin_parsimony_cache_dir / "embeddings").exists()


def test_cache_clear_prompt_treats_eof_as_no(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Closed stdin (e.g. CI without a tty) must not destroy the cache."""

    def _eof(_prompt: str) -> str:
        raise EOFError

    _seed(_pin_parsimony_cache_dir / "embeddings" / "foo" / "x.bin")
    monkeypatch.setattr("builtins.input", _eof)

    rc = main(["cache", "clear"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Aborted." in out
    assert (_pin_parsimony_cache_dir / "embeddings" / "foo" / "x.bin").exists()
