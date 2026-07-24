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
    for name in ("catalogs", "models", "connectors"):
        assert name in out
    assert f"root: {_pin_parsimony_cache_dir}" in out


def test_cache_info_table_after_seed(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "model.onnx", n_bytes=2048)

    rc = main(["cache", "info"])
    out = capsys.readouterr().out
    assert rc == 0
    # Models line: 1 file, 2.0 KB. Other dirs still empty.
    models_line = [ln for ln in out.splitlines() if ln.startswith("models")][0]
    assert "1" in models_line
    assert "2.0 KB" in models_line


def test_cache_info_json_emits_valid_json(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "model.onnx", n_bytes=512)
    rc = main(["cache", "info", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["root"] == str(_pin_parsimony_cache_dir)
    assert payload["subdirs"]["models"]["files"] == 1
    assert payload["subdirs"]["models"]["size_bytes"] == 512
    assert payload["subdirs"]["catalogs"]["exists"] is False


# ---------------------------------------------------------------------------
# parsimony cache clear
# ---------------------------------------------------------------------------


def test_cache_clear_yes_wipes_all_subdirs(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "models" / "a" / "x.bin")
    _seed(_pin_parsimony_cache_dir / "connectors" / "b" / "y.json", n_bytes=8)

    rc = main(["cache", "clear", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Cleared all subdirs" in out
    assert not (_pin_parsimony_cache_dir / "models").exists()
    assert not (_pin_parsimony_cache_dir / "connectors").exists()


def test_cache_clear_specific_subdir(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _seed(_pin_parsimony_cache_dir / "models" / "victim" / "x.bin")
    _seed(_pin_parsimony_cache_dir / "connectors" / "survivor" / "y.json", n_bytes=8)

    rc = main(["cache", "clear", "--subdir", "models", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "subdir 'models'" in out
    assert not (_pin_parsimony_cache_dir / "models").exists()
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
    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "x.bin")
    monkeypatch.setattr("parsimony.cli._stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    rc = main(["cache", "clear"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Aborted." in out
    assert (_pin_parsimony_cache_dir / "models" / "foo" / "x.bin").exists()


def test_cache_clear_prompt_confirms_on_yes(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "x.bin")
    monkeypatch.setattr("parsimony.cli._stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    rc = main(["cache", "clear"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Cleared" in out
    assert not (_pin_parsimony_cache_dir / "models").exists()


def test_cache_clear_prompt_treats_eof_as_no(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Closed stdin on an otherwise-interactive terminal must not destroy the cache."""

    def _eof(_prompt: str) -> str:
        raise EOFError

    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "x.bin")
    monkeypatch.setattr("parsimony.cli._stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", _eof)

    rc = main(["cache", "clear"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Aborted." in out
    assert (_pin_parsimony_cache_dir / "models" / "foo" / "x.bin").exists()


# ---------------------------------------------------------------------------
# parsimony cache clear — non-interactive stdin without --yes fails fast
# ---------------------------------------------------------------------------


def test_cache_clear_non_interactive_without_yes_errors_and_preserves_cache(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No tty and no --yes must fail fast with exit 2, never block on input().

    Regression test for parsimony#95: previously this called ``input()``
    unconditionally, which hangs forever (with zero output) on a closed or
    non-terminal stdin instead of erroring.
    """
    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "x.bin")
    monkeypatch.setattr("parsimony.cli._stdin_is_interactive", lambda: False)

    def _unreachable(_prompt: str) -> str:
        raise AssertionError("input() must not be called when stdin is non-interactive")

    monkeypatch.setattr("builtins.input", _unreachable)

    rc = main(["cache", "clear"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "--yes" in err
    assert (_pin_parsimony_cache_dir / "models" / "foo" / "x.bin").exists()


def test_cache_clear_non_interactive_with_yes_succeeds(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--yes still works unattended even though stdin is non-interactive."""
    _seed(_pin_parsimony_cache_dir / "models" / "foo" / "x.bin")
    monkeypatch.setattr("parsimony.cli._stdin_is_interactive", lambda: False)

    rc = main(["cache", "clear", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Cleared" in out
    assert not (_pin_parsimony_cache_dir / "models").exists()


def test_cache_clear_repo_non_interactive_without_yes_errors(
    _pin_parsimony_cache_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The --repo path shares the same non-interactive fail-fast guard."""
    _seed(_pin_parsimony_cache_dir / "catalogs" / "datasets--parsimony-dev--sdmx" / "a.parquet")
    monkeypatch.setattr("parsimony.cli._stdin_is_interactive", lambda: False)

    def _unreachable(_prompt: str) -> str:
        raise AssertionError("input() must not be called when stdin is non-interactive")

    monkeypatch.setattr("builtins.input", _unreachable)

    rc = main(["cache", "clear", "--repo", "parsimony-dev/sdmx"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "--yes" in err
    assert (_pin_parsimony_cache_dir / "catalogs" / "datasets--parsimony-dev--sdmx" / "a.parquet").exists()


# ---------------------------------------------------------------------------
# parsimony cache info --repos  /  clear --repo  (per-repo targeting)
# ---------------------------------------------------------------------------


def test_cache_info_repos_breaks_down_catalogs(
    _pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(_pin_parsimony_cache_dir / "catalogs" / "datasets--acme--fred" / "rows.parquet", n_bytes=4096)

    rc = main(["cache", "info", "--repos"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "CATALOG REPO" in out
    assert "acme/fred" in out


def test_cache_info_repos_json_carries_catalog_repos(
    _pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(_pin_parsimony_cache_dir / "catalogs" / "datasets--acme--fred" / "rows.parquet", n_bytes=256)
    rc = main(["cache", "info", "--repos", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["catalog_repos"][0]["repo_id"] == "acme/fred"
    assert payload["catalog_repos"][0]["size_bytes"] == 256


def test_cache_clear_repo_removes_only_that_repo(
    _pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(_pin_parsimony_cache_dir / "catalogs" / "datasets--parsimony-dev--sdmx" / "a.parquet")
    _seed(_pin_parsimony_cache_dir / "catalogs" / "datasets--acme--fred" / "b.parquet")

    rc = main(["cache", "clear", "--repo", "parsimony-dev/sdmx", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "catalogs for repo 'parsimony-dev/sdmx'" in out
    assert not (_pin_parsimony_cache_dir / "catalogs" / "datasets--parsimony-dev--sdmx").exists()
    assert (_pin_parsimony_cache_dir / "catalogs" / "datasets--acme--fred" / "b.parquet").exists()


def test_cache_clear_repo_absent_is_noop(_pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["cache", "clear", "--repo", "never/cached", "--yes"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Nothing to clear" in out


def test_cache_clear_repo_and_subdir_are_mutually_exclusive(
    _pin_parsimony_cache_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["cache", "clear", "--repo", "a/b", "--subdir", "catalogs", "--yes"])
    assert exc.value.code == 2
    assert "not allowed with" in capsys.readouterr().err
