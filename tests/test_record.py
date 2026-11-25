"""Run recording and replay (:mod:`parsimony.record`).

Every connector call saves its ``Result`` in a run folder for the current
process; ``PARSIMONY_REPLAY`` makes the same calls return a saved run's results.
The autouse conftest fixture sets ``PARSIMONY_RECORD=off``; the :func:`runs`
fixture below turns recording back on into a tmp folder.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from parsimony.connector import connector
from parsimony.result import Result

_RUN_NAME = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{6}")
_FILE_NAME = re.compile(r"(?P<connector>.+)-(?P<key>[0-9a-f]{12})\.(parquet|json)")
_SECRET = "sk-THIS-MUST-NOT-LEAK-1234567890"


@pytest.fixture
def runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from parsimony import record

    monkeypatch.setattr(record, "_run", None)
    monkeypatch.setattr(record, "_recorded", None)
    monkeypatch.setenv("PARSIMONY_RECORD", str(tmp_path / "runs"))
    monkeypatch.delenv("PARSIMONY_REPLAY", raising=False)
    return tmp_path / "runs"


def _run_folders(root: Path) -> list[Path]:
    return sorted(p for p in root.iterdir() if p.is_dir()) if root.exists() else []


def _only_run(root: Path) -> Path:
    folders = _run_folders(root)
    assert len(folders) == 1, folders
    return folders[0]


def _result_files(run: Path) -> list[Path]:
    return sorted(p for p in run.iterdir() if p.name != "run.json")


def _replay(monkeypatch: pytest.MonkeyPatch, run: Path, *, record_off: bool = True) -> None:
    monkeypatch.setenv("PARSIMONY_REPLAY", str(run))
    if record_off:
        monkeypatch.setenv("PARSIMONY_RECORD", "off")


# ---------------------------------------------------------------------------
# Test connectors: a counter shows whether the source was called.
# ---------------------------------------------------------------------------

calls: dict[str, int] = {}


def _count(name: str) -> None:
    calls[name] = calls.get(name, 0) + 1


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    calls.clear()
    _override.clear()


_override: dict[str, pd.DataFrame] = {}  # set "series" to change what the fake source returns


@connector
def series(series_id: str, scale: float = 1.0) -> pd.DataFrame:
    """Fetch a test series."""
    _count("series")
    if "series" in _override:
        return _override["series"]
    return pd.DataFrame({"date": ["2020-01-01", "2020-02-01"], "value": [1.0 * scale, 2.0 * scale], "id": series_id})


@connector(secrets=("api_key",))
def keyed(series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch a test series that needs a key."""
    _count("keyed")
    return pd.DataFrame({"value": [1.0]})


@connector
def detail(coin: str) -> dict[str, Any]:
    """Fetch a test record."""
    _count("detail")
    return {"coin": coin, "price": 1.5, "tags": ["a", "b"], "nested": {"x": None}}


@connector
def blob(name: str) -> bytes:
    """Fetch test bytes."""
    _count("blob")
    return b"\x00\x01"


@connector
def broken(name: str) -> pd.DataFrame:
    """Always fails."""
    raise RuntimeError("source down")


@connector
def composite(series_id: str) -> pd.DataFrame:
    """Combine two other connectors."""
    _count("composite")
    a = series(series_id).frame
    b = series(series_id, scale=2.0).frame
    return pd.concat([a, b], ignore_index=True)


_frames: dict[str, pd.DataFrame] = {}


@connector
def shaped(kind: str) -> pd.DataFrame:
    """Return the frame registered under *kind*."""
    _count("shaped")
    return _frames[kind]


@connector
def many(series_ids: list[str]) -> pd.DataFrame:
    """Fetch many series at once."""
    _count("many")
    return pd.DataFrame({"id": series_ids, "value": range(len(series_ids))})


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


def test_table_call_creates_run_folder_with_parquet(runs: Path) -> None:
    result = series("GDP")
    run = _only_run(runs)
    assert _RUN_NAME.fullmatch(run.name)
    assert (run / "run.json").is_file()
    files = _result_files(run)
    assert len(files) == 1
    m = _FILE_NAME.fullmatch(files[0].name)
    assert m and m["connector"] == "series" and files[0].suffix == ".parquet"

    saved = Result.from_parquet(files[0])
    pd.testing.assert_frame_equal(saved.frame, result.frame)
    assert saved.provenance.source == "series"
    assert saved.provenance.params == {"series_id": "GDP", "scale": 1.0}


def test_secret_never_written_and_does_not_change_key(runs: Path) -> None:
    keyed("GDP", api_key=_SECRET)
    keyed("GDP", api_key="another-value")
    keyed.bind(api_key=_SECRET)("GDP")
    run = _only_run(runs)
    files = _result_files(run)
    assert len(files) == 1  # same key whatever the secret's value
    for f in run.iterdir():
        data = f.read_bytes()
        assert _SECRET.encode() not in data
        assert b"another-value" not in data
        assert b"api_key" not in data


def test_same_call_twice_keeps_one_file_with_latest_result(runs: Path) -> None:
    series("GDP")
    later = pd.DataFrame({"date": ["2020-03-01"], "value": [9.0], "id": "GDP"})
    _override["series"] = later
    series("GDP")
    files = _result_files(_only_run(runs))
    assert len(files) == 1
    pd.testing.assert_frame_equal(Result.from_parquet(files[0]).frame, later)


def test_different_params_give_different_files(runs: Path) -> None:
    series("GDP")
    series("CPI")
    files = _result_files(_only_run(runs))
    keys = {_FILE_NAME.fullmatch(f.name)["key"] for f in files}  # type: ignore[index]
    assert len(files) == 2 and len(keys) == 2


def test_dict_result_saved_as_json(runs: Path) -> None:
    detail("btc")
    (f,) = _result_files(_only_run(runs))
    assert _FILE_NAME.fullmatch(f.name) and f.name.startswith("detail-") and f.suffix == ".json"
    saved = json.loads(f.read_text())
    assert saved["raw"] == {"coin": "btc", "price": 1.5, "tags": ["a", "b"], "nested": {"x": None}}
    assert saved["provenance"]["source"] == "detail"
    assert saved["provenance"]["params"] == {"coin": "btc"}


def test_unserialisable_result_is_not_saved_but_returned(runs: Path, capsys: pytest.CaptureFixture[str]) -> None:
    result = blob("x")
    assert result.raw == b"\x00\x01"
    assert _result_files(_only_run(runs)) == []
    err = capsys.readouterr().err
    assert err.count("parsimony: call not recorded") == 1
    assert "TypeError" in err


def test_failing_connector_leaves_no_run_folder(runs: Path) -> None:
    with pytest.raises(RuntimeError, match="source down"):
        broken("x")
    assert not runs.exists()


def test_unwritable_record_root_does_not_break_fetch(
    tmp_path: Path, runs: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    not_a_dir = tmp_path / "plain-file"
    not_a_dir.write_text("x")
    monkeypatch.setenv("PARSIMONY_RECORD", str(not_a_dir))
    result = series("GDP")
    assert list(result.frame["value"]) == [1.0, 2.0]
    assert capsys.readouterr().err.count("parsimony: call not recorded") == 1


def test_record_off_writes_nothing(runs: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PARSIMONY_RECORD", "off")
    before = sorted(tmp_path.rglob("*"))
    series("GDP")
    detail("btc")
    assert sorted(tmp_path.rglob("*")) == before
    assert not runs.exists()


def test_connector_calling_connectors_saves_every_call(runs: Path) -> None:
    composite("GDP")
    files = _result_files(_only_run(runs))
    assert len(files) == 3
    assert sorted(f.name.rsplit("-", 1)[0] for f in files) == ["composite", "series", "series"]


def test_deleted_run_folder_is_recreated(runs: Path) -> None:
    import shutil

    series("GDP")
    first = _only_run(runs)
    shutil.rmtree(first)
    series("CPI")
    second = _only_run(runs)
    assert second != first
    assert (second / "run.json").is_file()
    assert len(_result_files(second)) == 1


def test_no_call_no_folder(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    code = textwrap.dedent(
        """
        import parsimony
        from parsimony.connector import connector

        @connector
        def f(x: int) -> dict:
            '''Doc.'''
            return {"x": x}

        f.bind(x=1)
        """
    )
    env = {**os.environ, "PARSIMONY_RECORD": str(root)}
    env.pop("PARSIMONY_REPLAY", None)
    subprocess.run([sys.executable, "-c", code], env=env, check=True)
    assert not root.exists()


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def test_replay_returns_saved_frame_without_calling_source(runs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = series("GDP")
    run = _only_run(runs)
    _replay(monkeypatch, run)
    calls.clear()
    replayed = series("GDP")
    assert calls == {}
    pd.testing.assert_frame_equal(replayed.frame, original.frame)
    assert replayed.provenance.fetched_at == original.provenance.fetched_at
    assert replayed.provenance.source == "series"


def test_replay_dict_result(runs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = detail("btc")
    _replay(monkeypatch, _only_run(runs))
    calls.clear()
    replayed = detail("btc")
    assert calls == {}
    assert replayed.raw == original.raw
    assert replayed.provenance.fetched_at == original.provenance.fetched_at


def test_replay_unknown_call_raises(runs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    series("GDP")
    run = _only_run(runs)
    _replay(monkeypatch, run)
    with pytest.raises(LookupError) as exc_info:
        series("UNRATE")
    msg = str(exc_info.value)
    assert "series" in msg and str(run) in msg and "UNRATE" in msg and "PARSIMONY_REPLAY" in msg
    assert calls == {"series": 1}  # the live source was not contacted


@pytest.mark.parametrize("record_off", [True, False])
def test_replay_creates_no_run_folder(runs: Path, monkeypatch: pytest.MonkeyPatch, record_off: bool) -> None:
    from parsimony import record

    series("GDP")
    run = _only_run(runs)
    monkeypatch.setattr(record, "_run", None)  # as in a fresh process
    _replay(monkeypatch, run, record_off=record_off)
    series("GDP")
    assert _run_folders(runs) == [run]
    assert len(_result_files(run)) == 1


def test_replay_announced_once(runs: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    series("GDP")
    series("CPI")
    run = _only_run(runs)
    _replay(monkeypatch, run)
    capsys.readouterr()
    series("GDP")
    series("CPI")
    series("GDP")
    assert capsys.readouterr().err.count(f"parsimony: replaying {run}") == 1


def _datetime_index() -> pd.DataFrame:
    # Dates parsed from a source, as connectors produce them (Parquet keeps the values, not a ``freq``).
    index = pd.DatetimeIndex(pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]), name="date")
    return pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=index)


def _multi_index() -> pd.DataFrame:
    idx = pd.MultiIndex.from_tuples([("US", 2020), ("US", 2021), ("EA", 2020)], names=["area", "year"])
    return pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=idx)


def _filtered_index() -> pd.DataFrame:
    df = pd.DataFrame({"value": np.arange(6.0)})
    return df[df["value"] % 2 == 0]


@pytest.mark.parametrize(
    "make", [_datetime_index, _multi_index, _filtered_index], ids=["datetime", "multi", "filtered"]
)
def test_replay_keeps_index(runs: Path, monkeypatch: pytest.MonkeyPatch, make: Any) -> None:
    _frames["k"] = make()
    original = shaped("k")
    _replay(monkeypatch, _only_run(runs))
    replayed = shaped("k")
    assert calls == {"shaped": 1}
    pd.testing.assert_frame_equal(replayed.frame, original.frame)
    pd.testing.assert_frame_equal(replayed.frame, _frames["k"])


def test_replay_large_params(runs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ids = [f"SERIES_{i:05d}" for i in range(300)]
    assert len(json.dumps(ids)) > 2000
    original = many(ids)
    _replay(monkeypatch, _only_run(runs))
    replayed = many(ids)
    assert calls == {"many": 1}
    pd.testing.assert_frame_equal(replayed.frame, original.frame)


def test_switching_replay_folder_in_process(runs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony import record

    series("GDP")
    first = _only_run(runs)
    monkeypatch.setattr(record, "_run", None)  # start a second run
    _override["series"] = pd.DataFrame({"value": [42.0]})
    series("GDP")
    (second,) = [r for r in _run_folders(runs) if r != first]

    _override.clear()
    _replay(monkeypatch, first)
    assert list(series("GDP").frame["value"]) == [1.0, 2.0]
    _replay(monkeypatch, second)
    assert list(series("GDP").frame["value"]) == [42.0]


def test_replay_prototype_numbered_names(runs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = series("GDP")
    run = _only_run(runs)
    (f,) = _result_files(run)
    f.rename(run / f"001-{f.name}")
    _replay(monkeypatch, run)
    calls.clear()
    pd.testing.assert_frame_equal(series("GDP").frame, original.frame)
    assert calls == {}


# ---------------------------------------------------------------------------
# run.json, in subprocesses
# ---------------------------------------------------------------------------

_SCRIPT = textwrap.dedent(
    """
    from parsimony.connector import connector

    @connector
    def f(x: int) -> dict:
        '''Doc.'''
        return {"x": x}

    f(1)
    """
)


def _env(root: Path, **extra: str) -> dict[str, str]:
    env = {**os.environ, "PARSIMONY_RECORD": str(root), **extra}
    env.pop("PARSIMONY_REPLAY", None)
    return env


def test_run_json_names_script(tmp_path: Path) -> None:
    script = tmp_path / "analysis.py"
    script.write_text(_SCRIPT)
    root = tmp_path / "runs"
    subprocess.run([sys.executable, str(script)], env=_env(root), check=True, cwd=tmp_path)
    info = json.loads((_only_run(root) / "run.json").read_text())
    assert info == {"entry": str(script.resolve()), "sha256": hashlib.sha256(script.read_bytes()).hexdigest()}


def test_run_json_null_entry_for_dash_c(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    subprocess.run([sys.executable, "-c", _SCRIPT], env=_env(root), check=True, cwd=tmp_path)
    assert json.loads((_only_run(root) / "run.json").read_text()) == {"entry": None}


def test_concurrent_processes_get_separate_folders(tmp_path: Path) -> None:
    script = tmp_path / "analysis.py"
    script.write_text(_SCRIPT)
    root = tmp_path / "runs"
    procs = [subprocess.Popen([sys.executable, str(script)], env=_env(root), cwd=tmp_path) for _ in range(2)]
    assert [p.wait() for p in procs] == [0, 0]
    folders = _run_folders(root)
    assert len(folders) == 2
    assert all(_RUN_NAME.fullmatch(f.name) for f in folders)


def test_run_json_names_notebook(tmp_path: Path) -> None:
    pytest.importorskip("nbformat")
    pytest.importorskip("nbconvert")
    pytest.importorskip("ipykernel")
    import nbformat

    nb_path = tmp_path / "analysis.ipynb"
    nbformat.write(nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell(_SCRIPT)]), nb_path)
    root = tmp_path / "runs"
    cmd = [sys.executable, "-m", "jupyter", "nbconvert", "--to", "notebook", "--execute", "--stdout", str(nb_path)]
    subprocess.run(cmd, env=_env(root, JPY_SESSION_NAME=str(nb_path)), check=True, cwd=tmp_path, capture_output=True)
    info = json.loads((_only_run(root) / "run.json").read_text())
    assert info["entry"] == str(nb_path.resolve())
