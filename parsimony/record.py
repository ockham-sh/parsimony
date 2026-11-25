"""Record every connector call; replay a recorded run.

Each process that calls a connector gets one run folder. Every call saves
its ``Result`` there, one file per distinct call: a table with
``Result.to_parquet`` (the data plus the provenance the framework already
builds), anything else JSON can hold as a ``.json`` file. A repeated call
replaces its file. ``run.json`` names the script or notebook that made the calls.

    PARSIMONY_RECORD=off | <folder>   switch off, or choose where runs go
    PARSIMONY_REPLAY=<run folder>     connector calls return that run's results
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from platformdirs import user_data_dir

from parsimony.result import Provenance, Result

logger = logging.getLogger(__name__)

_run: Path | None = None
_recorded: tuple[str, dict[str, Path]] | None = None  # (replayed folder, key -> file)
_lock = threading.Lock()  # connector calls fanned out over threads must share one run folder


def _key(source: str, params: dict[str, Any]) -> str:
    blob = json.dumps([source, params], sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:12]


def _run_dir() -> Path:
    with _lock:
        return _create_run_dir()


def _create_run_dir() -> Path:
    global _run
    if _run is None or not _run.is_dir():  # first call, or the folder was deleted under us
        root = Path(os.environ.get("PARSIMONY_RECORD") or Path(user_data_dir("parsimony")) / "runs").expanduser()
        _run = root / f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{os.urandom(3).hex()}"
        _run.mkdir(parents=True)
        ipython = getattr(sys.modules.get("IPython"), "get_ipython", lambda: None)()
        notebook = os.environ.get("JPY_SESSION_NAME") or getattr(ipython, "user_ns", {}).get("__vsc_ipynb_file__")
        entry = Path(notebook or ("" if ipython else sys.argv[0]))  # Jupyter, VS Code, or a script
        info: dict[str, Any] = {"entry": None}
        if entry.is_file():
            info = {"entry": str(entry.resolve()), "sha256": hashlib.sha256(entry.read_bytes()).hexdigest()}
        (_run / "run.json").write_text(json.dumps(info))
    return _run


def _write(path: Path, write: Callable[[Path], object]) -> None:
    """Write through a hidden temporary file, so a reader never sees half a file."""
    tmp = path.with_name(f".{path.name}.{os.urandom(3).hex()}.tmp")
    try:
        write(tmp)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def save(result: Result) -> None:
    """Save a fetched result in this process's run folder. Never raises."""
    if os.environ.get("PARSIMONY_RECORD") == "off":
        return
    try:
        p = result.provenance
        stem = _run_dir() / f"{p.source}-{_key(p.source, p.params)}"
        if result.is_tabular:
            _write(Path(f"{stem}.parquet"), result.to_parquet)
        else:  # dict, list, str, number: anything JSON can hold
            text = json.dumps({"provenance": p.safe_dump(), "raw": result.raw})
            _write(Path(f"{stem}.json"), lambda tmp: tmp.write_text(text))
    except Exception as exc:  # noqa: BLE001 - recording must not break a fetch
        logger.warning("parsimony: call not recorded (%s: %s)", type(exc).__name__, exc)


def load(source: str, params: dict[str, Any]) -> Result | None:
    """The recorded result for this call when PARSIMONY_REPLAY is set, else None."""
    global _recorded
    setting = os.environ.get("PARSIMONY_REPLAY")
    if not setting:
        return None
    run = str(Path(setting).expanduser())
    if _recorded is None or _recorded[0] != run:
        files = [f for f in sorted(Path(run).glob("*-*.*")) if f.suffix in (".parquet", ".json")]
        _recorded = (run, {f.stem.rsplit("-", 1)[1]: f for f in files if not f.name.startswith(".")})
        logger.warning("parsimony: replaying %s", run)
    key = _key(source, params)
    if key not in _recorded[1]:
        raise LookupError(f"{source}({params}) is not in the recorded run {run}; unset PARSIMONY_REPLAY to fetch live")
    file = _recorded[1][key]
    if file.suffix == ".parquet":
        return Result.from_parquet(file)
    saved = json.loads(file.read_text())
    return Result(raw=saved["raw"], provenance=Provenance.model_validate(saved["provenance"]))
