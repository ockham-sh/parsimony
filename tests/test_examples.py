"""Smoke tests that shipped examples still run against the current API.

An example under ``examples/`` is documentation with executable syntax: ruff
lint/format catches syntax drift, but nothing catches an example calling a
removed keyword argument or a renamed attribute until a user runs it and hits
a traceback (parsimony#93). Actually executing the walkthrough here closes
that gap for the offline, no-network, no-API-key examples.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"


def test_catalog_walkthrough_runs_to_completion(_pin_parsimony_cache_dir: Path) -> None:
    """``examples/catalog_walkthrough.py`` is offline and keyless — it must exit 0."""
    result = subprocess.run(
        [sys.executable, str(EXAMPLES_DIR / "catalog_walkthrough.py")],
        cwd=EXAMPLES_DIR.parent,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    assert "All stages completed successfully." in result.stdout
