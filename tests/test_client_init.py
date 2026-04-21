"""Tests for ``parsimony.load_dotenv``, ``parsimony._cap_cell``, and the
``parsimony.client`` lazy-getter wiring (auto ``.env`` + auto stderr summary).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import parsimony
from parsimony import _cap_cell, _emit_fetch_summary, load_dotenv
from parsimony.connector import Connectors
from parsimony.result import Provenance, Result


@pytest.fixture(autouse=True)
def _reset_client_cache() -> None:
    """Ensure each test sees a fresh lazy ``client`` cache."""
    parsimony._client_cache = None


# ---------------------------------------------------------------------------
# _cap_cell
# ---------------------------------------------------------------------------


class TestCapCell:
    def test_short_string_passes_through(self) -> None:
        assert _cap_cell("abc") == "abc"

    def test_long_string_truncated_to_max_chars(self) -> None:
        long = "x" * 600
        capped = _cap_cell(long, max_chars=500)
        assert isinstance(capped, str)
        assert len(capped) == 500
        assert capped.endswith("…")

    def test_default_max_chars_is_500(self) -> None:
        long = "x" * 600
        assert len(_cap_cell(long)) == 500

    def test_int_passes_through(self) -> None:
        assert _cap_cell(42) == 42

    def test_float_passes_through(self) -> None:
        assert _cap_cell(3.14) == 3.14

    def test_none_passes_through(self) -> None:
        assert _cap_cell(None) is None

    def test_dict_passes_through(self) -> None:
        d = {"a": 1}
        assert _cap_cell(d) is d


# ---------------------------------------------------------------------------
# load_dotenv
# ---------------------------------------------------------------------------


class TestLoadDotenv:
    def test_loads_env_at_git_anchor(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / ".git").mkdir()
        (tmp_path / ".env").write_text("MY_KEY=loaded\n")
        sub = tmp_path / "sub" / "deep"
        sub.mkdir(parents=True)
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.chdir(sub)
        monkeypatch.delenv("MY_KEY", raising=False)

        load_dotenv()

        assert os_environ_get("MY_KEY") == "loaded"

    def test_loads_env_at_pyproject_anchor(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
        (tmp_path / ".env").write_text("PY_KEY=via_pyproject\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("PY_KEY", raising=False)

        load_dotenv()

        assert os_environ_get("PY_KEY") == "via_pyproject"

    def test_loads_env_at_mcp_json_anchor(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / ".mcp.json").write_text("{}")
        (tmp_path / ".env").write_text("MCP_KEY=via_mcp\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MCP_KEY", raising=False)

        load_dotenv()

        assert os_environ_get("MCP_KEY") == "via_mcp"

    def test_does_not_override_pre_existing_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / ".git").mkdir()
        (tmp_path / ".env").write_text("EXISTING=from_dotenv\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("EXISTING", "from_shell")

        load_dotenv()

        assert os_environ_get("EXISTING") == "from_shell"

    def test_stops_at_anchor_does_not_pick_up_ancestor_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Ancestor .env that we should NOT pick up because the inner
        # directory is itself a project root with its own .git.
        (tmp_path / ".env").write_text("ANCESTOR=should_not_load\n")
        inner = tmp_path / "inner"
        inner.mkdir()
        (inner / ".git").mkdir()  # inner is a project root, no .env here
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.chdir(inner)
        monkeypatch.delenv("ANCESTOR", raising=False)

        load_dotenv()

        assert os_environ_get("ANCESTOR") is None

    def test_no_op_when_cwd_outside_home(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Home is one place; cwd is a sibling outside it. Walk should be a no-op.
        home = tmp_path / "home"
        home.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / ".git").mkdir()
        (outside / ".env").write_text("OUTSIDE=leaked\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
        monkeypatch.chdir(outside)
        monkeypatch.delenv("OUTSIDE", raising=False)

        load_dotenv()

        assert os_environ_get("OUTSIDE") is None

    def test_idempotent(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / ".git").mkdir()
        (tmp_path / ".env").write_text("REPEAT=ok\n")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("REPEAT", raising=False)

        load_dotenv()
        load_dotenv()
        load_dotenv()

        assert os_environ_get("REPEAT") == "ok"


# ---------------------------------------------------------------------------
# _emit_fetch_summary
# ---------------------------------------------------------------------------


class TestEmitFetchSummary:
    def _make_result(self, data: object) -> Result:
        return Result(
            data=data,
            provenance=Provenance(source="fred_fetch", params={"series_id": "UNRATE"}),
        )

    def test_dataframe_emits_toon_to_stderr(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PARSIMONY_QUIET", raising=False)
        df = pd.DataFrame({"date": ["2024-01", "2024-02"], "value": [3.5, 3.6]})
        _emit_fetch_summary(self._make_result(df))

        err = capsys.readouterr().err
        assert "source: fred_fetch" in err
        assert "series_id: UNRATE" in err
        assert "rows: 2" in err
        assert "preview" in err
        assert "2024-01" in err

    def test_quiet_env_suppresses_emit(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PARSIMONY_QUIET", "1")
        df = pd.DataFrame({"a": [1, 2, 3]})
        _emit_fetch_summary(self._make_result(df))

        assert capsys.readouterr().err == ""

    def test_dataframe_head_capped_to_5(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PARSIMONY_QUIET", raising=False)
        df = pd.DataFrame({"x": list(range(100))})
        _emit_fetch_summary(self._make_result(df))

        err = capsys.readouterr().err
        assert "rows: 100" in err
        # Only first 5 row values in the preview body.
        for n in range(5):
            assert f"\n  {n}\n" in err or f",{n}\n" in err or err.count(str(n))
        assert "99" not in err

    def test_long_cell_truncated_in_preview(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PARSIMONY_QUIET", raising=False)
        long = "y" * 600
        df = pd.DataFrame({"text": [long]})
        _emit_fetch_summary(self._make_result(df))

        err = capsys.readouterr().err
        assert long not in err  # the full 600-char string should NOT appear
        assert "…" in err

    def test_scalar_emits_value_line(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PARSIMONY_QUIET", raising=False)
        _emit_fetch_summary(self._make_result("just-a-string"))

        err = capsys.readouterr().err
        assert "value: just-a-string" in err

    def test_series_emits_preview(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PARSIMONY_QUIET", raising=False)
        s = pd.Series([1.5, 2.5, 3.5], name="value")
        _emit_fetch_summary(self._make_result(s))

        err = capsys.readouterr().err
        assert "rows: 3" in err
        assert "preview" in err


# ---------------------------------------------------------------------------
# Lazy `client` getter wiring
# ---------------------------------------------------------------------------


class TestClientLazyGetter:
    def test_first_access_calls_load_dotenv_and_wraps_with_callback(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        called: dict[str, int] = {"load_dotenv": 0, "build": 0}

        def _fake_load() -> None:
            called["load_dotenv"] += 1

        fake_connectors = Connectors([])

        def _fake_build():
            called["build"] += 1
            return fake_connectors

        monkeypatch.setattr(parsimony, "load_dotenv", _fake_load)
        monkeypatch.setattr(
            "parsimony.discovery.build_connectors_from_env", _fake_build
        )

        client = parsimony.client

        assert called["load_dotenv"] == 1
        assert called["build"] == 1
        assert isinstance(client, Connectors)
        # with_callback returns a new Connectors with the observer wired.
        # We can't introspect the private _callbacks tuple via public API,
        # so settle for verifying it's a different instance (with_callback
        # returns a new Connectors).
        assert client is not fake_connectors

    def test_second_access_returns_cached(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        build_calls = {"n": 0}

        def _fake_build():
            build_calls["n"] += 1
            return Connectors([])

        monkeypatch.setattr(parsimony, "load_dotenv", lambda: None)
        monkeypatch.setattr(
            "parsimony.discovery.build_connectors_from_env", _fake_build
        )

        first = parsimony.client
        second = parsimony.client

        assert first is second
        assert build_calls["n"] == 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def os_environ_get(key: str) -> str | None:
    import os

    return os.environ.get(key)
