"""Behavioral tests for ``parsimony bundles`` CLI verbs.

Covers each verb's contract — argparse wiring, exit codes, --json schema
shape, --only filter, --fail-fast short-circuit, --pin SHA validation,
did-you-mean. The real argparse parser is exercised via ``add_subparser``;
only the target/store/provider boundary is stubbed.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from parsimony.bundles.discovery import DiscoveredSpec
from parsimony.bundles.spec import (
    CatalogPlan,
    CatalogSpec,
)
from parsimony.cli import bundles as cli
from parsimony.connector import Connectors, enumerator
from parsimony.plugins.discovery import DiscoveredProvider
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="example"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)


class _NoParams(BaseModel):
    pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest="verb")
    cli.add_subparser(subs)
    return parser


def _make_static_discovered(*, namespace: str, plugin_name: str = "fake") -> DiscoveredSpec:
    @enumerator(output=_OUTPUT, catalog=CatalogSpec.static(namespace=namespace))
    async def list_things(params: _NoParams) -> Result:
        """List."""
        import pandas as pd
        return Result.from_dataframe(
            pd.DataFrame({"code": ["a"], "title": ["A"]}),
            Provenance(source=namespace),
        )

    provider = DiscoveredProvider(
        name=plugin_name,
        module_path=f"parsimony_fake_{plugin_name}",
        connectors=Connectors([list_things]),
    )
    spec = list_things.properties["catalog"]
    return DiscoveredSpec(provider=provider, connector=list_things, spec=spec)


def _make_dynamic_discovered(*, plugin_name: str, namespaces: list[str]) -> DiscoveredSpec:
    async def _plan() -> AsyncIterator[CatalogPlan]:
        for ns in namespaces:
            yield CatalogPlan(namespace=ns)

    @enumerator(output=_OUTPUT, catalog=CatalogSpec(plan=_plan))
    async def list_dyn(params: _NoParams) -> Result:
        """List dynamic."""
        import pandas as pd
        return Result.from_dataframe(
            pd.DataFrame({"code": ["a"], "title": ["A"]}),
            Provenance(source="dyn"),
        )

    provider = DiscoveredProvider(
        name=plugin_name,
        module_path=f"parsimony_fake_{plugin_name}",
        connectors=Connectors([list_dyn]),
    )
    spec = list_dyn.properties["catalog"]
    return DiscoveredSpec(provider=provider, connector=list_dyn, spec=spec)


@pytest.fixture
def stub_iter_specs(monkeypatch):
    def _set(specs: list[DiscoveredSpec]):
        stub = lambda: iter(specs)  # noqa: E731
        monkeypatch.setattr("parsimony.cli.bundles.list_cmd.iter_specs", stub)
        monkeypatch.setattr("parsimony.cli.bundles.selection.iter_specs", stub)

    return _set


# ---------------------------------------------------------------------------
# `list` verb
# ---------------------------------------------------------------------------


class TestListVerb:
    def test_empty_returns_zero_and_helpful_text(self, stub_iter_specs, capsys):
        stub_iter_specs([])
        parser = _build_parser()
        args = parser.parse_args(["bundles", "list"])
        rc = cli.run(args)
        assert rc == 0
        assert "No catalogs discovered" in capsys.readouterr().out

    def test_renders_human_table(self, stub_iter_specs, capsys):
        stub_iter_specs([_make_static_discovered(namespace="alpha")])
        parser = _build_parser()
        args = parser.parse_args(["bundles", "list"])
        rc = cli.run(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "NAMESPACE" in out and "alpha" in out

    def test_json_output_one_record_per_row(self, stub_iter_specs, capsys):
        stub_iter_specs([
            _make_static_discovered(namespace="alpha"),
            _make_static_discovered(namespace="beta", plugin_name="other"),
        ])
        parser = _build_parser()
        args = parser.parse_args(["bundles", "list", "--json"])
        rc = cli.run(args)
        assert rc == 0
        lines = [
            line for line in capsys.readouterr().out.splitlines() if line.strip()
        ]
        assert len(lines) == 2
        for line in lines:
            row = json.loads(line)
            # Stable schema — every field present.
            assert set(row) == {"namespace", "plugin", "kind", "target", "connector"}


# ---------------------------------------------------------------------------
# `plan` verb
# ---------------------------------------------------------------------------


class TestPlanVerb:
    def test_dynamic_plan_yields_namespace_list(self, stub_iter_specs, capsys):
        spec = _make_dynamic_discovered(plugin_name="dyn_p", namespaces=["one", "two", "three"])
        stub_iter_specs([spec])
        parser = _build_parser()
        args = parser.parse_args(["bundles", "plan", spec.connector.name, "--json"])
        rc = cli.run(args)
        assert rc == 0
        lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
        assert len(lines) == 3
        names = {json.loads(line)["namespace"] for line in lines}
        assert names == {"one", "two", "three"}

    def test_unknown_connector_did_you_mean(self, stub_iter_specs, capsys):
        stub_iter_specs([_make_static_discovered(namespace="alpha")])
        parser = _build_parser()
        # Connector name is auto-derived from the function ("list_things").
        # Pass a near-miss so the difflib fuzzy match fires.
        args = parser.parse_args(["bundles", "plan", "list_thing"])
        rc = cli.run(args)
        assert rc == 64
        err = capsys.readouterr().err
        assert "not found" in err
        assert "Did you mean" in err


# ---------------------------------------------------------------------------
# `build` verb (--only filter, --fail-fast, did-you-mean)
# ---------------------------------------------------------------------------


@pytest.fixture
def stub_provider_and_build(monkeypatch):
    """Stub out the embedding provider construction and the build call."""

    class _FakeProvider:
        dimension = 8
        model_id = "sentence-transformers/all-MiniLM-L6-v2"
        revision = "0" * 40

    def fake_build_provider():
        return _FakeProvider()

    monkeypatch.setattr(
        "parsimony.cli.bundles.fanout._build_provider_from_env", fake_build_provider
    )

    built: list[str] = []

    async def fake_build_bundle_dir(*, namespace, plans, runner, out_dir, provider, embed_batch_size, **_):
        built.append(namespace)

        class _Manifest:
            def __init__(self):
                self.namespace = namespace
                self.entry_count = 1
                self.embedding_model = "sentence-transformers/all-MiniLM-L6-v2"
                self.embedding_model_revision = "0" * 40
                self.entries_sha256 = "a" * 64
                self.index_sha256 = "b" * 64

        return _Manifest()

    monkeypatch.setattr("parsimony.cli.bundles.fanout.build_bundle_dir", fake_build_bundle_dir)
    return built


class TestBuildVerb:
    def test_unknown_selector_did_you_mean(self, stub_iter_specs, capsys):
        stub_iter_specs([_make_static_discovered(namespace="alpha")])
        parser = _build_parser()
        args = parser.parse_args(["bundles", "build", "alpa"])
        rc = cli.run(args)
        assert rc == 64
        assert "Did you mean" in capsys.readouterr().err

    def test_only_filter_restricts_namespaces(
        self, stub_iter_specs, stub_provider_and_build, tmp_path, capsys
    ):
        stub_iter_specs([
            _make_static_discovered(namespace="alpha"),
            _make_static_discovered(namespace="beta", plugin_name="b"),
            _make_static_discovered(namespace="gamma", plugin_name="g"),
        ])
        parser = _build_parser()
        args = parser.parse_args([
            "bundles", "build",
            "--out", str(tmp_path),
            "--only", "alpha,gamma",
        ])
        rc = cli.run(args)
        assert rc == 0
        assert sorted(stub_provider_and_build) == ["alpha", "gamma"]

    def test_fail_fast_aborts_on_first_failure(
        self, stub_iter_specs, monkeypatch, tmp_path, capsys
    ):
        stub_iter_specs([
            _make_static_discovered(namespace="alpha"),
            _make_static_discovered(namespace="beta", plugin_name="b"),
        ])

        class _FakeProvider:
            dimension = 8
            model_id = "sentence-transformers/all-MiniLM-L6-v2"
            revision = "0" * 40

        monkeypatch.setattr(
            "parsimony.cli.bundles.fanout._build_provider_from_env", lambda: _FakeProvider()
        )
        attempted: list[str] = []

        async def fail_build(*, namespace, **_):
            attempted.append(namespace)
            raise RuntimeError(f"boom-{namespace}")

        monkeypatch.setattr("parsimony.cli.bundles.fanout.build_bundle_dir", fail_build)

        parser = _build_parser()
        args = parser.parse_args([
            "bundles", "build",
            "--out", str(tmp_path),
            "--fail-fast",
        ])
        rc = cli.run(args)
        assert rc == 2
        # fail-fast must abort before reaching beta.
        assert attempted == ["alpha"]


# ---------------------------------------------------------------------------
# `publish` verb (--dry-run skips upload, env-confirmed bypasses prompt)
# ---------------------------------------------------------------------------


class TestPublishVerb:
    def test_dry_run_does_not_construct_target(
        self, stub_iter_specs, stub_provider_and_build, monkeypatch, tmp_path, capsys
    ):
        stub_iter_specs([_make_static_discovered(namespace="alpha")])

        class _SentinelTarget:
            def __init__(self, **kwargs):
                raise AssertionError("HFBundleTarget must not be constructed during --dry-run")

            async def publish(self, *args, **kwargs):
                raise AssertionError("publish must not run during --dry-run")

        monkeypatch.setattr("parsimony.bundles.targets.HFBundleTarget", _SentinelTarget)

        parser = _build_parser()
        args = parser.parse_args(["bundles", "publish", "alpha", "--dry-run"])
        rc = cli.run(args)
        assert rc == 0
        assert "alpha" in stub_provider_and_build


# ---------------------------------------------------------------------------
# `eval` verb — --pin must be a 40-char SHA at argparse time
# ---------------------------------------------------------------------------


class TestEvalPinValidation:
    def test_short_pin_rejected_by_argparse(self, stub_iter_specs):
        stub_iter_specs([])
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "bundles", "eval", "alpha",
                "--queries", "x.jsonl",
                "--pin", "main",
            ])

    def test_branch_name_pin_rejected_by_argparse(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "bundles", "eval", "alpha",
                "--queries", "x.jsonl",
                "--pin", "release-1.0",
            ])

    def test_full_sha_pin_accepted(self):
        parser = _build_parser()
        args = parser.parse_args([
            "bundles", "eval", "alpha",
            "--queries", "x.jsonl",
            "--pin", "0" * 40,
        ])
        assert args.pin == "0" * 40

    def test_uppercase_sha_rejected(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "bundles", "eval", "alpha",
                "--queries", "x.jsonl",
                "--pin", "A" * 40,
            ])


# ---------------------------------------------------------------------------
# Run dispatch — unknown verb returns 64
# ---------------------------------------------------------------------------


def test_run_unknown_verb_returns_64(capsys):
    args = argparse.Namespace(bundles_verb="nonsense")
    rc = cli.run(args)
    assert rc == 64
    assert "Unknown" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# _emit_event surfaces next_action on its own line
# ---------------------------------------------------------------------------


def test_emit_event_prints_next_action_line(capsys):
    report = {
        "namespace": "alpha",
        "status": "failed",
        "elapsed_s": 0.1,
        "error_message": "BundleTooLargeError: oops",
        "next_action": "Split the namespace via a fan-out plan generator",
    }
    from parsimony.cli.bundles.fanout import _emit_event
    _emit_event(report, json_output=False)
    captured = capsys.readouterr()
    assert "→ next: Split the namespace" in captured.err


def test_emit_event_json_does_not_split_lines(capsys):
    report = {
        "namespace": "alpha",
        "status": "failed",
        "elapsed_s": 0.1,
        "error_message": "x",
        "next_action": "do thing",
    }
    from parsimony.cli.bundles.fanout import _emit_event
    _emit_event(report, json_output=True)
    out = capsys.readouterr().out.strip()
    parsed = json.loads(out)
    assert parsed["next_action"] == "do thing"


# ---------------------------------------------------------------------------
# Confirmation gate prints MODE summary
# ---------------------------------------------------------------------------


def test_confirm_publish_prints_mode_summary(capsys, stub_iter_specs, monkeypatch):
    monkeypatch.setattr(
        "parsimony.cli.bundles.publish_cmd.fetch_published_entry_count",
        lambda namespace: 100,
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_: "n")
    spec = _make_static_discovered(namespace="alpha")
    from parsimony.cli.bundles.publish_cmd import _confirm_publish
    approved = _confirm_publish(
        discovered=[spec],
        yes_flag=False,
        env_confirmed=False,
        allow_shrink=False,
        fail_fast=True,
    )
    assert approved is False
    out = capsys.readouterr().out
    assert "MODE" in out
    assert "allow-shrink=N" in out
    assert "fail-fast=Y" in out
    assert "100" in out  # current entry count shown
