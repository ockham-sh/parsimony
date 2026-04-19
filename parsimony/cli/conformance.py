"""``parsimony conformance verify`` subcommand.

Runs the full conformance suite against an installed plugin distribution
and emits a machine-readable JSON report. Usable as a regulated-finance
security-review artefact — exit code is the contract:

- ``0`` — every registered entry point in the named distribution conforms.
- ``1`` — at least one entry point failed conformance or could not be loaded.
- ``2`` — the named distribution is not installed.
"""

from __future__ import annotations

import importlib.metadata
import json
import sys
from dataclasses import dataclass, field
from typing import Any, TextIO

from parsimony.discovery.errors import PluginContractError, PluginImportError

__all__ = ["run"]


# ---------------------------------------------------------------------------
# Report shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _EntryPointReport:
    name: str
    module: str
    status: str  # "pass" | "fail" | "skip"
    reason: str | None
    conformance_detail: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "module": self.module,
            "status": self.status,
            "reason": self.reason,
            "conformance_detail": self.conformance_detail,
        }


@dataclass(frozen=True)
class _Report:
    distribution: str
    distribution_version: str | None
    entry_points: list[_EntryPointReport] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.entry_points) and all(
            ep.status == "pass" for ep in self.entry_points
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "distribution": self.distribution,
            "distribution_version": self.distribution_version,
            "passed": self.passed,
            "entry_points": [ep.to_dict() for ep in self.entry_points],
        }


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def _verify_entry_point(
    ep: importlib.metadata.EntryPoint,
) -> _EntryPointReport:
    """Load one entry point, run conformance against the loaded module.

    All plugin-contract failures expose a uniform ``to_report_dict()``
    shape (check / module_path / reason / next_action) so downstream
    consumers see one consistent structure regardless of which exception
    raised. Plain import exceptions are wrapped in a :class:`PluginImportError`
    for the same reason.
    """
    # Lazy import to avoid circular imports and to keep CLI cold-start small.
    from parsimony.testing import ConformanceError, assert_plugin_valid

    try:
        module = ep.load()
    except Exception as exc:
        # Plugin init runs arbitrary code; trap Exception so one bad plugin
        # does not fail the whole verification run. KeyboardInterrupt and
        # SystemExit deliberately propagate — users must be able to Ctrl+C.
        return _report_from(ep, PluginImportError(ep.value, exc))

    try:
        assert_plugin_valid(module)
    except (ConformanceError, PluginContractError, PluginImportError) as exc:
        return _report_from(ep, exc)

    return _EntryPointReport(
        name=ep.name,
        module=ep.value,
        status="pass",
        reason=None,
        conformance_detail=None,
    )


def _report_from(
    ep: importlib.metadata.EntryPoint,
    exc: Any,
) -> _EntryPointReport:
    """Build a failure report from any error type that exposes to_report_dict().

    Uses the structured ``check`` field as the top-level reason and the
    message as conformance_detail so operators see both the aspect that
    failed and the specific diagnostic.
    """
    fields = exc.to_report_dict()
    check = fields.get("check") or "contract"
    return _EntryPointReport(
        name=ep.name,
        module=ep.value,
        status="fail",
        reason=f"{check} failed",
        conformance_detail=str(exc),
    )


def _build_report(distribution_name: str) -> _Report | None:
    """Enumerate entry points for *distribution_name* and verify each.

    Returns ``None`` iff the distribution is not installed.
    """
    try:
        dist = importlib.metadata.distribution(distribution_name)
    except importlib.metadata.PackageNotFoundError:
        return None

    entry_points = [
        ep for ep in dist.entry_points if ep.group == "parsimony.providers"
    ]

    reports = [_verify_entry_point(ep) for ep in entry_points]
    return _Report(
        distribution=dist.metadata["Name"],
        distribution_version=dist.version,
        entry_points=reports,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(
    *,
    distribution_name: str,
    stream: TextIO | None = None,
) -> int:
    """Run ``parsimony conformance verify <distribution_name>``.

    Emits a JSON report on *stream* (defaults to stdout) and returns the
    process exit code.
    """
    out = stream if stream is not None else sys.stdout
    report = _build_report(distribution_name)
    if report is None:
        json.dump(
            {
                "distribution": distribution_name,
                "distribution_version": None,
                "passed": False,
                "error": "not_installed",
                "entry_points": [],
            },
            out,
            indent=2,
            sort_keys=True,
        )
        out.write("\n")
        return 2

    json.dump(report.to_dict(), out, indent=2, sort_keys=True)
    out.write("\n")
    return 0 if report.passed else 1
