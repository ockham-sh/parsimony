"""Sequential build/publish fan-out shared by ``build`` and ``publish`` verbs.

Owns the operational-error allowlist, per-bundle timeout enforcement, and
structured success/failure reporting consumed by ``--json`` consumers and
the human renderer.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

from parsimony.bundles.build import PlanRunner, build_bundle_dir
from parsimony.bundles.discovery import DiscoveredSpec
from parsimony.bundles.errors import BundleError
from parsimony.bundles.spec import CatalogPlan
from parsimony.cli.bundles._shared import (
    _PLAN_GEN_TIMEOUT_S,
    _build_provider_from_env,
    _materialize_with_timeout,
)
from parsimony.cli.bundles.selection import _spec_namespace


def _add_failure_flags(p: argparse.ArgumentParser) -> None:
    """Attach the failure-semantics + recovery flag set to a subparser."""
    p.add_argument(
        "--bundle-timeout",
        type=float,
        default=600.0,
        help="Per-bundle wall-clock cap in seconds (default: 600). "
        "A bundle exceeding this is failed and the orchestrator continues.",
    )
    p.add_argument(
        "--fail-fast",
        action="store_true",
        help="Abort the fan-out on first failure instead of continuing.",
    )
    p.add_argument(
        "--resume-from",
        default=None,
        help="Skip every bundle whose namespace sorts before this value (alphabetical).",
    )
    p.add_argument(
        "--only",
        default=None,
        help="Comma-separated list of namespaces; build only these.",
    )


def _parse_only(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    return {item.strip() for item in raw.split(",") if item.strip()}


# Operational error types that the fan-out catches and turns into a failure
# report. Programmer errors (AttributeError, KeyError, TypeError, …) MUST
# propagate so misconfigured fan-outs abort instead of being silently
# swallowed into a per-bundle failure row.
_FANOUT_OPERATIONAL_EXCEPTIONS: tuple[type[BaseException], ...] = (
    BundleError,
    OSError,
    RuntimeError,
    TimeoutError,
)


def _maybe_hf_hub_http_error() -> tuple[type[BaseException], ...]:
    """Optionally include ``HfHubHTTPError`` in the operational set if installed."""
    try:
        from huggingface_hub.utils import HfHubHTTPError
    except ImportError:
        return ()
    return (HfHubHTTPError,)


_FANOUT_OPERATIONAL = _FANOUT_OPERATIONAL_EXCEPTIONS + _maybe_hf_hub_http_error()


async def _fanout_build(
    *,
    discovered: list[DiscoveredSpec],
    out_root: Path,
    timestamp: str,
    embed_batch_size: int,
    json_output: bool,
    publish: bool,
    allow_shrink: bool = False,
    bundle_timeout_s: float = 600.0,
    fail_fast: bool = False,
    resume_from: str | None = None,
    only: set[str] | None = None,
) -> int:
    """Sequentially build (and optionally publish) every selected bundle.

    Failure semantics:

    - Every bundle runs under :func:`asyncio.timeout` of ``bundle_timeout_s``.
    - Operational failures (HfHubHTTPError, OSError, BundleError, RuntimeError,
      TimeoutError) are caught into a structured report; the loop continues
      unless ``fail_fast`` is set.
    - Programmer errors (AttributeError, KeyError, TypeError, …) propagate
      so misconfigured fan-outs abort.
    - ``resume_from`` skips namespaces lexicographically before the value.
    - ``only`` (a set) restricts execution to its members.
    """
    from parsimony.bundles.targets import HFBundleTarget

    try:
        provider = _build_provider_from_env()
    except (RuntimeError, ValueError) as exc:
        print(f"Embedding provider construction failed: {exc}", file=sys.stderr)
        return 3

    target = HFBundleTarget() if publish else None

    reports: list[dict[str, Any]] = []
    failures = 0
    successes = 0

    for d in discovered:
        spec = d.spec
        try:
            plans = await _materialize_with_timeout(spec)
        except TimeoutError:
            failures += 1
            report = _failure_report(
                _spec_namespace(spec),
                exc=TimeoutError(f"plan generator exceeded {_PLAN_GEN_TIMEOUT_S}s"),
                elapsed_s=0.0,
            )
            reports.append(report)
            _emit_event(report, json_output)
            if fail_fast:
                break
            continue
        except _FANOUT_OPERATIONAL as exc:
            failures += 1
            report = _failure_report(
                _spec_namespace(spec),
                exc=exc,
                elapsed_s=0.0,
            )
            reports.append(report)
            _emit_event(report, json_output)
            if fail_fast:
                break
            continue

        plans = [p for p in plans if _plan_in_scope(p, resume_from=resume_from, only=only)]

        plans_broke_out = False
        for plan in plans:
            namespace = plan.namespace
            bundle_dir = out_root / namespace / timestamp
            t0 = time.monotonic()
            try:
                async with asyncio.timeout(bundle_timeout_s):
                    manifest = await build_bundle_dir(
                        namespace=namespace,
                        plans=[plan],
                        runner=_runner_for(d.connector),
                        out_dir=bundle_dir,
                        provider=provider,
                        embed_batch_size=embed_batch_size,
                    )
            except TimeoutError:
                failures += 1
                report = _failure_report(
                    namespace,
                    exc=TimeoutError(f"bundle build exceeded {bundle_timeout_s}s"),
                    elapsed_s=time.monotonic() - t0,
                )
                reports.append(report)
                _emit_event(report, json_output)
                if fail_fast:
                    plans_broke_out = True
                    break
                continue
            except _FANOUT_OPERATIONAL as exc:
                failures += 1
                report = _failure_report(
                    namespace,
                    exc=exc,
                    elapsed_s=time.monotonic() - t0,
                )
                reports.append(report)
                _emit_event(report, json_output)
                if fail_fast:
                    plans_broke_out = True
                    break
                continue

            if publish and target is not None:
                try:
                    async with asyncio.timeout(bundle_timeout_s):
                        commit_sha = await target.publish(
                            bundle_dir,
                            namespace=namespace,
                            allow_shrink=allow_shrink,
                        )
                except TimeoutError:
                    failures += 1
                    report = _failure_report(
                        namespace,
                        exc=TimeoutError(f"bundle publish exceeded {bundle_timeout_s}s"),
                        elapsed_s=time.monotonic() - t0,
                    )
                    reports.append(report)
                    _emit_event(report, json_output)
                    if fail_fast:
                        plans_broke_out = True
                        break
                    continue
                except _FANOUT_OPERATIONAL as exc:
                    failures += 1
                    report = _failure_report(
                        namespace,
                        exc=exc,
                        elapsed_s=time.monotonic() - t0,
                    )
                    reports.append(report)
                    _emit_event(report, json_output)
                    if fail_fast:
                        plans_broke_out = True
                        break
                    continue
                report = _success_report(
                    manifest,
                    elapsed_s=time.monotonic() - t0,
                    status="published",
                    commit_sha=commit_sha,
                )
            else:
                report = _success_report(
                    manifest,
                    elapsed_s=time.monotonic() - t0,
                    status="ok",
                )
            successes += 1
            reports.append(report)
            _emit_event(report, json_output)
        if plans_broke_out:
            break

    if not json_output:
        print()
        print(f"Done. {successes} succeeded, {failures} failed.")

    if failures and not successes:
        return 2
    if failures:
        return 1
    return 0


def _plan_in_scope(
    plan: CatalogPlan,
    *,
    resume_from: str | None,
    only: set[str] | None,
) -> bool:
    if only is not None and plan.namespace not in only:
        return False
    return not (resume_from is not None and plan.namespace < resume_from)


def _success_report(
    manifest: Any,
    *,
    elapsed_s: float,
    status: str,
    commit_sha: str | None = None,
) -> dict[str, Any]:
    """Construct the structured per-bundle build/publish report row."""
    report: dict[str, Any] = {
        "namespace": manifest.namespace,
        "status": status,
        "entry_count": manifest.entry_count,
        "elapsed_s": round(elapsed_s, 3),
        "embedding_model": manifest.embedding_model,
        "embedding_model_revision": manifest.embedding_model_revision,
        "entries_sha256": manifest.entries_sha256,
        "index_sha256": manifest.index_sha256,
    }
    if commit_sha is not None:
        report["commit_sha"] = commit_sha
    return report


def _failure_report(
    namespace: str,
    *,
    exc: BaseException,
    elapsed_s: float,
) -> dict[str, Any]:
    """Construct a per-bundle failure report row.

    Lifts ``next_action`` out of :class:`BundleError` so the operator-facing
    output can render the recovery hint as its own line.
    """
    from parsimony.bundles.safety import format_exc_chain

    report: dict[str, Any] = {
        "namespace": namespace,
        "status": "failed",
        "error_class": type(exc).__name__,
        "error_message": format_exc_chain(exc),
        "elapsed_s": round(elapsed_s, 3),
    }
    next_action = getattr(exc, "next_action", None)
    if isinstance(next_action, str) and next_action:
        report["next_action"] = next_action
    return report


def _emit_event(report: dict[str, Any], json_output: bool) -> None:
    if json_output:
        print(json.dumps(report, sort_keys=True))
        return
    ns = report.get("namespace", "?")
    status = report.get("status", "?")
    elapsed = report.get("elapsed_s", "?")
    if status in {"ok", "published"}:
        entries = report.get("entry_count", "?")
        print(f"[{status}] {ns}  entries={entries}  elapsed_s={elapsed}")
        return
    err = report.get("error_message", "")
    print(f"[{status}] {ns}  elapsed_s={elapsed}  error={err}", file=sys.stderr)
    next_action = report.get("next_action")
    if isinstance(next_action, str) and next_action:
        print(f"   → next: {next_action}", file=sys.stderr)


def _runner_for(connector: Any) -> PlanRunner:
    """Adapt a discovered Connector into the PlanRunner shape used by ``build_bundle_dir``."""
    from parsimony.catalog.catalog import entries_from_table_result

    async def run(plan: CatalogPlan) -> Any:
        params: Any = None
        if connector.param_type is not None:
            params = connector.param_type(**plan.params)
        result = await connector(params)
        return entries_from_table_result(result)

    return run
