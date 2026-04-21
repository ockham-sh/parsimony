"""``parsimony`` command-line interface.

Two verbs:

* ``parsimony list`` — enumerate installed plugins and their declared
  catalogs. ``--strict`` folds the conformance suite in: exit non-zero on
  any plugin failure.
* ``parsimony publish --provider NAME --target URL_TEMPLATE`` — build one
  :class:`~parsimony.Catalog` per declared namespace and push to
  ``URL_TEMPLATE.format(namespace=...)``.

Wired as the ``parsimony`` console script in ``pyproject.toml``.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TextIO

from parsimony.discovery import DiscoveredProvider, discovered_providers
from parsimony.publish import publish_provider

__all__ = ["main"]


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parsimony",
        description="Parsimony CLI — connector framework for financial data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ls = subparsers.add_parser(
        "list",
        help="List discovered plugins and their catalogs.",
        description=(
            "Inspects the 'parsimony.providers' entry-point group. Shows each "
            "plugin's connectors, declared catalogs, and env-var status. "
            "With --strict, runs the conformance suite against each plugin "
            "and exits non-zero on any failure."
        ),
    )
    ls.add_argument("--json", dest="json_output", action="store_true", help="Emit JSON instead of a table.")
    ls.add_argument(
        "--strict",
        action="store_true",
        help="Run conformance checks; exit non-zero on any failure.",
    )

    pub = subparsers.add_parser(
        "publish",
        help="Build + push catalogs for a provider.",
        description=(
            "Build one parsimony Catalog per declared namespace and push it to "
            "the URL given by --target (with '{namespace}' substitution)."
        ),
    )
    pub.add_argument("--provider", required=True, help="Provider name (from 'parsimony list').")
    pub.add_argument(
        "--target",
        required=True,
        metavar="URL_TEMPLATE",
        help="Publish target URL template; must contain '{namespace}'.",
    )
    pub.add_argument(
        "--only",
        metavar="NAMESPACE",
        action="append",
        default=[],
        help="Only publish these namespaces (repeatable).",
    )
    pub.add_argument("--dry-run", action="store_true", help="Resolve catalogs and targets, skip enumerate + push.")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _build_parser().parse_args(argv)
    if args.command == "list":
        return _run_list(json_output=args.json_output, strict=args.strict)
    if args.command == "publish":
        return _run_publish(
            provider=args.provider,
            target=args.target,
            only=list(args.only or []) or None,
            dry_run=bool(args.dry_run),
        )
    return 2  # argparse raises before we get here


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _PluginRow:
    name: str
    module: str
    distribution: str | None
    version: str | None
    connector_count: int
    catalogs: list[str]
    env_vars_present: list[str]
    env_vars_missing: list[str]
    conformance: str  # "pass" | "fail"
    conformance_detail: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "module": self.module,
            "distribution": self.distribution,
            "version": self.version,
            "connector_count": self.connector_count,
            "catalogs": self.catalogs,
            "env_vars_present": self.env_vars_present,
            "env_vars_missing": self.env_vars_missing,
            "conformance": self.conformance,
            "conformance_detail": self.conformance_detail,
        }


def _run_list(*, json_output: bool, strict: bool) -> int:
    rows = _collect_rows(os.environ)
    if json_output:
        print(json.dumps([r.to_dict() for r in rows], indent=2))
    else:
        _render_table(rows, sys.stdout)
    if strict and any(r.conformance == "fail" for r in rows):
        return 1
    return 0


def _collect_rows(env: Any) -> list[_PluginRow]:
    from parsimony.testing import ConformanceError, assert_plugin_valid

    rows: list[_PluginRow] = []
    for provider in discovered_providers():
        present = sorted(var for var in provider.env_vars.values() if env.get(var))
        missing = sorted(var for var in provider.env_vars.values() if not env.get(var))

        module = provider.module or importlib.import_module(provider.module_path)
        try:
            assert_plugin_valid(module)
            conformance = "pass"
            detail = None
        except ConformanceError as exc:
            conformance = "fail"
            detail = str(exc)

        catalogs = _list_catalog_namespaces(module)

        rows.append(
            _PluginRow(
                name=provider.name,
                module=provider.module_path,
                distribution=provider.distribution_name,
                version=provider.version,
                connector_count=len(list(provider.connectors)),
                catalogs=catalogs,
                env_vars_present=present,
                env_vars_missing=missing,
                conformance=conformance,
                conformance_detail=detail,
            )
        )
    return rows


def _list_catalog_namespaces(module: Any) -> list[str]:
    """Return the static namespaces declared on *module* (best-effort, sync).

    Async CATALOGS generators are reported as ``[...]`` without iteration so
    ``parsimony list`` stays network-free.
    """
    raw = getattr(module, "CATALOGS", None)
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str):
                out.append(item[0])
        return out
    # callable / async generator — can't enumerate without running it
    return ["<dynamic>"]


def _render_table(rows: list[_PluginRow], stream: TextIO) -> None:
    if not rows:
        print("No parsimony plugins discovered (0 plugins).", file=stream)
        print(
            "Install one to get started, e.g. `pip install parsimony-fred`.",
            file=stream,
        )
        return

    header = ["NAME", "VERSION", "CONNECTORS", "CATALOGS", "ENV", "CONFORMANCE"]
    body: list[list[str]] = [header]
    for r in rows:
        env_cell = f"{len(r.env_vars_present)}/{len(r.env_vars_present) + len(r.env_vars_missing)}"
        if r.env_vars_missing:
            env_cell += f" (missing: {','.join(r.env_vars_missing)})"
        catalog_cell = ",".join(r.catalogs) if r.catalogs else "-"
        body.append(
            [
                r.name,
                r.version or "?",
                str(r.connector_count),
                catalog_cell,
                env_cell,
                r.conformance,
            ]
        )

    widths = [max(len(row[i]) for row in body) for i in range(len(header))]
    for i, row in enumerate(body):
        line = "  ".join(cell.ljust(widths[j]) for j, cell in enumerate(row))
        print(line, file=stream)
        if i == 0:
            print("  ".join("-" * w for w in widths), file=stream)

    print(file=stream)
    print(f"{len(rows)} plugin(s) discovered.", file=stream)
    for r in rows:
        if r.conformance == "fail":
            print(f"  ! {r.name}: {r.conformance_detail}", file=stream)


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


def _run_publish(
    *,
    provider: str,
    target: str,
    only: list[str] | None,
    dry_run: bool,
) -> int:
    if "{namespace}" not in target:
        print(f"error: --target {target!r} must contain '{{namespace}}'", file=sys.stderr)
        return 2
    try:
        report = asyncio.run(publish_provider(provider, target=target, only=only, dry_run=dry_run))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    for ns in report.published:
        print(f"  published: {ns}")
    for ns in report.skipped:
        print(f"  skipped (no rows): {ns}")
    for ns, err in report.failed:
        print(f"  FAILED: {ns}: {err}", file=sys.stderr)
    return 0 if report.ok else 1


def _provider_by_name(name: str) -> DiscoveredProvider:
    for p in discovered_providers():
        if p.name == name:
            return p
    raise ValueError(f"no parsimony provider named {name!r}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
