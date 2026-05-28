"""``parsimony`` command-line interface.

Two verbs:

* ``parsimony list`` — enumerate installed plugins and their connectors.
  ``--strict`` folds the conformance suite in: exit non-zero on
  any plugin failure.
* ``parsimony cache {path,info,clear}`` — inspect or clear the global
  parsimony cache (HF snapshots, ONNX models, connector scratch).

Wired as the ``parsimony`` console script in ``pyproject.toml``.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Sequence
from types import ModuleType
from typing import Any, TextIO, TypedDict

from parsimony import cache
from parsimony.discover import iter_providers

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
        help="List discovered plugins and their connectors.",
        description=(
            "Inspects the 'parsimony.providers' entry-point group. Shows each "
            "plugin's connectors and env-var status. "
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

    cc = subparsers.add_parser(
        "cache",
        help="Inspect or clear the parsimony cache.",
        description=(
            "Manage the global parsimony cache. The root resolves through "
            "PARSIMONY_CACHE_DIR (defaulting to "
            "platformdirs.user_cache_dir('parsimony')) and contains three "
            "named subdirectories: catalogs, models, connectors."
        ),
    )
    cc_sub = cc.add_subparsers(dest="cache_action", required=True)
    cc_sub.add_parser("path", help="Print the resolved cache root.")
    cc_info = cc_sub.add_parser("info", help="Show occupancy of each cache subdirectory.")
    cc_info.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit JSON instead of a table.",
    )
    cc_clear = cc_sub.add_parser("clear", help="Remove a cache subdirectory (or all of them).")
    cc_clear.add_argument(
        "--subdir",
        metavar="NAME",
        help="Clear only this subdir (catalogs, models, connectors).",
    )
    cc_clear.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _build_parser().parse_args(argv)
    if args.command == "list":
        return _run_list(json_output=args.json_output, strict=args.strict)
    if args.command == "cache":
        return _run_cache(args)
    return 2  # argparse raises before we get here


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


class _PluginRow(TypedDict):
    name: str
    module: str
    distribution: str | None
    version: str | None
    connector_count: int
    conformance: str  # "pass" | "fail" | "skipped"
    conformance_detail: str | None


def _run_list(*, json_output: bool, strict: bool) -> int:
    rows = _collect_rows(strict=strict)
    if json_output:
        payload: dict[str, Any] = {
            "plugins": [dict(r) for r in rows],
        }
        print(json.dumps(payload, indent=2))
    else:
        _render_table(rows, sys.stdout)
    if strict and any(r["conformance"] == "fail" for r in rows):
        return 1
    return 0


def _collect_rows(*, strict: bool) -> list[_PluginRow]:
    """Walk ``iter_providers`` metadata-only.

    Only imports each plugin when ``strict`` is requested because conformance
    needs the module.
    """
    from parsimony.testing import ConformanceError, assert_plugin_valid

    rows: list[_PluginRow] = []

    for provider in iter_providers():
        module: ModuleType | None = None
        connector_count = 0
        conformance = "skipped"
        detail: str | None = None

        if strict:
            try:
                module = importlib.import_module(provider.module_path)
                connectors = provider.load()
                connector_count = len(connectors)
                assert_plugin_valid(module)
                conformance = "pass"
            except ConformanceError as exc:
                conformance = "fail"
                detail = str(exc)
            except Exception as exc:  # noqa: BLE001 — plugin own arbitrary init code
                conformance = "fail"
                detail = f"{type(exc).__name__}: {exc}"

        rows.append(
            {
                "name": provider.name,
                "module": provider.module_path,
                "distribution": provider.dist_name,
                "version": provider.version,
                "connector_count": connector_count,
                "conformance": conformance,
                "conformance_detail": detail,
            }
        )
    return rows


def _render_table(rows: list[_PluginRow], stream: TextIO) -> None:
    if not rows:
        print("No parsimony plugins discovered (0 plugins).", file=stream)
        print(
            "Install one to get started, e.g. `pip install parsimony-fred`.",
            file=stream,
        )
        return

    header = ["NAME", "VERSION", "CONNECTORS", "CONFORMANCE"]
    body: list[list[str]] = [header]
    for r in rows:
        body.append(
            [
                r["name"],
                r["version"] or "?",
                str(r["connector_count"]) if r["connector_count"] else "?",
                r["conformance"],
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
        if r["conformance"] == "fail":
            print(f"  ! {r['name']}: {r['conformance_detail']}", file=stream)


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


def _run_cache(args: argparse.Namespace) -> int:
    if args.cache_action == "path":
        print(cache.root())
        return 0
    if args.cache_action == "info":
        report = cache.info()
        if args.json_output:
            print(json.dumps(report, indent=2))
        else:
            _render_cache_info(report, sys.stdout)
        return 0
    if args.cache_action == "clear":
        return _run_cache_clear(subdir=args.subdir, assume_yes=args.yes)
    return 2


def _human_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    units = ("KB", "MB", "GB", "TB")
    size = float(n)
    for unit in units:
        size /= 1024
        if size < 1024:
            return f"{size:.1f} {unit}"
    return f"{size:.1f} PB"


def _render_cache_info(report: dict[str, Any], stream: TextIO) -> None:
    header = ["SUBDIR", "FILES", "SIZE", "PATH"]
    body: list[list[str]] = [header]
    for name, entry in report["subdirs"].items():
        if entry["exists"]:
            body.append(
                [
                    name,
                    str(entry["files"]),
                    _human_size(entry["size_bytes"]),
                    entry["path"],
                ]
            )
        else:
            body.append([name, "-", "-", entry["path"]])

    widths = [max(len(row[i]) for row in body) for i in range(len(header))]
    for i, row in enumerate(body):
        line = "  ".join(cell.ljust(widths[j]) for j, cell in enumerate(row))
        print(line, file=stream)
        if i == 0:
            print("  ".join("-" * w for w in widths), file=stream)
    print(file=stream)
    print(f"root: {report['root']}", file=stream)


def _run_cache_clear(*, subdir: str | None, assume_yes: bool) -> int:
    report = cache.info()
    known = sorted(report["subdirs"])
    if subdir is not None and subdir not in report["subdirs"]:
        print(
            f"error: unknown cache subdir {subdir!r}; expected one of {known}",
            file=sys.stderr,
        )
        return 2

    targets = {subdir: report["subdirs"][subdir]} if subdir is not None else report["subdirs"]
    total_files = sum(s["files"] for s in targets.values())
    total_bytes = sum(s["size_bytes"] for s in targets.values())

    label = f"subdir {subdir!r}" if subdir else "all subdirs"
    if total_files == 0:
        print(f"Nothing to clear ({label} are empty).")
        return 0

    if not assume_yes:
        prompt = f"Remove {label} ({total_files} file(s), {_human_size(total_bytes)})? [y/N] "
        try:
            answer = input(prompt).strip().lower()
        except EOFError:
            answer = ""
        if answer not in ("y", "yes"):
            print("Aborted.")
            return 0

    cache.clear(subdir=subdir)
    print(f"Cleared {label} ({total_files} file(s), {_human_size(total_bytes)}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
