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
import logging
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
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Log progress to stderr, including catalog and model downloads.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ls = subparsers.add_parser(
        "list",
        help="List discovered plugins and their connectors.",
        description=(
            "Inspects the 'parsimony.providers' entry-point group. Shows each "
            "plugin's name, version, connector count, and conformance status. "
            "With --strict, imports each plugin to run the conformance suite and "
            "to list the credential (secret) parameters its connectors declare; "
            "exits non-zero on any failure."
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
            "platformdirs.user_cache_dir('parsimony')) and contains the named "
            f"subdirectories: {', '.join(cache._SUBDIRS)}."
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
    cc_info.add_argument(
        "--repos",
        action="store_true",
        help="Break the catalogs subdir down by Hugging Face repo (largest first).",
    )
    cc_clear = cc_sub.add_parser(
        "clear",
        help="Remove a cache subdirectory, a single catalog repo, or everything.",
    )
    cc_clear_target = cc_clear.add_mutually_exclusive_group()
    cc_clear_target.add_argument(
        "--subdir",
        metavar="NAME",
        help=f"Clear only this subdir ({', '.join(cache._SUBDIRS)}).",
    )
    cc_clear_target.add_argument(
        "--repo",
        metavar="ORG/NAME",
        help="Clear only the cached catalogs for this Hugging Face repo (e.g. parsimony-dev/sdmx).",
    )
    cc_clear.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt.",
    )

    return parser


def _configure_logging(*, verbose: bool) -> None:
    """Attach a stderr handler for ``parsimony``'s own logs.

    The library itself configures nothing — installing handlers is the
    application's call, not a library's. This *is* the application, so it is the
    right place to make parsimony's INFO records visible; without a handler here
    they are never even constructed, and a ``cache`` command that triggers a
    catalog download would stall with no explanation.

    Scoped to the ``parsimony`` logger rather than ``basicConfig`` so the CLI does
    not also turn on every third-party library's logging.
    """
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    package_logger = logging.getLogger("parsimony")
    package_logger.addHandler(handler)
    package_logger.setLevel(logging.INFO if verbose else logging.WARNING)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _build_parser().parse_args(argv)
    _configure_logging(verbose=args.verbose)
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
    # Declared credential parameter NAMES (strict mode only) are intentionally not
    # a field here; they ride in a side map so no row field carries secret-derived
    # data. The names themselves are public (each connector exposes them in its
    # signature and docstring) — they are parameter names like "api_key", never values.


def _run_list(*, json_output: bool, strict: bool) -> int:
    rows, secrets_by_name = _collect_rows(strict=strict)
    if json_output:
        payload: dict[str, Any] = {
            "plugins": [{**dict(r), "secrets": secrets_by_name.get(r["name"], [])} for r in rows],
        }
        print(json.dumps(payload, indent=2))
    else:
        _render_table(rows, secrets_by_name, sys.stdout)
    if strict and any(r["conformance"] == "fail" for r in rows):
        return 1
    return 0


def _collect_rows(*, strict: bool) -> tuple[list[_PluginRow], dict[str, list[str]]]:
    """Walk ``iter_providers`` metadata-only.

    Only imports each plugin when ``strict`` is requested because conformance
    needs the module. Returns the rows plus a side map of plugin name to its
    declared credential parameter names, kept out of the rows so no row field
    carries secret-derived data.
    """
    from parsimony.testing import ConformanceError, assert_plugin_valid

    rows: list[_PluginRow] = []
    secrets_by_name: dict[str, list[str]] = {}

    for provider in iter_providers():
        module: ModuleType | None = None
        connector_count = 0
        conformance = "skipped"
        detail: str | None = None

        try:
            connectors = provider.load()
            connector_count = len(connectors)
            if strict:
                # Declared credential parameter NAMES (e.g. "api_key"), not values.
                secrets_by_name[provider.name] = sorted({s for c in connectors for s in c.secrets})
        except Exception as exc:  # noqa: BLE001 — plugin own arbitrary init code
            if strict:
                conformance = "fail"
                detail = f"{type(exc).__name__}: {exc}"
            connector_count = 0

        if strict and conformance != "fail":
            try:
                module = importlib.import_module(provider.module_path)
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
    return rows, secrets_by_name


def _render_table(rows: list[_PluginRow], secrets_by_name: dict[str, list[str]], stream: TextIO) -> None:
    if not rows:
        print("No parsimony plugins discovered (0 plugins).", file=stream)
        print(
            "Install one to get started, e.g. `pip install parsimony-fred`.",
            file=stream,
        )
        return

    header = ["NAME", "VERSION", "CONNECTORS", "CONFORMANCE", "SECRETS"]
    body: list[list[str]] = [header]
    for r in rows:
        # Secrets are only inspected in --strict mode; "?" marks "not inspected",
        # "-" marks "inspected, declares none". These are public parameter names
        # (e.g. "api_key"), not values.
        names = secrets_by_name.get(r["name"], [])
        if r["conformance"] == "skipped":
            secrets_cell = "?"
        elif names:
            secrets_cell = ", ".join(names)
        else:
            secrets_cell = "-"
        body.append(
            [
                r["name"],
                r["version"] or "?",
                str(r["connector_count"]) if r["connector_count"] else "0",
                r["conformance"],
                secrets_cell,
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
        if args.repos:
            report["catalog_repos"] = cache.catalog_repos()
        if args.json_output:
            print(json.dumps(report, indent=2))
        else:
            _render_cache_info(report, sys.stdout)
        return 0
    if args.cache_action == "clear":
        return _run_cache_clear(subdir=args.subdir, repo=args.repo, assume_yes=args.yes)
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

    repos = report.get("catalog_repos")
    if repos is not None:
        print(file=stream)
        rheader = ["CATALOG REPO", "FILES", "SIZE"]
        rbody: list[list[str]] = [rheader]
        for entry in repos:
            rbody.append([entry["repo_id"], str(entry["files"]), _human_size(entry["size_bytes"])])
        if len(rbody) == 1:
            rbody.append(["(none cached)", "-", "-"])
        rwidths = [max(len(row[i]) for row in rbody) for i in range(len(rheader))]
        for i, row in enumerate(rbody):
            print("  ".join(cell.ljust(rwidths[j]) for j, cell in enumerate(row)), file=stream)
            if i == 0:
                print("  ".join("-" * w for w in rwidths), file=stream)


def _run_cache_clear(*, subdir: str | None, repo: str | None, assume_yes: bool) -> int:
    if repo is not None:
        return _run_cache_clear_repo(repo=repo, assume_yes=assume_yes)
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


def _run_cache_clear_repo(*, repo: str, assume_yes: bool) -> int:
    try:
        dirname = cache._catalog_dirname(repo)
    except ValueError as exc:
        print(f"error: invalid repo {repo!r}: {exc}", file=sys.stderr)
        return 2

    entry = next((r for r in cache.catalog_repos() if r["dirname"] == dirname), None)
    if entry is None:
        print(f"Nothing to clear (no cached catalogs for repo {repo!r}).")
        return 0

    label = f"catalogs for repo {entry['repo_id']!r}"
    if not assume_yes:
        prompt = f"Remove {label} ({entry['files']} file(s), {_human_size(entry['size_bytes'])})? [y/N] "
        try:
            answer = input(prompt).strip().lower()
        except EOFError:
            answer = ""
        if answer not in ("y", "yes"):
            print("Aborted.")
            return 0

    cache.clear_catalog_repo(repo)
    print(f"Cleared {label} ({entry['files']} file(s), {_human_size(entry['size_bytes'])}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
