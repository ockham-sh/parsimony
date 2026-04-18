"""``parsimony`` command-line interface.

Subcommands live in sibling modules; :func:`main` dispatches to them based
on ``sys.argv``. Entry point wired in ``pyproject.toml`` as ``parsimony``.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from parsimony.cli.bundles import add_subparser as add_bundles_subparser
from parsimony.cli.bundles import run as run_bundles
from parsimony.cli.list_plugins import run as run_list_plugins

__all__ = ["main"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parsimony",
        description="Parsimony CLI — connector framework for financial data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_plugins = subparsers.add_parser(
        "list-plugins",
        help="List discovered parsimony plugins and their status.",
        description=(
            "List plugins discovered via the 'parsimony.providers' entry-point "
            "group. Shows distribution, version, connector count, env var "
            "status, and conformance result. Exit code is non-zero when any "
            "plugin fails conformance."
        ),
    )
    list_plugins.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON instead of a table.",
    )

    add_bundles_subparser(subparsers)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "list-plugins":
        return run_list_plugins(json_output=args.json_output)

    if args.command == "bundles":
        return run_bundles(args)

    parser.error(f"unknown command: {args.command!r}")
    return 2  # unreachable — parser.error exits


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))
