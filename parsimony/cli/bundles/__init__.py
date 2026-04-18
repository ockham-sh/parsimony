"""``parsimony bundles`` — discover, plan, build, publish, and eval catalog bundles.

Verbs:

- ``list``      Show every discovered bundle spec (no plan materialization).
- ``plan``      Materialize one spec's plan generator, print resolved bundles.
- ``build``     Build bundles to a local directory; non-destructive.
- ``publish``   Build + upload to the HF Hub target; destructive.
- ``eval``      Run recall@K eval against a bundle's golden query suite.

Exit codes:

- ``0``  success
- ``1``  partial failure (≥1 bundle in fan-out failed)
- ``2``  total failure (every bundle in fan-out failed, or build aborted)
- ``3``  configuration error (missing token, invalid spec, model unavailable)
- ``64`` usage error (bad CLI args — argparse default)

Output policy:

- ``--json`` → NDJSON, one record per row (or per bundle event during fan-out).
- Default → human-readable table / structured progress lines.

Discovery never imports a plugin module's heavy deps; plan materialization
runs the dynamic generator under a 30s asyncio.timeout.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from parsimony.cli.bundles.build_cmd import _add_build, _run_build
from parsimony.cli.bundles.eval_cmd import _add_eval, _run_eval
from parsimony.cli.bundles.list_cmd import _add_list, _run_list
from parsimony.cli.bundles.plan_cmd import _add_plan, _run_plan
from parsimony.cli.bundles.publish_cmd import _add_publish, _run_publish

logger = logging.getLogger(__name__)

__all__ = ["add_subparser", "run"]


def add_subparser(subparsers: argparse._SubParsersAction[Any]) -> None:
    """Register the ``bundles`` subcommand on a top-level argparse subparsers."""
    parser = subparsers.add_parser(
        "bundles",
        help="Discover, plan, build, and publish catalog bundles.",
        description=(
            "Operate on catalog bundles declared by Parsimony provider plugins. "
            "Use 'list' to see what's discoverable, 'plan' to materialize a "
            "dynamic spec, 'build' to write a bundle locally, and 'publish' "
            "to upload to the HF Hub target."
        ),
    )
    bundle_subs = parser.add_subparsers(dest="bundles_verb", required=True)

    _add_list(bundle_subs)
    _add_plan(bundle_subs)
    _add_build(bundle_subs)
    _add_publish(bundle_subs)
    _add_eval(bundle_subs)


def run(args: argparse.Namespace) -> int:
    """Dispatch to the chosen verb. Returns the process exit code."""
    verb = args.bundles_verb
    if verb == "list":
        return _run_list(args)
    if verb == "plan":
        return _run_plan(args)
    if verb == "build":
        return _run_build(args)
    if verb == "publish":
        return _run_publish(args)
    if verb == "eval":
        return _run_eval(args)
    print(f"Unknown bundles verb {verb!r}", file=sys.stderr)
    return 64
