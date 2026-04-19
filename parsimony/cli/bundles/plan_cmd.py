"""``parsimony bundles plan`` — materialize one spec's plan generator."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from parsimony.bundles.errors import BundleError
from parsimony.cli.bundles._shared import _PLAN_GEN_TIMEOUT_S, _materialize_with_timeout
from parsimony.cli.bundles.selection import _did_you_mean_connector, _find_by_connector


def _add_plan(subs: argparse._SubParsersAction[Any]) -> None:
    p = subs.add_parser(
        "plan",
        help="Materialize the plan for a discovered spec; print resolved bundles.",
    )
    p.add_argument("connector", help="Connector name (e.g. 'enumerate_sdmx_series').")
    p.add_argument("--json", dest="json_output", action="store_true")


def _run_plan(args: argparse.Namespace) -> int:
    discovered = _find_by_connector(args.connector)
    if discovered is None:
        return _did_you_mean_connector(args.connector)
    try:
        plans = asyncio.run(_materialize_with_timeout(discovered.spec))
    except TimeoutError:
        print(
            f"Plan generator for connector={args.connector!r} exceeded "
            f"{_PLAN_GEN_TIMEOUT_S}s — aborted.",
            file=sys.stderr,
        )
        return 3
    except (BundleError, RuntimeError, OSError) as exc:
        print(f"Plan generator failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 3

    if args.json_output:
        for plan in plans:
            print(json.dumps({"namespace": plan.namespace, "params": dict(plan.params)}, sort_keys=True))
        return 0

    print(f"Resolved {len(plans)} bundle(s) for connector={args.connector!r}:")
    for plan in plans:
        params_str = ", ".join(f"{k}={v}" for k, v in sorted(plan.params.items())) or "—"
        print(f"  {plan.namespace}    {params_str}")
    return 0
