"""``parsimony bundles build`` — non-destructive local fan-out."""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
from pathlib import Path
from typing import Any

from parsimony.cli.bundles.fanout import (
    _add_failure_flags,
    _fanout_build,
    _parse_only,
)
from parsimony.cli.bundles.selection import _did_you_mean, _select_specs


def _add_build(subs: argparse._SubParsersAction[Any]) -> None:
    p = subs.add_parser(
        "build",
        help="Build bundles to a local directory (non-destructive).",
    )
    p.add_argument(
        "selector",
        nargs="?",
        help="Namespace, glob pattern, or omit to build every discovered spec.",
    )
    p.add_argument(
        "--out",
        default="./catalog-build",
        help="Output root directory (default: ./catalog-build).",
    )
    p.add_argument("--embed-batch-size", type=int, default=64)
    p.add_argument("--json", dest="json_output", action="store_true")
    _add_failure_flags(p)


def _run_build(args: argparse.Namespace) -> int:
    discovered = _select_specs(args.selector)
    if not discovered:
        return _did_you_mean(args.selector)

    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)
    timestamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    return asyncio.run(
        _fanout_build(
            discovered=discovered,
            out_root=out_root,
            timestamp=timestamp,
            embed_batch_size=args.embed_batch_size,
            json_output=args.json_output,
            publish=False,
            bundle_timeout_s=args.bundle_timeout,
            fail_fast=args.fail_fast,
            resume_from=args.resume_from,
            only=_parse_only(args.only),
        )
    )
