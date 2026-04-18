"""``parsimony bundles eval`` — recall@K against a JSONL golden query suite."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Final

from parsimony.cli.bundles._shared import _build_provider_from_env

_SHA40_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")


def _validate_pin_argparse(value: str) -> str:
    """Argparse type for ``--pin``: must be a 40-char hex SHA."""
    if not _SHA40_RE.match(value):
        raise argparse.ArgumentTypeError(
            f"--pin {value!r} is not a 40-char commit SHA; reject pins that aren't full SHAs "
            "to prevent tag-pointer drift"
        )
    return value


def _add_eval(subs: argparse._SubParsersAction[Any]) -> None:
    p = subs.add_parser(
        "eval",
        help="Run recall@K eval against a bundle's golden query suite.",
    )
    p.add_argument(
        "namespace",
        help="Bundle namespace to query (must already be loadable from the configured store).",
    )
    p.add_argument(
        "--queries",
        required=True,
        type=Path,
        help="Path to a JSONL golden query file. See parsimony.bundles.eval for the schema.",
    )
    p.add_argument(
        "--min-recall",
        dest="min_recall",
        type=float,
        default=0.8,
        help="Aggregate recall floor (default: 0.8). Below this, exit code is non-zero.",
    )
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Local bundle cache root (default: platformdirs).",
    )
    p.add_argument(
        "--pin",
        type=_validate_pin_argparse,
        default=None,
        help="Pin a specific revision SHA (40-char hex; default: latest published).",
    )
    p.add_argument("--json", dest="json_output", action="store_true")


def _run_eval(args: argparse.Namespace) -> int:
    from parsimony.bundles.eval import (
        format_summary,
        load_queries,
        per_query_failures,
        run_eval,
    )
    from parsimony.stores.hf_bundle import HFBundleCatalogStore

    try:
        queries = load_queries(args.queries)
    except (OSError, ValueError) as exc:
        print(f"Failed to load queries from {args.queries}: {exc}", file=sys.stderr)
        return 3
    if not queries:
        print(f"No queries found in {args.queries}.", file=sys.stderr)
        return 3

    try:
        provider = _build_provider_from_env()
    except (RuntimeError, ValueError) as exc:
        print(f"Embedding provider construction failed: {exc}", file=sys.stderr)
        return 3

    store = HFBundleCatalogStore(
        embeddings=provider,
        cache_dir=args.cache_dir,
        pin=args.pin,
    )

    return asyncio.run(
        _exec_eval(
            store=store,
            queries=queries,
            namespace=args.namespace,
            min_recall=args.min_recall,
            json_output=args.json_output,
            run_eval=run_eval,
            format_summary=format_summary,
            per_query_failures=per_query_failures,
        )
    )


async def _exec_eval(
    *,
    store: Any,
    queries: list[Any],
    namespace: str,
    min_recall: float,
    json_output: bool,
    run_eval: Any,
    format_summary: Any,
    per_query_failures: Any,
) -> int:
    loaded = await store.try_load_remote(namespace)
    if not loaded:
        print(f"No published bundle found for namespace={namespace!r}.", file=sys.stderr)
        return 3

    results = await run_eval(store, queries, namespace=namespace)
    summary = format_summary(results, namespace=namespace, threshold=min_recall)

    if json_output:
        print(json.dumps(summary, sort_keys=True))
    else:
        passed = "PASS" if summary["passed"] else "FAIL"
        print(
            f"[{passed}] namespace={namespace} queries={summary['queries']} "
            f"recall_mean={summary['recall_mean']} threshold={min_recall}"
        )
        for r in results:
            mark = "✓" if r.recall == 1.0 else ("~" if r.recall > 0 else "✗")
            print(f"  {mark} recall={r.recall:.2f}  query={r.query!r}")
            print(f"     expected={list(r.expected_codes)}")
            print(f"     top_codes={list(r.top_codes)}")

    failed_per_query = per_query_failures(results, queries)
    if failed_per_query:
        print(
            f"\n{len(failed_per_query)} query(ies) failed their per-query min_recall floor.",
            file=sys.stderr,
        )

    if not summary["passed"] or failed_per_query:
        return 1
    return 0
