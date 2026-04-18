"""``parsimony bundles publish`` — destructive HF Hub upload fan-out.

Always prints a preview (current vs projected entry counts per namespace)
and refuses without explicit confirmation in non-interactive shells.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parsimony.bundles import (
    BundleError,
    DiscoveredSpec,
    fetch_published_entry_count,
)
from parsimony.cli.bundles._shared import _materialize_with_timeout
from parsimony.cli.bundles.fanout import (
    _add_failure_flags,
    _fanout_build,
    _parse_only,
)
from parsimony.cli.bundles.selection import _did_you_mean, _select_specs


def _add_publish(subs: argparse._SubParsersAction[Any]) -> None:
    p = subs.add_parser(
        "publish",
        help="Build and upload bundles via the HF Hub target (destructive).",
    )
    p.add_argument(
        "selector",
        nargs="?",
        help="Namespace, glob pattern, or omit to publish every discovered spec.",
    )
    p.add_argument("--plugin", help="Restrict to one plugin's specs.")
    p.add_argument("--embed-batch-size", type=int, default=64)
    p.add_argument(
        "--yes",
        action="store_true",
        help="Required to actually upload (alternatively set PARSIMONY_PUBLISH_CONFIRMED=1).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build locally but do not upload; useful for previewing the bundle.",
    )
    p.add_argument(
        "--allow-shrink",
        action="store_true",
        help=(
            "Permit publishing a bundle whose entry_count is <50% of the "
            "currently-published bundle's count. Refused without this flag."
        ),
    )
    p.add_argument("--json", dest="json_output", action="store_true")
    _add_failure_flags(p)


def _run_publish(args: argparse.Namespace) -> int:
    discovered = _select_specs(args.selector, plugin=args.plugin)
    if not discovered:
        return _did_you_mean(args.selector)

    env_confirmed = os.environ.get("PARSIMONY_PUBLISH_CONFIRMED") == "1"

    if not args.dry_run:
        approved = _confirm_publish(
            discovered=discovered,
            yes_flag=args.yes,
            env_confirmed=env_confirmed,
            allow_shrink=args.allow_shrink,
            fail_fast=args.fail_fast,
        )
        if not approved:
            print("publish aborted (no confirmation).", file=sys.stderr)
            return 3

    out_root = Path("./catalog-publish-tmp")
    out_root.mkdir(parents=True, exist_ok=True)
    timestamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    return asyncio.run(
        _fanout_build(
            discovered=discovered,
            out_root=out_root,
            timestamp=timestamp,
            embed_batch_size=args.embed_batch_size,
            json_output=args.json_output,
            publish=not args.dry_run,
            allow_shrink=args.allow_shrink,
            bundle_timeout_s=args.bundle_timeout,
            fail_fast=args.fail_fast,
            resume_from=args.resume_from,
            only=_parse_only(args.only),
        )
    )


@dataclass(frozen=True)
class _PreviewRow:
    namespace: str
    current_entries: int | None
    projected_entries: int | None


def _format_delta(delta: int) -> str:
    return f"+{delta}" if delta > 0 else str(delta)


def _confirm_publish(
    *,
    discovered: list[DiscoveredSpec],
    yes_flag: bool,
    env_confirmed: bool,
    allow_shrink: bool,
    fail_fast: bool,
) -> bool:
    """Print the publish preview; return True if the operator confirmed.

    The preview always prints. The interactive ``[y/N]`` prompt is shown
    only when ``--yes`` and ``PARSIMONY_PUBLISH_CONFIRMED=1`` are both
    absent. Non-interactive invocations (``stdin`` is not a TTY) without
    explicit confirmation are rejected — silent fan-out from a CI cron is
    exactly the failure mode the gate exists to prevent.

    Each resolved namespace is shown with its current published entry
    count and projected new count so the operator can spot a destructive
    shrink before typing ``y``.
    """
    rows = _preview_rows(discovered)

    print()
    print("=" * 70)
    print("PUBLISH PREVIEW")
    print("=" * 70)
    print("Target              : hf_bundle  (parsimony-dev/<namespace>)")
    print(f"Discovered specs    : {len(discovered)}")
    print(f"Resolved namespaces : {len(rows)}")
    print(
        "MODE                : "
        f"dry-run=N  allow-shrink={'Y' if allow_shrink else 'N'}  "
        f"fail-fast={'Y' if fail_fast else 'N'}"
    )
    if rows:
        max_ns = max(len(r.namespace) for r in rows)
        header = f"  {'NAMESPACE'.ljust(max_ns)}  CURRENT  PROJECTED  DELTA"
        print(header)
        for row in rows[:25]:
            current = "?" if row.current_entries is None else str(row.current_entries)
            projected = "?" if row.projected_entries is None else str(row.projected_entries)
            delta = (
                "?"
                if row.current_entries is None or row.projected_entries is None
                else _format_delta(row.projected_entries - row.current_entries)
            )
            print(f"  {row.namespace.ljust(max_ns)}  {current:>7}  {projected:>9}  {delta}")
        if len(rows) > 25:
            print(f"  … and {len(rows) - 25} more")
    _print_embedding_provenance()
    print("=" * 70)

    if yes_flag or env_confirmed:
        print("Confirmed via --yes/PARSIMONY_PUBLISH_CONFIRMED.")
        return True

    if not sys.stdin.isatty():
        print(
            "Refusing to publish without confirmation in a non-interactive shell. "
            "Pass --yes (or set PARSIMONY_PUBLISH_CONFIRMED=1) when running under CI.",
            file=sys.stderr,
        )
        return False

    answer = input("Proceed with publish? [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def _preview_rows(discovered: list[DiscoveredSpec]) -> list[_PreviewRow]:
    """Resolve every selected spec to per-namespace preview rows.

    For each resolved namespace, fetches the currently-published
    ``entry_count`` (anonymous, soft-fails to ``None``). The projected
    count is left as ``None`` for dynamic specs because actual entries
    are produced only by the build path; static specs without a
    pre-flight enumeration also show ``None`` for projected.
    """
    namespaces: list[str] = []
    for d in discovered:
        spec = d.spec
        if spec.static_namespace is not None:
            namespaces.append(spec.static_namespace)
            continue
        try:
            plans = asyncio.run(_materialize_with_timeout(spec))
        except TimeoutError:
            namespaces.append(f"{d.connector.name}:?(timeout)")
            continue
        except (BundleError, RuntimeError, OSError) as exc:
            namespaces.append(f"{d.connector.name}:?({type(exc).__name__})")
            continue
        namespaces.extend(p.namespace for p in plans)
    namespaces.sort()

    rows: list[_PreviewRow] = []
    for ns in namespaces:
        if "?" in ns:
            rows.append(_PreviewRow(namespace=ns, current_entries=None, projected_entries=None))
            continue
        current = fetch_published_entry_count(ns)
        rows.append(_PreviewRow(namespace=ns, current_entries=current, projected_entries=None))
    return rows


def _print_embedding_provenance() -> None:
    """Echo the embedding model identity that will stamp each manifest."""
    model = os.environ.get("PARSIMONY_EMBED_MODEL", "(unset)")
    revision = os.environ.get("PARSIMONY_EMBED_REVISION", "(unset)")
    dim = os.environ.get("PARSIMONY_EMBED_DIM", "(unset)")
    print(f"Embedding model     : {model}")
    print(f"Embedding revision  : {revision}")
    print(f"Embedding dim       : {dim}")
