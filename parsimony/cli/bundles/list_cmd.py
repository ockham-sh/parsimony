"""``parsimony bundles list`` — show every discovered spec.

Pure metadata: never materializes a dynamic plan, so it stays fast even
when plugin generators do expensive work.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TextIO

from parsimony.bundles import DiscoveredSpec, iter_specs


def _add_list(subs: argparse._SubParsersAction[Any]) -> None:
    p = subs.add_parser(
        "list",
        help="List discovered catalog specs (does not materialize dynamic plans).",
    )
    p.add_argument("--plugin", help="Filter by plugin distribution name.")
    p.add_argument("--json", dest="json_output", action="store_true")


@dataclass(frozen=True)
class _ListRow:
    namespace: str
    plugin: str
    kind: str  # "static" | "dynamic"
    target: str
    connector: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "plugin": self.plugin,
            "kind": self.kind,
            "target": self.target,
            "connector": self.connector,
        }


def _row_for(d: DiscoveredSpec) -> _ListRow:
    spec = d.spec
    if spec.static_namespace is not None:
        kind, ns = "static", spec.static_namespace
    else:
        kind, ns = "dynamic", "(?)"
    return _ListRow(
        namespace=ns,
        plugin=d.provider.distribution_name or d.provider.name,
        kind=kind,
        target=spec.target,
        connector=d.connector.name,
    )


def _run_list(args: argparse.Namespace) -> int:
    rows: list[_ListRow] = []
    errors: list[dict[str, str]] = []
    for d in iter_specs():
        if args.plugin and (d.provider.distribution_name or d.provider.name) != args.plugin:
            continue
        try:
            rows.append(_row_for(d))
        except Exception as exc:  # malformed spec → partial state
            errors.append(
                {
                    "plugin": d.provider.distribution_name or d.provider.name,
                    "connector": d.connector.name,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    if args.json_output:
        out = sys.stdout
        for row in rows:
            print(json.dumps(row.to_dict(), sort_keys=True), file=out)
        for err in errors:
            print(json.dumps({"status": "error", **err}, sort_keys=True), file=out)
        return 1 if errors and not rows else (1 if errors else 0)

    if not rows and not errors:
        print(
            "No catalogs discovered. Install a plugin that declares one "
            "(e.g. 'pip install parsimony-treasury') or run 'parsimony list-plugins' "
            "to see what's installed.",
        )
        return 0

    rows.sort(key=lambda r: (r.namespace, r.plugin))
    _print_list_table(rows, errors, sys.stdout)
    if not errors:
        print(
            "\nRun 'parsimony bundles build <namespace>' to inspect locally, or "
            "'parsimony bundles publish <namespace>' to upload."
        )
    return 1 if errors else 0


def _print_list_table(rows: Sequence[_ListRow], errors: Sequence[dict[str, str]], out: TextIO) -> None:
    headers = ("NAMESPACE", "PLUGIN", "KIND", "TARGET", "CONNECTOR")
    width = [len(h) for h in headers]
    for r in rows:
        for i, value in enumerate((r.namespace, r.plugin, r.kind, r.target, r.connector)):
            width[i] = max(width[i], len(value))
    fmt = "  ".join(f"{{:<{w}}}" for w in width)
    print(fmt.format(*headers), file=out)
    print(fmt.format(*("-" * w for w in width)), file=out)
    for r in rows:
        print(fmt.format(r.namespace, r.plugin, r.kind, r.target, r.connector), file=out)
    for err in errors:
        print(
            f"\n[error] plugin={err['plugin']} connector={err['connector']}: {err['error']}",
            file=out,
        )
