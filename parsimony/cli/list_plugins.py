"""``parsimony list-plugins`` subcommand.

Inspects the ``parsimony.providers`` entry-point group and prints a table
(or JSON) describing each plugin: distribution, version, connector count,
env var resolution status, and a conformance pass/fail flag.

Exit code: 0 when all discovered plugins pass conformance; 1 when at least
one plugin fails. A clean empty installation (no plugins discovered) exits 0.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TextIO

from parsimony.plugins import discovered_providers
from parsimony.testing import ConformanceError, assert_plugin_valid

__all__ = ["run"]


@dataclass(frozen=True)
class _PluginRow:
    name: str
    module: str
    distribution: str | None
    version: str | None
    connector_count: int
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
            "env_vars_present": self.env_vars_present,
            "env_vars_missing": self.env_vars_missing,
            "conformance": self.conformance,
            "conformance_detail": self.conformance_detail,
        }


def _collect_rows(env: Mapping[str, str]) -> list[_PluginRow]:
    rows: list[_PluginRow] = []
    for provider in discovered_providers():
        present = sorted(var for var in provider.env_vars.values() if env.get(var))
        missing = sorted(var for var in provider.env_vars.values() if not env.get(var))

        # Conformance runs against the module discovery already imported —
        # cached on the DiscoveredProvider record.
        module = provider.module
        if module is None:
            import importlib

            module = importlib.import_module(provider.module_path)
        try:
            assert_plugin_valid(module)
            conformance = "pass"
            detail = None
        except ConformanceError as exc:
            conformance = "fail"
            detail = str(exc)

        rows.append(
            _PluginRow(
                name=provider.name,
                module=provider.module_path,
                distribution=provider.distribution_name,
                version=provider.version,
                connector_count=len(list(provider.connectors)),
                env_vars_present=present,
                env_vars_missing=missing,
                conformance=conformance,
                conformance_detail=detail,
            )
        )
    return rows


def _render_table(rows: list[_PluginRow], stream: TextIO) -> None:
    if not rows:
        print("No parsimony plugins discovered (0 plugins).", file=stream)
        print(
            "Install one to get started, e.g. `pip install parsimony-fred` "
            "or see the plugin contract in `docs/plugin-contract.md`.",
            file=stream,
        )
        return

    header = ["NAME", "VERSION", "MODULE", "CONNECTORS", "ENV", "CONFORMANCE"]
    body: list[list[str]] = [header]
    for r in rows:
        env_cell = f"{len(r.env_vars_present)}/{len(r.env_vars_present) + len(r.env_vars_missing)}"
        if r.env_vars_missing:
            env_cell += f" (missing: {','.join(r.env_vars_missing)})"
        body.append(
            [
                r.name,
                r.version or "?",
                r.module,
                str(r.connector_count),
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
            print(f"  ✗ {r.name}: {r.conformance_detail}", file=stream)


def run(*, json_output: bool = False, env: Mapping[str, str] | None = None) -> int:
    """Programmatic entry point for ``parsimony list-plugins``.

    Parameters
    ----------
    json_output:
        Emit machine-readable JSON instead of a human-friendly table.
    env:
        Environment dict override (for tests). Defaults to ``os.environ``.

    Returns
    -------
    int
        Process exit code. ``0`` if all plugins pass conformance (or none are
        installed); ``1`` if any plugin fails conformance.
    """
    resolved_env = env if env is not None else os.environ

    rows = _collect_rows(resolved_env)

    if json_output:
        payload = [r.to_dict() for r in rows]
        print(json.dumps(payload, indent=2))
    else:
        _render_table(rows, sys.stdout)

    any_fail = any(r.conformance == "fail" for r in rows)
    return 1 if any_fail else 0
