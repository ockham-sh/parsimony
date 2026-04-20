"""``parsimony bundles`` CLI — discover and build catalog bundles.

Two verbs, both designed to stay small:

* ``parsimony bundles list``

  Walks the plugin discovery layer and prints every connector that
  declares ``@enumerator(catalog=CatalogSpec(...))``. One line per spec
  with the provider, connector name, target, and static namespace (where
  one is declared).

* ``parsimony bundles build --target <url-template> [--only <name>]``

  For every discovered spec (or only the filtered subset), bind the
  connector's env-var dependencies from ``os.environ``, materialize
  the plan into concrete :class:`~parsimony.bundles.CatalogPlan` items,
  run the enumerator once per plan, ingest the rows into a fresh
  :class:`~parsimony.Catalog`, and push it to
  ``url-template.format(namespace=<plan.namespace>)``.

  Example::

      parsimony bundles build --target 'file:///tmp/catalogs/{namespace}'
      parsimony bundles build --target 'hf://ockham/catalog-{namespace}' --only fred

The orchestration is deliberately un-clever: one catalog per namespace,
write-then-push, no retry machinery, no safety harness. Plugins that
need richer pipelines can import :class:`~parsimony.Catalog` directly and
call ``catalog.push(url)`` themselves.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Sequence
from typing import Any

from parsimony.bundles.discovery import DiscoveredSpec, iter_specs
from parsimony.bundles.spec import CatalogPlan, materialize

logger = logging.getLogger(__name__)

__all__ = ["add_subparser", "run"]


# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------


def add_subparser(subparsers: Any) -> None:
    """Attach ``bundles list`` and ``bundles build`` to the top-level CLI."""
    bundles = subparsers.add_parser(
        "bundles",
        help="Discover and build catalog bundles from installed plugins.",
        description=(
            "Catalog bundles are published snapshots of Parsimony catalogs "
            "(Parquet rows + FAISS vectors + BM25 keywords). The 'list' verb "
            "shows which plugins declare catalogs; 'build' materializes them."
        ),
    )
    verbs = bundles.add_subparsers(dest="bundles_command", required=True)

    ls = verbs.add_parser("list", help="List discovered @enumerator(catalog=...) declarations.")
    ls.add_argument(
        "--only",
        metavar="NAME",
        action="append",
        default=[],
        help="Filter to provider or connector names (repeatable).",
    )

    build = verbs.add_parser(
        "build",
        help="Build a catalog for each discovered spec and push to --target.",
    )
    build.add_argument(
        "--target",
        required=True,
        metavar="URL_TEMPLATE",
        help=(
            "Publish target URL. Must contain '{namespace}' which is substituted "
            "per plan. Examples: 'file:///tmp/catalogs/{namespace}', "
            "'hf://ockham/catalog-{namespace}'."
        ),
    )
    build.add_argument(
        "--only",
        metavar="NAME",
        action="append",
        default=[],
        help="Filter to provider or connector names (repeatable).",
    )
    build.add_argument(
        "--dry-run",
        action="store_true",
        help="Materialize plans and bind deps, but skip enumeration and push.",
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> int:
    if args.bundles_command == "list":
        return _run_list(only=list(args.only or []))
    if args.bundles_command == "build":
        if "{namespace}" not in args.target:
            print(
                f"error: --target {args.target!r} must contain '{{namespace}}'",
                file=sys.stderr,
            )
            return 2
        return asyncio.run(
            _run_build(
                target_template=args.target,
                only=list(args.only or []),
                dry_run=bool(args.dry_run),
            )
        )
    print(f"error: unknown bundles subcommand {args.bundles_command!r}", file=sys.stderr)
    return 2


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def _run_list(*, only: Sequence[str]) -> int:
    specs = [s for s in iter_specs() if _keep(s, only)]
    if not specs:
        print("No catalog specs discovered.")
        return 0
    for s in specs:
        ns = s.spec.static_namespace or "(dynamic)"
        print(f"{s.provider.name}/{s.connector.name}  namespace={ns}  target={s.spec.target}")
    return 0


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


async def _run_build(
    *,
    target_template: str,
    only: Sequence[str],
    dry_run: bool,
) -> int:
    # Heavy import deferred so `parsimony bundles list` works without the
    # [catalog] extra installed.
    from parsimony._standard.catalog import Catalog

    specs = [s for s in iter_specs() if _keep(s, only)]
    if not specs:
        print("No catalog specs discovered.")
        return 0

    failures = 0
    for spec in specs:
        label = f"{spec.provider.name}/{spec.connector.name}"
        connector = _bind_deps(spec)
        plans = await materialize(spec.spec)
        if not plans:
            print(f"{label}: no plans materialized — skipping")
            continue

        for plan in plans:
            target_url = target_template.format(namespace=plan.namespace)
            print(f"{label} → {plan.namespace} → {target_url}")
            if dry_run:
                continue
            try:
                await _build_one(connector, plan, target_url, Catalog)
            except Exception as exc:  # keep broad — one bundle failing shouldn't abort the batch
                logger.exception("build failed for %s → %s", label, plan.namespace)
                print(f"  ! build failed: {exc}", file=sys.stderr)
                failures += 1

    return 0 if failures == 0 else 1


async def _build_one(
    connector: Any,
    plan: CatalogPlan,
    target_url: str,
    catalog_cls: Any,
) -> None:
    """Run *connector* with *plan.params*, ingest rows into a new catalog, push to *target_url*."""
    params_model = connector.param_type(**plan.params) if connector.param_type else None
    result = await connector(params_model) if params_model is not None else await connector()
    catalog = catalog_cls(plan.namespace)
    await catalog.index_result(result)
    await catalog.push(target_url)


def _bind_deps(spec: DiscoveredSpec) -> Any:
    """Return the connector with env-var deps bound from ``os.environ``.

    Required-but-missing deps raise; optional deps are left unbound. Matches
    :func:`parsimony.discovery._compose.build_connectors_from_env`'s shape.
    """
    connector = spec.connector
    env_vars = dict(spec.provider.env_vars)
    if not env_vars:
        return connector

    deps: dict[str, Any] = {}
    required = set(connector.dep_names)
    for dep_name, env_var in env_vars.items():
        value = os.environ.get(env_var, "")
        if not value:
            if dep_name in required:
                raise RuntimeError(
                    f"{spec.provider.name}/{spec.connector.name}: "
                    f"required env var {env_var!r} (→ dep {dep_name!r}) is not set"
                )
            continue
        deps[dep_name] = value
    return connector.bind_deps(**deps) if deps else connector


def _keep(spec: DiscoveredSpec, only: Sequence[str]) -> bool:
    if not only:
        return True
    return any(name in (spec.provider.name, spec.connector.name) for name in only)
