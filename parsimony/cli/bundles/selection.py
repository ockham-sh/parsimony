"""Spec selection helpers used across the build/publish/eval verbs.

Wraps :func:`parsimony.bundles.iter_specs` with namespace + glob + plugin
filtering plus did-you-mean suggestions for typo'd selectors.
"""

from __future__ import annotations

import difflib
import fnmatch
import sys

from parsimony.bundles.discovery import DiscoveredSpec, iter_specs
from parsimony.bundles.spec import CatalogSpec


def _spec_namespace(spec: CatalogSpec) -> str:
    return spec.static_namespace or "(dynamic)"


def _all_static_namespaces() -> list[str]:
    out: list[str] = []
    for d in iter_specs():
        if d.spec.static_namespace is not None:
            out.append(d.spec.static_namespace)
    return sorted(out)


def _all_connector_names() -> list[str]:
    return sorted(d.connector.name for d in iter_specs())


def _select_specs(
    selector: str | None,
    *,
    plugin: str | None = None,
) -> list[DiscoveredSpec]:
    """Filter discovered specs by namespace selector + optional plugin."""
    chosen: list[DiscoveredSpec] = []
    for d in iter_specs():
        if plugin and (d.provider.distribution_name or d.provider.name) != plugin:
            continue
        if selector is None:
            chosen.append(d)
            continue
        ns = d.spec.static_namespace
        if ns is not None:
            if ns == selector or fnmatch.fnmatch(ns, selector):
                chosen.append(d)
        else:
            # Dynamic specs match a connector-name selector or a glob on it.
            if d.connector.name == selector or fnmatch.fnmatch(d.connector.name, selector):
                chosen.append(d)
    return chosen


def _find_by_connector(name: str) -> DiscoveredSpec | None:
    for d in iter_specs():
        if d.connector.name == name:
            return d
    return None


def _did_you_mean(selector: str | None) -> int:
    if selector is None:
        print("No catalogs discovered to operate on.", file=sys.stderr)
        return 0
    candidates = _all_static_namespaces() + _all_connector_names()
    near = difflib.get_close_matches(selector, candidates, n=3, cutoff=0.6)
    msg = f"Selector {selector!r} did not match any namespace or dynamic connector."
    if near:
        msg += f" Did you mean: {', '.join(near)}?"
    msg += " Run 'parsimony bundles list' to see all discoverable bundles."
    print(msg, file=sys.stderr)
    return 64


def _did_you_mean_connector(name: str) -> int:
    candidates = _all_connector_names()
    near = difflib.get_close_matches(name, candidates, n=3, cutoff=0.6)
    msg = f"Connector {name!r} not found among discovered specs."
    if near:
        msg += f" Did you mean: {', '.join(near)}?"
    print(msg, file=sys.stderr)
    return 64
