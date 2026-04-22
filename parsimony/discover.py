"""Plugin discovery for ``parsimony.providers`` entry points.

Three functions plus one frozen dataclass — no cache, no singleton, no
import-time side effects. Consumers cache at their level if they want.

* :func:`iter_providers` — metadata-only enumeration (no plugin imports).
* :func:`load` — strict: load named providers; raise if any is absent.
* :func:`load_all` — forgiving: load every installed provider; log and skip
  failures.

Provider metadata (homepage, version) is read from distribution metadata
via :mod:`importlib.metadata` — plugins no longer export module-level
``__version__`` or ``PROVIDER_METADATA`` dicts.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import logging
from collections.abc import Iterator
from dataclasses import dataclass

from parsimony.connector import Connectors

__all__ = ["Provider", "iter_providers", "load", "load_all"]


GROUP = "parsimony.providers"
_logger = logging.getLogger("parsimony.discover")


def _dist_name(ep: importlib.metadata.EntryPoint) -> str | None:
    """Return the distribution name owning *ep*, or ``None`` if unresolvable."""
    dist = getattr(ep, "dist", None)
    if dist is None:
        return None
    meta = getattr(dist, "metadata", None)
    if meta is None:
        return None
    try:
        value = meta["Name"]
    except (KeyError, TypeError):
        return None
    return value if isinstance(value, str) else None


def _dist_version(ep: importlib.metadata.EntryPoint) -> str | None:
    """Return the distribution version owning *ep*, or ``None`` if unresolvable."""
    dist = getattr(ep, "dist", None)
    if dist is None:
        return None
    return getattr(dist, "version", None)


def _dist_homepage(dist_name: str | None) -> str | None:
    """Look up the ``Homepage`` URL for *dist_name* from its PEP 621 metadata.

    Reads ``importlib.metadata.metadata(dist_name)`` and accepts either the
    core-metadata ``Home-page`` field or a ``[project.urls]`` entry whose key
    (case-insensitively) equals ``"homepage"``. Returns ``None`` if no match.
    """
    if not dist_name:
        return None
    try:
        meta = importlib.metadata.metadata(dist_name)
    except importlib.metadata.PackageNotFoundError:
        return None
    try:
        homepage = meta["Home-page"]
    except (KeyError, TypeError):
        homepage = None
    if isinstance(homepage, str) and homepage and homepage != "UNKNOWN":
        return homepage
    # PEP 621 `[project.urls]` encodes as repeated "Project-URL: key, url"
    for raw in meta.get_all("Project-URL") or ():
        if not isinstance(raw, str):
            continue
        key, _, url = raw.partition(",")
        if key.strip().lower() == "homepage" and url.strip():
            return url.strip()
    return None


@dataclass(frozen=True)
class Provider:
    """Installed plugin record — metadata only, no module reference."""

    name: str
    module_path: str
    dist_name: str | None
    version: str | None

    @property
    def homepage(self) -> str | None:
        """Homepage URL from the plugin's distribution metadata, if declared."""
        return _dist_homepage(self.dist_name)

    def load(self) -> Connectors:
        """Import the plugin module; return its ``CONNECTORS`` export.

        Raises :class:`TypeError` if the module does not export a
        :class:`Connectors` instance named ``CONNECTORS``.
        """
        mod = importlib.import_module(self.module_path)
        obj = getattr(mod, "CONNECTORS", None)
        if not isinstance(obj, Connectors):
            raise TypeError(f"{self.module_path} must export CONNECTORS: Connectors")
        return obj


def iter_providers() -> Iterator[Provider]:
    """Enumerate installed providers. Metadata-only; no plugin imports.

    Raises :class:`RuntimeError` if two distributions register the same
    provider name — the kernel refuses to guess which one wins.
    """
    seen: dict[str, str] = {}
    for ep in importlib.metadata.entry_points(group=GROUP):
        dist_name = _dist_name(ep)
        if ep.name in seen:
            raise RuntimeError(
                f"two distributions register provider {ep.name!r}: {seen[ep.name]!r} and {dist_name!r}. Uninstall one."
            )
        seen[ep.name] = dist_name or "?"
        yield Provider(
            name=ep.name,
            module_path=ep.value,
            dist_name=dist_name,
            version=_dist_version(ep),
        )


def load(*names: str) -> Connectors:
    """Strict: load named providers; raise if any name is absent."""
    by_name = {p.name: p for p in iter_providers()}
    missing = [n for n in names if n not in by_name]
    if missing:
        available = sorted(by_name)
        raise LookupError(f"providers not installed: {missing}. Available: {available}")
    return Connectors.merge(*(by_name[n].load() for n in names))


def load_all() -> Connectors:
    """Forgiving: load every installed provider; log and skip failures."""
    loaded: list[Connectors] = []
    for p in iter_providers():
        try:
            loaded.append(p.load())
        except Exception as exc:
            _logger.warning("failed to load %s: %s", p.name, exc)
    return Connectors.merge(*loaded)
