"""Lazy-populate wrapper for :class:`~parsimony.BaseCatalog`.

:class:`LazyNamespaceCatalog` wraps any concrete catalog and, on the first
:meth:`search` for a namespace the catalog hasn't seen, tries to fill it
from either (a) a published bundle loaded via a user-supplied loader, or
(b) a live ``@enumerator`` found in a :class:`~parsimony.Connectors`
collection. Confirmed misses are cached so cold queries don't re-probe.

This behaviour used to live on :class:`~parsimony.Catalog`; extracting it
keeps the ABC small and lets every catalog implementation opt into auto-
population only when the caller wants it::

    from parsimony import Catalog
    from parsimony.bundles import LazyNamespaceCatalog

    base = Catalog("multi")
    wrapped = LazyNamespaceCatalog(
        base,
        connectors=client,
        bundle_loader=lambda ns: Catalog.from_url(f"hf://ockham/{ns}"),
    )
    matches = await wrapped.search("inflation", namespaces=["fred"])
"""

from __future__ import annotations

import builtins
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from parsimony.catalog.catalog import BaseCatalog, entries_from_table_result
from parsimony.catalog.embedder_info import EmbedderInfo
from parsimony.catalog.models import (
    IndexResult,
    SeriesEntry,
    SeriesMatch,
    normalize_code,
)
from parsimony.connector import Connectors
from parsimony.errors import ConnectorError
from parsimony.result import ColumnRole

logger = logging.getLogger(__name__)

BundleLoader = Callable[[str], Awaitable[BaseCatalog | None]]
"""Async callable that returns a catalog for a given namespace, or ``None``.

Implementations typically compose a URL from the namespace and call
:meth:`parsimony.Catalog.from_url`; returning ``None`` signals "no bundle
published for this namespace yet" without raising.
"""


def _template_to_regex(template: str) -> re.Pattern[str]:
    """Compile a namespace template into a reverse-resolution regex.

    ``"sdmx_series_{agency}_{dataset_id}"`` → pattern matching
    ``"sdmx_series_<agency>_<dataset_id>"`` with named groups. Literal
    template segments are regex-escaped; placeholders become ``(?P<name>.+?)``
    (non-greedy so adjacent placeholders don't merge).
    """
    parts = re.split(r"(\{[A-Za-z_][A-Za-z0-9_]*\})", template)
    regex = ""
    for part in parts:
        if part.startswith("{") and part.endswith("}"):
            name = part[1:-1]
            regex += rf"(?P<{name}>.+?)"
        else:
            regex += re.escape(part)
    return re.compile(f"^{regex}$")


def _find_enumerator(connectors: Connectors, namespace: str) -> tuple[Any, dict[str, Any]] | None:
    """Find an ``@enumerator`` whose KEY column declares *namespace*.

    Returns ``(enumerator, extracted_params)`` or ``None``. For static namespaces
    ``extracted_params`` is ``{}``. For template namespaces (e.g. declared as
    ``"sdmx_series_{agency}_{dataset_id}"``), the resolved *namespace* is
    reverse-matched against the template and the captured groups are returned
    as kwargs suitable for ``enumerator.param_type(**extracted)``.
    """
    for conn in connectors:
        oc = conn.output_config
        if oc is None:
            continue
        roles = {c.role for c in oc.columns}
        if ColumnRole.DATA in roles:
            continue
        if ColumnRole.TITLE not in roles:
            continue
        for col in oc.columns:
            if col.role != ColumnRole.KEY or col.namespace is None:
                continue
            if not col.namespace_is_template:
                if col.namespace == namespace:
                    return conn, {}
                continue
            match = _template_to_regex(col.namespace).match(namespace)
            if match is not None:
                return conn, dict(match.groupdict())
    return None


class LazyNamespaceCatalog(BaseCatalog):
    """Decorator catalog that auto-populates missing namespaces.

    Wraps a *base* catalog and, on every :meth:`search`, ensures each
    requested namespace is present by trying (in order):

    1. A cached hit — ``base.list_namespaces()`` already includes the namespace.
    2. A bundle fetch — ``bundle_loader(namespace)`` returns a catalog which
       this wrapper :meth:`~BaseCatalog.extend` s into the base.
    3. A live enumerator from *connectors* whose KEY column declares
       *namespace* (static or template). The enumerator is invoked, its rows
       ingested, and the namespace marked resolved.

    Confirmed misses are cached so the same cold namespace is not re-probed
    on every query. Call :meth:`invalidate` after publishing a new bundle
    or binding a new connector.
    """

    def __init__(
        self,
        base: BaseCatalog,
        *,
        connectors: Connectors | None = None,
        bundle_loader: BundleLoader | None = None,
    ) -> None:
        if connectors is None and bundle_loader is None:
            raise ValueError(
                "LazyNamespaceCatalog needs at least one source of cold-namespace "
                "data: pass connectors=... or bundle_loader=..."
            )
        self._base = base
        self._connectors = connectors
        self._bundle_loader = bundle_loader
        self._attempted: set[str] = set()

    @property
    def name(self) -> str:  # type: ignore[override]
        return self._base.name

    @property
    def entries(self) -> builtins.list[SeriesEntry]:
        return self._base.entries

    @property
    def embedder_info(self) -> EmbedderInfo | None:
        return self._base.embedder_info

    # ------------------------------------------------------------------
    # Pass-through persistence
    # ------------------------------------------------------------------

    async def upsert(self, entries: builtins.list[SeriesEntry]) -> None:
        await self._base.upsert(entries)

    async def get(self, namespace: str, code: str) -> SeriesEntry | None:
        return await self._base.get(namespace, code)

    async def exists(self, keys: builtins.list[tuple[str, str]]) -> set[tuple[str, str]]:
        return await self._base.exists(keys)

    async def delete(self, namespace: str, code: str) -> None:
        await self._base.delete(namespace, code)

    async def list_namespaces(self) -> builtins.list[str]:
        return await self._base.list_namespaces()

    async def list(
        self,
        *,
        namespace: str | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[SeriesEntry], int]:
        return await self._base.list(namespace=namespace, q=q, limit=limit, offset=offset)

    async def close(self) -> None:
        await self._base.close()

    # ------------------------------------------------------------------
    # Search with auto-populate
    # ------------------------------------------------------------------

    async def search(
        self,
        query: str,
        limit: int,
        *,
        namespaces: builtins.list[str] | None = None,
    ) -> builtins.list[SeriesMatch]:
        if namespaces:
            for ns in namespaces:
                await self._ensure_namespace(ns)
        return await self._base.search(query, limit, namespaces=namespaces)

    # ------------------------------------------------------------------
    # Cache control
    # ------------------------------------------------------------------

    def invalidate(self, namespace: str | None = None) -> None:
        """Drop cached resolution(s) so the next lookup re-probes.

        Pass ``None`` to clear the full cache; pass a namespace to drop one
        entry (useful after publishing a new bundle).
        """
        if namespace is None:
            self._attempted.clear()
            return
        self._attempted.discard(normalize_code(namespace))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _ensure_namespace(self, namespace: str) -> None:
        ns = normalize_code(namespace)
        if ns in self._attempted:
            return

        existing = await self._base.list_namespaces()
        if ns in existing:
            self._attempted.add(ns)
            return

        if await self._try_bundle(ns):
            self._attempted.add(ns)
            return

        if await self._try_enumerate(ns):
            self._attempted.add(ns)
            return

        self._attempted.add(ns)
        logger.debug("Namespace %r not available from bundle loader or enumerator", ns)

    async def _try_bundle(self, namespace: str) -> bool:
        if self._bundle_loader is None:
            return False
        try:
            remote = await self._bundle_loader(namespace)
        except Exception as exc:  # bundle-loader contract: None or successful load; other errors are bugs
            logger.warning("Bundle loader failed for %s: %s", namespace, exc)
            return False
        if remote is None:
            return False
        await self._base.extend(remote)
        return True

    async def _try_enumerate(self, namespace: str) -> bool:
        if self._connectors is None:
            return False
        match = _find_enumerator(self._connectors, namespace)
        if match is None:
            return False
        enumerator, extracted = match

        try:
            if enumerator.param_type:
                params = enumerator.param_type(**extracted) if extracted else enumerator.param_type()
            else:
                params = None
            result = await enumerator(params)
            entries = entries_from_table_result(result)
            if not entries:
                return False
            idx: IndexResult = await self._base.ingest(entries, force=True)
            logger.info("Enumerated %s: %d entries", namespace, idx.indexed)
            return idx.indexed > 0
        except ConnectorError as exc:
            logger.warning("Enumerator failed for %s: %s", namespace, exc)
            return False
