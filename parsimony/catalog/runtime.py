"""Ad-hoc, in-memory catalogs over runtime data.

This is the *convenience* path, not the way catalogs are built. A real catalog —
curated, persistable, optionally vector-backed — is assembled through the
lifecycle on :class:`~parsimony.catalog.Catalog` itself: construct, load entities
(see :func:`~parsimony.entity.entities_from_dataframe` for explicit column roles),
:meth:`~parsimony.catalog.Catalog.build`, and optionally
:meth:`~parsimony.catalog.Catalog.save`.

:func:`auto_catalog` exists for the opposite case: a coding agent (or any
caller) holding a DataFrame it produced this moment and wanting to find a needle
in it. There is nothing to curate and nothing to persist — every row is a row, and
the only question is "which rows match." It returns a catalog that is *already
built*, so the build gate cannot be tripped.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable

import numpy as np
import pandas as pd

from parsimony.catalog.catalog import Catalog
from parsimony.entity import Entity

#: Recursion bound for :func:`_json_safe`. A cell nested deeper than this falls
#: back to ``str`` — metadata is for search, not faithful reconstruction, so a
#: pathological structure becomes searchable text instead of a ``RecursionError``.
_MAX_JSON_DEPTH = 20


def _is_null(value: object) -> bool:
    """True for None / NaN / NaT scalars; False for any container or real value."""
    if value is None:
        return True
    try:
        flag = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(flag) if isinstance(flag, (bool, np.bool_)) else False


def _json_safe(value: object, _depth: int = 0) -> object:
    """Coerce a cell to a JSON-friendly scalar/list/dict for entity metadata.

    numpy scalars unwrap via ``.item()``; nested lists/dicts recurse up to
    ``_MAX_JSON_DEPTH`` (beyond which the structure falls back to ``str``);
    anything else falls back to ``str`` so structured ``column: value`` search
    still has text to match against.
    """
    if not isinstance(value, (str, bytes)) and hasattr(value, "item"):
        with contextlib.suppress(ValueError, AttributeError):
            value = value.item()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if _depth >= _MAX_JSON_DEPTH:
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(item, _depth + 1) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item, _depth + 1) for key, item in value.items()}
    return str(value)


def _row_text(values: Iterable[object]) -> str:
    """Whitespace-join the non-null cells of a row into broad-search title text."""
    parts = [str(value).strip() for value in values if not _is_null(value) and str(value).strip()]
    return " ".join(parts)


def auto_catalog(df: pd.DataFrame, *, name: str = "output") -> Catalog:
    """Wrap a DataFrame in an already-built, BM25-searchable catalog, in memory.

    For finding rows in data you already hold — *not* for building a catalog to
    keep. One :class:`~parsimony.entity.Entity` per row: ``code`` is the row's
    positional index (so ``df.iloc[int(match.code)]`` recovers the full row),
    ``title`` is the whitespace-joined non-null cell text (broad search), and
    ``metadata`` maps each column to its cell value (structured ``column: value``
    search). Every field is BM25-indexed under the catalog's default index policy.

    The returned catalog is already built — call
    :meth:`~parsimony.catalog.Catalog.search` directly and repeatedly; each query
    scores against the prebuilt indexes with no re-index, and the build gate that
    guards a hand-assembled catalog cannot be tripped.

    Columns named ``code``, ``title``, or ``namespace`` collide with the reserved
    :class:`Entity` fields: their values are still stored in ``metadata``, but a
    structured ``code:`` / ``title:`` / ``namespace:`` query resolves to the
    Entity's own field (the row position / joined row text), not the column.
    Duplicate column names are rejected — they cannot be distinct metadata keys.

    Search is BM25 only — which works on a bare ``pip install parsimony-core`` (no
    extra). Semantic (vector) search is intentionally unavailable: a runtime frame
    is fresh data with no prebuilt vector index, and the typical caller (a sandboxed
    agent) has neither network nor an embedder. To build a curated, persistable,
    optionally vector-backed catalog, use the :class:`~parsimony.catalog.Catalog`
    lifecycle directly instead.
    """
    if df.columns.duplicated().any():
        dupes = sorted({str(col) for col in df.columns[df.columns.duplicated(keep=False)]})
        raise ValueError(
            f"auto_catalog requires unique column names; duplicated: {dupes}. "
            "Rename or deduplicate the columns before indexing."
        )
    catalog = Catalog(name)  # indexes=None -> default BM25 policy, materialized at build()
    namespace = catalog.name
    entities: list[Entity] = []
    for position, record in enumerate(df.to_dict("records")):
        metadata = {str(col): _json_safe(value) for col, value in record.items() if not _is_null(value)}
        code = str(position)
        title = _row_text(record.values()) or code
        entities.append(Entity(namespace=namespace, code=code, title=title, metadata=metadata))
    catalog.set_entities(entities)
    catalog.build()
    return catalog


__all__ = ["auto_catalog"]
