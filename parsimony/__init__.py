"""Parsimony — typed connectors and a hybrid-search catalog for financial data.

Flat module layout. Heavy symbols (:class:`Catalog` and its FAISS /
sentence-transformers / huggingface-hub stack) load lazily on first access
via :pep:`562` so that ``import parsimony`` stays cheap.

* :class:`Connectors` is an immutable collection of :class:`Connector` objects;
  callers use ``await connectors[name](**kwargs)``. Each connector validates
  its input through a Pydantic param model.
* :class:`CatalogBackend` is the structural contract every catalog matches.
  :class:`Catalog` is the canonical implementation (Parquet rows + FAISS
  vectors + BM25 keywords + RRF) and is loaded lazily.
* Connector plugins are discovered through the ``parsimony.providers``
  entry-point group. Catalog publishing reads ``CATALOGS`` / optional
  ``RESOLVE_CATALOG`` on the plugin module — see :func:`parsimony.publish.publish`.
"""

from __future__ import annotations

import os
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from parsimony.connector import (
    Connector,
    Connectors,
    ResultCallback,
    connector,
    enumerator,
    loader,
)
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.stores import InMemoryDataStore, LoadResult

try:
    __version__ = version("parsimony-core")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"


__all__ = [
    # --- Connector primitives ---
    "Connector",
    "Connectors",
    "ResultCallback",
    "connector",
    "enumerator",
    "loader",
    # --- Result system ---
    "Column",
    "ColumnRole",
    "OutputConfig",
    "Provenance",
    "Result",
    # --- Catalog (lazy) ---
    "Catalog",
    "CatalogBackend",
    "EmbedderInfo",
    "EmbeddingProvider",
    "IndexResult",
    "LiteLLMEmbeddingProvider",
    "SentenceTransformerEmbedder",
    "SeriesEntry",
    "SeriesMatch",
    "catalog_key",
    "code_token",
    "normalize_code",
    "normalize_entity_code",
    "parse_catalog_url",
    "series_match_from_entry",
    # --- Data persistence ---
    "InMemoryDataStore",
    "LoadResult",
    # --- Errors ---
    "ConnectorError",
    "EmptyDataError",
    "ParseError",
    "PaymentRequiredError",
    "ProviderError",
    "RateLimitError",
    "UnauthorizedError",
    # --- Convenience ---
    "client",
    "load_dotenv",
]


# Heavy symbols — loaded lazily via PEP 562 so ``import parsimony`` does not
# pull torch / faiss / huggingface-hub. Keys are the public attribute names;
# values are ``(module, attribute)``.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Catalog": ("parsimony.catalog", "Catalog"),
    "CatalogBackend": ("parsimony.catalog", "CatalogBackend"),
    "EmbedderInfo": ("parsimony.embedder", "EmbedderInfo"),
    "EmbeddingProvider": ("parsimony.embedder", "EmbeddingProvider"),
    "IndexResult": ("parsimony.catalog", "IndexResult"),
    "LiteLLMEmbeddingProvider": ("parsimony.embedder", "LiteLLMEmbeddingProvider"),
    "SentenceTransformerEmbedder": ("parsimony.embedder", "SentenceTransformerEmbedder"),
    "SeriesEntry": ("parsimony.catalog", "SeriesEntry"),
    "SeriesMatch": ("parsimony.catalog", "SeriesMatch"),
    "catalog_key": ("parsimony.catalog", "catalog_key"),
    "code_token": ("parsimony.catalog", "code_token"),
    "normalize_code": ("parsimony.catalog", "normalize_code"),
    "normalize_entity_code": ("parsimony.catalog", "normalize_entity_code"),
    "parse_catalog_url": ("parsimony.catalog", "parse_catalog_url"),
    "series_match_from_entry": ("parsimony.catalog", "series_match_from_entry"),
}


_client_cache: Any = None

_DOTENV_ANCHOR_FILES = (".git", "pyproject.toml", ".mcp.json")


def load_dotenv() -> None:
    """Apply the nearest project-local ``.env`` to ``os.environ``.

    Walks upward from ``Path.cwd()`` until it finds a directory containing
    ``.git``, ``pyproject.toml``, or ``.mcp.json`` (the project root anchor);
    if a sibling ``.env`` exists, its key=value pairs are applied to
    ``os.environ`` only when not already present (pre-existing env always
    wins). Stops at ``$HOME`` or the filesystem root, whichever comes first;
    a search starting outside ``$HOME`` is a no-op so a stray ``/tmp/.env``
    can't leak in. Idempotent — safe to call multiple times.

    Designed for the ``python -c "..."`` agent-escape-hatch context where
    the user maintains a project ``.env`` for their MCP / agent setup and
    wants the subprocess to inherit those keys without ``--env-file`` flag
    discipline.
    """
    from dotenv import load_dotenv as _dotenv_load

    home = Path.home().resolve()
    try:
        cwd = Path.cwd().resolve()
    except OSError:
        return
    if not _path_under(cwd, home):
        return

    for directory in (cwd, *cwd.parents):
        candidate = directory / ".env"
        if candidate.is_file():
            _dotenv_load(candidate, override=False)
            return
        if any((directory / marker).exists() for marker in _DOTENV_ANCHOR_FILES):
            return
        if directory == home:
            return


def _path_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _cap_cell(value: Any, max_chars: int = 500) -> Any:
    """Truncate a string cell so a single rogue upstream value can't blow the
    agent's context budget. Non-strings pass through unchanged."""
    if isinstance(value, str) and len(value) > max_chars:
        return value[: max_chars - 1] + "…"
    return value


def _emit_fetch_summary(result: Result) -> None:
    """Write a TOON-encoded summary of *result* to ``sys.stderr``.

    Wired into :data:`client` via :meth:`Connectors.with_callback` so an
    agent invoking ``parsimony.client[...]`` from a ``python -c "..."``
    subprocess sees source / params / shape / head(5) without `print()`
    discipline. Honors ``PARSIMONY_QUIET=1`` so callers that have their
    own structured drain (e.g. parsimony-agents' executor) can opt out.
    """
    if os.environ.get("PARSIMONY_QUIET"):
        return
    try:
        import pandas as pd
        from toon_format import encode
    except ImportError:
        return

    prov = result.provenance
    payload: dict[str, Any] = {
        "source": prov.source,
        "params": dict(prov.params),
    }
    data = result.data
    if isinstance(data, pd.DataFrame):
        payload["rows"] = int(len(data))
        payload["preview"] = data.head(5).map(_cap_cell).to_dict("records")
    elif isinstance(data, pd.Series):
        payload["rows"] = int(len(data))
        payload["preview"] = data.head(5).map(_cap_cell).to_dict()
    else:
        payload["value"] = _cap_cell(str(data))

    sys.stderr.write(encode(payload) + "\n")


def __getattr__(name: str) -> Any:
    global _client_cache

    if name == "client":
        if _client_cache is None:
            from parsimony.discovery import build_connectors_from_env

            load_dotenv()
            _client_cache = build_connectors_from_env().with_callback(_emit_fetch_summary)
        return _client_cache

    spec = _LAZY_IMPORTS.get(name)
    if spec is not None:
        import importlib

        module = importlib.import_module(spec[0])
        return getattr(module, spec[1])

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
