"""Parsimony — typed connectors and a hybrid-search catalog for financial data.

Flat module layout. Heavy symbols (:class:`Catalog` and its FAISS /
sentence-transformers / huggingface-hub stack) load lazily on first access
via :pep:`562` so that ``import parsimony`` stays cheap.

* :class:`Connectors` is an immutable collection of :class:`Connector` objects;
  callers use ``connectors[name](**kwargs)``. The callable signature is
  the connector's parameter surface.
* :class:`Catalog` is the canonical implementation (Parquet rows + HybridIndex
  over FAISS vectors and BM25 keywords with ZScoreFusion) and is loaded lazily.
* Connector plugins are discovered through the ``parsimony.providers``
  entry-point group via :mod:`parsimony.discover`.
"""

from __future__ import annotations

import importlib
import warnings
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

from parsimony import cache as cache  # re-export so ``from parsimony import cache`` works
from parsimony import discover as discover  # re-export so ``from parsimony import discover`` works

# Static re-exports for type checkers and linters (CodeQL py/undefined-export,
# mypy, pyright). At runtime the names below are resolved by ``__getattr__``
# from ``_LAZY_IMPORTS`` so heavy deps (FAISS, torch, huggingface-hub) stay
# lazy; ``TYPE_CHECKING`` keeps the imports out of the runtime path.
if TYPE_CHECKING:
    from parsimony.catalog import (
        BM25Index,
        Catalog,
        CatalogIndex,
        CatalogMatch,
        Entity,
        HybridIndex,
        VectorIndex,
        auto_catalog,
    )
    from parsimony.ranking import (
        RRF,
        MinMaxScoreFusion,
        Ranker,
        Ranking,
        ZScoreFusion,
    )
    from parsimony.stores import InMemoryDataStore, LoadResult
from parsimony.connector import (
    Connector,
    Connectors,
    connector,
    enumerator,
    loader,
)
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.namespace import Namespace
from parsimony.result import Column, ColumnRole, OutputSpec, Provenance, Result

try:
    __version__ = version("parsimony-core")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

# tqdm emits a ``TqdmWarning: "IProgress not found..."`` at *import* of
# ``tqdm.autonotebook`` (pulled in transitively by sentence-transformers /
# huggingface-hub / transformers), before any runtime ``TQDM_DISABLE`` can catch
# it. It is pure noise for a non-notebook process. Register the filter here (at
# package import, before any heavy lazy import fires). Match by message only —
# ``category=TqdmWarning`` would force a hard ``import tqdm`` at package import
# (defeating the cheap-import invariant), and ``module=`` is unreliable because
# the warning is attributed to the importer frame via ``stacklevel``.
warnings.filterwarnings("ignore", message="IProgress not found")


__all__ = [
    "Connector",
    "Connectors",
    "Namespace",
    "connector",
    "enumerator",
    "loader",
    "Result",
    "OutputSpec",
    "Column",
    "ColumnRole",
    "Provenance",
    "ConnectorError",
    "EmptyDataError",
    "InvalidParameterError",
    "ParseError",
    "PaymentRequiredError",
    "ProviderError",
    "RateLimitError",
    "UnauthorizedError",
    "cache",
    "discover",
    "Catalog",
    "Entity",
    "CatalogMatch",
    "BM25Index",
    "VectorIndex",
    "HybridIndex",
    "CatalogIndex",
    "auto_catalog",
    "InMemoryDataStore",
    "LoadResult",
    "RRF",
    "Ranker",
    "Ranking",
    "ZScoreFusion",
    "MinMaxScoreFusion",
]


# Heavy symbols — loaded lazily via PEP 562 so ``import parsimony`` does not
# pull torch / faiss / huggingface-hub. Keys are public attribute names; values
# are ``(module, attribute)``.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "BM25Index": ("parsimony.catalog", "BM25Index"),
    "Catalog": ("parsimony.catalog", "Catalog"),
    "Entity": ("parsimony.catalog", "Entity"),
    "CatalogIndex": ("parsimony.catalog", "CatalogIndex"),
    "CatalogMatch": ("parsimony.catalog", "CatalogMatch"),
    "HybridIndex": ("parsimony.catalog", "HybridIndex"),
    "VectorIndex": ("parsimony.catalog", "VectorIndex"),
    "auto_catalog": ("parsimony.catalog", "auto_catalog"),
    "InMemoryDataStore": ("parsimony.stores", "InMemoryDataStore"),
    "LoadResult": ("parsimony.stores", "LoadResult"),
    "RRF": ("parsimony.ranking", "RRF"),
    "Ranker": ("parsimony.ranking", "Ranker"),
    "Ranking": ("parsimony.ranking", "Ranking"),
    "ZScoreFusion": ("parsimony.ranking", "ZScoreFusion"),
    "MinMaxScoreFusion": ("parsimony.ranking", "MinMaxScoreFusion"),
}


def __getattr__(name: str) -> Any:
    spec = _LAZY_IMPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module 'parsimony' has no attribute {name!r}")
    return getattr(importlib.import_module(spec[0]), spec[1])
