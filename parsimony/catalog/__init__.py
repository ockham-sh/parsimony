"""Catalog entries, indexes, ranking, and portable snapshots."""

from parsimony.catalog.catalog import Catalog
from parsimony.catalog.filters import (
    F,
    FieldContains,
    FieldIn,
    FieldMatches,
    FieldPrefix,
    Filter,
    FilterLike,
    all_of,
    any_of,
    as_filter,
)
from parsimony.catalog.indexes import BM25Index, CatalogIndex, HybridIndex, VectorIndex
from parsimony.catalog.models import (
    BroadSearchUnavailableError,
    CatalogMatch,
    CatalogValueMatch,
    ComponentSearchDetail,
    FieldSearchDetail,
    SearchDetail,
    UnknownIndexedFieldError,
    field_text,
    field_values,
    normalize_entity_code,
)
from parsimony.catalog.remote import resolve_catalog_dir
from parsimony.catalog.runtime import auto_catalog
from parsimony.entity import Entity, code_token, entity_key, normalize_namespace

__all__ = [
    "BM25Index",
    "BroadSearchUnavailableError",
    "Catalog",
    "Entity",
    "CatalogIndex",
    "CatalogMatch",
    "CatalogValueMatch",
    "ComponentSearchDetail",
    "F",
    "FieldContains",
    "FieldIn",
    "FieldMatches",
    "FieldPrefix",
    "FieldSearchDetail",
    "Filter",
    "FilterLike",
    "HybridIndex",
    "SearchDetail",
    "UnknownIndexedFieldError",
    "VectorIndex",
    "all_of",
    "any_of",
    "as_filter",
    "code_token",
    "entity_key",
    "field_text",
    "field_values",
    "normalize_entity_code",
    "normalize_namespace",
    "auto_catalog",
    "resolve_catalog_dir",
]
