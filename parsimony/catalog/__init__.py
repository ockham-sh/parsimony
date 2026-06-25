"""Catalog entries, indexes, ranking, and portable snapshots."""

from parsimony.catalog.catalog import Catalog
from parsimony.catalog.indexes import BM25Index, CatalogIndex, DisMaxIndex, HybridIndex, VectorIndex
from parsimony.catalog.models import (
    BroadSearchConfigError,
    BroadSearchUnavailableError,
    CatalogMatch,
    CatalogValueMatch,
    UnknownIndexedFieldError,
    field_text,
    field_values,
    normalize_entity_code,
)
from parsimony.catalog.query import StructuredQuery, parse_query
from parsimony.catalog.remote import resolve_catalog_dir
from parsimony.catalog.runtime import auto_catalog
from parsimony.entity import Entity, code_token, entity_key, normalize_namespace

__all__ = [
    "BM25Index",
    "BroadSearchConfigError",
    "BroadSearchUnavailableError",
    "Catalog",
    "Entity",
    "CatalogIndex",
    "CatalogMatch",
    "CatalogValueMatch",
    "DisMaxIndex",
    "HybridIndex",
    "StructuredQuery",
    "UnknownIndexedFieldError",
    "VectorIndex",
    "code_token",
    "entity_key",
    "field_text",
    "field_values",
    "normalize_entity_code",
    "normalize_namespace",
    "parse_query",
    "auto_catalog",
    "resolve_catalog_dir",
]
