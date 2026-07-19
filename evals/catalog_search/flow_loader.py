"""Catalog loading for validation scripts (extracted from fusion_harness)."""

from __future__ import annotations

from parsimony_sdmx.connectors import series_search as ss

from parsimony.catalog import Catalog


def load_flow(agency: str, flow: str) -> tuple[Catalog, list[str]]:
    agency_id = ss._parse_agency(agency)
    namespace = ss.series_namespace(agency_id, flow.lower())
    path = ss._resolve_catalog_path(namespace, label=f"{agency}/{flow}")
    catalog = Catalog.load(f"file://{path}")
    dsd_order = ss._dims_from_schema(catalog._backend.column_names())
    fields = ss._search_surface(catalog, "q", None, dsd_order)
    assert isinstance(fields, list) and fields
    return catalog, fields
