"""SDMX connectors for parsimony.

Registered with parsimony core via the ``parsimony.providers`` entry point::

    [project.entry-points."parsimony.providers"]
    sdmx = "parsimony_sdmx:PROVIDER"

Direct use::

    from parsimony_sdmx import CONNECTORS, sdmx
"""

from __future__ import annotations

from parsimony.connectors import ProviderSpec

from parsimony_sdmx._connectors import (
    CONNECTORS,
    SdmxCodelistParams,
    SdmxDatasetCodelistsParams,
    SdmxDsdParams,
    SdmxFetchParams,
    SdmxListDatasetsParams,
    SdmxSeriesKeysParams,
    enumerate_sdmx_dataset_codelists,
    institution_source_from_dataset_key,
    sdmx,
    sdmx_agency_namespace,
    sdmx_codelist,
    sdmx_codelist_namespace,
    sdmx_dsd,
    sdmx_list_datasets,
    sdmx_namespace_from_dataset_key,
    sdmx_series_keys,
)

__all__ = [
    "CONNECTORS",
    "PROVIDER",
    "SdmxCodelistParams",
    "SdmxDatasetCodelistsParams",
    "SdmxDsdParams",
    "SdmxFetchParams",
    "SdmxListDatasetsParams",
    "SdmxSeriesKeysParams",
    "enumerate_sdmx_dataset_codelists",
    "institution_source_from_dataset_key",
    "sdmx_agency_namespace",
    "sdmx",
    "sdmx_codelist",
    "sdmx_codelist_namespace",
    "sdmx_dsd",
    "sdmx_list_datasets",
    "sdmx_namespace_from_dataset_key",
    "sdmx_series_keys",
]

PROVIDER = ProviderSpec(name="sdmx", connectors=CONNECTORS)
