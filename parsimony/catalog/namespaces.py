"""Extract catalog namespaces from enumerators.

This module provides utilities to discover all available catalog namespaces
by scanning registered enumerators across all providers.
"""

from __future__ import annotations

import importlib
import logging

from parsimony.connector import ColumnRole, Connector
from parsimony.connectors import PROVIDERS

logger = logging.getLogger(__name__)


def extract_enumerator_namespaces() -> list[str]:
    """Extract all catalog namespaces from registered enumerators.

    Scans all provider modules, finds connectors tagged with "enumerator",
    and extracts the namespace from each KEY column's OutputConfig.

    Returns:
        Sorted list of unique namespace strings, e.g. ['fred', 'sdmx_ecb_datasets', ...].
        Returns empty list if no enumerators are found.

    Raises:
        ImportError: If a provider module fails to import (only in strict mode).
        In lenient mode (default), logs and skips the provider.
    """
    namespaces: set[str] = set()

    for spec in PROVIDERS:
        try:
            module = importlib.import_module(spec.module)
            connectors = getattr(module, "CONNECTORS", None)
            if connectors is None:
                continue

            # Find all enumerators
            enumerators = connectors.filter(tags=["enumerator"])
            for connector in enumerators:
                namespace = _extract_namespace_from_connector(connector)
                if namespace:
                    namespaces.add(namespace)
        except Exception as e:
            logger.warning(
                f"Failed to load provider {spec.module}: {e}. "
                "Skipping namespace extraction."
            )
            continue

    return sorted(namespaces)


def _extract_namespace_from_connector(connector: Connector) -> str | None:
    """Extract the namespace from an enumerator connector.

    Looks for a KEY column in the connector's OutputConfig and returns its namespace.

    Args:
        connector: A Connector instance, typically tagged with "enumerator".

    Returns:
        The namespace string if found, None otherwise.
    """
    if connector.output_config is None or connector.output_config.columns is None:
        return None

    for column in connector.output_config.columns:
        if column.role == ColumnRole.KEY and column.namespace:
            return column.namespace

    return None
