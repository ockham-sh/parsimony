"""Bridge connector/tabular outputs into discoverable entities."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pandas as pd

from parsimony.connector import Connector
from parsimony.entity import Entity
from parsimony.result import OutputConfig, Result, TabularResult


def _dataframe_from_raw(raw: Any) -> pd.DataFrame:
    if isinstance(raw, TabularResult):
        return raw.data
    if isinstance(raw, Result):
        data = raw.data
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, pd.Series):
            return pd.DataFrame(data)
    if isinstance(raw, pd.DataFrame):
        return raw
    if isinstance(raw, pd.Series):
        return pd.DataFrame(raw)
    raise TypeError(f"Cannot extract catalog entities from {type(raw)!r}")


def entities_from_raw(
    raw: Any,
    output: OutputConfig,
) -> list[Entity]:
    """Map a tabular connector result to entities using *output* column roles."""
    if isinstance(raw, list):
        raise TypeError(
            "Connectors must not return list[Entity]; return a DataFrame and project entities in catalog build code"
        )
    if isinstance(raw, Result) and isinstance(raw.data, list):
        raise TypeError("Connectors must not return list[Entity]; return a DataFrame")
    df = _dataframe_from_raw(raw)
    return output.build_entities(df)


async def entities_from_connector(
    source: Connector | Callable[..., Awaitable[Any]],
    output: OutputConfig,
    **kwargs: Any,
) -> list[Entity]:
    """Invoke *source* and convert its return value into catalog entries."""
    if isinstance(source, Connector):
        result = await source(**kwargs)
        return entities_from_raw(result, output)
    raw = await source(**kwargs)
    return entities_from_raw(raw, output)


def lazy_catalog_dir(provider: str, namespace: str) -> str:
    """Return the on-disk lazy-cache directory for a provider catalog namespace."""
    from parsimony import cache

    return str(cache.connectors_dir(provider) / "catalogs" / namespace)


__all__ = [
    "entities_from_connector",
    "entities_from_raw",
    "lazy_catalog_dir",
]
