# Guide

## Define a Connector

```python
from parsimony import connector

@connector
async def fetch_price(symbol: str, api_key: str = "") -> pd.DataFrame:
    """Fetch price data for a symbol."""
    ...
```

Function parameters are connector parameters. Use normal Python defaults for optional values.

## Bind Values

```python
configured = fetch_price.bind(api_key="secret")
result = await configured(symbol="AAPL")
```

The configured connector exposes only `symbol`. Provenance records `symbol`, not the bound key.

## Compose Connectors

```python
bundle = Connectors.merge(Connectors([configured]), other_connectors)
result = await bundle["fetch_price"]("AAPL")
```

## Tool Schemas

```python
schema = configured.to_json_schema()
```

Schema projection is optional. Python-only connectors can use richer parameter types and fail only when exported as tools.
