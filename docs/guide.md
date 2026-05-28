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
bundle = configured + other_connectors
result = await bundle["fetch_price"]("AAPL")
```

## Exposed signature

```python
list(configured.exposed_signature.parameters)
```

After `bind`, only unbound parameters remain in the exposed signature.
