# Quickstart

```python
from parsimony import Connectors, connector

@connector
async def hello(name: str) -> str:
    """Return a greeting."""
    return f"hello {name}"

result = await hello(name="world")
print(result.data)
```

For credentials, bind once:

```python
fred = fred_fetch.bind(api_key="...")
result = await fred(series_id="GDP")
```

Binding returns a new connector with a smaller public parameter surface.
