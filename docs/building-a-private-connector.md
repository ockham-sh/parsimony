# Building a Private Connector

Private connectors use the same contract as public ones: async function, `@connector`, optional output schema, and `CONNECTORS` export.

```python
@connector(tags=["your_source", "tool"])
async def your_fetch(symbol: str, api_key: str = "") -> pd.DataFrame:
    """Fetch private source data."""
    key = api_key or os.environ.get("YOUR_API_KEY", "")
    if not key:
        raise UnauthorizedError("your_source", env_var="YOUR_API_KEY")
    ...

CONNECTORS = Connectors([your_fetch])
```

Operators can create configured variants:

```python
runtime = CONNECTORS.bind(api_key="secret")
```

For non-secret resources such as database pools, bind the object before exposing the connector to agents.
