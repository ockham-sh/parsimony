# Connector Implementation Guide

## Shape

A connector is an async callable:

```python
@connector(output=OUTPUT, tags=["tool"])
async def my_source_fetch(series_id: str, api_key: str = "") -> pd.DataFrame:
    """Fetch observations for a series."""
    key = api_key or os.environ.get("MY_SOURCE_API_KEY", "")
    if not key:
        raise UnauthorizedError("my_source", env_var="MY_SOURCE_API_KEY")
    ...
```

Use normal Python parameters. Use Pydantic models only inside the connector body for validation — never as a single public ``params`` argument.

## Binding

Tests and applications can bind operator-supplied values:

```python
bound = CONNECTORS.bind(api_key="test-key")
result = await bound["my_source_fetch"](series_id="CPI")
```

Bound values are omitted from provenance.

## Errors

Raise `ConnectorError` subclasses for provider/runtime failures. Use `TypeError` and `ValueError` for programmer mistakes.

## Checklist

- [ ] Connector is async.
- [ ] Description is non-empty.
- [ ] Output schema roles are correct.
- [ ] Auth behavior is explicit in connector code.
- [ ] Tests assert success, typed errors, and no secret leakage.
