# FAQ

## Do connector parameters need to be Pydantic models?

No. A connector is a normal async callable. Pydantic models can still be used as ordinary parameter annotations when that is useful.

## How do I hide an API key from agents and provenance?

Bind it before exposing the connector:

```python
runtime = fred_fetch.bind(api_key="secret")
```

The returned connector no longer exposes `api_key`, and provenance records only call-time parameters.

## Does Parsimony read environment variables for me?

No. Connector implementations own provider-specific auth. They can read env vars internally or accept explicit parameters that operators bind.

## When does JSON compatibility matter?

Only when converting a connector to a tool schema. Python execution does not require JSON-compatible annotations.
