# Cookbook

## Bind a Credential

```python
fred = FRED.bind(api_key="YOUR_FRED_KEY")
result = await fred["fred_fetch"](series_id="GDP")
```

## Merge Sources

```python
bundle = Connectors.merge(fred, SDMX)
```

## Export a Tool Schema

```python
schema = fred["fred_search"].to_json_schema()
```

If a public parameter cannot be represented as JSON Schema, bind it first or keep the connector Python-only.
