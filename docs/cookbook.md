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

## Inspect exposed parameters

```python
fred["fred_search"].exposed_signature
```

Bind secrets before passing connectors to untrusted callers.
