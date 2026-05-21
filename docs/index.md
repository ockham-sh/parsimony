# Parsimony

Parsimony is a minimal connector framework for financial data.

- Write async Python functions.
- Decorate them with `@connector`.
- Bind operator-supplied values once.
- Receive standardized `Result` objects with provenance.

```python
connectors = discover.load_all()
result = await connectors["fred_fetch"](series_id="GDP")
```

Connector packages decide how they handle credentials. Operators can use `bind` to create configured connector variants.
