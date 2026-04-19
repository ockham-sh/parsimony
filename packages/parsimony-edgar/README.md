# parsimony-edgar

SEC EDGAR connectors for [parsimony](https://parsimony.dev). Wraps
[`edgartools`](https://github.com/dgunning/edgartools) to expose 15 connectors
covering company search, profiles, financial statements, filing search,
filing documents, sections, tables, and insider trades.

## Install

```bash
pip install parsimony-edgar
```

The provider is registered via the `parsimony.providers` entry-point group
and is picked up automatically by `build_connectors_from_env()`.

## Direct use

```python
from parsimony_edgar import sec_edgar_find_company

result = await sec_edgar_find_company(query="Apple")
```
