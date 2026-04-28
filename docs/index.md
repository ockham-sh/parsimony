# parsimony

**Typed, composable data connectors with searchable catalogs for Python.**

Replace API-wrapper boilerplate with a declarative `@connector` system:
Pydantic-validated parameters, standardized `Result` outputs with
provenance, composable routing across sources, and an optional
vector-searchable `Catalog` for entity discovery.

```python
from parsimony_fred import CONNECTORS as fred

connectors = fred.bind_env()      # reads FRED_API_KEY
result = await connectors["fred_fetch"](series_id="UNRATE")
result.data         # pandas DataFrame
result.provenance   # source, params, fetched_at
```

## Ecosystem

parsimony ships as three pip-installable pieces:

- **[parsimony-core](guide.md)** — the kernel: `@connector`, `@enumerator`,
  `@loader` decorators producing typed `Result`s with provenance, plus an
  optional vector-searchable `Catalog`.
- **[parsimony-connectors](connectors/index.md)** — official connectors for
  FRED, SDMX, FMP, SEC EDGAR, Polymarket, central banks, and more. Each is
  a standalone PyPI distribution; the kernel discovers them at runtime via
  Python entry points.
- **[parsimony-mcp](mcp-server/index.md)** — MCP stdio server exposing any
  installed connector as a tool to Claude Desktop, Claude Code, Cursor, and
  other MCP-compatible agent runtimes.

## Get started

- **[Quickstart](quickstart.md)** — install and run your first fetch in 5 minutes
- **[Guide](guide.md)** — how to use parsimony in real code
- **[Connectors](connectors/index.md)** — browse the connector catalog
- **[Reference](contract.md)** — plugin contract, API, architecture
