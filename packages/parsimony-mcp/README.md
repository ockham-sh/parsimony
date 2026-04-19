# parsimony-mcp

[Model Context Protocol](https://modelcontextprotocol.io/) server adapter for
[parsimony](https://parsimony.dev). Exposes any
`parsimony.connector.Connectors` collection as MCP tools.

## Install

```bash
pip install parsimony-mcp
```

## CLI

After install, the `parsimony-mcp` console script starts an MCP server over
stdio with every connector tagged `tool` that `build_connectors_from_env()`
produces (so set the relevant API keys via env vars first):

```bash
FRED_API_KEY=... parsimony-mcp
```

## Library

```python
from parsimony.connectors import build_connectors_from_env
from parsimony_mcp import create_server

server = create_server(build_connectors_from_env().filter(tags=["tool"]))
```

The server's `instructions` are auto-generated from each connector's
description, so the connected agent gets full context on what tools exist
and how to call them.
