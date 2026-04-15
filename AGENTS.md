# parsimony

## Commands

```bash
make check    # lint + typecheck + test
make format   # ruff format + auto-fix
```

## Key files

| What | Where |
|------|-------|
| Decorators, `Connectors` | `parsimony/connector.py` |
| Result types, `OutputConfig` | `parsimony/result.py` |
| Error hierarchy | `parsimony/errors.py` |
| Provider registry | `parsimony/connectors/__init__.py` |
| Adding a connector | [CONTRIBUTING.md](CONTRIBUTING.md#adding-a-new-connector) |
| Architecture | [docs/architecture.md](docs/architecture.md) |
| Full API reference | [docs/api-reference.md](docs/api-reference.md) |
| Connector patterns | [docs/connector-implementation-guide.md](docs/connector-implementation-guide.md) |

## Rules

- Python 3.11+; `X | None` not `Optional[X]`; line length 120
- All connectors `async def`; immutable by default (`frozen=True`)
- Raise `ConnectorError` subclasses, never bare `Exception`
- Never log API keys; no `print()`; no hardcoded secrets
- Run `make check` before any commit
