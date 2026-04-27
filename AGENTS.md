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
| `CatalogBackend` Protocol, `Catalog` | `parsimony/catalog.py` |
| Plugin discovery | `parsimony/discover.py` |
| Publish orchestrator (`CATALOGS`) | `parsimony/publish.py` |
| CLI (`list`, `publish`, `cache`) | `parsimony/cli.py` |
| Global cache (root, subdirs, `TTLDiskCache`) | `parsimony/cache.py` |
| Conformance suite | `parsimony/testing.py` |
| Error hierarchy | `parsimony/errors.py` |
| HTTP transport | `parsimony/transport.py` |
| Plugin contract (authoritative) | [docs/contract.md](docs/contract.md) |
| Architecture | [docs/architecture.md](docs/architecture.md) |
| API reference | [docs/api-reference.md](docs/api-reference.md) |

## Rules

- Python 3.11+; `X | None` not `Optional[X]`; line length 120
- All connectors `async def`; immutable by default (`frozen=True`)
- Raise `ConnectorError` subclasses, never bare `Exception`
- Never log API keys; no `print()`; no hardcoded secrets
- No provider-specific code in the kernel — `test_kernel_purity.py` enforces this
- All cache writes go under `parsimony.cache.root()` (the
  `PARSIMONY_CACHE_DIR` / `platformdirs.user_cache_dir("parsimony")`
  tree). No repo-relative cache paths. Connector authors use
  `parsimony.cache.connectors_dir("<provider>")` for scratch
- Run `make check` before any commit
