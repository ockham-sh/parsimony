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
| Result types, `OutputSpec` | `parsimony/result.py` |
| `Catalog` | `parsimony/catalog/catalog.py` (package `parsimony.catalog`) |
| Catalog search helpers | `parsimony/catalog/search.py` |
| Composable catalog filters | `parsimony/catalog/filters.py` |
| FAISS/BM25 pure functions | `parsimony/indexes.py` |
| Catalog index types | `parsimony/catalog/indexes.py` |
| Plugin discovery | `parsimony/discover.py` |
| CLI (`list`, `cache`) | `parsimony/cli.py` |
| Agent skill | `skills/parsimony/SKILL.md` — installed by `npx skills add ockham-sh/parsimony` / `gh skill install` / `uvx npx-skills add`; also force-included into the wheel at `parsimony/skills/` so `importlib.resources.files("parsimony")` can reach it at runtime for the embedded-agent path |
| Global cache (root, subdirs, `TTLDiskCache`) | `parsimony/cache.py` |
| Conformance suite | `parsimony/testing.py` |
| Error hierarchy | `parsimony/errors.py` |
| HTTP transport | `parsimony/transport/` (package; `helpers.py` for connectors) |
| Plugin contract (authoritative) | [docs/contract.md](docs/contract.md) |
| API reference | [docs/reference/api.md](docs/reference/api.md) |

## Rules

- Python 3.11+; `X | None` not `Optional[X]`; line length 120
- All connectors synchronous (`def`, not `async def`); immutable by default (`frozen=True`)
- Raise `ConnectorError` subclasses, never bare `Exception`
- Never log API keys; no `print()`; no hardcoded secrets
- No provider-specific code in the kernel — `test_kernel_purity.py` enforces this
- All cache writes go under `parsimony.cache.root()` (the
  `PARSIMONY_CACHE_DIR` / `platformdirs.user_cache_dir("parsimony")`
  tree). No repo-relative cache paths. Connector authors use
  `parsimony.cache.connectors_dir("<provider>")` for scratch
- Run `make check` before any commit
