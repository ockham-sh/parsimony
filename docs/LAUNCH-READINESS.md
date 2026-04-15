# Open-Source Launch Readiness Report

**Date:** 2026-04-15
**Package:** parsimony v0.1.0a1
**Repository:** github.com/ockham-sh/parsimony

---

## Executive Summary

Parsimony is architecturally sound and well-designed for open-source launch. The core framework (connector decorators, result system, catalog, error hierarchy) is clean, well-tested, and production-ready. The issues found are almost entirely packaging, documentation, and CI polish -- not architectural or code-quality problems.

**Verdict: Ready to launch after the fixes below.**

---

## What's Excellent (Keep As-Is)

### Architecture
- **Decorator-based connector pattern** (`@connector`, `@enumerator`, `@loader`) is elegant and intuitive
- **Frozen dataclasses + immutable collections** throughout -- thread-safe by design
- **Typed error hierarchy** (ConnectorError subtree) with provider attribution is best-in-class
- **Pydantic validation at boundaries** -- no raw dicts cross the public API
- **Lazy loading in `__init__.py`** keeps `import parsimony` fast
- **OutputConfig with ColumnRole** is a genuinely novel abstraction for data connectors
- **Provenance tracking** on every result is a strong differentiator

### Code Quality
- No TODO/FIXME/HACK comments in source
- No print statements (proper logging throughout)
- No hardcoded secrets in examples or source
- Consistent code style (ruff configured, 120-char lines)
- Type hints on all public APIs
- Enforced conventions (EC-1 through EC-5) from council reviews

### Documentation
- Comprehensive docs: quickstart, user guide, architecture, API reference, connector guide
- 7 working examples covering all major use cases
- Well-structured PR and issue templates
- Security policy with response timeline
- Contributor guide with step-by-step process

### Test Infrastructure
- 23 test files covering core framework, providers, stores, MCP
- 80% coverage threshold enforced
- Integration test marker for live API tests
- Clean test isolation (no shared mutable state)

---

## Issues Fixed (This Audit)

### Critical (Would Block Launch)

1. **LICENSE copyright said "Flowstack"** -- changed to "Ockham.sh"
2. **Python version upper bound `<3.13`** -- removed; now `>=3.11` (3.13 users couldn't install)
3. **`from parsimony import client` crashed** without API keys -- now uses `lenient=True` with cached singleton
4. **mkdocs nav referenced wrong filename** -- `connector-guide.md` fixed to `connector-implementation-guide.md`
5. **Bug report template said "ockham"** -- updated to "parsimony" throughout
6. **Bug report listed non-existent IBKR connector** -- removed, added missing providers

### Important (Would Hurt Adoption)

7. **No `py.typed` marker** -- added for PEP 561 (mypy users get type checking)
8. **No `__version__` attribute** -- added via `importlib.metadata`
9. **`pydantic-core` pinned as explicit dep** -- removed (transitive via pydantic)
10. **CI only ran pytest** -- added ruff, mypy, coverage, pip caching, Python 3.13
11. **No `[dev]` extra** -- added (pytest, pytest-asyncio, pytest-cov, ruff, mypy)
12. **Eager imports pulled in pandas+pyarrow** on `import parsimony` -- moved to lazy loading
13. **`parsimony.client` rebuilt on every access** -- now cached singleton
14. **README data sources table listed only 7 of 24 providers** -- expanded to all
15. **Docs showed dict calling convention** that code actually rejects -- removed
16. **CONTRIBUTING.md linked `../../CODE_OF_CONDUCT.md`** (monorepo path) -- fixed to `CODE_OF_CONDUCT.md`
17. **No CHANGELOG `[Unreleased]` section** -- added
18. **Bare `Exception` catch in `data_store.py`** violated EC-1 -- narrowed to specific types

### Polish

19. **`.gitignore` missing `.mypy_cache/`, `.ruff_cache/`, `.claude/`, `.council/`** -- added
20. **No `dependabot.yml`** -- added for pip + GitHub Actions updates
21. **PyPI classifiers missing** `Typing :: Typed`, Python versions, License -- added
22. **Example docstring referenced `poetry run`** but build backend is hatchling -- fixed
23. **CONTRIBUTING.md install commands** now use `[dev]` extra instead of manual pip installs

---

## Remaining Items (Your Decision)

### Before Public Push

1. **Remove PLAN-*.md files** (7 files) -- internal planning documents with implementation details. Either:
   - `git rm PLAN-*.md` before pushing to public repo, or
   - Add `PLAN-*.md` to `.gitignore` if you want them in the monorepo only

2. **Remove `.council/` directory** -- contains internal review output with local file paths (`/home/espinet/...`). Already added to `.gitignore` for future, but existing tracked files need removal.

3. **Remove `.claude/` worktrees** -- development artifacts. Already added to `.gitignore`.

4. **Decide on `conventions.md`** -- references "council reviews" which is internal governance. Could be valuable to keep (shows project maturity) or could confuse contributors.

### Nice-to-Have (Not Blocking)

5. **Add `__all__` to submodules** (`result.py`, `errors.py`, `connector.py`) -- low priority since main `__init__.py` controls the public API, but good practice

6. **Add root `conftest.py`** at `tests/` level -- currently only `test_mcp/` has one. Not needed yet since tests are well-isolated.

7. **Consider `uv` in CI** -- faster than pip, matches your dev setup recommendation

8. **Consider adding a `Makefile` or `justfile`** with common commands:
   ```
   make test      # pytest
   make lint      # ruff check + format
   make typecheck # mypy
   make docs      # mkdocs serve
   ```

9. **Consider `CNAME` file cleanup** -- there's a CNAME file at repo root (likely for docs hosting). Verify it points to the right domain.

10. **Consider a GitHub Actions badge** in README that links to the new CI workflow name (renamed from "Test" to "CI")

---

## Architecture Assessment

### Strengths
- Clean module dependency graph: `errors` -> `result` -> `connector` -> `catalog` -> `stores`
- No circular imports
- Lazy loading prevents import-time heaviness
- Provider registry pattern (`ProviderSpec`) makes adding connectors trivial
- MCP integration is well-isolated in `mcp/` subpackage

### Things to Watch Post-Launch
- **24 provider modules** is a lot of surface area to maintain. Consider community ownership model for less-used providers
- **`build_connectors_from_env()` imports all provider modules** even if credentials are missing. This means installing parsimony pulls in all provider code. Not a problem now, but could be as the provider count grows.
- **SQLite catalog** stores everything in `~/.parsimony/`. Document this path prominently so users know where their data lives.

---

## Files Modified

```
parsimony/__init__.py          -- lazy imports, __version__, cached client
parsimony/py.typed             -- NEW (PEP 561 marker)
parsimony/stores/data_store.py -- narrowed exception catch
pyproject.toml                 -- Python version, classifiers, deps, dev extra
LICENSE                        -- copyright holder
README.md                      -- expanded data sources table
CONTRIBUTING.md                -- fixed paths, simplified install
CHANGELOG.md                   -- added [Unreleased]
.gitignore                     -- added cache dirs, .claude, .council
.github/workflows/test.yml     -- comprehensive CI (lint, typecheck, test)
.github/dependabot.yml         -- NEW
.github/ISSUE_TEMPLATE/bug_report.yml -- fixed naming, updated providers
docs/quickstart.md             -- removed dict calling example
examples/end_to_end_demo.py    -- fixed poetry reference
mkdocs.yml                     -- fixed nav filename
```
