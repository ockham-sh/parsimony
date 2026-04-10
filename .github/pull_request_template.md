## What does this PR do?

<!-- Link to the issue: Closes #123 -->

## Type of change

- [ ] Bug fix
- [ ] New connector
- [ ] New feature (non-connector)
- [ ] Documentation
- [ ] Refactoring / maintenance

## For new connectors

- [ ] Module created in `ockham/connectors/`
- [ ] Pydantic params models with docstrings
- [ ] `@connector`, `@enumerator`, or `@loader` decorators applied
- [ ] `OutputConfig` with explicit `Column` roles
- [ ] Docstrings include workflow chaining hints
- [ ] `CONNECTORS` / `FETCH_CONNECTORS` exported
- [ ] Wired into `build_connectors_from_env()` (if API key needed)
- [ ] Tests added in `tests/`
- [ ] `CHANGELOG.md` updated

## Testing

```bash
# How to verify
pytest tests/test_<module>.py -v
```

## Checklist

- [ ] `ruff check .` passes
- [ ] `ruff format --check .` passes
- [ ] `mypy ockham/` passes
- [ ] Tests pass locally
