## What does this PR do?

<!-- Link to the issue: Closes #123 -->

## Type of change

- [ ] Bug fix
- [ ] New connector
- [ ] New feature (non-connector)
- [ ] Documentation
- [ ] Refactoring / maintenance

## For new connectors

- [ ] Connector package exports `CONNECTORS`
- [ ] Connector signatures and docstrings are clear
- [ ] `@connector`, `@enumerator`, or `@loader` decorators applied
- [ ] `OutputSpec` with explicit `Column` roles
- [ ] Docstrings include workflow chaining hints
- [ ] Auth/env behavior lives in connector implementation or explicit `.bind(...)`
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
- [ ] `mypy parsimony/` passes
- [ ] Tests pass locally
