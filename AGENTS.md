# parsimony

`parsimony` is a framework, not an application. The bar for every addition is correspondingly high.

## Mindset

The library is named after the principle: do not multiply entities beyond necessity. Internalize this section before writing code — the right posture here differs from typical application work.

### Underlying principles

- **The library is a public language.** Every primitive, decorator, option, name, and rule expands the vocabulary that future developers and agents — building catalogs and connectors no one here will see — must learn to read this code correctly. The cost of an addition is paid forever by many; the benefit is usually local.
- **Cognitive load is the scarce resource.** Implementation effort is cheap relative to the long-term cost of concepts users have to hold in their heads. Optimize the framework for what it asks of its readers, not for what it asks of its authors.
- **Generalization is what justifies a primitive.** A shape that fits the immediate problem is not yet an abstraction. A shape that recognizably fits situations beyond it might be. Until then, an inline solution is usually the honest choice.
- **Asymmetry of regret.** Adding a concept later, with the benefit of real use cases, is straightforward. Removing one in use is expensive and often impossible. When uncertain, lean toward doing less.
- **Clarity is a user-facing feature.** Part of what users adopt this library for is the discipline it imposes on itself. A cleaner signature, a removed concept, or a sharper name is worth real implementation cost.

### How this changes how you work

These follow from the principles above; treat them as orientation, not rules.

- Spend more of your time on the *shape* of the API than on the code that realizes it. Iterate in plain language until the shape feels obviously right; only then write it down.
- Treat each new concept — primitive, option, name, rule — as something the framework will carry indefinitely. That cost is sometimes worth paying, but it is never zero.
- Prefer extending or refining existing primitives over introducing new ones. Coherence with what is already there is itself a form of design.
- A half-formed abstraction is usually worse than no abstraction. Discarding a draft and choosing the inline solution is a normal and healthy outcome of thinking carefully.
- Configuration and options enlarge the surface every user must navigate. They are sometimes the right answer, but the burden of justification sits with the addition, not with its absence.

### Before introducing a new abstraction

Pause and consider, for yourself:

- Does the shape make sense for situations beyond the immediate caller, or only for this one?
- Could an existing primitive be extended to cover this naturally?
- What does a future reader need to learn — new vocabulary, new rules, new edge cases — to use the surrounding code correctly?
- If this turns out to be the wrong shape, what does undoing it cost?

Discomfort with these questions is information. It usually means the change should stay small, local, and inline rather than commit the framework to a new concept.

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
