# Errors

Parsimony connectors raise a small, typed exception taxonomy for *operational*
failures — the things that go wrong when you talk to a real data provider. Every
exception in the family carries a `provider` attribute and a default message
written for an agent loop to read, so a consumer can decide what to do next from a
single `str(exc)` call without parsing prose.

This page covers the taxonomy, when each error is raised, the agent-facing default
messages, and the `message=` override escape hatch. These types live in
`parsimony.errors`; the eight typed errors below are also re-exported from the
top-level `parsimony` package.

## Operational errors, not programmer errors

The taxonomy is scoped deliberately. It describes runtime failures from a provider
— bad credentials, a 429, an empty result set, an unparseable body. It is **not**
for programmer or usage errors.

!!! note "Where the line is"
    Misusing the framework itself stays a plain Python exception:

    - Calling a connector with an unknown keyword, or binding an already-bound
      parameter, raises `TypeError`.
    - Building a malformed `OutputConfig`, or a bad `Entity`, raises `ValueError`
      or a Pydantic `ValidationError`.

    The typed `ConnectorError` family is reserved for the connector–provider
    boundary. Keep your own validation of *call-time arguments* in
    `InvalidParameterError` (below), but leave structural framework misuse as the
    native exception.

## The hierarchy

```text
ConnectorError                 (base; carries .provider)
├── UnauthorizedError          401/403 — bad or missing credentials
├── PaymentRequiredError       402 / plan restriction
├── RateLimitError             429 — burst or quota
├── ProviderError              5xx / 4xx / timeout (carries .status_code)
├── EmptyDataError             200 but no rows
├── ParseError                 200 but unparseable
└── InvalidParameterError      invalid call-time arguments
```

A ninth type, `CatalogNotFoundError`, also subclasses `ConnectorError` but belongs
to the [Catalog](../catalog/index.md) layer rather than connectors — see
[Catalog-side errors](#catalog-side-errors) below.

Because everything descends from `ConnectorError`, you can catch the whole family
with one `except` and read the source without string-matching:

```python
from parsimony.errors import ConnectorError, UnauthorizedError


def attempt() -> None:
    raise UnauthorizedError("fred")


try:
    attempt()
except ConnectorError as exc:
    print(exc.provider)  # fred — no message parsing needed
```

You can import the eight connector-facing types from the top level
(`from parsimony import RateLimitError`) or, equivalently, from the submodule
(`from parsimony.errors import RateLimitError`). This page uses the submodule path
throughout for clarity.

## ConnectorError — the base

```python
class ConnectorError(Exception):
    def __init__(self, message: str, *, provider: str) -> None: ...
```

`ConnectorError` stores `provider` and passes `message` straight to `Exception`, so
`str(exc)` always returns the resolved message and `exc.provider` always names the
source.

!!! warning "Constructor argument order differs from the subclasses"
    The base takes `message` first and `provider` as a keyword-only argument. Most
    typed subclasses **flip this**: `provider` is the first positional and
    `message` is an optional override. Writing `UnauthorizedError("some text")`
    sets `provider="some text"` — the first positional is always `provider` for the
    typed subclasses.

## The typed subclasses

Each subclass stashes its discriminating data on instance attributes (so a
programmatic reader never has to parse the message), and builds a class-aware
default message embedding the right agent-loop directive.

### UnauthorizedError — 401/403, bad credentials

```python
class UnauthorizedError(ConnectorError):
    def __init__(
        self,
        provider: str,
        message: str | None = None,
        *,
        env_var: str | None = None,
    ) -> None: ...
```

Raise this when the credentials themselves are missing, invalid, or expired. Scope
matters: a *plan-tier* mismatch is `PaymentRequiredError`, not `UnauthorizedError`,
regardless of which HTTP status the upstream actually returned.

The keyword-only `env_var` names the environment variable the agent should set;
when present, the default message tells the agent exactly which variable to export.

```python
from parsimony.errors import UnauthorizedError

exc = UnauthorizedError("fred", env_var="FRED_API_KEY")
print(str(exc))
# fred: API credentials missing or invalid — set the FRED_API_KEY env var
# (and ensure it is exported). DO NOT retry with different arguments.
print(exc.provider, exc.env_var)  # fred FRED_API_KEY
```

Without `env_var`, the message is the generic
`"{provider}: API credentials missing or invalid. DO NOT retry with different arguments."`

!!! warning "env_var is keyword-only"
    `UnauthorizedError("fred", "FRED_API_KEY")` passes the second positional as
    `message`, not `env_var` — so the env-var hint is lost and the string is used
    verbatim. Always write `env_var="FRED_API_KEY"`.

### PaymentRequiredError — 402 / plan restriction

```python
class PaymentRequiredError(ConnectorError):
    def __init__(self, provider: str, message: str | None = None) -> None: ...
```

Raise this when the user's plan does not include the endpoint or parameter set.
This covers HTTP 402 and any non-standard status whose *body* indicates a plan
restriction rather than a bad key. The default message:

```text
{provider}: this endpoint or parameter set is not included in your plan.
DO NOT retry; try a different connector or inform the user.
```

### RateLimitError — 429, burst or quota

```python
class RateLimitError(ConnectorError):
    def __init__(
        self,
        provider: str,
        retry_after: float,
        *,
        quota_exhausted: bool = False,
        message: str | None = None,
    ) -> None: ...
```

`retry_after` is a required positional **duration in seconds** — how long to wait,
not an absolute time. The keyword-only `quota_exhausted` discriminates the two
flavours of 429: a short burst limit (`False`, the default) versus a billing-period
quota that will not clear by waiting a few seconds (`True`).

```python
from parsimony.errors import RateLimitError

burst = RateLimitError("fred", retry_after=30.0)
print(str(burst))
# fred: rate limit hit, retry after 30s. DO NOT retry this tool immediately;
# pick a different connector, ask the user, or stop.
assert burst.quota_exhausted is False

quota = RateLimitError("fred", retry_after=0.0, quota_exhausted=True)
assert "quota exhausted" in str(quota)
```

!!! warning "retry_after rejects epoch timestamps"
    If `retry_after > 86400` (24 hours in seconds), the constructor raises
    `ValueError` — not a `RateLimitError` — on the assumption you passed an absolute
    Unix timestamp instead of a duration. Pass `60.0`, not `1700000000.0`.

    ```python
    try:
        RateLimitError("fred", retry_after=1_700_000_000.0)
    except ValueError as exc:
        assert "Unix epoch" in str(exc)
    ```

### ProviderError — 5xx, 4xx, or timeout

```python
class ProviderError(ConnectorError):
    def __init__(
        self,
        provider: str,
        status_code: int,
        message: str | None = None,
    ) -> None: ...
```

The catch-all for HTTP failures that are not a more specific case. `status_code`
is required and preserved on the instance; the default message branches by bucket:

| `status_code`     | Default message gist                                              |
|-------------------|-------------------------------------------------------------------|
| `408`             | upstream timed out — do not immediately retry                     |
| `500`–`599`       | server error, likely transient — pick a different connector       |
| any other (e.g. `404`) | request rejected — do not retry with the same parameters     |

```python
from parsimony.errors import ProviderError

assert "timed out" in str(ProviderError("fmp", status_code=408))
assert "transient" in str(ProviderError("fmp", status_code=503))
rejected = ProviderError("fmp", status_code=404)
assert "HTTP 404" in str(rejected)
assert "DO NOT retry with the same parameters" in str(rejected)
assert rejected.status_code == 404
```

The `408` bucket is checked before the `5xx` range, which is why the
[HTTP transport](http-transport.md) layer maps connection timeouts to
`ProviderError(status_code=408)` — they land in the timeout branch uniformly.

### EmptyDataError — 200, no rows

```python
class EmptyDataError(ConnectorError):
    def __init__(
        self,
        provider: str,
        message: str | None = None,
        query_params: dict[str, Any] | None = None,
    ) -> None: ...
```

A successful request that returned an empty result set. This is a **valid
operational outcome**, so its default message is the only one in the taxonomy with
**no** `DO NOT retry` directive — adjusting parameters is the recovery path, and the
message invites it. `query_params` is kept for diagnostic context and normalized to
an empty dict when omitted.

```python
from parsimony.errors import EmptyDataError

exc = EmptyDataError("fred", query_params={"series_id": "UNRATE"})
assert exc.query_params == {"series_id": "UNRATE"}
assert "Adjust parameters" in str(exc)
assert "DO NOT" not in str(exc)            # empty is a valid outcome
assert EmptyDataError("fred").query_params == {}  # None → {}
```

### ParseError — 200, unparseable

```python
class ParseError(ConnectorError):
    def __init__(self, provider: str, message: str | None = None) -> None: ...
```

The provider returned `200` but the body could not be turned into the declared
shape — connector-side schema drift, or an upstream that broke its own contract.
Retrying the same call will not help, and the default message says so.

`ParseError` is also raised **by the framework itself**: when a connector returns
raw data that fails to coerce against its declared
[`OutputConfig`](results.md), `Connector.__call__` catches the underlying
`ValueError` and re-raises it as `ParseError(self.name, str(exc))`. You generally
do not raise `ParseError` for schema-coercion failures yourself — the framework
does it for you, with `provider` set to the connector's name.

```python
import pandas as pd

from parsimony import Column, ColumnRole, OutputConfig, ParseError, connector


@connector(
    output=OutputConfig(
        columns=[Column(name="value", dtype="numeric", role=ColumnRole.DATA)]
    )
)
def broken() -> pd.DataFrame:
    """Return values that cannot be coerced to numeric, forcing a ParseError."""
    return pd.DataFrame({"value": ["not", "a", "number"]})


try:
    broken()
except ParseError as exc:
    print(exc.provider)        # broken — the connector's name
    print(isinstance(exc, ParseError))  # True
```

### InvalidParameterError — bad call-time arguments

```python
class InvalidParameterError(ConnectorError):
    def __init__(self, provider: str, message: str) -> None: ...
```

Raise this from inside a connector when a *call-time* argument is invalid before
you make the upstream request — for example, `start_date` after `end_date`. It is
the one typed subclass where `message` is **required**: there is no generic default
worth fabricating for a parameter error, so the message you pass becomes
`str(exc)` verbatim with no decoration.

```python
from parsimony.errors import InvalidParameterError

exc = InvalidParameterError("fred", "start_date must precede end_date")
assert str(exc) == "start_date must precede end_date"
assert exc.provider == "fred"
```

!!! note "Framework-level argument errors are TypeError"
    `InvalidParameterError` is for *semantic* validation you perform inside a
    connector body. Passing an unknown keyword, or binding a parameter twice, is
    caught earlier by the framework as `TypeError` — those never reach your
    connector code. See [Calling, binding, and composing](calling-binding-composing.md).

## The agent-facing default messages

For each typed subclass, the kernel-built default message *is* the canonical
agent-facing string. Every default embeds class-aware semantics plus the
appropriate agent-loop directive, so a downstream consumer (an agent tool bridge,
a sandbox) can render the next-step guidance with a single `str(exc)`:

| Error                  | Directive in the default message                         |
|------------------------|----------------------------------------------------------|
| `UnauthorizedError`    | DO NOT retry with different arguments (set the env var)  |
| `PaymentRequiredError` | DO NOT retry; try a different connector or inform the user |
| `RateLimitError` (burst) | DO NOT retry immediately; pick another connector or stop |
| `RateLimitError` (quota) | DO NOT retry; use another connector or wait for the cycle |
| `ProviderError` (408)  | DO NOT immediately retry; pick a different connector     |
| `ProviderError` (5xx)  | likely transient; pick another connector; do not loop    |
| `ProviderError` (other 4xx) | DO NOT retry with the same parameters               |
| `ParseError`           | DO NOT retry; pick a different connector or report       |
| `EmptyDataError`       | *(no DO NOT directive)* — adjust parameters and try again |

!!! tip "Programmatic readers should use attributes, not strings"
    Inspect `exc.status_code`, `exc.retry_after`, `exc.quota_exhausted`,
    `exc.env_var`, or `exc.query_params` for control flow. The message strings are
    stable but exist for the agent's prose context, not for branching.

## Overriding the message

Every typed subclass except `InvalidParameterError` (whose `message` is mandatory)
accepts an optional `message=` that wins verbatim — the kernel default is then not
built. Use it when you carry agent-useful upstream context the kernel cannot
construct, such as a provider's own JSON `error_code`. The discriminating
attributes are still set even when you override.

```python
from parsimony.errors import PaymentRequiredError

exc = PaymentRequiredError(
    "premium", message="error_code=10005: historical-data restriction"
)
assert str(exc) == "error_code=10005: historical-data restriction"
assert exc.provider == "premium"  # attribute still set
```

!!! warning "An override bypasses the kernel's safety wording"
    When you supply `message=`, you own the agent-facing text. Keep it free of
    URLs, tokens, and raw upstream prose — those can leak credentials or carry
    prompt-injection vectors into an agent loop. Prefer the kernel default unless
    the extra context is genuinely worth the responsibility.

    Note that the gate is on truthiness across every typed subclass (whether the
    constructor writes `if message:` or `message or <default>`), so an empty
    string `""` falls through to the default rather than overriding it.

## How HTTP statuses become typed errors

You rarely map statuses by hand. The [HTTP transport](http-transport.md) layer
does it for you: `check_status` maps a response's status code to the right typed
error (no chained `HTTPStatusError`, so no query-string can leak through
`__cause__`), and `HttpClient.request` maps a transport failure (timeout, or any
other failure that never produced a response) to a typed error internally,
chained from the original exception and stripped of URLs and secrets.

| Upstream                 | Mapped to                                              |
|--------------------------|--------------------------------------------------------|
| `401` / `403`            | `UnauthorizedError`                                    |
| `402`                    | `PaymentRequiredError`                                 |
| `429`                    | `RateLimitError` (with `retry_after` parsed from the response) |
| any other status         | `ProviderError(status_code=...)`                       |
| connection timeout       | `ProviderError(status_code=408)`                       |
| other transport failure (connection refused, DNS, protocol error) | `ProviderError(status_code=503)` |

The retry-after parser clamps its result into `(0, 86400]` precisely so it never
trips `RateLimitError`'s epoch guard — the two are coupled by design.

The `fetch_json` / `fetch_csv` / `fetch_text` helpers route every request through
one path that maps every failure mode — a non-2xx status via `check_status`, and
a timeout or any other transport error inside `request()` — to the typed
taxonomy above, so a raw `httpx` exception never escapes a fetch helper. The
transport-failure branch embeds only the exception *type name*, never its
string, which can carry the URL and its secrets.

## Catalog-side errors

`CatalogNotFoundError` also subclasses `ConnectorError`, but it is raised by the
[Catalog](../catalog/index.md) layer when a configured or lazily-cached catalog
bundle is missing or unreachable (a bad URL, a missing Hugging Face repo, an absent
on-disk cache with no build path). It is **not** re-exported from the top-level
`parsimony` package — import it from the submodule:

```python
from parsimony.errors import CatalogNotFoundError

exc = CatalogNotFoundError("Catalog bundle not present at file:///tmp/missing")
print(str(exc))
# Catalog bundle not present at file:///tmp/missing. DO NOT retry.
print(exc.provider)  # catalog
```

Its constructor mirrors the **base** signature, not the typed subclasses:
`message` is positional and `provider` is keyword-only (defaulting to `"catalog"`).
It guarantees a `DO NOT retry` directive idempotently — appending `. DO NOT retry.`
only if the phrase is not already present — so agents do not tight-loop on a bad
URL.

## See also

- [Defining connectors](defining-connectors.md) — where a schema `ValueError` becomes a `ParseError`
- [Calling, binding, and composing](calling-binding-composing.md) — the `TypeError` boundary for framework misuse
- [Results and output schemas](results.md) — the success counterpart these errors are raised in lieu of
- [HTTP transport](http-transport.md) — the layer that maps HTTP statuses and timeouts to these typed errors
