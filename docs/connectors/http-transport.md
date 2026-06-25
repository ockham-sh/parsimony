# HTTP transport

Most connectors fetch from an HTTP API, and every one of them needs the same
unglamorous plumbing: a base URL, default credentials, transient-retry logic,
secret redaction in logs, and a translation from `httpx` failures into the
typed [errors](errors.md) that agents understand. Parsimony's `parsimony.transport`
package provides that layer so connector authors write the fetch, not the
plumbing.

These symbols are **not** re-exported from the top-level `parsimony` package.
Import the primitives from `parsimony.transport` and the convenience constructors
from `parsimony.transport.helpers`:

```python
from parsimony.transport import (
    HttpClient,
    HttpRetryPolicy,
    DEFAULT_HTTP_RETRY_POLICY,
    map_http_error,
    map_timeout_error,
    map_transport_error,
    parse_retry_after,
    pooled_client,
    redact_url,
    redact_params_for_logging,
    redact_sensitive_text,
)
from parsimony.transport.helpers import (
    fetch_json,
    fetch_text,
    fetch_csv,
    make_http_client,
    make_api_key_client,
)
```

The layer is built on [`httpx`](https://www.python-httpx.org/), a base dependency
of `parsimony-core`, so everything on this page runs with only the core install
(no `standard`/`litellm` extras and no plugin).

## `HttpClient`

`HttpClient` is a wrapper around `httpx.Client`. It holds the
provider's base URL, default headers and query params, a timeout, TLS settings,
redirect policy, and a retry policy. Its one method, `request()`, issues a
single logical request and returns the **raw** `httpx.Response`.

```python
class HttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        verify_ssl: bool = True,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        follow_redirects: bool = True,
        max_redirects: int = 5,
        _transport: httpx.BaseTransport | None = None,
        shared_client: httpx.Client | None = None,
        retry_policy: HttpRetryPolicy | None = DEFAULT_HTTP_RETRY_POLICY,
    ) -> None: ...
```

| Parameter | Default | Behavior |
|---|---|---|
| `base_url` | required | Provider root. Its trailing slash is stripped on construction. |
| `timeout` | `30.0` | Per-request timeout in seconds. |
| `verify_ssl` | `True` | TLS certificate verification. |
| `headers` | `None` | Default headers merged into every request. |
| `query_params` | `None` | Default query params merged into every request (the API-key idiom). |
| `follow_redirects` | `True` | Whether `httpx` follows redirects. |
| `max_redirects` | `5` | Redirect chain cap. |
| `_transport` | `None` | Inject an `httpx.BaseTransport` (e.g. `httpx.MockTransport`) for tests. |
| `shared_client` | `None` | Reuse one pooled `httpx.Client` instead of opening a fresh one per request. |
| `retry_policy` | `DEFAULT_HTTP_RETRY_POLICY` | An `HttpRetryPolicy`, or `None` to disable retries. Validated on construction. |

The `base_url` property returns the stored URL with its trailing slash already
removed:

```python
from parsimony.transport import HttpClient

http = HttpClient("https://api.example.com/v1/")
assert http.base_url == "https://api.example.com/v1"
```

### Sending a request

`request()` builds the URL as `base_url + "/" + path.lstrip("/")`, merges the
default query params and headers with the per-call ones (the per-call values
win), emits a redacted structured log line, runs the retry loop, logs any
redirect chain and the final response, and returns the response object.

```python
def request(
    self,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
) -> httpx.Response: ...
```

Call it directly. The example below injects an `httpx.MockTransport` so it runs
offline:

```python
import httpx
from parsimony.transport import HttpClient, HttpRetryPolicy


def handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"ok": True}, request=request)


http = HttpClient(
    "https://api.example.com",
    timeout=5.0,
    headers={"X-App": "demo"},
    query_params={"apikey": "secret"},  # merged into every request
    retry_policy=HttpRetryPolicy(max_attempts=2, base_delay_s=0.0, jitter_s=0.0),
    _transport=httpx.MockTransport(handler),
)
response = http.request("GET", "/status")
assert response.status_code == 200
```

!!! warning "`request()` never calls `raise_for_status()`"
    `request()` returns the raw response for **any** status — including 4xx and
    5xx, and even after retries are exhausted. A 503 looks just like a 200 unless
    you check. Call `response.raise_for_status()` yourself and feed the resulting
    `httpx.HTTPStatusError` to [`map_http_error`](#mapping-errors), or use the
    [`fetch_json`](#fetch_json) helper which does both for you.

### One client per request, by default

By default each `request()` call opens a short-lived `httpx.Client` inside
a `with` block and closes it when the call returns. The cost is that a tight
fan-out loop pays for a fresh connection each time — use
[`pooled_client`](#connection-pooling) to opt into pooling when one logical
operation issues many requests.

## Retries and backoff

`HttpRetryPolicy` is a frozen dataclass describing when and how `HttpClient`
retries transient failures.

```python
@dataclass(frozen=True)
class HttpRetryPolicy:
    max_attempts: int = 3
    base_delay_s: float = 0.25
    max_delay_s: float = 8.0
    jitter_s: float = 0.1
    retryable_methods: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
    retryable_statuses: frozenset[int] = frozenset({500, 502, 503, 504})
```

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | `3` | Total attempts for a retryable method (1 = no retry). |
| `base_delay_s` | `0.25` | Base of the exponential backoff. |
| `max_delay_s` | `8.0` | Hard cap on any single delay. Must be `> 0`. |
| `jitter_s` | `0.1` | Upper bound of the uniform random jitter added per delay. |
| `retryable_methods` | `{GET, HEAD, OPTIONS}` | Only these idempotent methods are retried. |
| `retryable_statuses` | `{500, 502, 503, 504}` | Response statuses that trigger a retry. |

`DEFAULT_HTTP_RETRY_POLICY` is the module-level validated instance with exactly
these defaults; it is the default value of `HttpClient(retry_policy=...)`.

The retry rules:

- **Methods.** A non-idempotent method (e.g. `POST`) always runs exactly one
  attempt regardless of the policy. Only methods in `retryable_methods` retry.
- **Responses.** A response is retried only when its status is in
  `retryable_statuses`, the method is retryable, and there are attempts left.
- **Exceptions.** A raised exception is retried only when it is one of
  `httpx.TimeoutException`, `httpx.ConnectError`, `httpx.ReadError`, or
  `httpx.RemoteProtocolError` and there are attempts left; any other exception
  re-raises immediately.
- **Backoff.** For a normal retryable status the delay is
  `base_delay_s * 2 ** (attempt - 1)` plus uniform jitter in `[0, jitter_s)`,
  capped at `max_delay_s`. When a **429** is itself retried — only if you add
  `429` to `retryable_statuses`, which the default set does **not** — the delay
  instead comes from the server's `Retry-After` via
  [`parse_retry_after`](#parse_retry_after), still clamped to `max_delay_s`.
- **Disabling.** `retry_policy=None` collapses `max_attempts` to 1 and turns off
  both exception and status retries.

When attempts run out, `request()` returns the last response unchanged so you can
still map its status to a typed error.

!!! tip "Make retry tests deterministic"
    The first retry under the defaults waits `~0.25s` plus jitter. In tests, set
    `base_delay_s=0.0` and `jitter_s=0.0` so retries fire instantly.

The policy validates itself on construction via `HttpClient` (and at import for
the module default). `validate()` raises `ValueError` for `max_attempts < 1`,
`base_delay_s < 0`, `max_delay_s <= 0`, or `jitter_s < 0` — note `max_delay_s`
must be strictly positive while the other delays may be `0`.

```python
from parsimony.transport import HttpRetryPolicy

# Honor Retry-After on a 429, but never wait longer than max_delay_s.
policy = HttpRetryPolicy(max_delay_s=8.0)
assert policy.backoff_seconds(1, retry_after=30.0) == 8.0   # clamped down
assert policy.backoff_seconds(1, retry_after=2.0) == 2.0
```

## Mapping errors

`request()` returns the raw response and surfaces raw `httpx` exceptions on
transport failure; it is your job to turn those into typed errors. The mapper
functions translate them into the typed `parsimony.errors` hierarchy so the rest
of your connector — and any agent consuming it — sees consistent, class-aware
errors: `map_http_error` for a non-2xx status, `map_timeout_error` for a
timeout, and `map_transport_error` for any other transport failure (connection
refused, DNS failure, protocol error). (The [`fetch_json`](#fetch_json) helper —
and its `fetch_text` / `fetch_csv` siblings — apply all three mappers for you, so
they raise the typed errors directly rather than raw `httpx` exceptions.) Each
function is `NoReturn` (it always raises) and chains the original via
`raise ... from exc`, so the traceback and `__cause__` are preserved.

### `map_http_error`

```python
def map_http_error(
    exc: httpx.HTTPStatusError,
    *,
    provider: str,
    op_name: str,
    env_var: str | None = None,
) -> NoReturn: ...
```

| Upstream status | Raised error |
|---|---|
| 401, 403 | `UnauthorizedError` (carries `env_var` when supplied) |
| 402 | `PaymentRequiredError` |
| 429 | `RateLimitError` with `retry_after` from `parse_retry_after` |
| any other | `ProviderError` carrying `status_code` |

The optional `env_var=` names the environment variable a missing credential
would come from; it is threaded into the `UnauthorizedError` so the agent-facing
message can name it.

```python
import httpx
from parsimony.errors import RateLimitError
from parsimony.transport import map_http_error

request = httpx.Request("GET", "https://api.example.com/v1/data?api_key=secret")
response = httpx.Response(429, headers={"Retry-After": "30"}, request=request)
exc = httpx.HTTPStatusError("rate limited", request=request, response=response)

try:
    map_http_error(exc, provider="example", op_name="get_data")
except RateLimitError as err:
    assert err.provider == "example"
    assert err.retry_after == 30.0
    assert err.__cause__ is exc        # original chained
    assert "secret" not in str(err)    # message never leaks the API key
```

!!! warning "Messages never embed the URL or credentials"
    The default messages from `map_http_error`/`map_timeout_error` name only the
    `provider` and `op_name`; they deliberately omit the request URL and any
    secret. If you want a URL in a message you build yourself, redact it first
    with [`redact_url`](#redaction). See [Errors](errors.md) for the
    `message=` override escape hatch and its security caveat.

### `map_timeout_error`

```python
def map_timeout_error(exc: httpx.TimeoutException, *, provider: str, op_name: str) -> NoReturn: ...
```

Always raises `ProviderError` with `status_code=408` — the HTTP "request
timeout" semantic — so downstream code can treat a timeout uniformly with other
transport failures. There is no dedicated timeout exception class; key off
`status_code == 408`.

```python
import httpx
from parsimony.errors import ProviderError
from parsimony.transport import map_timeout_error

try:
    map_timeout_error(httpx.TimeoutException("slow"), provider="example", op_name="get_data")
except ProviderError as err:
    assert err.status_code == 408
```

### `map_transport_error`

```python
def map_transport_error(exc: httpx.TransportError, *, provider: str, op_name: str) -> NoReturn: ...
```

The catch-all for a transport failure that never produced a response — a
connection refused, DNS failure, read/write error, or protocol error (everything
under `httpx.TransportError` that is **not** a timeout). It raises `ProviderError`
with `status_code=503` ("the provider could not be reached" — transient, treat
like a 5xx). Only the exception *type name* is embedded in the message, never its
string, which can carry the request URL and its secrets. Because
`httpx.TimeoutException` is itself a `TransportError` subclass, catch it (with
`map_timeout_error`) **before** falling through to this handler.

```python
import httpx
from parsimony.errors import ProviderError
from parsimony.transport import map_transport_error

try:
    map_transport_error(httpx.ConnectError("refused"), provider="example", op_name="get_data")
except ProviderError as err:
    assert err.status_code == 503
```

### `parse_retry_after`

```python
def parse_retry_after(response: httpx.Response, *, default: float = 60.0) -> float: ...
```

Extracts a retry delay (in seconds) from a 429 response, in order:

1. the numeric `Retry-After` header (seconds);
2. the `X-Ratelimit-Reset` header read as a Unix epoch, returning
   `max(1.0, reset - now)`;
3. the `default` (60.0 seconds).

Every candidate must fall in `(0, 86400]`; anything outside — for instance a raw
epoch timestamp mistakenly placed in `Retry-After` — is skipped and the next
source is tried. This 24-hour cap is a paired invariant with `RateLimitError`,
which raises `ValueError` for a `retry_after` above 86400 on the theory that such
a value is a mis-encoded timestamp, not a duration.

```python
import httpx
from parsimony.transport import parse_retry_after

resp = httpx.Response(429, headers={"Retry-After": "42"}, request=httpx.Request("GET", "https://x.test"))
assert parse_retry_after(resp) == 42.0

no_header = httpx.Response(429, headers={}, request=httpx.Request("GET", "https://x.test"))
assert parse_retry_after(no_header, default=30.0) == 30.0
```

## Redaction

Secret redaction is the security backbone of the transport layer — it is what
keeps API keys out of your logs and exception messages. `HttpClient.request`
applies it automatically to its own log lines; the functions are also exported
for you to use when you build messages or log statements yourself.

| Function | Returns | Marker | Notes |
|---|---|---|---|
| `redact_url(url)` | `str` | `***` | Masks sensitive **query-param values**; leaves non-sensitive params intact; returns the URL unchanged if it has no query string. |
| `redact_params_for_logging(params)` | `dict` | `***REDACTED***` | Shallow copy of a params dict with sensitive values masked. |
| `redact_sensitive_text(text)` | `str` | `***` | Scans arbitrary text for embedded `http(s)` URLs and applies `redact_url` to each. |

A parameter is "sensitive" when its name (lowercased, with hyphens normalized to
underscores) matches the built-in set: `api_key`, `apikey`, `api_token`, `token`,
`access_token`, `refresh_token`, `id_token`, `client_secret`, `secret`,
`password`, `authorization`, `registrationkey`.

```python
from parsimony.transport import redact_url, redact_params_for_logging, redact_sensitive_text

assert redact_url("https://x.test/p?api_key=abc&series=UNRATE") == \
    "https://x.test/p?api_key=%2A%2A%2A&series=UNRATE"

msg = redact_sensitive_text("failed at https://x.test/p?token=t1&series=A")
assert "t1" not in msg and "series=A" in msg

log_params = redact_params_for_logging(
    {"series_id": "UNRATE", "api_key": "sk", "session_token": "x"}
)
assert log_params == {
    "series_id": "UNRATE",
    "api_key": "***REDACTED***",
    "session_token": "***REDACTED***",  # any *_token key is caught too
}
```

!!! warning "Two redactors, two rules, two markers"
    `redact_params_for_logging` masks any key ending in `_token` (e.g.
    `session_token`) **in addition** to the explicit set, and uses the marker
    `***REDACTED***`. `redact_url` uses the explicit name set **only** — it does
    **not** apply the `_token` suffix rule — and its marker is `***`. So a
    `?session_token=x` query param is masked in structured logs but **not** by
    `redact_url` unless its exact name is in the set. `redact_url` also only
    touches the query string: secrets in the path or userinfo are left alone.

When `HttpClient.request` follows a redirect, it logs the final URL with the
query string stripped entirely (scheme, host, and path only) so no redirect-chain
secret leaks into the logs.

## Connection pooling

For a single logical operation that issues many requests — an enumerator loop, a
screener fan-out — open one underlying `httpx.Client` and reuse it.
`pooled_client` is a context manager that does this: it builds one client
from the source `HttpClient`'s configuration (base URL, headers, query params,
timeout, TLS, transport) and yields a new `HttpClient` that routes every request
through it.

```python
import httpx
from parsimony.transport import HttpClient, pooled_client


def handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"path": request.url.path}, request=request)


http = HttpClient(
    "https://api.example.com",
    query_params={"apikey": "k"},
    _transport=httpx.MockTransport(handler),
)
statuses: list[int] = []
with pooled_client(http) as shared:  # one httpx.Client reused
    for key in ("a", "b", "c"):
        response = shared.request("GET", f"/data/{key}")
        statuses.append(response.status_code)
assert statuses == [200, 200, 200]
```

The yielded client shares one connection pool for the whole `with` block.
Do not hold onto it past the block. You can also create a pooled client directly
with `client.with_shared_client(httpx_client)`, which returns a new `HttpClient`
reusing the supplied `httpx.Client`.

## Convenience constructors and `fetch_json`

The `parsimony.transport.helpers` submodule provides thin constructors and a
one-call GET-and-parse helper. These are the symbols most connector bodies
actually touch.

!!! note "Helper timeout differs from `HttpClient`"
    `make_http_client` and `make_api_key_client` default `timeout` to **15.0**
    seconds, shorter than `HttpClient`'s own intrinsic default of `30.0`.

### `make_http_client` and `make_api_key_client`

```python
def make_http_client(
    base_url: str,
    *,
    query_params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> HttpClient: ...


def make_api_key_client(
    base_url: str,
    *,
    api_key: str,
    api_key_param: str = "apikey",
    timeout: float = 15.0,
) -> HttpClient: ...
```

`make_api_key_client` is the common case: it pre-sets the API key as a default
query parameter (named `apikey` unless you override `api_key_param`), so every
request the returned client makes carries the key without you threading it
through each call. This pairs naturally with `Connector.bind` — bind the key once
at provider-setup time so it never appears on the connector's call surface. See
[Calling, binding, and composing](calling-binding-composing.md).

### `fetch_json`

```python
def fetch_json(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    provider: str,
    op_name: str,
    env_var: str | None = None,
) -> Any: ...
```

`fetch_json` is the recommended one-liner for a JSON GET. It:

1. drops any param whose value is `None` (so optional connector arguments map to
   "omit this query param" rather than "send `None`");
2. issues `GET /{path}` through `http.request`;
3. calls `response.raise_for_status()`;
4. maps `httpx.HTTPStatusError` via `map_http_error`, `httpx.TimeoutException`
   via `map_timeout_error`, and any other `httpx.TransportError` via
   `map_transport_error` — turning every transport failure into a typed error;
5. returns `response.json()`, or raises a typed
   [`ParseError`](errors.md) if the 200 body is not valid JSON.

```python
import httpx
from parsimony.transport import HttpClient, HttpRetryPolicy
from parsimony.transport.helpers import fetch_json


def handler(request: httpx.Request) -> httpx.Response:
    # `start=None` was dropped; `series_id` was sent.
    assert b"start" not in request.url.query
    assert b"series_id" in request.url.query
    return httpx.Response(200, json={"series_id": "UNRATE", "value": 3.9}, request=request)


http = HttpClient(
    "https://api.example.com/v1",
    retry_policy=HttpRetryPolicy(max_attempts=1),
    _transport=httpx.MockTransport(handler),
)
result = fetch_json(
    http,
    path="series",
    params={"series_id": "UNRATE", "start": None},  # None is dropped
    provider="example",
    op_name="get_series",
)
print(result)  # {'series_id': 'UNRATE', 'value': 3.9}
```

`fetch_json` has two siblings on the same submodule that share its request path
and error mapping but parse the body differently: `fetch_text` returns the raw
response body as a `str`, and `fetch_csv` parses it into a `pandas.DataFrame`
(extra keyword arguments pass through to `pandas.read_csv`, an unparseable body
raises [`ParseError`](errors.md), and a body with no rows raises
[`EmptyDataError`](errors.md)). Reach for `fetch_csv` whenever a provider serves
CSV instead of JSON.

In a real provider plugin you would build the client once with
`make_api_key_client`, pass the live API key, and call `fetch_json` from inside
your `@connector` body — letting the framework wrap the returned data in a
[`Result`](results.md) and surfacing any transport failure as a
[typed error](errors.md).

## Logging

The module logs through `logging.getLogger("parsimony.transport")`. At `INFO` it
emits the request line (with a redacted URL and redacted params), a redirect
summary, and the response status and size; at `WARNING` it emits retry notices.
Structured `extra` fields include `http_method`, `http_url`, `http_path`,
`http_params`, `http_status_code`, `http_response_size`, `http_redirect_hops`,
and `http_redirect_target`. No environment variables configure this layer
directly — all tuning is per `HttpClient` / `HttpRetryPolicy` instance.

## See also

- [Errors](errors.md) — the typed exception hierarchy these mappers raise, and the `message=` override caveat.
- [Authoring a provider plugin](../plugins/authoring.md) — building a `parsimony-<name>` distribution that uses this transport layer.
- [Calling, binding, and composing](calling-binding-composing.md) — `bind` an API key once so it stays off the connector's call surface.
- [Results and output schemas](results.md) — how the framework wraps connector return values into a `Result`.
