"""Typed exceptions for connector operational errors.

**Not for programmer errors** — those stay as ``TypeError``, ``ValueError``,
or Pydantic ``ValidationError``.

Hierarchy::

    ConnectorError
    ├── UnauthorizedError      (401/403 — bad credentials)
    ├── PaymentRequiredError    (402 — plan restriction)
    ├── RateLimitError          (429 — burst or quota)
    ├── ProviderError           (5xx / unexpected status)
    ├── EmptyDataError          (200 but no rows)
    └── ParseError              (200 but unparseable)

.. rubric:: Agent-facing contract

For typed subclasses the kernel-built default message IS the canonical
agent-facing string. Each default embeds class-aware semantics and the
appropriate agent-loop directive (``DO NOT retry`` / ``pick a different
connector`` / etc.) so consumers (the MCP bridge, the terminal sandbox)
can render with a single ``str(exc)`` call.

The ``message`` keyword on each typed subclass remains as an escape hatch
for connector authors who carry agent-useful context the kernel cannot
construct (e.g. an upstream ``error_code`` from a JSON body). Authors who
override take responsibility for the agent-facing text — keep it free of
URLs, tokens, and upstream-derived prose that could carry credentials or
prompt-injection vectors.
"""

from __future__ import annotations

__all__ = [
    "ConnectorError",
    "EmptyDataError",
    "ParseError",
    "PaymentRequiredError",
    "ProviderError",
    "RateLimitError",
    "UnauthorizedError",
]

from typing import Any


class ConnectorError(Exception):
    """Base for all connector operational errors.

    Every subclass carries ``provider: str`` so callers can identify the
    source without parsing message strings.
    """

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message)
        self.provider = provider


class UnauthorizedError(ConnectorError):
    """Invalid or missing API credentials (HTTP 401/403).

    Scope: the credentials themselves are missing/invalid/expired. Plan-
    tier mismatches and per-endpoint plan restrictions are
    :class:`PaymentRequiredError`, regardless of which HTTP status the
    upstream delivered.

    ``env_var`` names the environment variable the agent should set to
    fix the failure. When provided, the default message tells the agent
    which variable to set.
    """

    def __init__(
        self,
        provider: str,
        message: str | None = None,
        *,
        env_var: str | None = None,
    ) -> None:
        self.env_var = env_var
        if message:
            msg = message
        elif env_var:
            msg = (
                f"{provider}: API credentials missing or invalid — set the "
                f"{env_var} env var (and ensure it is exported). DO NOT retry "
                f"with different arguments."
            )
        else:
            msg = (
                f"{provider}: API credentials missing or invalid. DO NOT retry "
                f"with different arguments."
            )
        super().__init__(msg, provider=provider)


class PaymentRequiredError(ConnectorError):
    """User's plan does not permit this endpoint or parameter set.

    Covers HTTP 402 and any 401/403 / non-standard status whose body
    indicates a plan restriction rather than a bad key (CoinGecko's
    ``error_code in {10005, 10006, 10012}`` is the canonical example).
    """

    def __init__(self, provider: str, message: str | None = None) -> None:
        msg = message or (
            f"{provider}: this endpoint or parameter set is not included in "
            f"your plan. DO NOT retry; try a different connector or inform "
            f"the user."
        )
        super().__init__(msg, provider=provider)


class RateLimitError(ConnectorError):
    """Provider rate-limit response (HTTP 429).

    Check :attr:`quota_exhausted`: ``False`` = burst limit (retry after
    :attr:`retry_after`), ``True`` = billing-period quota (do not retry).
    """

    def __init__(
        self,
        provider: str,
        retry_after: float,
        *,
        quota_exhausted: bool = False,
        message: str | None = None,
    ) -> None:
        if retry_after > 86_400:
            raise ValueError(
                f"retry_after={retry_after!r} looks like a Unix epoch timestamp, not a duration. "
                "Pass seconds-until-retry (e.g. 60.0), not an absolute timestamp."
            )
        self.retry_after = retry_after
        self.quota_exhausted = quota_exhausted
        if message:
            msg = message
        elif quota_exhausted:
            msg = (
                f"{provider}: API quota exhausted for the current billing "
                f"period. DO NOT retry; use a different connector or wait "
                f"for the next billing cycle."
            )
        else:
            msg = (
                f"{provider}: rate limit hit, retry after {retry_after:.0f}s. "
                f"DO NOT retry this tool immediately; pick a different "
                f"connector, ask the user, or stop."
            )
        super().__init__(msg, provider=provider)


class ProviderError(ConnectorError):
    """Remote provider returned an HTTP error (5xx, 4xx, or timeout).

    Carries ``status_code``. The default message branches by bucket:

    * ``408`` → upstream timeout, do not immediately retry.
    * ``500–599`` → server error, likely transient.
    * other 4xx → request rejected, do not retry with same parameters.
    """

    def __init__(
        self,
        provider: str,
        status_code: int,
        message: str | None = None,
    ) -> None:
        self.status_code = status_code
        if message:
            msg = message
        elif status_code == 408:
            msg = (
                f"{provider}: upstream timed out. DO NOT immediately retry; "
                f"pick a different connector or inform the user the upstream "
                f"is slow."
            )
        elif 500 <= status_code <= 599:
            msg = (
                f"{provider}: server error (HTTP {status_code}); likely "
                f"transient. Pick a different connector or inform the user; "
                f"do not loop."
            )
        else:
            msg = (
                f"{provider}: request rejected (HTTP {status_code}). DO NOT "
                f"retry with the same parameters."
            )
        super().__init__(msg, provider=provider)


class EmptyDataError(ConnectorError):
    """Provider returned HTTP 200 but the result set is empty.

    Valid operational outcome — adjusting parameters is the recovery
    path, so the default message does not carry a ``DO NOT retry``
    directive. Carries ``query_params`` for diagnostic context.
    """

    def __init__(
        self,
        provider: str,
        message: str | None = None,
        query_params: dict[str, Any] | None = None,
    ) -> None:
        self.query_params = query_params or {}
        msg = message or (
            f"{provider}: no data returned for the given parameters. "
            f"Adjust parameters or try a different connector."
        )
        super().__init__(msg, provider=provider)


class ParseError(ConnectorError):
    """Provider returned HTTP 200 but the response could not be parsed.

    Indicates a connector-side schema drift (or an upstream that broke
    its own contract). Retrying the same call will not help.
    """

    def __init__(self, provider: str, message: str | None = None) -> None:
        msg = message or (
            f"{provider}: response could not be parsed — likely a connector-"
            f"side schema drift, retrying will not help. DO NOT retry; pick "
            f"a different connector or report."
        )
        super().__init__(msg, provider=provider)
