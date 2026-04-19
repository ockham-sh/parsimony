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

    Every subclass carries ``provider: str`` so callers can identify the source
    without parsing message strings.
    """

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(message)
        self.provider = provider


class UnauthorizedError(ConnectorError):
    """Invalid or missing API credentials (HTTP 401/403).

    This is a configuration error — the credentials are wrong or absent.
    Do not retry; fix the credentials.
    """

    def __init__(self, provider: str, message: str | None = None) -> None:
        msg = message or f"{provider}: invalid or missing API credentials"
        super().__init__(msg, provider=provider)


class PaymentRequiredError(ConnectorError):
    """The user's API plan does not permit access to this endpoint (HTTP 402).

    This is a terminal error — never retry.  The user must upgrade their plan
    or use a different connector.
    """

    def __init__(self, provider: str, message: str | None = None) -> None:
        msg = message or f"{provider}: your plan is not eligible for this data request"
        super().__init__(msg, provider=provider)


class RateLimitError(ConnectorError):
    """The provider returned a rate-limit response (HTTP 429).

    Callers should check :attr:`quota_exhausted`:

    * ``False`` — burst limit; may retry after :attr:`retry_after` seconds.
    * ``True`` — billing-period quota exhausted; do not retry.
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
                f"{provider}: API quota exhausted for the current billing period. "
                "Upgrade your plan or wait for the next billing cycle."
            )
        else:
            msg = f"{provider}: rate limit hit, retry after {retry_after:.0f}s"
        super().__init__(msg, provider=provider)


class ProviderError(ConnectorError):
    """The remote provider returned an HTTP error (5xx or unexpected status).

    Carries ``status_code`` for programmatic handling.
    """

    def __init__(
        self,
        provider: str,
        status_code: int,
        message: str | None = None,
    ) -> None:
        self.status_code = status_code
        msg = message or f"{provider}: provider error (HTTP {status_code})"
        super().__init__(msg, provider=provider)


class EmptyDataError(ConnectorError):
    """Provider returned HTTP 200 but the result set is empty.

    This is a valid operational outcome — the query succeeded but has no data.
    Carries ``query_params`` for diagnostic context in batch pipelines.
    """

    def __init__(
        self,
        provider: str,
        message: str | None = None,
        query_params: dict[str, Any] | None = None,
    ) -> None:
        self.query_params = query_params or {}
        msg = message or f"{provider}: no data returned"
        super().__init__(msg, provider=provider)


class ParseError(ConnectorError):
    """Provider returned HTTP 200 but the response could not be parsed.

    The provider's response format was unexpected or malformed.
    """

    def __init__(self, provider: str, message: str | None = None) -> None:
        msg = message or f"{provider}: failed to parse provider response"
        super().__init__(msg, provider=provider)
