"""Query the official registry of installable ``parsimony-<name>`` connector packages.

Distinct from :mod:`parsimony.discover`, which only inspects what is
**already installed**: this module answers a different question — "what
official package could I install to cover this data source?" The two
compose: check :func:`parsimony.discover.iter_providers` first, and fall
back to :func:`list_available` (and then ``pip install``) only when the
source you need is not already installed.

The registry is a thin client over one canonical, versioned manifest:

    https://parsimony.dev/connectors.json

That URL is a stable facade (``ockham-sh/landing-page#12``) over the
official manifest ``ockham-sh/parsimony-connectors`` generates from its own
package metadata (``scripts/gen_roster.py``, tracked in
``ockham-sh/parsimony-connectors#83``). Callers depend only on the
canonical URL above, never on that repo's raw-file layout.

Resilience is deliberately minimal, matching the manifest's own design:

* No TTL cache, no persisted response, no refresh/invalidation state.
  :func:`list_available` re-fetches on every call — cache the
  :class:`ConnectorRegistry` yourself if you call this on a hot path.
* On any network, HTTP, or schema failure, falls back to a read-only
  snapshot bundled in this wheel at release time
  (``parsimony/_data/connectors.json`` — see ``parsimony/_data/README.md``).
  That snapshot is **not** a runtime cache: it is never written, refreshed,
  or invalidated after install: it is fixed at build time and only used
  when the live endpoint is unreachable or invalid.
* Fallback use is always visible: a warning naming the canonical URL is
  logged, and :attr:`ConnectorRegistry.source` records which path was used
  so a caller (or the CLI) can say so too.
* If the bundled fallback also fails to load, :func:`list_available` raises
  :class:`RegistryError` carrying both failure causes — at that point there
  is nothing left to silently paper over.
"""

from __future__ import annotations

import importlib.resources
import logging
from dataclasses import dataclass
from typing import Literal

import httpx
from pydantic import BaseModel, ValidationError

__all__ = [
    "CANONICAL_MANIFEST_URL",
    "ConnectorRegistry",
    "InstallableConnector",
    "RegistryError",
    "list_available",
]

_logger = logging.getLogger("parsimony.registry")

CANONICAL_MANIFEST_URL = "https://parsimony.dev/connectors.json"
_MANIFEST_SCHEMA_VERSION = 1
_DEFAULT_FETCH_TIMEOUT_SECONDS = 5.0


# ---------------------------------------------------------------------------
# Wire contract (private) — the shared shape coordinated with
# ockham-sh/parsimony-connectors#83 and ockham-sh/landing-page#12. Do not
# rename these fields without updating both.
# ---------------------------------------------------------------------------


class _ManifestConnector(BaseModel):
    package: str
    provider: str
    entry_point: str
    connector_count: int
    keyless: bool


class _Manifest(BaseModel):
    schema_version: int
    generated_at: str
    connectors: list[_ManifestConnector]


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InstallableConnector:
    """One installable ``parsimony-<name>`` package from the official registry."""

    package: str
    provider: str
    entry_point: str
    connector_count: int
    keyless: bool


@dataclass(frozen=True)
class ConnectorRegistry:
    """The result of one :func:`list_available` call."""

    generated_at: str
    connectors: tuple[InstallableConnector, ...]
    source: Literal["remote", "bundled"]


class RegistryError(Exception):
    """Neither the canonical registry endpoint nor the bundled fallback could be loaded.

    Carries both underlying failures — :attr:`remote_error` and
    :attr:`bundled_error` — so a caller can report exactly what was tried
    rather than just whichever exception happened to be raised last.
    """

    def __init__(self, message: str, *, remote_error: Exception, bundled_error: Exception) -> None:
        super().__init__(message)
        self.remote_error = remote_error
        self.bundled_error = bundled_error


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _rows_from_manifest(manifest: _Manifest) -> tuple[InstallableConnector, ...]:
    return tuple(
        InstallableConnector(
            package=c.package,
            provider=c.provider,
            entry_point=c.entry_point,
            connector_count=c.connector_count,
            keyless=c.keyless,
        )
        for c in sorted(manifest.connectors, key=lambda c: c.package)
    )


def _validate_manifest(raw: bytes) -> _Manifest:
    """Parse and schema-check *raw* manifest bytes.

    Rejects a ``schema_version`` this release does not understand — a v2
    manifest with a renamed/removed field should fail loudly here rather
    than silently produce nonsense rows.
    """
    manifest = _Manifest.model_validate_json(raw)
    if manifest.schema_version != _MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported manifest schema_version {manifest.schema_version!r}; this parsimony-core "
            f"release understands schema_version {_MANIFEST_SCHEMA_VERSION}. Upgrade parsimony-core."
        )
    return manifest


def _fetch_remote_manifest(*, url: str, timeout: float) -> _Manifest:
    response = httpx.get(url, timeout=timeout)
    response.raise_for_status()
    return _validate_manifest(response.content)


def _load_bundled_manifest() -> _Manifest:
    """Load the wheel-bundled ``connectors.json`` snapshot.

    Copied verbatim from ``ockham-sh/parsimony-connectors``' generated
    manifest at release time (see ``parsimony/_data/README.md``); never
    written or refreshed after install.
    """
    text = importlib.resources.files("parsimony").joinpath("_data", "connectors.json").read_text(encoding="utf-8")
    return _validate_manifest(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_available(
    *,
    url: str = CANONICAL_MANIFEST_URL,
    timeout: float = _DEFAULT_FETCH_TIMEOUT_SECONDS,
) -> ConnectorRegistry:
    """Query the official registry of installable ``parsimony-<name>`` packages.

    Always tries the canonical remote endpoint first. *url* and *timeout*
    exist for testing; production callers should rely on the defaults so a
    client never hard-depends on anything but the one canonical contract.

    On any network, HTTP, or schema-validation failure, falls back to the
    read-only snapshot bundled in this wheel and logs a warning on the
    ``parsimony.registry`` logger naming both the fact that the bundled
    roster may be stale and the canonical URL to check by hand. Raises
    :class:`RegistryError` if the bundled fallback also fails to load.
    """
    try:
        manifest = _fetch_remote_manifest(url=url, timeout=timeout)
        return ConnectorRegistry(
            generated_at=manifest.generated_at,
            connectors=_rows_from_manifest(manifest),
            source="remote",
        )
    except (httpx.HTTPError, ValidationError, ValueError) as remote_exc:
        _logger.warning(
            "could not load the live connector registry from %s (%s: %s); falling back to the bundled "
            "snapshot, which may predate newly published connectors. Check %s directly for the current list.",
            url,
            type(remote_exc).__name__,
            remote_exc,
            CANONICAL_MANIFEST_URL,
        )
        try:
            manifest = _load_bundled_manifest()
        except (FileNotFoundError, OSError, ValidationError, ValueError) as bundled_exc:
            raise RegistryError(
                f"the connector registry is unavailable: the canonical endpoint failed "
                f"({type(remote_exc).__name__}: {remote_exc}) and the bundled fallback also failed "
                f"({type(bundled_exc).__name__}: {bundled_exc}). Check network access, or visit "
                f"{CANONICAL_MANIFEST_URL} directly.",
                remote_error=remote_exc,
                bundled_error=bundled_exc,
            ) from bundled_exc
        return ConnectorRegistry(
            generated_at=manifest.generated_at,
            connectors=_rows_from_manifest(manifest),
            source="bundled",
        )
