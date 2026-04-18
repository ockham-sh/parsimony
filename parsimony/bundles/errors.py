"""Typed errors for the catalog bundle pipeline.

Six classes cover every failure mode:

- :class:`BundleError` — base, never raised directly.
- :class:`BundleNotFoundError` — the requested namespace has no bundle on
  HuggingFace Hub (legitimately missing, not an error for the caller).
- :class:`BundleIntegrityError` — every download/manifest/SHA/model failure.
- :class:`BundleTooLargeError` — projected or actual artifact exceeds the
  format size cap. Raised at build-time before write and at download-time
  before bytes hit disk.
- :class:`BundleStaleError` — staleness check determined the published
  bundle is up to date; build was skipped intentionally. Not a failure.
- :class:`BundleSpecError` — a plugin's ``catalog={...}`` declaration is
  malformed. Raised at decorator import time so the failure is loud and
  early.

All errors carry enough context (namespace, resource, next action) that a
caller's traceback is a self-contained bug report — no need to re-run with
extra logging.
"""

from __future__ import annotations


class BundleError(Exception):
    """Base class for every error raised by the catalog bundle pipeline."""

    def __init__(
        self,
        message: str,
        *,
        namespace: str | None = None,
        resource: str | None = None,
        next_action: str | None = None,
    ) -> None:
        parts = [message]
        if namespace is not None:
            parts.append(f"namespace={namespace!r}")
        if resource is not None:
            parts.append(f"resource={resource!r}")
        if next_action is not None:
            parts.append(f"next_action={next_action!r}")
        super().__init__(" | ".join(parts))
        self.message = message
        self.namespace = namespace
        self.resource = resource
        self.next_action = next_action


class BundleNotFoundError(BundleError):
    """The requested namespace has no bundle published on HuggingFace Hub."""


class BundleIntegrityError(BundleError):
    """The bundle is unusable: download failed, manifest invalid, SHA mismatch,
    shape mismatch, model load failed, pinned revision unavailable, etc.

    The message is the discriminator — callers typically just log and retry
    or fail, not branch on subtype.
    """


class BundleTooLargeError(BundleError):
    """Projected or actual artifact size exceeds the format size cap.

    Raised on the build path (before write) when the projected parquet/index
    size would exceed the cap, and on the read path (before download) when
    HF Hub repo metadata reports a file over the cap. Hard error — never
    truncate, never warn-and-continue. Resolution: split the namespace via
    a fan-out plan generator.
    """

    def __init__(
        self,
        message: str,
        *,
        namespace: str | None = None,
        resource: str | None = None,
        actual_bytes: int | None = None,
        cap_bytes: int | None = None,
    ) -> None:
        super().__init__(
            message,
            namespace=namespace,
            resource=resource,
            next_action="Split the namespace via a fan-out plan generator",
        )
        self.actual_bytes = actual_bytes
        self.cap_bytes = cap_bytes


class BundleStaleError(BundleError):
    """Build was skipped because the published bundle is already up to date.

    Not a failure — used as a control-flow signal so the orchestrator can
    record ``skipped_unchanged`` in the per-bundle report.
    """


class BundleSpecError(BundleError):
    """A plugin's ``catalog={...}`` declaration on ``@enumerator`` is malformed.

    Raised at decorator import time (not first-publish time) so plugin
    authors get fast feedback and consumers never load a half-broken plugin.
    """


__all__ = [
    "BundleError",
    "BundleIntegrityError",
    "BundleNotFoundError",
    "BundleSpecError",
    "BundleStaleError",
    "BundleTooLargeError",
]
