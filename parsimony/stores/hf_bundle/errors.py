"""Typed errors raised by the HF bundle store.

Three classes cover every failure mode:

- :class:`BundleError` — base, never raised directly.
- :class:`BundleNotFoundError` — the requested namespace has no bundle on
  HuggingFace Hub (legitimately missing, not an error for the caller).
- :class:`BundleIntegrityError` — every other failure: network errors,
  malformed manifests, SHA mismatches, model-load failures, missing pinned
  revisions. The message always says what went wrong specifically.

All errors carry enough context (namespace, resource, next action) that a
caller's traceback is a self-contained bug report — no need to re-run with
extra logging.
"""

from __future__ import annotations


class BundleError(Exception):
    """Base class for every error raised by the HF bundle store."""

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


__all__ = [
    "BundleError",
    "BundleIntegrityError",
    "BundleNotFoundError",
]
