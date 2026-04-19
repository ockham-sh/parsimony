"""Typed errors for the catalog bundle pipeline.

Three classes cover every failure mode:

- :class:`BundleError` — every runtime failure (download, manifest, SHA,
  shape, size cap, model load). Callers log the message or fail.
- :class:`BundleNotFoundError` — the requested namespace has no bundle on
  HuggingFace Hub. Distinct because a caller legitimately catches this as a
  non-error control-flow signal.
- :class:`BundleSpecError` — raised at decorator import time when a plugin's
  ``catalog={...}`` declaration is malformed, so authors get fast feedback
  and consumers never load a half-broken plugin.

Errors carry enough context (namespace, resource, next action) that a
caller's traceback is a self-contained bug report — no need to re-run with
extra logging.
"""

from __future__ import annotations


class BundleError(Exception):
    """Base class for every runtime error raised by the catalog bundle pipeline."""

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
    """The requested namespace has no bundle published on HuggingFace Hub.

    Distinct from :class:`BundleError` only because callers catch it as a
    non-error control-flow signal — e.g. ``try_load_remote`` returns False.
    """


class BundleSpecError(BundleError):
    """A plugin's ``catalog={...}`` declaration on ``@enumerator`` is malformed.

    Raised at decorator import time (not first-publish time) so plugin
    authors get fast feedback and consumers never load a half-broken plugin.
    """


__all__ = [
    "BundleError",
    "BundleNotFoundError",
    "BundleSpecError",
]
