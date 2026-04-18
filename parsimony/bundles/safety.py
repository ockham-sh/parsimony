"""Security helpers for the catalog bundle pipeline.

Three concerns live here:

1. **Token scrubbing.** Bundle uploads use HF Hub via ``huggingface_hub``
   which wraps ``requests`` which wraps ``urllib3``. Any of those layers
   can echo an ``Authorization: Bearer hf_…`` header into an exception
   message. :func:`scrub_token` redacts token-shaped substrings, and
   :func:`format_exc_chain` walks the full exception chain so the redactor
   sees the whole message. Rule: any re-raised exception message gets
   passed through ``scrub_token(format_exc_chain(exc), token)``.

2. **Shrink guard.** A transient upstream outage during enumeration
   (one agency 500s, half the series go missing) must not produce a
   half-empty bundle that overwrites the live one. The
   :func:`shrink_guard` callable refuses to publish when the fresh
   ``entry_count`` is less than :data:`SHRINK_GUARD_RATIO` of the
   published ``entry_count``. The ``allow_shrink`` parameter or
   ``--allow-shrink`` CLI flag is the deliberate override.

3. **Anonymous remote-manifest probe.** :func:`fetch_published_entry_count`
   does an anonymous read on the live ``manifest.json`` — no token needed
   for read access to public ``parsimony-dev/*`` repos. The downloaded
   file is size-capped to :data:`~parsimony.bundles.format.MAX_MANIFEST_BYTES`
   before parsing so a tampered or DoS-sized remote can't OOM the worker.
"""

from __future__ import annotations

import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Final

from parsimony.bundles.format import (
    MANIFEST_FILENAME,
    MAX_MANIFEST_BYTES,
    hf_repo_id,
)

logger = logging.getLogger(__name__)


# Bearer tokens (hf_… 20+ chars), Authorization: lines, and urllib-style
# x-api-key values in any form we might accidentally surface via exceptions.
TOKEN_LIKE_RE: Final[re.Pattern[str]] = re.compile(
    r"(hf_[A-Za-z0-9]{20,}|Bearer\s+\S+|Authorization:\s*\S+|"
    r"x-api-key\s*[:=]\s*\S+)",
    re.IGNORECASE,
)


def format_exc_chain(exc: BaseException) -> str:
    """Walk ``__cause__`` / ``__context__`` and join messages.

    huggingface_hub chains through HfHubHTTPError / ConnectionError / urllib3
    ProtocolError — any of those layers can carry a URL with a token shoved
    into the query string or a header echo. We read the chain so the
    redactor catches all of it. Capped at 5 levels.
    """
    parts: list[str] = []
    cur: BaseException | None = exc
    for _ in range(5):
        if cur is None:
            break
        parts.append(f"{type(cur).__name__}: {cur}")
        cur = cur.__cause__ or cur.__context__
    return " | ".join(parts)


def scrub_token(message: str, token: str) -> str:
    """Redact any token-shaped substring before re-raising."""
    text = message
    if token:
        text = text.replace(token, "[REDACTED]")
    text = TOKEN_LIKE_RE.sub("[REDACTED]", text)
    return text


# ---------------------------------------------------------------------------
# Shrink guard
# ---------------------------------------------------------------------------


SHRINK_GUARD_RATIO: Final[float] = 0.5
"""Fresh bundle must have at least this fraction of the published bundle's
``entry_count`` to publish without ``allow_shrink=True``. Set deliberately
loose (50%) so the guard catches catastrophic drops without false-positiving
on slow shrinkage from upstream catalog cleanup."""


def fetch_published_entry_count(namespace: str) -> int | None:
    """Return ``entry_count`` of the currently-published bundle, or ``None``.

    Anonymous probe — no token required, no cache writes. Returns ``None``
    on any failure (no published bundle yet, network error, malformed
    manifest, manifest exceeds size cap).
    """
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        return None
    repo_id = hf_repo_id(namespace)
    try:
        with tempfile.TemporaryDirectory(prefix="parsimony-guard-") as tmp:
            path_str = hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=MANIFEST_FILENAME,
                local_dir=tmp,
                token=False,
            )
            path = Path(path_str)
            size = path.stat().st_size
            if size > MAX_MANIFEST_BYTES:
                logger.warning(
                    "guard.manifest_too_large namespace=%s size=%d cap=%d",
                    namespace,
                    size,
                    MAX_MANIFEST_BYTES,
                )
                return None
            data = json.loads(path.read_text(encoding="utf-8"))
            count = data.get("entry_count")
            return int(count) if isinstance(count, int) else None
    except Exception as exc:
        logger.warning("guard.fetch_published_manifest_failed namespace=%s exc=%s", namespace, exc)
        return None


def shrink_guard(
    namespace: str,
    *,
    fresh_entry_count: int,
    allow_shrink: bool = False,
) -> dict[str, float | int | None]:
    """Refuse to publish when the fresh bundle would shrink the catalog by more
    than :data:`SHRINK_GUARD_RATIO`.

    Returns a metadata dict reporting the previous count and observed ratio
    (suitable for inclusion in a publish report). Raises ``RuntimeError``
    when the guard trips and ``allow_shrink`` is ``False``.

    Pass-through (no published bundle yet, or fetch failed) returns
    ``{"previous_entry_count": None, "shrink_ratio": None}``.
    """
    previous = fetch_published_entry_count(namespace)
    if previous is None or previous <= 0:
        return {"previous_entry_count": None, "shrink_ratio": None}

    ratio = fresh_entry_count / previous
    if ratio < SHRINK_GUARD_RATIO and not allow_shrink:
        raise RuntimeError(
            f"Fresh bundle for namespace={namespace!r} has {fresh_entry_count} entries vs "
            f"{previous} currently published ({ratio:.1%}); "
            "refusing to publish. Pass allow_shrink=True (or --allow-shrink) to override."
        )
    return {"previous_entry_count": previous, "shrink_ratio": ratio}


__all__ = [
    "SHRINK_GUARD_RATIO",
    "TOKEN_LIKE_RE",
    "fetch_published_entry_count",
    "format_exc_chain",
    "scrub_token",
    "shrink_guard",
]
