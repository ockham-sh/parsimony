from __future__ import annotations

from ockham.catalog.models import SeriesEntry


def build_embedding_text(entry: SeriesEntry) -> str:
    """Compose text embedded for semantic search."""
    parts = [entry.title]
    if entry.metadata:
        meta_parts = [f"{k}: {v}" for k, v in entry.metadata.items() if v is not None]
        if meta_parts:
            parts.append(", ".join(meta_parts))
    if entry.tags:
        parts.append(f"tags: {', '.join(entry.tags)}")
    return " | ".join(parts)
