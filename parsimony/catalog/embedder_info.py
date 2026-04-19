"""Identity of an embedder, persisted in catalog metadata.

The fields are deliberately small. They identify *which model* produced the
vectors so that downstream readers can construct or validate an embedder that
matches at query time. The catalog records this in its ``meta.json`` (or
equivalent) when publishing a snapshot.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class EmbedderInfo(BaseModel):
    """Persisted identity of an embedding model used for a catalog."""

    model: str = Field(description="Model identifier (e.g. ``BAAI/bge-small-en-v1.5``).")
    dim: int = Field(description="Vector dimension produced by the model.")
    normalize: bool = Field(default=True, description="Whether vectors are L2-normalized at production time.")
    package: str | None = Field(
        default=None,
        description=(
            "Optional install hint surfaced in error messages when a catalog "
            "is loaded without the dependencies needed to instantiate its "
            "embedder (e.g. ``parsimony-core[standard]``). Not used for resolution."
        ),
    )
