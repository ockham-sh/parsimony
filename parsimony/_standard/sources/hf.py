"""``hf://`` scheme: Hugging Face Hub dataset repository.

Requires ``huggingface-hub`` (installed by ``parsimony-core[standard]``). Snapshot
files are uploaded to the dataset repository's root; existing snapshots are
replaced wholesale.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsimony._standard.catalog import Catalog
    from parsimony._standard.embedder import EmbeddingProvider

REPO_TYPE = "dataset"


def _strip(url: str) -> str:
    prefix = "hf://"
    if not url.startswith(prefix):
        raise ValueError(f"Expected URL to start with {prefix!r}; got {url!r}")
    return url[len(prefix) :]


async def load(url: str, *, embedder: EmbeddingProvider | None = None) -> Catalog:
    from parsimony._standard.catalog import Catalog

    repo_id = _strip(url)
    local = await asyncio.to_thread(_snapshot_download, repo_id)
    return await Catalog.load(local, embedder=embedder)


async def push(catalog: Catalog, url: str) -> None:
    repo_id = _strip(url)
    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "snapshot"
        await catalog.save(staging)
        await asyncio.to_thread(_upload_folder, repo_id, str(staging))


def _snapshot_download(repo_id: str) -> Path:
    from huggingface_hub import snapshot_download

    return Path(snapshot_download(repo_id=repo_id, repo_type=REPO_TYPE))


def _upload_folder(repo_id: str, folder_path: str) -> None:
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=repo_id, repo_type=REPO_TYPE, exist_ok=True)
    api.upload_folder(folder_path=folder_path, repo_id=repo_id, repo_type=REPO_TYPE)
