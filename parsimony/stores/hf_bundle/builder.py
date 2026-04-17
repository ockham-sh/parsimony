"""Bundle builder: enumerate -> embed -> Parquet + FAISS + manifest -> upload.

The builder is a local-only tool. It runs each enumerator against live
APIs, embeds the resulting rows with a local sentence-transformers model,
writes the three-file bundle to a local directory, and optionally uploads
to HuggingFace Hub.

Pure vs impure split (Fowler R4):

- :func:`assemble_table` is pure — given entries, returns an Arrow table.
- :func:`build_faiss_index` is pure — given vectors + params, returns a
  FAISS index in memory.
- :func:`write_bundle_dir` is the I/O surface — takes manifest, table,
  index and writes three files atomically to a directory. SHAs are
  computed *after* writing, not before, so the manifest reflects what's
  actually on disk.

Environment requirements for ``build_and_write``:

- A ``SentenceTransformersEmbeddingProvider`` instance (caller builds it).
- An enumerator callable returning ``SemanticTableResult`` (Tier A connectors).
- A local output directory.

The CLI ``python -m parsimony.stores.hf_bundle.builder build <namespace>
<out_dir>`` runs a single Tier A enumerator and writes the bundle.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import importlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from parsimony.catalog.arrow_adapters import entries_to_arrow_table
from parsimony.catalog.catalog import _entries_from_table_result
from parsimony.catalog.models import EmbeddingProvider, SeriesEntry
from parsimony.stores.hf_bundle.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    ENTRIES_PARQUET_SCHEMA,
    FAISS_HNSW_EF_CONSTRUCTION,
    FAISS_HNSW_EF_SEARCH_DEFAULT,
    FAISS_HNSW_M,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    BundleManifest,
    hf_repo_id,
)

logger = logging.getLogger(__name__)

# Tier A namespaces whose enumerators already exist in parsimony.connectors.
TIER_A_NAMESPACES: tuple[str, ...] = (
    "snb",
    "riksbank",
    "boc",
    "rba",
    "bde",
    "treasury",
)

# Map namespace -> (module name, enumerator symbol).
_ENUMERATOR_MODULE: dict[str, tuple[str, str]] = {
    "snb": ("parsimony.connectors.snb", "enumerate_snb"),
    "riksbank": ("parsimony.connectors.riksbank", "enumerate_riksbank"),
    "boc": ("parsimony.connectors.boc", "enumerate_boc"),
    "rba": ("parsimony.connectors.rba", "enumerate_rba"),
    "bde": ("parsimony.connectors.bde", "enumerate_bde"),
    "treasury": ("parsimony.connectors.treasury", "enumerate_treasury"),
}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def assemble_table(entries: list[SeriesEntry]) -> pa.Table:
    """Pure: entries -> Arrow table with dense row_id."""
    return entries_to_arrow_table(entries)


def build_faiss_index(
    vectors: list[list[float]],
    *,
    dim: int,
    m: int = FAISS_HNSW_M,
    ef_construction: int = FAISS_HNSW_EF_CONSTRUCTION,
    ef_search: int = FAISS_HNSW_EF_SEARCH_DEFAULT,
) -> Any:
    """Pure: vectors + params -> in-memory FAISS HNSWFlat index.

    Vectors must already be L2-normalized (providers wrap
    ``normalize_embeddings=True``). FAISS is imported here, not at module
    scope, so tools that inspect this module (e.g., ``help``) don't need
    FAISS installed.
    """
    import faiss
    import numpy as np

    if not vectors:
        raise ValueError("Cannot build FAISS index over zero vectors")
    arr = np.asarray(vectors, dtype=np.float32)
    if arr.ndim != 2 or arr.shape[1] != dim:
        raise ValueError(f"Expected (N, {dim}) float32 array, got shape {arr.shape}")

    # HNSW with inner-product metric over L2-normalized vectors == cosine.
    index = faiss.IndexHNSWFlat(dim, m, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = ef_construction
    index.hnsw.efSearch = ef_search
    index.add(arr)
    return index


def write_faiss_index(index: Any, path: Path) -> None:
    """Serialize a FAISS index to disk at *path*."""
    import faiss

    faiss.write_index(index, str(path))


def sha256_file(path: Path, *, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            buf = fh.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def current_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = result.stdout.strip()
    return sha if len(sha) == 40 else None


# ---------------------------------------------------------------------------
# I/O orchestration (impure, top-to-bottom so side effects are visible)
# ---------------------------------------------------------------------------


async def embed_entries_in_batches(
    entries: list[SeriesEntry],
    *,
    provider: EmbeddingProvider,
    batch_size: int = 32,
) -> list[list[float]]:
    """Embed each entry's text, returning vectors in matching order.

    Embedding text is the entry's title plus metadata/tags (same function
    used at bundle-time embedding and query-time encoding, so build and
    query see the same text shape).
    """
    from parsimony.catalog.catalog import build_embedding_text

    texts = [build_embedding_text(e) for e in entries]
    out: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        vectors = await provider.embed_texts(batch)
        if len(vectors) != len(batch):
            raise RuntimeError(f"embed_texts returned {len(vectors)} vectors for {len(batch)} texts")
        out.extend(vectors)
    return out


def write_bundle_dir(
    out_dir: Path,
    *,
    namespace: str,
    entries: list[SeriesEntry],
    vectors: list[list[float]],
    provider: EmbeddingProvider,
    git_sha: str | None = None,
    ef_search: int = FAISS_HNSW_EF_SEARCH_DEFAULT,
) -> BundleManifest:
    """Write the three-file bundle to *out_dir* and return the manifest.

    Layout (exact):

    - ``out_dir/entries.parquet``
    - ``out_dir/index.faiss``
    - ``out_dir/manifest.json``

    SHAs are computed from the written files (not the in-memory objects)
    so the manifest reflects exactly what ships.
    """
    if len(entries) != len(vectors):
        raise ValueError(f"entries ({len(entries)}) and vectors ({len(vectors)}) length mismatch")
    if not entries:
        raise ValueError("Cannot build a bundle with zero entries")

    out_dir.mkdir(parents=True, exist_ok=True)

    entries_path = out_dir / ENTRIES_FILENAME
    index_path = out_dir / INDEX_FILENAME
    manifest_path = out_dir / MANIFEST_FILENAME

    table = assemble_table(entries)
    if table.schema != ENTRIES_PARQUET_SCHEMA:
        raise RuntimeError("assemble_table produced a schema that doesn't match the bundle contract")
    pq.write_table(table, entries_path)

    dim = provider.dimension
    index = build_faiss_index(vectors, dim=dim, ef_search=ef_search)
    write_faiss_index(index, index_path)

    entries_sha = sha256_file(entries_path)
    index_sha = sha256_file(index_path)

    model_id = getattr(provider, "model_id", None)
    revision = getattr(provider, "revision", None)
    if model_id is None or revision is None:
        raise RuntimeError(
            "Provider must expose model_id and revision attributes for the bundle manifest; "
            "use a SentenceTransformersEmbeddingProvider"
        )

    manifest = BundleManifest(
        namespace=namespace,
        built_at=datetime.now(UTC),
        entry_count=len(entries),
        embedding_model=model_id,
        embedding_model_revision=revision,
        embedding_dim=dim,
        faiss_hnsw_ef_search=ef_search,
        entries_sha256=entries_sha,
        index_sha256=index_sha,
        builder_git_sha=git_sha or current_git_sha(),
    )
    manifest_path.write_text(
        manifest.model_dump_json(indent=2, round_trip=True) + "\n",
        encoding="utf-8",
    )

    # Safety: assert we didn't leave stray files in the bundle directory.
    actual = {p.name for p in out_dir.iterdir() if p.is_file()}
    extras = actual - BUNDLE_FILENAMES
    if extras:
        logger.warning("Unexpected extra files in bundle dir: %s", sorted(extras))

    return manifest


# ---------------------------------------------------------------------------
# Enumerator resolution
# ---------------------------------------------------------------------------


def _resolve_enumerator(namespace: str) -> Any:
    spec = _ENUMERATOR_MODULE.get(namespace)
    if spec is None:
        raise ValueError(f"No enumerator registered for namespace {namespace!r}")
    module_name, symbol = spec
    module = importlib.import_module(module_name)
    try:
        return getattr(module, symbol)
    except AttributeError as exc:
        raise ValueError(f"Enumerator symbol {symbol!r} not found in {module_name}") from exc


async def run_enumerator(namespace: str) -> list[SeriesEntry]:
    """Run the Tier A enumerator for *namespace* and return catalog rows."""
    conn = _resolve_enumerator(namespace)
    params = conn.param_type() if conn.param_type else None
    result = await conn(params)
    return _entries_from_table_result(result)


# ---------------------------------------------------------------------------
# Top-level build + upload
# ---------------------------------------------------------------------------


async def build_bundle(
    namespace: str,
    *,
    out_dir: Path,
    provider: EmbeddingProvider,
    embed_batch_size: int = 32,
) -> BundleManifest:
    """End-to-end: enumerate → embed → write bundle. Returns the manifest."""
    logger.info("catalog.build.start namespace=%s", namespace)
    t0 = time.monotonic()

    entries = await run_enumerator(namespace)
    if not entries:
        raise RuntimeError(f"Enumerator for {namespace!r} returned zero entries")

    logger.info(
        "catalog.build.enumerated namespace=%s entries=%d elapsed_s=%.2f",
        namespace,
        len(entries),
        time.monotonic() - t0,
    )

    t1 = time.monotonic()
    vectors = await embed_entries_in_batches(entries, provider=provider, batch_size=embed_batch_size)
    logger.info(
        "catalog.build.embedded namespace=%s vectors=%d elapsed_s=%.2f",
        namespace,
        len(vectors),
        time.monotonic() - t1,
    )

    manifest = write_bundle_dir(
        out_dir,
        namespace=namespace,
        entries=entries,
        vectors=vectors,
        provider=provider,
    )
    logger.info(
        "catalog.build.written namespace=%s out_dir=%s entries=%d total_s=%.2f",
        namespace,
        out_dir,
        manifest.entry_count,
        time.monotonic() - t0,
    )
    return manifest


def upload_bundle(
    bundle_dir: Path,
    *,
    namespace: str,
    token: str | None = None,
) -> str:
    """Upload a bundle directory to the ``parsimony-dev`` HF dataset repo.

    Returns the commit SHA of the upload. Token is read from HF_TOKEN or
    HUGGING_FACE_HUB_TOKEN env vars if not provided. Exception args are
    scrubbed of any token-shaped substring before being re-raised.

    urllib3 / huggingface_hub / requests DEBUG logs are silenced for the
    duration of the upload: a DEBUG handler on any of those loggers would
    otherwise emit the raw Authorization header before Python exception
    handling gets a chance to redact (Hunt R5).
    """
    from huggingface_hub import HfApi

    if token is None:
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if not token:
        raise RuntimeError(
            "HF upload requires a token: set HF_TOKEN or HUGGING_FACE_HUB_TOKEN "
            "(the write-scoped token for parsimony-dev)"
        )

    # Sanity: only the three allowed files should be in the bundle dir.
    extras = [p.name for p in bundle_dir.iterdir() if p.is_file() and p.name not in BUNDLE_FILENAMES]
    if extras:
        raise RuntimeError(f"Refusing to upload extra files in bundle dir: {extras}")

    api = HfApi(token=token)
    repo_id = hf_repo_id(namespace)
    with _silence_http_debug_logs():
        try:
            api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True, private=False)
            commit = api.upload_folder(
                repo_id=repo_id,
                repo_type="dataset",
                folder_path=str(bundle_dir),
                allow_patterns=list(BUNDLE_FILENAMES),
                commit_message=f"publish catalog bundle for {namespace}",
            )
        except Exception as exc:
            raise RuntimeError(_scrub_token(_format_exc_chain(exc), token)) from None
    return getattr(commit, "oid", None) or str(commit)


# Bearer tokens (hf_... 20+ chars), Authorization: lines, and urllib-style
# x-api-key values in any form we might accidentally surface via exceptions.
_TOKEN_LIKE_RE = re.compile(
    r"(hf_[A-Za-z0-9]{20,}|Bearer\s+\S+|Authorization:\s*\S+|"
    r"x-api-key\s*[:=]\s*\S+)",
    re.IGNORECASE,
)


def _format_exc_chain(exc: BaseException) -> str:
    """Collect ``str(exc)`` + PEP 678 notes + nested cause/context messages.

    huggingface_hub chains through HfHubHTTPError / ConnectionError / urllib3
    ProtocolError — any of those layers can carry a URL with a token shoved
    into the query string or a header echo. We read the whole chain so the
    redactor catches all of it.
    """
    parts: list[str] = []
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        parts.append(f"{type(cur).__name__}: {cur}")
        for arg in getattr(cur, "args", ()):
            if isinstance(arg, str):
                parts.append(arg)
        for note in getattr(cur, "__notes__", None) or ():
            parts.append(str(note))
        cur = cur.__cause__ or cur.__context__
    return " | ".join(parts)


def _scrub_token(message: str, token: str) -> str:
    """Redact any token-shaped substring before re-raising."""
    text = message
    if token:
        text = text.replace(token, "[REDACTED]")
    text = _TOKEN_LIKE_RE.sub("[REDACTED]", text)
    return text


@contextlib.contextmanager
def _silence_http_debug_logs() -> Iterator[None]:
    """Temporarily raise noisy HTTP library loggers above DEBUG.

    Only silences the *handler* path that would emit raw headers — we don't
    touch WARNING/ERROR, so upload errors still surface.
    """
    noisy = ("urllib3", "requests", "huggingface_hub", "httpcore", "httpx")
    saved: list[tuple[logging.Logger, int]] = []
    try:
        for name in noisy:
            lg = logging.getLogger(name)
            saved.append((lg, lg.level))
            if lg.level == logging.NOTSET or lg.level < logging.INFO:
                lg.setLevel(logging.INFO)
        yield
    finally:
        for lg, level in saved:
            lg.setLevel(level)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_provider_from_env() -> EmbeddingProvider:
    """Construct the default build-time embedding provider from env vars.

    ``PARSIMONY_EMBED_MODEL`` (default ``sentence-transformers/all-MiniLM-L6-v2``)
    ``PARSIMONY_EMBED_REVISION`` (required — model commit SHA)
    ``PARSIMONY_EMBED_DIM`` (default 384)
    """
    from parsimony.embeddings.sentence_transformers import (
        SentenceTransformersEmbeddingProvider,
    )

    model_id = os.environ.get("PARSIMONY_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    revision = os.environ.get("PARSIMONY_EMBED_REVISION")
    if not revision:
        raise RuntimeError(
            "PARSIMONY_EMBED_REVISION must be set to the full 40-char commit SHA "
            "of the embedding model pinned for this build"
        )
    dim = int(os.environ.get("PARSIMONY_EMBED_DIM", "384"))
    return SentenceTransformersEmbeddingProvider(model_id=model_id, revision=revision, expected_dim=dim)


def _fetch_published_entry_count(namespace: str) -> int | None:
    """Return ``entry_count`` of the currently-published bundle, or ``None``.

    Used by :func:`publish_bundle` to guard against atomic-replace with a
    much smaller bundle (Friedman R2). Anonymous HEAD only — no write token
    required, no cache writes.
    """
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        return None
    repo_id = hf_repo_id(namespace)
    try:
        with tempfile.TemporaryDirectory(prefix="parsimony-guard-") as tmp:
            path = hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=MANIFEST_FILENAME,
                local_dir=tmp,
                token=False,
            )
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            count = data.get("entry_count")
            return int(count) if isinstance(count, int) else None
    except Exception as exc:
        logger.warning("guard.fetch_published_manifest_failed namespace=%s exc=%s", namespace, exc)
        return None


_SHRINK_GUARD_RATIO = 0.5


def publish_bundle(
    namespace: str,
    *,
    provider: EmbeddingProvider,
    embed_batch_size: int = 32,
    dry_run: bool = False,
    allow_shrink: bool = False,
    keep_dir: Path | None = None,
) -> dict[str, Any]:
    """Build + upload (or dry-run) a bundle; return a structured report.

    Guards:

    - ``dry_run=True`` builds locally, echoes the manifest, skips the
      upload.
    - ``allow_shrink=False`` (default) refuses to publish when the fresh
      ``entry_count`` is less than 50% of the currently-published
      bundle's count. Pass ``True`` to override.
    - ``keep_dir`` copies the built bundle into a caller-owned directory
      before the tempdir is cleaned up.
    """
    tmp_ctx = tempfile.TemporaryDirectory(prefix="parsimony-bundle-")
    with tmp_ctx as tmp:
        out_dir = Path(tmp)
        manifest = asyncio.run(
            build_bundle(
                namespace,
                out_dir=out_dir,
                provider=provider,
                embed_batch_size=embed_batch_size,
            )
        )
        report: dict[str, Any] = {
            "namespace": namespace,
            "entry_count": manifest.entry_count,
            "manifest": manifest.model_dump(mode="json"),
            "dry_run": dry_run,
        }

        if keep_dir is not None:
            keep_dir = Path(keep_dir)
            keep_dir.mkdir(parents=True, exist_ok=True)
            for name in BUNDLE_FILENAMES:
                src = out_dir / name
                dst = keep_dir / name
                dst.write_bytes(src.read_bytes())
            report["kept_dir"] = str(keep_dir)

        if dry_run:
            report["status"] = "dry_run"
            return report

        published = _fetch_published_entry_count(namespace)
        if published is not None and published > 0:
            ratio = manifest.entry_count / published
            report["previous_entry_count"] = published
            report["shrink_ratio"] = ratio
            if ratio < _SHRINK_GUARD_RATIO and not allow_shrink:
                raise RuntimeError(
                    f"Fresh bundle has {manifest.entry_count} entries vs "
                    f"{published} currently published ({ratio:.1%}); "
                    "refusing to publish. Pass --allow-shrink to override."
                )

        commit_sha = upload_bundle(out_dir, namespace=namespace)
        report["status"] = "published"
        report["commit_sha"] = commit_sha
        return report


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="parsimony.stores.hf_bundle.builder")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="Build a bundle for a namespace")
    build.add_argument("namespace", help="Namespace slug (e.g. snb, riksbank)")
    build.add_argument("out_dir", help="Output directory for the bundle")
    build.add_argument("--batch-size", type=int, default=32)

    publish = sub.add_parser("publish", help="Build and upload a bundle")
    publish.add_argument("namespace")
    publish.add_argument("--batch-size", type=int, default=32)
    publish.add_argument(
        "--dry-run",
        action="store_true",
        help="Build locally and print the manifest; do not upload.",
    )
    publish.add_argument(
        "--yes",
        action="store_true",
        help="Required to actually publish. Prevents a stray CLI invocation "
        "from atomically replacing a production bundle.",
    )
    publish.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Permit publishing a bundle whose entry_count is <50% of the "
        "currently-published bundle's count. Refused without this flag.",
    )
    publish.add_argument(
        "--keep-dir",
        default=None,
        help="Copy the built bundle into this directory before tempdir cleanup (useful for post-publish diffing).",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    provider = _build_provider_from_env()

    if args.command == "build":
        out_dir = Path(args.out_dir)
        manifest = asyncio.run(
            build_bundle(
                args.namespace,
                out_dir=out_dir,
                provider=provider,
                embed_batch_size=args.batch_size,
            )
        )
        print(json.dumps(manifest.model_dump(mode="json"), indent=2, default=str))  # noqa: T201
        return

    if args.command == "publish":
        if not args.dry_run and not args.yes:
            print(  # noqa: T201
                "publish is destructive: replaces the live bundle on HuggingFace Hub. "
                "Pass --yes to confirm, or --dry-run to preview without uploading.",
                file=sys.stderr,
            )
            sys.exit(2)

        keep_dir = Path(args.keep_dir) if args.keep_dir else None
        report = publish_bundle(
            args.namespace,
            provider=provider,
            embed_batch_size=args.batch_size,
            dry_run=args.dry_run,
            allow_shrink=args.allow_shrink,
            keep_dir=keep_dir,
        )
        print(json.dumps(report, indent=2, default=str))  # noqa: T201
        return


if __name__ == "__main__":
    main()


__all__ = [
    "TIER_A_NAMESPACES",
    "assemble_table",
    "build_bundle",
    "build_faiss_index",
    "embed_entries_in_batches",
    "main",
    "run_enumerator",
    "upload_bundle",
    "write_bundle_dir",
]
