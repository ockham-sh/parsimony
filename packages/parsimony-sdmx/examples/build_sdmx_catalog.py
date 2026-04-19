"""Build a parsimony catalog from any SDMX dataset and (optionally) push to the Hub.

End-to-end demo of the prebuilt-catalog workflow:

1. Pull the full series list for an SDMX dataset (e.g. ``ECB-YC``, ``ECB-EXR``).
2. Embed each series with the standard sentence-transformers model.
3. Persist the result as a Parquet + FAISS snapshot.
4. Optionally publish it to a Hugging Face Hub dataset repository.

Run::

    # local snapshot only
    uv run python build_sdmx_catalog.py ECB-YC --out ./build/ecb-yc

    # also push to the Hub (HF_TOKEN must be set, repo must exist or be createable)
    uv run python build_sdmx_catalog.py ECB-YC --out ./build/ecb-yc --push hf://ockham/ecb-yc-catalog

Requires (besides ``parsimony-sdmx``)::

    pip install 'parsimony-core[standard]'
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from parsimony import Catalog, SentenceTransformerEmbedder
from parsimony_sdmx import SdmxSeriesKeysParams, sdmx_series_keys

logger = logging.getLogger(__name__)


async def build(
    dataset_key: str,
    *,
    out: Path,
    push_url: str | None,
    model: str,
    builder: str,
    catalog_name: str | None = None,
) -> None:
    """Build a Parquet+FAISS catalog for *dataset_key* and write it to *out*."""
    logger.info("Fetching series list for %s ...", dataset_key)
    series = await sdmx_series_keys(SdmxSeriesKeysParams(dataset_key=dataset_key))
    logger.info("Got %d series rows", len(series.df))

    logger.info("Loading embedder %s ...", model)
    name = catalog_name or dataset_key.lower().replace("-", "_")
    catalog = Catalog(name, embedder=SentenceTransformerEmbedder(model=model))

    logger.info("Indexing series into catalog ...")
    index_result = await catalog.index_result(series, extra_tags=[f"dataset:{dataset_key}"])
    logger.info(
        "Indexed: %d / %d (skipped %d, errors %d)",
        index_result.indexed,
        index_result.total,
        index_result.skipped,
        index_result.errors,
    )

    logger.info("Saving snapshot to %s ...", out)
    await catalog.save(out, builder=builder)

    if push_url:
        logger.info("Pushing to %s ...", push_url)
        await catalog.push(push_url)
        logger.info("Push complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dataset_key", help="SDMX dataset identifier (e.g. ECB-YC, ECB-EXR)")
    parser.add_argument("--out", type=Path, required=True, help="Local snapshot directory (will be replaced)")
    parser.add_argument("--push", default=None, help="Optional URL to publish to (e.g. hf://owner/repo)")
    parser.add_argument(
        "--model",
        default="BAAI/bge-small-en-v1.5",
        help="Sentence-Transformers model identifier (default: BAAI/bge-small-en-v1.5)",
    )
    parser.add_argument(
        "--builder",
        default="examples/build_sdmx_catalog.py",
        help="Provenance string recorded in meta.json#build.builder",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Catalog name written to meta.json (defaults to a normalized form of dataset_key)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    asyncio.run(
        build(
            args.dataset_key,
            out=args.out,
            push_url=args.push,
            model=args.model,
            builder=args.builder,
            catalog_name=args.name,
        )
    )


if __name__ == "__main__":
    main()
