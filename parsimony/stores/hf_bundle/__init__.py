"""HuggingFace Parquet + FAISS catalog bundle store.

Public API:

- :class:`~parsimony.stores.hf_bundle.store.HFBundleCatalogStore` — the
  :class:`~parsimony.stores.catalog_store.CatalogStore` implementation that
  reads Parquet + FAISS bundles from HuggingFace Hub.
- :class:`~parsimony.stores.hf_bundle.format.BundleManifest` — the Pydantic
  wire contract for ``manifest.json``.
- Error hierarchy: :class:`BundleError` (base),
  :class:`BundleNotFoundError`, :class:`BundleIntegrityError`.

Heavy dependencies (``faiss``, ``sentence_transformers``, ``huggingface_hub``)
are imported lazily inside the store and builder methods so that importing
this package itself is cheap.
"""

from parsimony.stores.hf_bundle.errors import (
    BundleError,
    BundleIntegrityError,
    BundleNotFoundError,
)
from parsimony.stores.hf_bundle.format import (
    BUNDLE_FILENAMES,
    ENTRIES_FILENAME,
    ENTRIES_PARQUET_SCHEMA,
    HF_ORG,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    BundleManifest,
    hf_repo_id,
)

__all__ = [
    "BUNDLE_FILENAMES",
    "BundleError",
    "BundleIntegrityError",
    "BundleNotFoundError",
    "BundleManifest",
    "ENTRIES_FILENAME",
    "ENTRIES_PARQUET_SCHEMA",
    "HF_ORG",
    "INDEX_FILENAME",
    "MANIFEST_FILENAME",
    "hf_repo_id",
]
