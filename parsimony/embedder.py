"""Embedding providers used by the standard :class:`parsimony.Catalog`.

:class:`EmbeddingProvider` is the structural contract every embedder
satisfies. It is *not* a plugin axis — users instantiate one of the bundled
implementations (or write their own conforming class) and pass it to
``VectorIndex(..., embedder=...)``. Three implementations ship out of the box:

* :class:`SentenceTransformerEmbedder` — local model
  (``sentence-transformers/all-MiniLM-L6-v2`` by default, 384-dim, 6 layers).
  Requires ``parsimony[catalog]``.
* :class:`OnnxEmbedder` — same model via ONNX Runtime with dynamic int8
  quantization. 2-3× faster than the PyTorch path on x86 CPUs with AVX2 /
  AVX_VNNI; ~4× smaller on disk. Requires ``parsimony[standard-onnx]``.
* :class:`LiteLLMEmbeddingProvider` — hosted embeddings via the
  `litellm <https://github.com/BerriAI/litellm>`_ unified API (OpenAI,
  Gemini, Cohere, Voyage, …). Requires ``parsimony[litellm]``.

All classes import their heavy dependencies lazily so that
``import parsimony`` does not pull torch, onnxruntime, or litellm into
memory.
"""

from __future__ import annotations

import hashlib
import logging
import math
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PARSIMONY_CATALOG_PACKAGE = "parsimony[catalog]"
PARSIMONY_STANDARD_ONNX_PACKAGE = "parsimony[standard-onnx]"
PARSIMONY_LITELLM_PACKAGE = "parsimony[litellm]"

_LITELLM_BATCH_SIZE = 100
_ONNX_DEFAULT_BATCH_SIZE = 64


class EmbedderInfo(BaseModel):
    """Persisted identity of an embedding model used for a catalog."""

    model: str = Field(description="Model identifier (e.g. ``sentence-transformers/all-MiniLM-L6-v2``).")
    dim: int = Field(description="Vector dimension produced by the model.")
    normalize: bool = Field(default=True, description="Whether vectors are L2-normalized at production time.")
    package: str | None = Field(
        default=None,
        description=(
            "Optional install hint surfaced in error messages when a catalog "
            "is loaded without the dependencies needed to instantiate its "
            "embedder (e.g. ``parsimony[catalog]``). Not used for resolution."
        ),
    )


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Text-to-vector embedding consumed by the standard catalog."""

    @property
    def dimension(self) -> int: ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embeddings for corpus documents (indexing)."""
        ...

    def embed_query(self, query: str) -> list[float]:
        """Single embedding optimized for retrieval queries."""
        ...

    def info(self) -> EmbedderInfo:
        """Persisted identity for this embedder, used in catalog metadata."""
        ...


_MODEL_CACHE: dict[tuple[str, str | None], SentenceTransformer] = {}
_MODEL_CACHE_LOCK = threading.Lock()


def _load_shared_model(model_name: str, device: str | None) -> SentenceTransformer:
    """Load a SentenceTransformer once per ``(model, device)`` and share it process-wide.

    Catalog snapshots loaded from disk each reconstruct their ``VectorIndex`` with a fresh
    embedder (see ``VectorIndex._require_embedder``), so without this cache the same model is
    reloaded once per loaded catalog — seconds each, and it dominates a multi-codelist agent
    run. ``normalize``/``batch_size`` are applied at encode time and do not affect the loaded
    model, so the cache keys on ``(model, device)`` only.

    This in-memory cache is intentionally ``SentenceTransformerEmbedder``-only — it is the
    default embedder (``parsimony[catalog]``) and the PyTorch constructor re-reads the
    full model into memory on every instantiation, with nothing cached between instances.
    ``OnnxEmbedder`` (the opt-in ``parsimony[standard-onnx]`` path, used for a 2-3×
    faster int8 inference) deliberately has no analogue here: its one genuinely expensive
    step — exporting the model to ONNX and quantizing it — is cached on *disk* by
    ``OnnxEmbedder._prepare_cache`` and reused across every instance and process, so building
    a fresh ``InferenceSession`` per embedder is comparatively cheap. The trade-off is that a
    load-many-catalogs run still rebuilds the Onnx session/tokenizer per instance; that
    smaller cost was left uncached on purpose rather than mirrored here.
    """
    key = (model_name, device)
    cached = _MODEL_CACHE.get(key)
    if cached is not None:
        return cached
    with _MODEL_CACHE_LOCK:
        cached = _MODEL_CACHE.get(key)
        if cached is not None:
            return cached
        model = _load_sentence_transformer(model_name, device)
        _MODEL_CACHE[key] = model
        return model


def _load_sentence_transformer(model_name: str, device: str | None) -> SentenceTransformer:
    """Load a SentenceTransformer from the local cache without touching the Hub.

    ``SentenceTransformer(repo_id, local_files_only=True)`` does NOT reliably stay
    offline: the underlying transformers auto-resolution still fires blocking
    ``HEAD`` requests to huggingface.co (e.g. ``processor_config.json``), which
    stall for tens of seconds on a slow network even when the model is fully
    cached. Resolving the cached snapshot to a local directory with
    ``snapshot_download(..., local_files_only=True)`` — whose ``local_files_only``
    *is* honoured — and constructing from that path bypasses Hub resolution
    entirely. The Hub is contacted only on a genuine cache miss (first run), to
    download the model once.
    """
    # Announced and timed *around the import*, which pulls in torch and is the
    # dominant cost on a first load — several seconds against a fraction of one
    # for materializing the weights. Timing only the construction would report a
    # tenth of the wait the caller actually experiences, which is the kind of
    # log that teaches a reader to distrust the rest.
    #
    # This path needs a pair at all because suppressing the ``Loading weights``
    # bar (see ``_quiet_weight_loading``) took away its only signal, and it runs
    # on the first semantic search of every process even when the model is fully
    # cached. Unlike a tqdm bar these survive redirection off a tty. Any download
    # pair nests inside this interval — the model is fetched, then read.
    logger.info("Loading embedding model %s", model_name)
    started = time.monotonic()

    from sentence_transformers import SentenceTransformer

    model: SentenceTransformer
    # A model given as a local path is already resolved — load it directly.
    if Path(model_name).is_dir():
        with _quiet_weight_loading():
            model = SentenceTransformer(model_name, device=device)
    else:
        local_dir = _resolve_cached_model_dir(model_name)
        with _quiet_weight_loading():
            if local_dir is None:
                # Hub unavailable / repo unresolvable — let SentenceTransformer resolve it.
                model = SentenceTransformer(model_name, device=device)
            else:
                model = SentenceTransformer(local_dir, device=device)

    logger.info("Loaded embedding model %s in %.1fs", model_name, time.monotonic() - started)
    return model


@contextmanager
def _quiet_weight_loading() -> Iterator[None]:
    """Silence transformers' ``Loading weights`` bar for the duration of a model load.

    Materializing the model writes a tqdm bar to stderr on every fresh process.
    It is progress on parsimony's own internals — not on anything the caller asked
    for — so it is noise in agent and pipeline logs, and it is drawn unconditionally
    (unlike Hugging Face's download bars, which hide themselves off a TTY).

    Deliberately *not* ``transformers.utils.logging.disable_progress_bar()``: that
    also calls ``huggingface_hub``'s global ``disable_progress_bars()``, which would
    take the catalog and model *download* bars with it — the one progress signal a
    human watching a slow first call actually wants. Toggling the module flag that
    transformers' own ``tqdm`` wrapper consults keeps the blast radius to this load,
    and the prior value is restored so a caller's explicit setting survives.
    """
    try:
        from transformers.utils import logging as hf_logging
    except ImportError:  # transformers absent (e.g. a non-torch embedder path)
        yield
        return
    prior = hf_logging._tqdm_active
    hf_logging._tqdm_active = False
    try:
        yield
    finally:
        hf_logging._tqdm_active = prior


def _resolve_cached_model_dir(model_name: str) -> str | None:
    """Resolve *model_name* to a local model directory, downloading once if missing.

    ``snapshot_download``'s ``local_files_only`` *is* honoured (unlike the same
    kwarg on ``SentenceTransformer``), so the cached path resolves with zero
    network. Returns ``None`` when the Hub is unavailable or the repo can't be
    resolved, so the caller falls back to SentenceTransformer's own resolution.
    """
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        return None
    # Catch OSError, not bare Exception: a cache miss raises LocalEntryNotFoundError
    # (a FileNotFoundError) and every network failure (HfHubHTTPError, ConnectionError,
    # Timeout) is a requests IOError/OSError — so OSError covers the "can't resolve,
    # fall back" cases while letting a real bug (TypeError, bad-id ValueError) surface.
    try:
        return snapshot_download(model_name, local_files_only=True)  # cached → offline
    except OSError:
        pass
    # Only reached on a genuine cache miss, where this is a ~90 MB download that
    # stalls the first search of a fresh install. Bracket it the same way the
    # catalog fetch is bracketed: without the pair, a captured log cannot tell a
    # one-time model download from a hung process.
    logger.info("Downloading embedding model %s from Hugging Face", model_name)
    started = time.monotonic()
    try:
        resolved = snapshot_download(model_name)  # first run: download once
    except OSError:
        return None
    logger.info("Downloaded embedding model %s in %.1fs", model_name, time.monotonic() - started)
    return resolved


class SentenceTransformerEmbedder:
    """Wraps a :class:`sentence_transformers.SentenceTransformer` model.

    Default model is :data:`DEFAULT_MODEL`. The first attribute access that
    needs the model triggers loading; instantiation alone is cheap.
    """

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        normalize: bool = True,
        device: str | None = None,
        batch_size: int = 64,
    ) -> None:
        self._model_name = model
        self._normalize = normalize
        self._device = device
        self._batch_size = batch_size
        self._model: SentenceTransformer | None = None

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def normalize(self) -> bool:
        return self._normalize

    @property
    def dimension(self) -> int:
        model = self._get_model()
        dim_fn = getattr(model, "get_embedding_dimension", model.get_sentence_embedding_dimension)
        dim = dim_fn()
        if dim is None:  # pragma: no cover
            raise RuntimeError(
                f"sentence-transformers model {self._model_name!r} did not report an embedding dimension"
            )
        return int(dim)

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model=self._model_name,
            dim=self.dimension,
            normalize=self._normalize,
            package=PARSIMONY_CATALOG_PACKAGE,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return self._encode(texts)

    def embed_query(self, query: str) -> list[float]:
        return self._encode([query])[0]

    def _encode(self, texts: list[str]) -> list[list[float]]:
        model = self._get_model()
        # Length-bucketed batching for the same reason as
        # OnnxEmbedder._encode_all: sentence-transformers' encode() also
        # uses dynamic per-batch padding, so sorting near-equal-length
        # docs together eliminates pad-token compute. ~1.6× speedup on
        # the diverse-short-text corpora typical of catalog rows.
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        sorted_texts = [texts[i] for i in order]
        sorted_vectors = model.encode(
            sorted_texts,
            batch_size=self._batch_size,
            convert_to_numpy=True,
            normalize_embeddings=self._normalize,
            show_progress_bar=False,
        )
        out: list[list[float]] = [None] * len(texts)  # type: ignore[list-item]
        for sorted_pos, original_pos in enumerate(order):
            out[original_pos] = sorted_vectors[sorted_pos].tolist()
        return out

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            self._model = _load_shared_model(self._model_name, self._device)
        return self._model


class OnnxEmbedder:
    """ONNX Runtime embedder with optional int8 dynamic quantization.

    Exports a HuggingFace encoder model (default
    ``sentence-transformers/all-MiniLM-L6-v2``) to ONNX on first use,
    optionally quantizes the weights to int8, caches
    both artifacts on disk, and runs inference via ONNX Runtime's
    ``CPUExecutionProvider``. On x86 CPUs with AVX2 / AVX_VNNI the int8
    path is 2-3× faster than the PyTorch ``SentenceTransformerEmbedder``
    for the same model and weights, at a ~4× smaller on-disk footprint.

    Vector outputs are L2-normalized to match the standard
    Catalog's inner-product FAISS index and stay bit-compatible with
    catalogs built by the PyTorch-backed embedder.

    Cache layout::

        parsimony.cache.models_dir() / <model_slug> / {fp32,int8}/
            ├── model.onnx (or model_quantized.onnx for int8)
            ├── tokenizer.json
            └── …

    Defaults to :func:`parsimony.cache.models_dir` (resolved through
    ``PARSIMONY_CACHE_DIR`` then
    ``platformdirs.user_cache_dir("parsimony")``). Pass ``cache_dir=`` to
    override the parent that contains the per-model layout (mostly for
    tests).
    """

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        normalize: bool = True,
        quantize: bool = True,
        batch_size: int = _ONNX_DEFAULT_BATCH_SIZE,
        max_seq_length: int = 512,
        intra_op_threads: int | None = None,
        cache_dir: str | Path | None = None,
    ) -> None:
        self._model_name = model
        self._normalize = normalize
        self._quantize = quantize
        self._batch_size = batch_size
        self._max_seq_length = max_seq_length
        self._intra_op_threads = intra_op_threads
        self._cache_dir_override = Path(cache_dir) if cache_dir is not None else None
        self._session: Any = None  # onnxruntime.InferenceSession
        self._tokenizer: Any = None  # transformers.PreTrainedTokenizerBase
        self._input_names: tuple[str, ...] | None = None
        self._dimension: int | None = None

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def normalize(self) -> bool:
        return self._normalize

    @property
    def dimension(self) -> int:
        self._ensure_session()
        assert self._dimension is not None
        return self._dimension

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model=self._model_name,
            dim=self.dimension,
            normalize=self._normalize,
            package=PARSIMONY_STANDARD_ONNX_PACKAGE,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return self._encode_all(texts)

    def embed_query(self, query: str) -> list[float]:
        return self._encode_all([query])[0]

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _encode_all(self, texts: list[str]) -> list[list[float]]:
        self._ensure_session()
        import numpy as np

        # Length-bucketed batching: dynamic padding pads every doc in a
        # batch to the longest one. Sorting by length (proxy: character
        # count) before batching keeps each batch's lengths near-uniform,
        # so padding overhead is ~0 instead of ~25-30% on diverse corpora.
        # Measured ~1.6× throughput on real ECB/HICP composite labels at
        # batch_size=32. We compute on the sorted order and remap results
        # back to the caller's input order so the public contract is
        # preserved.
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        sorted_texts = [texts[i] for i in order]

        sorted_vectors: list[list[float]] = []
        for start in range(0, len(sorted_texts), self._batch_size):
            chunk = sorted_texts[start : start + self._batch_size]
            enc = self._tokenizer(
                chunk,
                padding=True,
                truncation=True,
                max_length=self._max_seq_length,
                return_tensors="np",
            )
            feed = {name: enc[name] for name in self._input_names or () if name in enc}
            outputs = self._session.run(None, feed)
            last_hidden = outputs[0]  # (batch, seq, dim)
            mask = enc["attention_mask"][..., None].astype("float32")
            summed = (last_hidden * mask).sum(axis=1)
            counts = np.maximum(mask.sum(axis=1), 1e-12)
            pooled = summed / counts
            if self._normalize:
                norms = np.linalg.norm(pooled, axis=1, keepdims=True)
                pooled = pooled / np.maximum(norms, 1e-12)
            sorted_vectors.extend(vec.tolist() for vec in pooled.astype("float32"))

        out: list[list[float]] = [None] * len(texts)  # type: ignore[list-item]
        for sorted_pos, original_pos in enumerate(order):
            out[original_pos] = sorted_vectors[sorted_pos]
        return out

    def _ensure_session(self) -> None:
        if self._session is not None:
            return

        try:
            import onnxruntime as ort
            from transformers import AutoTokenizer
        except ImportError as exc:
            raise ImportError(
                f"OnnxEmbedder requires onnxruntime + transformers. "
                f"Install with: pip install '{PARSIMONY_STANDARD_ONNX_PACKAGE}'"
            ) from exc

        model_dir = self._prepare_cache()
        onnx_path = self._pick_onnx_file(model_dir)

        sess_opts = ort.SessionOptions()
        sess_opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        if self._intra_op_threads is not None:
            sess_opts.intra_op_num_threads = self._intra_op_threads
        self._session = ort.InferenceSession(
            str(onnx_path),
            sess_options=sess_opts,
            providers=["CPUExecutionProvider"],
        )
        self._input_names = tuple(inp.name for inp in self._session.get_inputs())
        self._tokenizer = AutoTokenizer.from_pretrained(str(model_dir))
        # Probe embedding dimension once; one-time cost, amortized over all queries.
        probe = self._encode_batch_raw(["dimension probe"])
        self._dimension = len(probe[0])

    def _encode_batch_raw(self, texts: list[str]) -> list[list[float]]:
        import numpy as np

        enc = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=self._max_seq_length,
            return_tensors="np",
        )
        feed = {name: enc[name] for name in self._input_names or () if name in enc}
        outputs = self._session.run(None, feed)
        last_hidden = outputs[0]
        mask = enc["attention_mask"][..., None].astype("float32")
        pooled = (last_hidden * mask).sum(axis=1) / np.maximum(mask.sum(axis=1), 1e-12)
        if self._normalize:
            norms = np.linalg.norm(pooled, axis=1, keepdims=True)
            pooled = pooled / np.maximum(norms, 1e-12)
        return [vec.tolist() for vec in pooled.astype("float32")]

    def _prepare_cache(self) -> Path:
        """Ensure an ONNX (optionally int8) model + tokenizer are cached locally."""
        root = self._cache_root()
        slug = _slug_model(self._model_name)
        variant = "int8" if self._quantize else "fp32"
        target = root / slug / variant

        if _has_onnx_model(target):
            return target

        try:
            from optimum.onnxruntime import ORTModelForFeatureExtraction
            from transformers import AutoTokenizer
        except ImportError as exc:
            raise ImportError(
                f"OnnxEmbedder needs optimum + onnxruntime to export/quantize. "
                f"Install with: pip install '{PARSIMONY_STANDARD_ONNX_PACKAGE}'"
            ) from exc

        fp32_dir = root / slug / "fp32"
        if not _has_onnx_model(fp32_dir):
            logger.info("ONNX export: %s → %s", self._model_name, fp32_dir)
            started = time.monotonic()
            fp32_dir.mkdir(parents=True, exist_ok=True)
            ort_model = ORTModelForFeatureExtraction.from_pretrained(self._model_name, export=True)
            ort_model.save_pretrained(fp32_dir)
            AutoTokenizer.from_pretrained(self._model_name).save_pretrained(fp32_dir)
            logger.info("ONNX export complete: %s in %.1fs", fp32_dir, time.monotonic() - started)

        if not self._quantize:
            return fp32_dir

        logger.info("ONNX quantize (int8, avx2): %s → %s", self._model_name, target)
        quantize_started = time.monotonic()
        target.mkdir(parents=True, exist_ok=True)
        from optimum.onnxruntime import ORTQuantizer
        from optimum.onnxruntime.configuration import AutoQuantizationConfig

        quantizer = ORTQuantizer.from_pretrained(fp32_dir)
        # avx2 preset: correct for AVX2+AVX_VNNI CPUs without AVX-512. Dynamic
        # quantization — no calibration dataset needed, tiny cosine drift in
        # practice for encoder-only models.
        qconfig = AutoQuantizationConfig.avx2(is_static=False, per_channel=False)
        quantizer.quantize(save_dir=target, quantization_config=qconfig)
        # Copy tokenizer alongside the quantized model.
        AutoTokenizer.from_pretrained(fp32_dir).save_pretrained(target)
        logger.info("ONNX quantize complete: %s in %.1fs", target, time.monotonic() - quantize_started)
        return target

    def _pick_onnx_file(self, model_dir: Path) -> Path:
        # Quantized exports produce model_quantized.onnx; fp32 exports
        # produce model.onnx. Prefer the quantized artifact when present.
        quantized = model_dir / "model_quantized.onnx"
        if quantized.exists():
            return quantized
        plain = model_dir / "model.onnx"
        if plain.exists():
            return plain
        candidates = list(model_dir.glob("*.onnx"))
        if not candidates:
            raise FileNotFoundError(f"No ONNX model found in {model_dir}")
        return candidates[0]

    def _cache_root(self) -> Path:
        if self._cache_dir_override is not None:
            return self._cache_dir_override
        from parsimony import cache

        return cache.models_dir()


def _slug_model(model_name: str) -> str:
    """Filesystem-safe slug for a HF model id (e.g. ``sentence-transformers/all-MiniLM-L6-v2``)."""
    safe = model_name.replace("/", "__").replace(":", "_")
    # Guard against pathological names with an 8-char hash suffix.
    digest = hashlib.sha1(model_name.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    return f"{safe}-{digest}"


def _has_onnx_model(target: Path) -> bool:
    if not target.is_dir():
        return False
    if not any(target.glob("*.onnx")):
        return False
    # Tokenizer files live alongside; a fully-prepared cache has both.
    has_tokenizer = any((target / name).exists() for name in ("tokenizer.json", "tokenizer_config.json"))
    return has_tokenizer


class LiteLLMEmbeddingProvider:
    """Hosted embeddings via the `litellm`_ unified API.

    Identity (model + dimension) is supplied at construction time; this
    class does not introspect the remote endpoint. Outputs are L2-normalized
    so they round-trip cleanly with the inner-product FAISS index.

    .. _litellm: https://github.com/BerriAI/litellm
    """

    def __init__(self, *, model: str, dimension: int, batch_size: int = _LITELLM_BATCH_SIZE) -> None:
        self._model = model
        self._dimension = dimension
        self._batch_size = batch_size

    @property
    def dimension(self) -> int:
        return self._dimension

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(
            model=self._model,
            dim=self._dimension,
            normalize=True,
            package=PARSIMONY_LITELLM_PACKAGE,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        for start in range(0, len(texts), self._batch_size):
            chunk = texts[start : start + self._batch_size]
            response = self._embed(chunk, task_type="RETRIEVAL_DOCUMENT")
            vectors = _validate_litellm_response(response, len(chunk), self._dimension)
            out.extend(_l2_normalize(v) for v in vectors)
        return out

    def embed_query(self, query: str) -> list[float]:
        response = self._embed([query], task_type="RETRIEVAL_QUERY")
        vectors = _validate_litellm_response(response, 1, self._dimension)
        return _l2_normalize(vectors[0])

    def _embed(self, input_texts: list[str], *, task_type: str) -> Any:
        try:
            import litellm
        except ImportError as exc:
            raise ImportError(
                f"LiteLLMEmbeddingProvider requires litellm. Install with: pip install '{PARSIMONY_LITELLM_PACKAGE}'"
            ) from exc

        kwargs: dict[str, Any] = {"model": self._model, "input": input_texts, "task_type": task_type}
        t0 = time.monotonic()
        try:
            try:
                response = litellm.embedding(**kwargs, dimensions=self._dimension)
            except TypeError:
                response = litellm.embedding(**kwargs, output_dimensionality=self._dimension)
        except Exception as exc:
            logger.error(
                "Embedding call failed: model=%s, batch_size=%d, error=%s",
                self._model,
                len(input_texts),
                exc,
            )
            raise RuntimeError(f"Embedding call failed for model {self._model!r}: {exc}") from exc
        logger.info(
            "Embedding call: model=%s, batch_size=%d, duration=%.2fs",
            self._model,
            len(input_texts),
            time.monotonic() - t0,
        )
        return response


def _l2_normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm <= 0:
        return vec
    return [x / norm for x in vec]


def _validate_litellm_response(response: Any, expected_count: int, expected_dim: int) -> list[list[float]]:
    items = response["data"] if isinstance(response, dict) else getattr(response, "data", None)
    if items is None:
        raise ValueError(f"Embedding response missing 'data'; got {type(response).__name__}")
    if len(items) != expected_count:
        raise ValueError(f"Embedding response returned {len(items)} items, expected {expected_count}")
    out: list[list[float]] = []
    for i, item in enumerate(items):
        if hasattr(item, "embedding"):
            vec = item.embedding
        elif isinstance(item, dict) and "embedding" in item:
            vec = item["embedding"]
        else:
            raise ValueError(f"Embedding item {i} missing 'embedding' field")
        if len(vec) != expected_dim:
            raise ValueError(f"Embedding item {i} has dimension {len(vec)}, expected {expected_dim}")
        out.append(list(vec))
    return out


__all__ = [
    "DEFAULT_MODEL",
    "EmbedderInfo",
    "EmbeddingProvider",
    "LiteLLMEmbeddingProvider",
    "OnnxEmbedder",
    "SentenceTransformerEmbedder",
]
