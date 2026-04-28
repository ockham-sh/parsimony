# Catalog partitioning — post-refactor design

**Status:** Open design question. Tackle after the refactor lands. Surfaced here
so the refactor does not accidentally prejudge the answer.

---

## Context

- **Product shift:** the agent-facing surface is moving toward a "workspace"
  model where a heavy one-time setup (download a provider's whole catalog tree
  once, iterate locally) is acceptable. This relaxes the previous "first query
  must be sub-second" constraint that drove fine-grained per-namespace
  fetching.
- **Scale:** SDMX is the bounding case. ESTAT alone has ~8k dataflows and an
  estimated ~20M+ series. Across all agencies parsimony publishes, the ceiling
  is plausibly 100M+ series.
- **Single-catalog ceiling (approx., 384-dim float32):**
  - 20M × 384 × 4 B ≈ **30 GB** unquantized FAISS
  - with int8 quantization ≈ **8 GB**
  - with PQ (16 sub-quantizers × 256 centroids) ≈ **1–2 GB**, accuracy loss ~5–15%

## The two-step problem

Current SDMX design has agents do:

1. Search the *dataset* catalog (titles / descriptions of dataflows).
2. Once a dataset is picked, search the *series* catalog scoped to that dataset.

This is fragile: agents don't know a priori which series live in which dataset,
so step 1's relevance is guessed from dataflow descriptions. When the description
is terse or aliased, step 1 misdirects step 2.

## Partitioning options

| Scheme | Catalogs / provider | Per-catalog size | Search quality | Publish cost | Agent UX |
|---|---|---|---|---|---|
| **Per-dataflow** (status quo) | ~10k (SDMX) | 1–50 MB | best (focused) | O(10k) builds | 2-step, agent guesses |
| **Per-agency** | ~20 | 100 MB – 8 GB | good | O(20) builds | 1-step per agency |
| **Thematic groupings** | ~5–50 (curated) | 50 MB – 1 GB | good | O(50) builds | 1-step per theme |
| **Single per-provider, quantized** | 1 | 1–8 GB | degraded (5–15% recall loss) | O(1) | 1-step, broadest |

## What the refactor prejudges (and doesn't)

**Does not prejudge:**
- Partition granularity. `CATALOGS = [...]` accepts any scheme — 1 catalog, 20,
  10k, or a dynamic factory yielding whatever count.
- Single-step vs multi-step search. An agent can query one catalog or compose
  multiple. Nothing in the kernel insists on either.
- Whether dataflow discovery lives in its own catalog or as metadata columns on
  series entries.

**Does prejudge:**
- Canonical format is Parquet + FAISS + BM25. Partitioning happens by publishing
  multiple catalogs, not by internal partitioning within one catalog.
- One HF dataset repo per provider. Sub-dirs within the repo are the namespaces.

## Options to evaluate after refactor

### Option A — per-agency + metadata-rich entries
- Publish one catalog per SDMX agency (~20 catalogs, e.g. `sdmx_ecb`, `sdmx_estat`).
- Each entry = one series, with dataflow id / dimensions / description in METADATA.
- Agents search a single per-agency catalog; no dataflow dance.
- **Risk:** ESTAT catalog ~8 GB quantized. Download is tolerable in workspace
  mode, but FAISS memory footprint at agent-runtime may need quantized indexes
  or on-disk search (FAISS `IndexIVFPQ` / `OnDiskInvertedLists`).

### Option B — per-dataflow + discoverability layer
- Per-dataflow catalogs (~10k).
- Plus one "dataflow index" catalog per provider (~1 per provider) that enumerates
  dataflows with rich descriptions for step-1 discovery.
- Agent: search discovery catalog → load one dataflow catalog → search it.
- **Risk:** N+1 downloads for broad queries; the two-step problem persists unless
  the discovery catalog is very well-curated.

### Option C — thematic groupings
- Plugin author curates ~20–50 theme catalogs (monetary, GDP, trade, labor, …)
  aggregating related dataflows across agencies.
- **Risk:** requires per-provider curation; not automated.

### Option D — quantized single catalog
- One 20M-row catalog per provider, aggressive quantization (IVF-PQ).
- **Risk:** measurable accuracy loss on semantic search. Need to A/B against
  per-agency to know if the loss is tolerable for finance use cases.

## Decision triggers

- If IVF-PQ quality measured against per-agency is within ~5% recall@10 →
  Option D (simplest).
- If agent discovery UX is the binding constraint → Option A or C.
- If workspace download size is the binding constraint → Option B.
- If curation cost is unacceptable → not Option C.

## Open questions

1. What's the actual recall@10 degradation from IVF-PQ on a 20M-series ESTAT
   corpus with `BAAI/bge-small-en-v1.5` embeddings? (Measure before deciding.)
2. Does `FAISS IndexIVFPQ` with `on_disk_index=True` give us memory-bounded
   search at 20M-scale on a consumer laptop? (Workspace deployment target.)
3. Does the "dataflow index" catalog (Option B) need richer metadata than the
   current `sdmx_datasets` enumerator produces — e.g. sample series titles,
   data availability ranges, unit summaries? (Agent UX experiment.)
4. If Option A wins: is the right granularity "per-agency" or "per-(agency,
   frequency)" (monthly ECB vs daily ECB)? Affects catalog count by ~4x.

## Out of scope for the refactor

This note is a TODO. The refactor only needs to support all four options without
prejudging which wins — the `CATALOGS` + optional `RESOLVE_CATALOG` model does.
Revisit with real benchmark numbers after the refactor lands.
