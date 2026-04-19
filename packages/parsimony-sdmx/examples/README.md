# parsimony-sdmx examples

End-to-end demos of the prebuilt-catalog workflow against live SDMX endpoints.

## Quickstart: fetch one dataset

```bash
uv run python quickstart_sdmx.py
```

Pulls a small slice of an ECB dataset and prints the resulting DataFrame. Good
for verifying your environment can reach the SDMX endpoint at all.

## Build a publishable catalog

```bash
# yield curve catalog
uv run python build_sdmx_catalog.py ECB-YC --out ./build/ecb-yc

# exchange-rate catalog
uv run python build_sdmx_catalog.py ECB-EXR --out ./build/ecb-exr

# also push to the Hub (set HF_TOKEN first)
uv run python build_sdmx_catalog.py ECB-YC \
  --out ./build/ecb-yc \
  --push hf://ockham/ecb-yc-catalog
```

What it does:

1. Calls `sdmx_series_keys` to enumerate every series in the dataset.
2. Embeds each series title with `BAAI/bge-small-en-v1.5` (override with `--model`).
3. Writes a three-file snapshot (`meta.json`, `entries.parquet`, `embeddings.faiss`)
   to `--out`.
4. Optionally publishes the snapshot to a Hugging Face Hub dataset repository.

## Use a published catalog

Once the catalog is on the Hub, anyone can pull it without re-embedding:

```python
from parsimony import Catalog

cat = await Catalog.from_url("hf://ockham/ecb-yc-catalog")
matches = await cat.search("euro area 10y government bond yield", limit=10)
for m in matches:
    print(m.code, m.similarity, m.title)
```

`Catalog.from_url` reads `meta.json` and constructs a
`SentenceTransformerEmbedder` for the recorded model
(`BAAI/bge-small-en-v1.5` by default). Index-time and query-time embeddings
are guaranteed to match — the embedder identity is owned by the catalog.

## Required packages

```bash
pip install 'parsimony-core[standard]' parsimony-sdmx
```
