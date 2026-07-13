"""Juraj's idea 1, simulated on the 37-case battery.

The proposal: split each series' structured description into per-facet
documents that KEEP the concept label ("Reference area: Germany"), embed each
document, score the query against every document by pure embedding
similarity, rank rows by aggregating their documents' similarities. No
lexical containment, no coverage. (Idea 2 — the fine-tuned projection
embedder — cannot be simulated; this is idea 1 with the stock encoder, the
SAME model our VectorIndex ships, so the comparison isolates architecture.)

Columns:
  cov_nsum    shipped (coverage, norm_sum) — reference
  juraj_max   row = max over its facet docs of cos(query, "Concept: Value")
  juraj_sum   row = sum over its facet docs (agreement accumulates)
  jbare_sum   sum over BARE value docs ("Germany") — isolates the prefix
"""

from __future__ import annotations

import time

import numpy as np
from full_eval import CASES, flow_data, rank_case, target_rank

from parsimony.catalog.indexes import collect_vector_indexes

CONCEPT = {
    "freq": "Frequency",
    "FREQ": "Frequency",
    "geo": "Reference area",
    "REF_AREA": "Reference area",
    "s_adj": "Seasonal adjustment",
    "age": "Age",
    "sex": "Sex",
    "unit": "Unit of measure",
    "CURRENCY": "Currency",
    "CURRENCY_DENOM": "Currency denominator",
    "EXR_TYPE": "Exchange rate type",
    "EXR_SUFFIX": "Series variation",
    "na_item": "National accounts item",
    "coicop": "COICOP item",
    "BOP_ITEM": "BOP item",
    "bop_item": "BOP item",
    "stk_flow": "Stock or flow",
    "partner": "Partner area",
    "ICP_ITEM": "ICP item",
    "ICP_SUFFIX": "Series variation",
    "DATA_TYPE_FM": "Data type",
}

JCONFIGS = ["cov_nsum", "juraj_max", "juraj_sum", "jbare_sum"]


def concept_name(field: str) -> str:
    base = field[:-6] if field.endswith("_label") else field
    return CONCEPT.get(base, base.replace("_", " ").capitalize())


def unit(vecs) -> np.ndarray:  # noqa: ANN001
    m = np.asarray(vecs, dtype=np.float32)
    n = np.linalg.norm(m, axis=-1, keepdims=True)
    return m / np.maximum(n, 1e-12)


_DOCS: dict[tuple[str, str], dict] = {}


def juraj_docs(key, catalog, data):  # noqa: ANN001
    """Embed the per-facet documents for one flow (distinct values only)."""
    if key in _DOCS:
        return _DOCS[key]
    emb = None
    for f in data.fields:
        vis = collect_vector_indexes(catalog.index_for(f))
        if vis:
            emb = vis[0]._require_embedder()
            break
    assert emb is not None, f"no vector index in {key}"
    facet_fields = [f for f in data.fields if f != "title"]
    pairs = sorted({(f, lab[f]) for lab in data.labels.values() for f in facet_fields if lab[f].strip()})
    prefixed = [f"{concept_name(f)}: {v}" for f, v in pairs]
    bare = [v for _, v in pairs]
    pvecs = unit(emb.embed_texts(prefixed))
    bvecs = unit(emb.embed_texts(bare))
    out = {
        "emb": emb,
        "fields": facet_fields,
        "prefixed": {p: pvecs[i] for i, p in enumerate(pairs)},
        "bare": {p: bvecs[i] for i, p in enumerate(pairs)},
    }
    _DOCS[key] = out
    return out


def juraj_rankings(docs, data, query: str) -> dict[str, list[str]]:  # noqa: ANN001
    qv = unit(docs["emb"].embed_texts([query]))[0]
    psim = {p: float(v @ qv) for p, v in docs["prefixed"].items()}
    bsim = {p: float(v @ qv) for p, v in docs["bare"].items()}

    def row_scores(code: str) -> tuple[float, float, float]:
        lab = data.labels[code]
        keys = [(f, lab[f]) for f in docs["fields"] if lab[f].strip()]
        ps = [psim[k] for k in keys]
        bs = [bsim[k] for k in keys]
        return (max(ps, default=0.0), sum(ps), sum(bs))

    scored = {c: row_scores(c) for c in data.labels}
    return {
        "juraj_max": sorted(scored, key=lambda c: (-scored[c][0], c)),
        "juraj_sum": sorted(scored, key=lambda c: (-scored[c][1], c)),
        "jbare_sum": sorted(scored, key=lambda c: (-scored[c][2], c)),
    }


def main() -> None:
    per_class: dict[str, dict[str, list[int | None]]] = {}
    sentinel_rows: list[tuple[str, dict[str, int | None], str]] = []
    print(f"{'case':<58} " + " ".join(f"{c:>10}" for c in JCONFIGS))
    for case in CASES:
        catalog, data = flow_data(case.agency, case.flow)
        docs = juraj_docs((case.agency, case.flow), catalog, data)
        t0 = time.perf_counter()
        rankings = juraj_rankings(docs, data, case.query)
        rankings["cov_nsum"] = rank_case(catalog, data, case.query)["cov_nsum"]
        ranks = {cfg: target_rank(rankings[cfg], data, case.want) for cfg in JCONFIGS}
        label = f"{case.klass:<4}{case.agency}/{case.flow}: {case.query[:36]}"
        if case.sentinel:
            sentinel_rows.append((label, ranks, case.note))
            continue
        bucket = per_class.setdefault(case.klass, {cfg: [] for cfg in JCONFIGS})
        for cfg in JCONFIGS:
            bucket[cfg].append(ranks[cfg])
        print(
            f"{label:<58} "
            + " ".join(f"{(ranks[c] if ranks[c] else '—')!s:>10}" for c in JCONFIGS)
            + f"   [{time.perf_counter() - t0:.1f}s]"
        )

    def mrr(vals: list[int | None]) -> float:
        return sum((1.0 / v if v else 0.0) for v in vals) / max(len(vals), 1)

    def hit10(vals: list[int | None]) -> str:
        n = sum(1 for v in vals if v and v <= 10)
        return f"{n}/{len(vals)}"

    print("\n=== per-class MRR ===")
    hdr = f"{'class':<8} " + " ".join(f"{c:>10}" for c in JCONFIGS)
    print(hdr)
    totals: dict[str, list[int | None]] = {cfg: [] for cfg in JCONFIGS}
    for klass in sorted(per_class):
        vals = per_class[klass]
        for cfg in JCONFIGS:
            totals[cfg].extend(vals[cfg])
        print(f"{klass:<8} " + " ".join(f"{mrr(vals[c]):>10.3f}" for c in JCONFIGS))
    print("-" * len(hdr))
    print(f"{'ALL MRR':<8} " + " ".join(f"{mrr(totals[c]):>10.3f}" for c in JCONFIGS))
    print(f"{'hit@10':<8} " + " ".join(f"{hit10(totals[c]):>10}" for c in JCONFIGS))

    print("\n=== sentinels (documented limits) ===")
    for label, ranks, note in sentinel_rows:
        print(f"{label:<58} " + " ".join(f"{(ranks[c] if ranks[c] else '—')!s:>10}" for c in JCONFIGS) + f"   {note}")


if __name__ == "__main__":
    main()
