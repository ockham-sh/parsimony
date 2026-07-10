"""Can Juraj-style votes complement the shipped design?

Keep the coverage band (the measured 0.2-MRR advantage), swap what orders
rows INSIDE a band:

  cov_nsum   shipped: sum of per-field TOP-NORMALIZED fused scores
  cov_jsum   sum of raw cos(query, "Concept: Value") — absolute, no
             per-field normalization (no manufactured confidence)
  cov_jdual  sum of max(cos to bare doc, cos to prefixed doc) per facet
  cov_both   nsum + jdual (both bounded by n_fields; parameterless sum)

Same battery, same targets, sentinels reported separately.
"""

from __future__ import annotations

import time

from full_eval import CASES, flow_data, target_rank, value_tables
from juraj_probe import juraj_docs, unit

from parsimony.catalog.indexes import consumed_value_tokens
from parsimony.indexes import tokenize

CONFIGS = ["cov_nsum", "cov_jsum", "cov_jdual", "cov_both"]


def rankings(catalog, data, docs, query: str) -> dict[str, list[str]]:  # noqa: ANN001
    fields = data.fields
    tables = value_tables(catalog, fields, query)
    tops = {f: (max(t.values()) if t else 0.0) for f, t in tables.items()}
    consumed = {f: consumed_value_tokens(catalog.index_for(f), query) for f in fields}
    qtokens = frozenset(tokenize(query))
    qnorm = query.strip().casefold()
    qv = unit(docs["emb"].embed_texts([query]))[0]
    psim = {p: float(v @ qv) for p, v in docs["prefixed"].items()}
    bsim = {p: float(v @ qv) for p, v in docs["bare"].items()}

    def grade(code: str) -> tuple[float, float, float, float]:
        lab = data.labels[code]
        union: set[str] = set()
        equal = False
        for f in fields:
            vt = consumed[f].get(lab[f])
            if vt is not None:
                union.update(vt)
                if lab[f].strip().casefold() == qnorm:
                    equal = True
        cov = 1.0 if equal else ((len(union & qtokens) / len(qtokens)) if qtokens else 0.0)
        nsum = sum(tables[f].get(lab[f], 0.0) / tops[f] for f in fields if tops[f] > 0)
        keys = [(f, lab[f]) for f in docs["fields"] if lab[f].strip()]
        jsum = sum(psim[k] for k in keys)
        jdual = sum(max(psim[k], bsim[k]) for k in keys)
        return cov, nsum, jsum, jdual

    g = {c: grade(c) for c in data.labels}
    return {
        "cov_nsum": sorted(g, key=lambda c: (-g[c][0], -g[c][1], c)),
        "cov_jsum": sorted(g, key=lambda c: (-g[c][0], -g[c][2], c)),
        "cov_jdual": sorted(g, key=lambda c: (-g[c][0], -g[c][3], c)),
        "cov_both": sorted(g, key=lambda c: (-g[c][0], -(g[c][1] + g[c][3]), c)),
    }


def main() -> None:
    per_class: dict[str, dict[str, list[int | None]]] = {}
    sentinel_rows: list[tuple[str, dict[str, int | None], str]] = []
    print(f"{'case':<58} " + " ".join(f"{c:>10}" for c in CONFIGS))
    for case in CASES:
        catalog, data = flow_data(case.agency, case.flow)
        docs = juraj_docs((case.agency, case.flow), catalog, data)
        t0 = time.perf_counter()
        ranked = rankings(catalog, data, docs, case.query)
        ranks = {cfg: target_rank(ranked[cfg], data, case.want) for cfg in CONFIGS}
        label = f"{case.klass:<4}{case.agency}/{case.flow}: {case.query[:36]}"
        if case.sentinel:
            sentinel_rows.append((label, ranks, case.note))
            continue
        bucket = per_class.setdefault(case.klass, {cfg: [] for cfg in CONFIGS})
        for cfg in CONFIGS:
            bucket[cfg].append(ranks[cfg])
        print(
            f"{label:<58} "
            + " ".join(f"{(ranks[c] if ranks[c] else '—')!s:>10}" for c in CONFIGS)
            + f"   [{time.perf_counter() - t0:.1f}s]"
        )

    def mrr(vals: list[int | None]) -> float:
        return sum((1.0 / v if v else 0.0) for v in vals) / max(len(vals), 1)

    def hit10(vals: list[int | None]) -> str:
        n = sum(1 for v in vals if v and v <= 10)
        return f"{n}/{len(vals)}"

    print("\n=== per-class MRR ===")
    hdr = f"{'class':<8} " + " ".join(f"{c:>10}" for c in CONFIGS)
    print(hdr)
    totals: dict[str, list[int | None]] = {cfg: [] for cfg in CONFIGS}
    for klass in sorted(per_class):
        vals = per_class[klass]
        for cfg in CONFIGS:
            totals[cfg].extend(vals[cfg])
        print(f"{klass:<8} " + " ".join(f"{mrr(vals[c]):>10.3f}" for c in CONFIGS))
    print("-" * len(hdr))
    print(f"{'ALL MRR':<8} " + " ".join(f"{mrr(totals[c]):>10.3f}" for c in CONFIGS))
    print(f"{'hit@10':<8} " + " ".join(f"{hit10(totals[c]):>10}" for c in CONFIGS))

    print("\n=== sentinels (documented limits — lower is better) ===")
    for label, ranks, note in sentinel_rows:
        print(f"{label:<58} " + " ".join(f"{(ranks[c] if ranks[c] else '—')!s:>10}" for c in CONFIGS) + f"   {note}")


if __name__ == "__main__":
    main()
