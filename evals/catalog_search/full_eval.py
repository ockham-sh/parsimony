"""FULL ranking eval: query taxonomy x pipeline configurations, on real catalogs.

QUERY TAXONOMY (classes):
  Q1  exact-label            query IS one field value (incl. case variants)
  Q2  verbose-subset         query fully contains value(s) + noise words
  Q3  multi-facet            several dimensions named, target = intersection
  Q4  aggregate-vs-children  parent label exact, children repeat the phrase
  Q5  false-friend           near-duplicate labels (ANR/AVR class)
  Q6  semantic-gap           synonym concept, zero token overlap
  Q7  facet+gap              consumable facet + semantic-gap concept (user case)
  Q8  nondiscriminating      facet consumed by every row in the flow
  Q9  facet-split            facet divides the flow (annual vs quarterly)
  Q10 unsatisfiable-facet    facet no row has; rest must still rank
  Q11 natural-language       stopword-heavy full sentences
  Q12 typo                   misspelled concept words
  Q13 code-like              user puts codes in free text (USD, B1GQ)
  Q14 prefixed-label         value findable only with its prefix ("HICP - Energy")
  Q15 crossfield-dup         a token consumable in several fields ("euro")
  Q16 one-word               single ambiguous concept ("gdp", "inflation")
  Q17 measure-word           average / end-of-period / spot disambiguation

PIPELINE CONFIGS (tier, score) — tier sorts first, score breaks ties:
  title_only   no tier; title fuzzy only                  (pre-#48 behavior)
  no_tier      no tier; raw max across all surface fields
  sentinel     exact string-equal pin; raw max            (intermediate #69)
  cov_pool     coverage; raw max over TOP-5-value tables  (shipped connector)
  cov_rawmax   coverage; raw max over full tables         (shipped core deep)
  cov_nsum     coverage; sum of per-field top-normalized  (candidate)
  covidf_nsum  IDF-weighted coverage; norm_sum            (common-words-weigh-less)

Rows ranked = the ENTIRE flow for every config (no pool-limit artifacts).
Target = metadata predicate (never key regexes). Public catalog data only.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from dataclasses import field as dfield

from flow_loader import load_flow

from parsimony.catalog.indexes import (
    _fused_value_scores,
    _value_texts_from_index,
    consumed_value_tokens,
    embed_query_vectors,
)
from parsimony.indexes import tokenize

EQ, HAS, NE = "eq", "contains", "ne"


@dataclass(frozen=True)
class Case:
    klass: str
    agency: str
    flow: str
    query: str
    want: tuple[tuple[str, str, str], ...]  # (field, op, value) AND-ed
    note: str = ""
    sentinel: bool = False  # documented design limit: tracked, not gated at rank 1


CASES = [
    # Q1 exact-label
    Case("Q1", "ECB", "BOP", "current account", (("BOP_ITEM_label", EQ, "current account"),)),
    Case("Q1", "ECB", "BOP", "CURRENT ACCOUNT", (("BOP_ITEM_label", EQ, "current account"),), "case variant"),
    Case(
        "Q1",
        "ESTAT",
        "prc_hicp_aind",
        "annual average rate of change",
        (("unit_label", EQ, "annual average rate of change"),),
    ),
    Case("Q1", "ECB", "EXR", "swiss franc", (("CURRENCY_label", EQ, "swiss franc"),)),
    # Q2 verbose-subset
    Case(
        "Q2",
        "ECB",
        "BOP",
        "current account balance euro area quarterly",
        (("BOP_ITEM_label", EQ, "current account"), ("FREQ_label", EQ, "quarterly")),
    ),
    Case(
        "Q2",
        "ECB",
        "EXR",
        "swiss franc euro exchange rate monthly average",
        (("CURRENCY_label", EQ, "swiss franc"), ("FREQ_label", EQ, "monthly"), ("EXR_SUFFIX_label", EQ, "average")),
    ),
    Case(
        "Q2",
        "ESTAT",
        "prc_hicp_aind",
        "annual average rate of change energy germany",
        (
            ("unit_label", EQ, "annual average rate of change"),
            ("coicop_label", EQ, "energy"),
            ("geo_label", EQ, "germany"),
        ),
    ),
    Case(
        "Q2",
        "ESTAT",
        "namq_10_gdp",
        "gross domestic product at market prices germany quarterly",
        (("na_item_label", EQ, "gross domestic product at market prices"), ("geo_label", EQ, "germany")),
    ),
    # Q3 multi-facet
    Case(
        "Q3",
        "ESTAT",
        "une_rt_m",
        "unemployment germany total monthly",
        (("geo_label", EQ, "germany"), ("age_label", EQ, "total"), ("sex_label", EQ, "total")),
    ),
    Case(
        "Q3",
        "ESTAT",
        "bop_eu6_q",
        "current account balance quarterly",
        (("bop_item_label", EQ, "current account"), ("stk_flow_label", EQ, "balance"), ("freq_label", EQ, "quarterly")),
    ),
    # Q4 aggregate-vs-children
    Case("Q4", "ESTAT", "bop_eu6_q", "current account", (("bop_item_label", EQ, "current account"),)),
    Case("Q4", "ESTAT", "teibp050", "current account", (("bop_item_label", EQ, "current account"),)),
    # Q5 false-friend
    Case("Q5", "ECB", "ICP", "annual rate of change", (("ICP_SUFFIX_label", EQ, "annual rate of change"),)),
    Case(
        "Q5",
        "ECB",
        "ICP",
        "annual average rate of change",
        (("ICP_SUFFIX_label", EQ, "annual average rate of change"),),
    ),
    Case("Q5", "ESTAT", "prc_hicp_aind", "annual average index", (("unit_label", EQ, "annual average index"),)),
    # Q6 semantic-gap
    Case("Q6", "ESTAT", "une_rt_m", "unemployment young people", (("age_label", EQ, "less than 25 years"),)),
    Case(
        "Q6",
        "ESTAT",
        "namq_10_gdp",
        "economic output france",
        (("na_item_label", EQ, "gross domestic product at market prices"), ("geo_label", EQ, "france")),
    ),
    # Q7 facet+gap (the user's adversarial class)
    Case(
        "Q7", "ESTAT", "une_rt_m", "monthly unemployment rate young people", (("age_label", EQ, "less than 25 years"),)
    ),
    Case(
        "Q7",
        "ESTAT",
        "une_rt_m",
        "monthly youth unemployment germany",
        (("age_label", EQ, "less than 25 years"), ("geo_label", EQ, "germany")),
    ),
    # Q8 nondiscriminating facet
    Case(
        "Q8",
        "ESTAT",
        "une_rt_m",
        "monthly unemployment germany",
        (("geo_label", EQ, "germany"), ("age_label", EQ, "total"), ("sex_label", EQ, "total")),
    ),
    Case(
        "Q8",
        "ESTAT",
        "prc_hicp_manr",
        "monthly inflation germany",
        (("geo_label", EQ, "germany"), ("coicop_label", EQ, "all-items hicp")),
    ),
    # Q9 facet-split (incl. the user's A/B case shape)
    Case(
        "Q9",
        "ESTAT",
        "bop_eu6_q",
        "quarterly services",
        (("bop_item_label", EQ, "services"), ("freq_label", EQ, "quarterly")),
    ),
    Case(
        "Q9",
        "ESTAT",
        "bop_eu6_q",
        "annual services",
        (("bop_item_label", EQ, "services"), ("freq_label", EQ, "annual")),
    ),
    # Q10 unsatisfiable facet
    Case(
        "Q10",
        "ESTAT",
        "une_rt_m",
        "quarterly unemployment young people",
        (("age_label", EQ, "less than 25 years"),),
        "flow is monthly-only",
    ),
    # Q11 natural language
    Case(
        "Q11",
        "ESTAT",
        "une_rt_m",
        "what is the unemployment rate for young people in germany",
        (("age_label", EQ, "less than 25 years"), ("geo_label", EQ, "germany")),
    ),
    Case(
        "Q11",
        "ECB",
        "EXR",
        "how much is the us dollar worth against the euro",
        (("CURRENCY_label", EQ, "us dollar"), ("CURRENCY_DENOM_label", EQ, "euro")),
    ),
    # Q12 typo
    Case("Q12", "ECB", "BOP", "curent account", (("BOP_ITEM_label", EQ, "current account"),)),
    Case("Q12", "ECB", "EXR", "swiss frank euro", (("CURRENCY_label", EQ, "swiss franc"),)),
    # Q13 code-like
    Case(
        "Q13",
        "ECB",
        "EXR",
        "USD EUR exchange rate",
        (("CURRENCY_label", EQ, "us dollar"), ("CURRENCY_DENOM_label", EQ, "euro")),
    ),
    Case(
        "Q13",
        "ESTAT",
        "namq_10_gdp",
        "B1GQ germany",
        (("na_item_label", EQ, "gross domestic product at market prices"), ("geo_label", EQ, "germany")),
    ),
    # Q14 prefixed-label
    Case(
        "Q14",
        "ECB",
        "ICP",
        "hicp energy germany monthly",
        (("ICP_ITEM_label", EQ, "hicp - energy"), ("REF_AREA_label", EQ, "germany"), ("FREQ_label", EQ, "monthly")),
    ),
    # Q15 crossfield-dup token
    Case("Q15", "ECB", "BOP", "current account euro", (("BOP_ITEM_label", EQ, "current account"),)),
    # Q16 one-word
    Case("Q16", "ESTAT", "namq_10_gdp", "gdp", (("na_item_label", EQ, "gross domestic product at market prices"),)),
    Case("Q16", "ECB", "ICP", "inflation", (("ICP_ITEM_label", HAS, "overall index"),)),
    # Q17 measure-word
    Case(
        "Q17",
        "ECB",
        "EXR",
        "us dollar euro end of period",
        (("CURRENCY_label", EQ, "us dollar"), ("EXR_SUFFIX_label", EQ, "end-of-period")),
    ),
    Case(
        "Q17",
        "ECB",
        "YC",
        "yield curve spot rate 10-year maturity",
        (("DATA_TYPE_FM_label", EQ, "yield curve spot rate, 10-year maturity"),),
    ),
    Case(
        "Q17",
        "ECB",
        "YC",
        "10 year government bond yield",
        (("DATA_TYPE_FM_label", EQ, "yield curve spot rate, 10-year maturity"),),
    ),
    # ---- SENTINELS: documented design limits (see design_limits_probe.py, q18_probe.py) ----
    Case(
        "Q19",
        "ESTAT",
        "une_rt_m",
        "unemployment excluding young people",
        (("age_label", NE, "less than 25 years"),),
        "negation ignored by bag-of-words + bi-encoder",
        sentinel=True,
    ),
    Case(
        "Q20",
        "ESTAT",
        "une_rt_m",
        "unemployment germny young people",
        (("geo_label", EQ, "germany"), ("age_label", EQ, "less than 25 years")),
        "typo'd short facet gives the vector nothing to grip",
        sentinel=True,
    ),
    Case(
        "Q21",
        "ESTAT",
        "namq_10_gdp",
        "gross domestic product 2015 prices germany",
        (("geo_label", EQ, "germany"), ("unit_label", HAS, "2015"), ("na_item_label", HAS, "gross domestic")),
        "parenthetical-number units are fuzzy-only",
        sentinel=True,
    ),
    Case(
        "Q22",
        "ECB",
        "ICP",
        "annual index germany",
        (("ICP_ITEM_label", HAS, "overall index"), ("REF_AREA_label", EQ, "germany")),
        "facet-only query: full-coverage band has no concept signal",
        sentinel=True,
    ),
]

CONFIGS = ["title_only", "auto_filter", "cov_pool", "cov_nsum", "cov1_nsum", "nsum_only"]
#: Blend probes: rank by nsum + lambda*cov instead of the lexicographic tier.
#: Answers "why not just boost exact/consumed matches?" with measurements.
BLEND_LAMBDAS = [0.5, 1.0, 2.0, 5.0, 10.0]
CONFIGS += [f"blend_l{lam:g}" for lam in BLEND_LAMBDAS]


@dataclass
class FlowData:
    fields: list[str]
    labels: dict[str, dict[str, str]]  # code -> {field: label} (title included)
    idf: dict[str, float] = dfield(default_factory=dict)


_FLOWS: dict[tuple[str, str], tuple[object, FlowData]] = {}


def flow_data(agency: str, flow: str) -> tuple[object, FlowData]:
    key = (agency, flow)
    if key in _FLOWS:
        return _FLOWS[key]
    catalog, fields = load_flow(agency, flow)
    code_col = catalog._backend_config.code_column
    cols = list(dict.fromkeys([code_col, *[catalog._coverage_column(f) for f in fields]]))
    labels: dict[str, dict[str, str]] = {}
    df: dict[str, int] = {}
    for row in catalog._backend.iter_rows(columns=cols):
        code = str(row.get(code_col, "")).strip()
        lab = {f: str(row.get(catalog._coverage_column(f), "") or "") for f in fields}
        labels[code] = lab
        for token in {t for v in lab.values() for t in tokenize(v)}:
            df[token] = df.get(token, 0) + 1
    n = max(len(labels), 1)
    idf = {t: math.log(n / c) for t, c in df.items()}
    _FLOWS[key] = (catalog, FlowData(fields=fields, labels=labels, idf=idf))
    return _FLOWS[key]


def value_tables(catalog, fields, query):  # noqa: ANN001
    qv = embed_query_vectors(query, [catalog.index_for(f) for f in fields])
    tables: dict[str, dict[str, float]] = {}
    for f in fields:
        index = catalog.index_for(f)
        scores, _kinds = _fused_value_scores(index, query, query_vectors=qv)
        texts = _value_texts_from_index(index)
        tables[f] = {texts[vid]: float(s) for vid, s in scores.items() if 0 <= vid < len(texts) and s > 0}
    return tables


def rank_case(catalog, data: FlowData, query: str) -> dict[str, list[str]]:  # noqa: ANN001
    """Return {config: ranked list of codes} for one query."""
    fields = data.fields
    tables = value_tables(catalog, fields, query)
    tops = {f: (max(t.values()) if t else 0.0) for f, t in tables.items()}
    top5 = {f: dict(sorted(t.items(), key=lambda kv: -kv[1])[:5]) for f, t in tables.items()}

    consumed = {f: consumed_value_tokens(catalog.index_for(f), query) for f in fields}
    qtokens = frozenset(tokenize(query))
    qnorm = query.strip().casefold()
    idf_q = sum(data.idf.get(t, math.log(len(data.labels) or 1)) for t in qtokens) or 1.0

    def scores_for(code: str) -> dict[str, float]:
        lab = data.labels[code]
        raw = {f: tables[f].get(lab[f], 0.0) for f in fields}
        union: set[str] = set()
        equal_hit = False
        for f in fields:
            vt = consumed[f].get(lab[f])
            if vt is not None:
                union.update(vt)
                if lab[f].strip().casefold() == qnorm:
                    equal_hit = True
        cov = (len(union & qtokens) / len(qtokens)) if qtokens else (1.0 if equal_hit else 0.0)
        if equal_hit:
            cov = 1.0
        cov_idf = (sum(data.idf.get(t, 0.0) for t in union & qtokens) / idf_q) if qtokens else cov
        if equal_hit:
            cov_idf = 1.0
        nsum = sum(raw[f] / tops[f] for f in fields if tops[f] > 0)
        # auto_filter: the user's proposed architecture — every field with a
        # consumed value becomes a HARD filter (row's label must be one of the
        # consumed values); rank inside the slice by title search only.
        in_slice = all(lab[f] in consumed[f] for f in fields if consumed[f])
        return {
            "auto_filter_tier": 1.0 if in_slice else 0.0,
            "title_only": raw.get("title", 0.0),
            "no_tier": max(raw.values(), default=0.0),
            "sentinel_tier": 1.0 if equal_hit else 0.0,
            "rawmax": max(raw.values(), default=0.0),
            "pool_rawmax": max((top5[f].get(lab[f], 0.0) for f in fields), default=0.0),
            "cov": cov,
            "cov1": 1.0 if cov >= 1.0 else 0.0,
            "cov_idf": cov_idf,
            "nsum": nsum,
        }

    per_code = {code: scores_for(code) for code in data.labels}

    def ranked(tier_key: str | None, score_key: str) -> list[str]:
        return sorted(
            per_code,
            key=lambda c: (
                -(per_code[c][tier_key] if tier_key else 0.0),
                -per_code[c][score_key],
                c,
            ),
        )

    def ranked_blend(lam: float) -> list[str]:
        return sorted(per_code, key=lambda c: (-(per_code[c]["nsum"] + lam * per_code[c]["cov"]), c))

    out = {
        "title_only": ranked(None, "title_only"),
        "auto_filter": ranked("auto_filter_tier", "title_only"),
        "cov_pool": ranked("cov", "pool_rawmax"),
        "cov_nsum": ranked("cov", "nsum"),
        # Decision probes for the graded-coverage question: does partial
        # coverage earn its tier, or does a binary full-consumption pin
        # (cov1_nsum) or no tier at all (nsum_only) hold the gate?
        "cov1_nsum": ranked("cov1", "nsum"),
        "nsum_only": ranked(None, "nsum"),
    }
    for lam in BLEND_LAMBDAS:
        out[f"blend_l{lam:g}"] = ranked_blend(lam)
    return out


def target_rank(ranked: list[str], data: FlowData, want) -> int | None:  # noqa: ANN001
    def ok(code: str) -> bool:
        lab = data.labels[code]
        for f, op, val in want:
            got = lab.get(f, "").strip().casefold()
            if op == EQ and got != val:
                return False
            if op == HAS and val not in got:
                return False
            if op == NE and got == val:
                return False
        return True

    return next((i for i, code in enumerate(ranked, 1) if ok(code)), None)


def main() -> None:
    lines: list[str] = []
    per_class: dict[str, dict[str, list[int | None]]] = {}
    header = f"{'case':<58} " + " ".join(f"{c:>11}" for c in CONFIGS)
    print(header)
    lines.append(header)
    for case in CASES:
        catalog, data = flow_data(case.agency, case.flow)
        n_targets = sum(1 for c in data.labels if target_rank([c], data, case.want) == 1)
        if n_targets == 0:
            row = f"{case.klass} {case.agency}/{case.flow} {case.query[:34]!r}: INVALID (0 target rows)"
            print(row)
            lines.append(row)
            continue
        t0 = time.perf_counter()
        rankings = rank_case(catalog, data, case.query)
        ranks = {cfg: target_rank(rankings[cfg], data, case.want) for cfg in CONFIGS}
        bucket = per_class.setdefault(case.klass, {cfg: [] for cfg in CONFIGS})
        for cfg in CONFIGS:
            bucket[cfg].append(ranks[cfg])
        label = f"{case.klass:<4}{case.agency}/{case.flow}: {case.query[:38]}"
        row = f"{label:<58} " + " ".join(f"{(ranks[c] if ranks[c] else '—')!s:>11}" for c in CONFIGS)
        print(row + f"   [{time.perf_counter() - t0:.1f}s, targets={n_targets}]")
        lines.append(row)

    def mrr(vals: list[int | None]) -> float:
        return sum((1.0 / v if v else 0.0) for v in vals) / max(len(vals), 1)

    def hit10(vals: list[int | None]) -> str:
        n = sum(1 for v in vals if v and v <= 10)
        return f"{n}/{len(vals)}"

    print("\n=== per-class MRR (higher is better) ===")
    lines.append("\n=== per-class MRR ===")
    hdr = f"{'class':<8} " + " ".join(f"{c:>11}" for c in CONFIGS)
    print(hdr)
    lines.append(hdr)
    totals: dict[str, list[int | None]] = {cfg: [] for cfg in CONFIGS}
    for klass in sorted(per_class):
        vals = per_class[klass]
        for cfg in CONFIGS:
            totals[cfg].extend(vals[cfg])
        row = f"{klass:<8} " + " ".join(f"{mrr(vals[c]):>11.3f}" for c in CONFIGS)
        print(row)
        lines.append(row)
    row = f"{'ALL MRR':<8} " + " ".join(f"{mrr(totals[c]):>11.3f}" for c in CONFIGS)
    print("-" * len(hdr))
    print(row)
    lines.append(row)
    row2 = f"{'hit@10':<8} " + " ".join(f"{hit10(totals[c]):>11}" for c in CONFIGS)
    print(row2)
    lines.append(row2)

    from pathlib import Path

    Path("full_eval_report.txt").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
