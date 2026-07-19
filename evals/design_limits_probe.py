"""Red-team probes: one attack per design assumption of (coverage, norm_sum).

  L1  morphology        no stemming: plural facet word misses the label
  L2  diacritics        'vis-a-vis' vs label token 'vis-à-vis'
  L3  long facet labels multiword official phrasing is never consumable
  L4  negation          'excluding X' — does X get promoted anyway?
  L5  accidental union  peripheral tokens from several fields stack coverage
  L6  noise fields      single-word query: do irrelevant fields' best-match
                        normalization (their 1.0) reorder the target band?
  L7  number tokens     '2015 prices' vs label 'Chain linked volumes (2015)…'
  L8  hyphen forms      '10-year' vs '10 year' — identical results?
  L9  order blindness   containment is a token SET: scrambled word order
                        still earns coverage 1.0
  L10 typo'd facet      misspelled geo: does the row survive semantically?

Native catalog.search on real catalogs (synthetic only where reality lacks the
shape). Public rows only.
"""

from __future__ import annotations

from flow_loader import load_flow


def top(catalog, fields, query, k=4):  # noqa: ANN001
    return catalog.search(query, fields=fields, limit=300), k


def show(matches, k, pick=lambda m: ""):  # noqa: ANN001
    for i, m in enumerate(matches[:k], 1):
        print(f"    {i}. cov={m.coverage:.2f} score={m.score:.2f}  {m.code}  {pick(m)}")


def rank_where(matches, pred):  # noqa: ANN001
    return next((i for i, m in enumerate(matches, 1) if pred(m)), None)


def main() -> None:
    exr, exr_f = load_flow("ECB", "EXR")
    une, une_f = load_flow("ESTAT", "une_rt_m")
    aind, aind_f = load_flow("ESTAT", "prc_hicp_aind")
    icp, icp_f = load_flow("ECB", "ICP")
    namq, namq_f = load_flow("ESTAT", "namq_10_gdp")
    yc, yc_f = load_flow("ECB", "YC")

    print("== L1 morphology: 'swiss francs euro' (plural) ==")
    m, k = top(exr, exr_f, "swiss francs euro")
    r = rank_where(m, lambda x: x.metadata.get("CURRENCY_label") == "Swiss franc")
    print(f"  Swiss-franc row rank: {r}   (singular query ranked 1)")
    show(m, 3, lambda x: x.metadata.get("CURRENCY_label", ""))

    print("\n== L2 diacritics: 'core inflation differential vis-a-vis the euro area' ==")
    m, k = top(aind, aind_f, "core inflation differential vis-a-vis the euro area")
    r = rank_where(m, lambda x: x.metadata.get("unit_label", "").startswith("Core inflation"))
    cov = next((x.coverage for x in m if x.metadata.get("unit_label", "").startswith("Core inflation")), None)
    print(f"  target unit rank: {r}, its coverage: {cov}  (1.0 would mean the accent didn't block containment)")

    print("\n== L3 long facet labels: 'seasonally adjusted unemployment germany' ==")
    m, k = top(une, une_f, "seasonally adjusted unemployment germany")
    r = rank_where(
        m,
        lambda x: (
            x.metadata.get("geo_label") == "Germany"
            and x.metadata.get("s_adj_label", "").startswith("Seasonally adjusted")
        ),
    )
    print(f"  first SA+Germany row rank: {r}")
    show(m, 3, lambda x: f"{x.metadata.get('s_adj_label', '')[:34]} | {x.metadata.get('geo_label', '')}")

    print("\n== L4 negation: 'unemployment excluding young people' ==")
    m, k = top(une, une_f, "unemployment excluding young people")
    r_young = rank_where(m, lambda x: x.metadata.get("age_label") == "Less than 25 years")
    r_not = rank_where(m, lambda x: x.metadata.get("age_label") != "Less than 25 years")
    print(f"  first YOUNG row: {r_young}, first non-young row: {r_not}  (young first = negation ignored)")

    print("\n== L5 accidental union: 'annual index germany' on ICP (which concept wins?) ==")
    m, k = top(icp, icp_f, "annual index germany")
    show(m, 4, lambda x: f"{x.metadata.get('ICP_SUFFIX_label', '')[:22]} | {x.metadata.get('ICP_ITEM_label', '')[:30]}")

    print("\n== L6 noise fields on single word: 'germany' on une_rt_m ==")
    m, k = top(une, une_f, "germany")
    ok = all(x.metadata.get("geo_label") == "Germany" for x in m[:10])
    print(f"  top-10 all Germany: {ok}")
    show(
        m,
        3,
        lambda x: (
            f"{x.metadata.get('geo_label', '')} | {x.metadata.get('age_label', '')} {x.metadata.get('sex_label', '')}"
        ),
    )

    print("\n== L7 number tokens: 'gross domestic product 2015 prices germany' ==")
    m, k = top(namq, namq_f, "gross domestic product 2015 prices germany")
    r = rank_where(
        m,
        lambda x: (
            x.metadata.get("geo_label") == "Germany"
            and "2015" in x.metadata.get("unit_label", "")
            and x.metadata.get("na_item_label", "").startswith("Gross domestic")
        ),
    )
    print(f"  first GDP+2015-unit+DE row rank: {r}")
    show(m, 3, lambda x: f"{x.metadata.get('unit_label', '')[:36]} | {x.metadata.get('geo_label', '')}")

    print("\n== L8 hyphen forms: '10-year spot rate' vs '10 year spot rate' on YC ==")
    m1, _ = top(yc, yc_f, "10-year spot rate")
    m2, _ = top(yc, yc_f, "10 year spot rate")
    same = [x.code for x in m1[:5]] == [x.code for x in m2[:5]]
    print(f"  identical top-5: {same}   top1: {m1[0].code} / {m2[0].code}")

    print("\n== L9 order blindness: 'change of rate annual' on ICP suffix ==")
    m, k = top(icp, ["ICP_SUFFIX_label"], "change of rate annual")
    print(
        f"  top1 cov={m[0].coverage:.2f}: {m[0].metadata.get('ICP_SUFFIX_label', '')!r} "
        f"(1.0 = scrambled order still counts as full containment)"
    )

    print("\n== L10 typo'd facet: 'unemployment germny young people' ==")
    m, k = top(une, une_f, "unemployment germny young people")
    r = rank_where(
        m, lambda x: x.metadata.get("geo_label") == "Germany" and x.metadata.get("age_label") == "Less than 25 years"
    )
    print(f"  DE+young row rank: {r}")
    show(m, 3, lambda x: f"{x.metadata.get('geo_label', '')} | {x.metadata.get('age_label', '')}")


if __name__ == "__main__":
    main()
