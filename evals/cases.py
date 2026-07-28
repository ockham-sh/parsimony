"""Shared case corpus for the SDMX ranking batteries, on real catalogs.

This module is the query taxonomy and the flow loader, nothing else. The ranking
policy under test lives in the connector; the acceptance gate that scores this
corpus is ``native_battery.py``.

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

Target = metadata predicate (never key regexes). Public catalog data only.
"""

from __future__ import annotations

from dataclasses import dataclass

from flow_loader import load_flow

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
    # ---- SENTINELS: documented design limits (negation / typo facets BM25+bi-encoder miss) ----
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
        "facet-only: target needs 'overall', which the query never names; use resolve+filter",
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
    fields: dict[str, float]  # ranking field -> weight, as the connector declares it
    labels: dict[str, dict[str, str]]  # code -> {field: label} (title included)


_FLOWS: dict[tuple[str, str], tuple[object, FlowData]] = {}


def flow_data(agency: str, flow: str) -> tuple[object, FlowData]:
    key = (agency, flow)
    if key in _FLOWS:
        return _FLOWS[key]
    catalog, fields = load_flow(agency, flow)
    code_col = catalog._backend_config.code_column
    cols = list(dict.fromkeys([code_col, *[catalog._physical_column(f) for f in fields]]))
    labels: dict[str, dict[str, str]] = {}
    for row in catalog._backend.iter_rows(columns=cols):
        code = str(row.get(code_col, "")).strip()
        labels[code] = {f: str(row.get(catalog._physical_column(f), "") or "") for f in fields}
    _FLOWS[key] = (catalog, FlowData(fields=fields, labels=labels))
    return _FLOWS[key]
