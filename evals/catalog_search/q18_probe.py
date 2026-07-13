"""Q18: peripheral-facet vs core-concept, across coverage bands.

Synthetic multi-concept LFS-shaped catalog (memory backend, hybrid label
indexes so the semantic path is live). The core concept 'unemployment' is NOT
consumable (label carries extra tokens); the facet 'males' IS. The fully-right
row (males+young+unemployment) is deliberately ABSENT so constraint and
concept genuinely conflict.

Rankings compared:
  lex    — shipped: (coverage, score) lexicographic
  blend  — coverage + score/n_fields, one float (crosses bands)
"""

from __future__ import annotations

from parsimony.catalog import BM25Index, Catalog, Entity
from parsimony.catalog.indexes import HybridIndex, VectorIndex

INDICS = ["Unemployment according to ILO definition", "Active population", "Employment"]
SEXES = ["Males", "Females", "Total"]
AGES = ["Less than 25 years", "From 25 to 74 years", "Total"]
GEOS = ["Germany", "France"]


def build() -> Catalog:
    rows = []
    for i, indic in enumerate(INDICS):
        for s, sex in enumerate(SEXES):
            for a, age in enumerate(AGES):
                for g, geo in enumerate(GEOS):
                    # The conflict: unemployment series exist ONLY for sex=Total.
                    if indic.startswith("Unemployment") and sex != "Total":
                        continue
                    code = f"{'UAE'[i]}.{'MFT'[s]}.{'YOT'[a]}.{'DF'[g]}"
                    rows.append(
                        Entity(
                            namespace="lfs",
                            code=code,
                            title=f"{indic} - {sex} - {age} - {geo}",
                            metadata={
                                "indic_label": indic,
                                "sex_label": sex,
                                "age_label": age,
                                "geo_label": geo,
                            },
                        )
                    )
    catalog = Catalog(
        "lfs",
        indexes={
            "title": BM25Index(),
            "indic_label": HybridIndex(components=[BM25Index(), VectorIndex()]),
            "sex_label": HybridIndex(components=[BM25Index(), VectorIndex()]),
            "age_label": HybridIndex(components=[BM25Index(), VectorIndex()]),
            "geo_label": BM25Index(),
        },
        default_field="title",
    )
    catalog.set_entities(rows)
    catalog.build()
    return catalog


def main() -> None:
    catalog = build()
    fields = ["title", "indic_label", "sex_label", "age_label", "geo_label"]
    n = len(fields)
    query = "males young unemployment"

    matches = catalog.search(query, fields=fields, limit=200)

    def show(title: str, ordered) -> None:  # noqa: ANN001
        print(f"\n[{title}] query={query!r} — top 8")
        for i, m in enumerate(ordered[:8], 1):
            mark = "*" if m.metadata["indic_label"].startswith("Unemployment") else " "
            print(
                f"  {mark}{i}. cov={m.coverage:.2f} score={m.score:.3f}  "
                f"{m.metadata['indic_label'][:28]:<28} {m.metadata['sex_label']:<8} {m.metadata['age_label']}"
            )

    show("lex (shipped)", matches)
    blended = sorted(matches, key=lambda m: (-(m.coverage + m.score / n), m.code))
    show(f"blend = cov + score/{n}", blended)

    def first_unemployment(ordered) -> int | None:  # noqa: ANN001
        return next(
            (
                i
                for i, m in enumerate(ordered, 1)
                if m.metadata["indic_label"].startswith("Unemployment")
                and m.metadata["age_label"] == "Less than 25 years"
            ),
            None,
        )

    print(
        f"\nfirst young-UNEMPLOYMENT row: lex rank {first_unemployment(matches)}, "
        f"blend rank {first_unemployment(blended)}"
    )


if __name__ == "__main__":
    main()
