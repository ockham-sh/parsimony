"""Acceptance: the 37-case taxonomy battery through the SHIPPED SDMX ranking path.

Measures what the connector actually does — ``Catalog.multi_field_search`` over the
connector-declared label weights, in catalog ``score`` order (no hidden facet tier).
Free-text is an exploratory shortlist; agents commit via ``sdmx_dimension_search`` +
``filter=``.

Gate on hit@10 — the property an agent needs (gold on the returned page). MRR is
reported as a diagnostic regression canary. Numbers are measured; do not edit
them without re-running this script from the connectors venv.
"""

from __future__ import annotations

from cases import CASES, EQ, HAS, flow_data

#: Measured post–coverage-removal gate (score-order ranking). Update only after re-running.
GATE_MRR = 0.678
GATE_HIT10 = 29


def satisfies(labels: dict[str, str], want) -> bool:  # noqa: ANN001
    for f, op, val in want:
        got = labels.get(f, "").strip().casefold()
        if op == EQ and got != val:
            return False
        if op == HAS and val not in got:
            return False
        if op not in (EQ, HAS) and got == val:  # NE
            return False
    return True


def _rank(matches, data, case) -> int | None:  # noqa: ANN001
    return next(
        (
            i
            for i, m in enumerate(matches, 1)
            if m.code in data.labels and satisfies(data.labels[m.code], case.want)
        ),
        None,
    )


def main() -> None:
    rr = 0.0
    hits10 = 0
    n = 0
    sentinel_rows = []
    print(f"{'case':<62} {'rank':>7}")
    for case in CASES:
        catalog, data = flow_data(case.agency, case.flow)
        matches = catalog.multi_field_search(case.query, fields=data.fields, limit=300)
        assert all(0.0 <= m.score <= 1.0 + 1e-9 for m in matches), "score out of (0, 1]"
        if matches:
            assert abs(matches[0].score - 1.0) < 1e-9, "top score must be 1.0"

        rank = _rank(matches, data, case)

        label = f"{case.klass:<4}{case.agency}/{case.flow}: {case.query[:40]}"
        if case.sentinel:
            sentinel_rows.append((label, rank, case.note))
            continue
        n += 1
        rr += (1.0 / rank) if rank else 0.0
        hits10 += 1 if rank and rank <= 10 else 0
        print(f"{label:<62} {(rank if rank else '—')!s:>7}")

    mrr = rr / n
    print(
        f"\nALL MRR = {mrr:.3f}   hit@10 = {hits10}/{n}"
        f"   (gate: hit@10 >= {GATE_HIT10}, MRR >= {GATE_MRR})"
    )
    if hits10 < GATE_HIT10 or round(mrr, 3) < GATE_MRR:
        raise SystemExit(
            f"FAIL gate: MRR={mrr:.3f} hit@10={hits10} (need hit@10 >= {GATE_HIT10}, MRR >= {GATE_MRR})"
        )
    print("\n=== sentinels (documented limits — must not get WORSE) ===")
    for label, rank, note in sentinel_rows:
        print(f"{label:<62} {(rank if rank else '—')!s:>7}   {note}")


if __name__ == "__main__":
    main()
