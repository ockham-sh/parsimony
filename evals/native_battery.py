"""Acceptance: the 37-case taxonomy battery through NATIVE catalog.search().

Native ranks must match the eval's winning. Public catalog rows only.
"""

from __future__ import annotations

from full_eval import CASES, EQ, HAS, flow_data


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


def main() -> None:
    total_rr = 0.0
    hits10 = 0
    n = 0
    sentinel_rows = []
    print(f"{'case':<62} {'native':>7}")
    for case in CASES:
        catalog, data = flow_data(case.agency, case.flow)
        matches = catalog.search(case.query, fields=data.fields, limit=300)
        rank = next(
            (
                i
                for i, m in enumerate(matches, 1)
                if m.code in data.labels and satisfies(data.labels[m.code], case.want)
            ),
            None,
        )
        assert all(0.0 <= m.coverage <= 1.0 for m in matches)
        assert all(0.0 <= m.score <= len(data.fields) + 1e-9 for m in matches), "score out of [0, n_fields]"
        label = f"{case.klass:<4}{case.agency}/{case.flow}: {case.query[:40]}"
        if case.sentinel:
            sentinel_rows.append((label, rank, case.note))
            continue
        n += 1
        total_rr += (1.0 / rank) if rank else 0.0
        hits10 += 1 if rank and rank <= 10 else 0
        print(f"{label:<62} {(rank if rank else '—')!s:>7}")
    print(f"\nnative ALL MRR = {total_rr / n:.3f}   hit@10 = {hits10}/{n}   (gate: >= 0.798, >= 31)")
    print("\n=== sentinels (documented limits — must not get WORSE) ===")
    for label, rank, note in sentinel_rows:
        print(f"{label:<62} {(rank if rank else '—')!s:>7}   {note}")


if __name__ == "__main__":
    main()
