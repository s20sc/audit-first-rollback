#!/usr/bin/env python3
"""Re-verify the shipped chaos-grid summary against four hypotheses.

    python scripts/sign_check.py

Loads ``data/chaos-grid/summary.json`` (produced by run_chaos_grid.py)
and re-checks:

  H1  audit-first is consistent in every cell,
  H2  fail-open is consistent in every Class-B cell,
  H3  fail-open is inconsistent in every Class-A and Class-C cell,
  H4  audit-first wall-clock p95 is within the 500 ms recovery budget.

Exits non-zero if any check fails.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARY = REPO_ROOT / "data" / "chaos-grid" / "summary.json"

CLASS_B = ["B1", "B2", "B3", "B4"]
CLASS_AC = ["A1", "A2", "A3", "A4", "C1", "C2", "C3", "C4"]
ALL_CELLS = ["A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4",
             "C1", "C2", "C3", "C4"]


def main(argv: list[str] | None = None) -> int:
    path = Path(argv[0]) if argv else SUMMARY
    if not path.exists():
        print(f"summary not found: {path}\n"
              f"run: python scripts/run_chaos_grid.py --trials 50",
              file=sys.stderr)
        return 2
    summary = json.loads(path.read_text())
    n = summary["n_trials_per_cell"]
    by_cell = summary["by_cell"]
    af = summary["by_posture"]["audit-first"]

    fails: list[str] = []
    h1 = all(by_cell[f"{c}__audit-first"]["consistent"] == n for c in ALL_CELLS)
    h2 = all(by_cell[f"{c}__fail-open"]["consistent"] == n for c in CLASS_B)
    h3 = all(by_cell[f"{c}__fail-open"]["consistent"] == 0 for c in CLASS_AC)
    p95 = af["wall_clock_p95_ms"]
    h4 = p95 <= 500

    for ok, label in [
        (h1, f"H1 audit-first {n}/{n} in every cell"),
        (h2, f"H2 fail-open {n}/{n} in every Class-B cell"),
        (h3, "H3 fail-open 0/N in every Class-A/C cell"),
        (h4, f"H4 audit-first wall-clock p95 ({p95}ms) <= 500ms"),
    ]:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            fails.append(label)

    print(f"Overall: {'PASS' if not fails else 'FAIL'} "
          f"(audit-first {af['consistent_rate']*100:.1f}% / "
          f"fail-open {summary['by_posture']['fail-open']['consistent_rate']*100:.1f}% consistent; "
          f"p50={af['wall_clock_p50_ms']}ms p95={p95}ms)")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
