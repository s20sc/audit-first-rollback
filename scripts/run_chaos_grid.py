#!/usr/bin/env python3
"""Run the twelve-cell chaos grid and write raw / summary / sign-check.

    python scripts/run_chaos_grid.py --trials 50 --seed 112026

Outputs three files under ``data/chaos-grid/``:

  * ``raw.jsonl``     — one record per trial.
  * ``summary.json``  — per-posture and per-cell aggregates with Wilson CIs.
  * ``sign_check.json`` — H1..H4 verdict.

With ``--trials 50`` the full grid is 12 cells x 50 trials x 2 postures =
1,200 trials. The default soak makes each trial sleep its canary polls, so
a full run takes a few minutes; pass ``--no-sleep`` for a fast
consistency-only check (latency figures are then meaningless).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harness.chaos_grid import (  # noqa: E402
    ALL_CELLS, ALL_POSTURES, CELLS, cell_by_name, run_trial,
    trial_record_to_dict,
)

DATA_DIR = REPO_ROOT / "data" / "chaos-grid"


def _wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if q <= 0:
        return s[0]
    if q >= 100:
        return s[-1]
    rank = (q / 100) * (len(s) - 1)
    lo, hi = math.floor(rank), math.ceil(rank)
    if lo == hi:
        return s[lo]
    return s[lo] + (rank - lo) * (s[hi] - s[lo])


async def _run(n_trials: int, seed: int, sleep: bool) -> list[dict]:
    records: list[dict] = []
    for posture in ALL_POSTURES:
        for name in ALL_CELLS:
            cell = cell_by_name(name)
            for i in range(n_trials):
                rec = await run_trial(posture=posture, cell=cell,
                                      trial_index=i, sleep=sleep)
                records.append(trial_record_to_dict(rec))
    return records


def _summarise(records: list[dict], n_trials: int) -> dict:
    by_cell: dict[str, dict] = {}
    for posture in ALL_POSTURES:
        for name in ALL_CELLS:
            cell = [r for r in records
                    if r["posture"] == posture and r["cell"] == name]
            k = sum(1 for r in cell if r["consistent"])
            n = len(cell)
            lo, hi = _wilson_ci(k, n)
            by_cell[f"{name}__{posture}"] = {
                "consistent": k, "n": n, "rate": round(k / n, 4) if n else 0.0,
                "ci95_wilson": [round(lo, 4), round(hi, 4)],
            }
    by_posture: dict[str, dict] = {}
    for posture in ALL_POSTURES:
        trials = [r for r in records if r["posture"] == posture]
        k = sum(1 for r in trials if r["consistent"])
        n = len(trials)
        lo, hi = _wilson_ci(k, n)
        lat = [r["wall_clock_ms"] for r in trials if r["wall_clock_ms"] > 0]
        by_posture[posture] = {
            "consistent_count": k, "n_trials": n,
            "consistent_rate": round(k / n, 4) if n else 0.0,
            "ci95_wilson": [round(lo, 4), round(hi, 4)],
            "wall_clock_p50_ms": int(_percentile(lat, 50)),
            "wall_clock_p95_ms": int(_percentile(lat, 95)),
        }
    return {
        "experiment": "audit_first_rollback_chaos_grid",
        "n_trials_per_cell": n_trials,
        "n_cells": len(ALL_CELLS),
        "n_total_trials": len(records),
        "by_posture": by_posture,
        "by_cell": by_cell,
    }


def _sign_check(summary: dict, n_trials: int) -> dict:
    h1, h2, h3 = [], [], []
    af, fo = "audit-first", "fail-open"
    for name in ALL_CELLS:                       # H1: audit-first all consistent
        c = summary["by_cell"][f"{name}__{af}"]
        if c["consistent"] != n_trials:
            h1.append(f"{name}: {c['consistent']}/{n_trials}")
    for name in [c.name for c in CELLS if c.failure_class == "B"]:  # H2: B fail-open consistent
        c = summary["by_cell"][f"{name}__{fo}"]
        if c["consistent"] != n_trials:
            h2.append(f"{name}: {c['consistent']}/{n_trials}")
    for name in [c.name for c in CELLS if c.failure_class != "B"]:  # H3: A/C fail-open inconsistent
        c = summary["by_cell"][f"{name}__{fo}"]
        if c["consistent"] != 0:
            h3.append(f"{name}: {c['consistent']}/{n_trials}")
    p95 = summary["by_posture"][af]["wall_clock_p95_ms"]
    h4 = [] if p95 <= 500 else [f"audit-first wall-clock p95 {p95}ms > 500ms"]
    verdict = "PASS" if not (h1 or h2 or h3 or h4) else "FAIL"
    return {
        "verdict": verdict,
        "checks": {
            "H1_audit_first_all_cells_consistent": {"passed": not h1, "failures": h1},
            "H2_fail_open_class_B_consistent": {"passed": not h2, "failures": h2},
            "H3_fail_open_class_A_C_inconsistent": {"passed": not h3, "failures": h3},
            "H4_audit_first_p95_within_500ms": {"passed": not h4, "failures": h4},
        },
        "measurements": {
            "audit_first_consistent_rate": summary["by_posture"][af]["consistent_rate"],
            "fail_open_consistent_rate": summary["by_posture"][fo]["consistent_rate"],
            "audit_first_wall_clock_p50_ms": summary["by_posture"][af]["wall_clock_p50_ms"],
            "audit_first_wall_clock_p95_ms": p95,
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Twelve-cell audit-first chaos grid")
    ap.add_argument("--trials", type=int, default=50,
                    help="trials per (cell x posture); default 50 -> 1,200 total")
    ap.add_argument("--seed", type=int, default=112026,
                    help="reserved for reproducibility metadata")
    ap.add_argument("--no-sleep", action="store_true",
                    help="skip canary-soak sleeps (fast; latency meaningless)")
    ap.add_argument("--outdir", type=Path, default=DATA_DIR)
    args = ap.parse_args(argv)

    args.outdir.mkdir(parents=True, exist_ok=True)
    records = asyncio.run(_run(args.trials, args.seed, sleep=not args.no_sleep))
    summary = _summarise(records, args.trials)
    summary["seed"] = args.seed
    sign = _sign_check(summary, args.trials)

    (args.outdir / "raw.jsonl").write_text(
        "".join(json.dumps(r, sort_keys=True) + "\n" for r in records))
    (args.outdir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True))
    (args.outdir / "sign_check.json").write_text(
        json.dumps(sign, indent=2, sort_keys=True))

    print(json.dumps({
        "verdict": sign["verdict"],
        "n_total_trials": summary["n_total_trials"],
        **sign["measurements"],
    }, indent=2, sort_keys=True))
    print(f"wrote {args.outdir}/{{raw.jsonl,summary.json,sign_check.json}}")
    return 0 if sign["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
