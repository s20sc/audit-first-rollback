#!/usr/bin/env python3
"""Extract the latency distribution from a chaos-grid run.

    python scripts/run_latency.py

Reads ``data/chaos-grid/raw.jsonl`` (run run_chaos_grid.py first) and
writes ``data/latency/summary.json`` with per-posture percentiles, the
per-cell median latency, and the sorted per-posture latency samples that
back the empirical latency CDF figure in the paper.

Latency is wall-clock from the upgrade request to the terminal status
read. It is emergent from the canary-soak model (one poll interval per
poll), so cells that fail early are fast and cells that saturate the
canary window are slow; absolute numbers track the canary window/poll
configuration, not a fitted target.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW = REPO_ROOT / "data" / "chaos-grid" / "raw.jsonl"
OUT_DIR = REPO_ROOT / "data" / "latency"


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    rank = (q / 100) * (len(s) - 1)
    lo, hi = math.floor(rank), math.ceil(rank)
    return s[lo] if lo == hi else s[lo] + (rank - lo) * (s[hi] - s[lo])


def main(argv: list[str] | None = None) -> int:
    if not RAW.exists():
        print(f"raw trials not found: {RAW}\n"
              f"run: python scripts/run_chaos_grid.py --trials 50",
              file=sys.stderr)
        return 2
    records = [json.loads(line) for line in RAW.read_text().splitlines() if line]
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    by_posture: dict[str, dict] = {}
    cdf: dict[str, list[int]] = {}
    for posture in ("audit-first", "fail-open"):
        lat = sorted(r["wall_clock_ms"] for r in records
                     if r["posture"] == posture and r["wall_clock_ms"] > 0)
        cdf[posture] = lat
        by_posture[posture] = {
            "n": len(lat),
            "p50_ms": int(_percentile(lat, 50)),
            "p95_ms": int(_percentile(lat, 95)),
            "p99_ms": int(_percentile(lat, 99)),
            "max_ms": lat[-1] if lat else 0,
        }

    per_cell: dict[str, int] = {}
    cells = sorted(set(r["cell"] for r in records))
    for cell in cells:
        lat = [r["wall_clock_ms"] for r in records
               if r["cell"] == cell and r["posture"] == "audit-first"]
        per_cell[cell] = int(_percentile(lat, 50))

    out = {
        "experiment": "audit_first_rollback_latency",
        "slo_p95_ms": 500,
        "slo_pass": by_posture["audit-first"]["p95_ms"] <= 500,
        "by_posture": by_posture,
        "per_cell_median_ms": per_cell,
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(out, indent=2, sort_keys=True))
    (OUT_DIR / "cdf.json").write_text(json.dumps(cdf, sort_keys=True))

    print(json.dumps(out, indent=2, sort_keys=True))
    print(f"wrote {OUT_DIR}/{{summary.json,cdf.json}}")
    return 0 if out["slo_pass"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
