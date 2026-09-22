#!/usr/bin/env python3
"""One-command reproduction harness for the audit-first rollback artifact.

    python reproduce.py            # full grid (12 x 50 x 2 = 1,200 trials)
    python reproduce.py --quick    # 12 x 5 x 2 = 120 trials, no soak sleep

Runs the regression tests, the twelve-cell chaos grid, the latency
extract, and the sign-check, then prints the headline result. Results go
to out/; the committed reference run in data/ is never overwritten. The full
run sleeps each canary poll and takes a few minutes; --quick skips the
soak sleeps (so latency figures are meaningless but consistency is not).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _run(cmd: list[str]) -> int:
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=ROOT)


def main() -> int:
    ap = argparse.ArgumentParser(description="Reproduce the audit-first artifact")
    ap.add_argument("--quick", action="store_true",
                    help="5 trials/cell, skip canary-soak sleeps (fast)")
    args = ap.parse_args()

    py = sys.executable
    trials = ["--trials", "5", "--no-sleep"] if args.quick else ["--trials", "50"]

    try:
        import pytest  # noqa: F401
        has_pytest = True
    except ImportError:
        has_pytest = False
    if has_pytest:
        rc = _run([py, "-m", "pytest", "tests/", "-q"])
        if rc != 0:
            print("tests failed", file=sys.stderr)
            return rc
    else:
        print("\n(pytest not installed; skipping tests. "
              "Install with: pip install -e \".[dev]\")")

    rc = _run([py, "scripts/run_chaos_grid.py", *trials])
    if rc != 0:
        print("chaos grid sign-check did not pass", file=sys.stderr)
        return rc

    if not args.quick:
        _run([py, "scripts/run_latency.py", "--root", "out"])

    return _run([py, "scripts/sign_check.py", "out/chaos-grid/summary.json"])


if __name__ == "__main__":
    sys.exit(main())
