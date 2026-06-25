# Reference data

Committed output of a 1,200-trial chaos-grid run
(`python scripts/run_chaos_grid.py --trials 50`). Re-running overwrites these
files in place; the run is deterministic in its consistency outcomes, and
latency figures vary slightly with host timing.

## `chaos-grid/`

| File | Contents |
|------|----------|
| `raw.jsonl` | One JSON record per trial (1,200 lines): cell, posture, terminal status, live version, consistency, wall-clock and rollback-path latency |
| `summary.json` | Per-posture and per-cell aggregates with Wilson 95% confidence intervals |
| `sign_check.json` | PASS/FAIL verdict over hypotheses H1-H4 |

Per-trial record fields:

| Field | Meaning |
|-------|---------|
| `cell` / `failure_class` | A1-C4 and its class (A/B/C) |
| `posture` | `audit-first` or `fail-open` |
| `final_job_status` | terminal status reached (`ROLLED_BACK` / `FAILED` / `PROMOTED`) |
| `final_live_value` | active version after the trial |
| `expected_status` / `expected_live_value` | the audit-first ideal for the cell |
| `consistent` | whether `(final_job_status, final_live_value)` equals the ideal |
| `wall_clock_ms` / `rollback_path_ms` | end-to-end and rollback-path latency |

## `latency/`

| File | Contents |
|------|----------|
| `summary.json` | Per-posture latency percentiles (p50/p95/p99/max), per-cell median, and the 500 ms SLO verdict |
| `cdf.json` | Sorted per-posture wall-clock samples backing the empirical latency CDF |
