# Audit-First Rollback

Reference implementation and reproduction harness for the paper
*Audit-First Rollback Semantics for Safety-Critical Deployment Pipelines*.

A deployment pipeline reaches its provisional-active phase: the new version
of a capability is live in the runtime's active-version map, but the audit
chain has not yet recorded a terminal promotion. If the process crashes here,
two views disagree — live state says the new version is running, the audit
chain says nothing was promoted. **Audit-first rollback** makes the audit
chain the source of truth: on any failure inside a provisional region the
rollback closure runs first, and the terminal audit record is written only
after live state has been reverted to match the chain. A companion
**provisional state machine** partitions pipeline states into committed (live
state and audit chain agree) and provisional (live state flipped, no terminal
record yet), each provisional state carrying a rollback closure and a bounded
deadline.

This repository is a self-contained, standalone model of that rule. It runs
in one Python process with no external dependencies so the twelve-cell
fault-injection grid can be cloned, run, and inspected without a deployment
runtime.

## Contents

| Path | Role |
|------|------|
| `src/audit_first.py` | The audit-first guard pattern: the try/except shape that runs the rollback closure before the terminal write and distinguishes `ROLLED_BACK` (closure succeeded) from `FAILED` (closure itself raised) |
| `src/audit_chain.py` | Append-only audit-chain interface and an in-memory reference implementation |
| `src/rollback_closure.py` | The rollback-closure factory: captures the pre-flip snapshot at construction time |
| `src/job_store.py` | Per-capability locked job store (in-memory) |
| `src/status.py` | Terminal `JobStatus` enum |
| `harness/chaos_grid.py` | The twelve-cell fault-injection driver: three failure classes (A metric-side, B rollback-internal, C audit-write-side) x four cells, under both postures |
| `harness/mocks.py` | In-process mocks for the metric source, active-version map, and status writer |
| `scripts/run_chaos_grid.py` | Run the grid; writes `data/chaos-grid/{raw.jsonl,summary.json,sign_check.json}` |
| `scripts/run_latency.py` | Extract the latency distribution into `data/latency/` |
| `scripts/sign_check.py` | Re-verify the four hypotheses against the committed summary |
| `reproduce.py` | One command: tests + grid + latency + sign-check |
| `data/chaos-grid/` | Committed reference run (1,200 trials) backing the consistency table |
| `data/latency/` | Latency percentiles and the per-posture CDF samples |
| `tests/` | Regression tests for the guard's success, rollback, and rollback-raise paths |

## Requirements

Python 3.11+. No runtime dependencies (standard library only). The
chaos grid and latency scripts run with a bare install; `reproduce.py`
and the test suite additionally need `pytest`, in the `dev` extra.

```bash
pip install -e .           # scripts only (stdlib)
pip install -e ".[dev]"    # adds pytest/ruff/mypy for reproduce.py and tests
```

## Reproduce

```bash
python reproduce.py             # tests + full grid + latency + sign-check
python reproduce.py --quick     # 5 trials/cell, no soak sleep (fast smoke)
```

Or step by step:

```bash
# Twelve-cell grid, 50 trials/cell/posture = 1,200 trials (a few minutes).
python scripts/run_chaos_grid.py --trials 50

# Latency distribution and SLO check.
python scripts/run_latency.py

# Re-verify the four hypotheses against the committed summary.
python scripts/sign_check.py
```

A clean run prints:

```
  PASS  H1 audit-first 50/50 in every cell
  PASS  H2 fail-open 50/50 in every Class-B cell
  PASS  H3 fail-open 0/N in every Class-A/C cell
  PASS  H4 audit-first wall-clock p95 (<=500ms)
Overall: PASS (audit-first 100.0% / fail-open 33.3% consistent)
```

## What the grid shows

Each cell injects one fail-stop failure during a canary deployment and records
the terminal `(status, live version)`. A trial is **consistent** iff that pair
equals the audit-first ideal for the cell — i.e. the audit chain truthfully
describes live state.

| Class | Failure | Audit-first | Fail-open |
|-------|---------|-------------|-----------|
| A (A1-A4) | metric poll raises (first / third / final poll / post-poll compute) | `ROLLED_BACK`, from-version — consistent | `FAILED`, to-version — **inconsistent** |
| B (B1-B4) | rollback closure raises (KeyError / RuntimeError / TimeoutError / cancelled) | `FAILED`, to-version — consistent (cannot revert; says so) | `FAILED`, to-version — consistent (coincidental) |
| C (C1-C4) | status/audit-write fault at varying points: mid-canary (C1), success-path write (C2), post-rollback audit write (C3), and a metric raise with a concurrent upgrade (C4) | `ROLLED_BACK`/`FAILED`, from-version — consistent | `FAILED`, to-version — **inconsistent** |

Audit-first reaches a coherent terminal in all twelve cells (600/600). Fail-open,
which omits the rollback closure, is coherent only in the four Class-B cells —
where rollback would have raised anyway, so omitting it changes nothing (200/600).
Latency is emergent from the canary-soak model (one poll interval per poll, same
0.3 s window / 0.05 s poll interval as the reference deployment): cells that fail
early are fast, cells that saturate the window are slow, and audit-first's 95th
percentile stays within the 500 ms recovery-latency objective.

## Scope

This is a standalone model, not the production deployment runtime. It reproduces
the per-cell consistency outcomes and the latency *structure* (a trimodal
distribution within the 500 ms budget); absolute latencies track the canary
window and poll interval rather than any specific production hardware. Three
failure surfaces are out of scope by construction and noted in the paper:
operating-system-level uncatchable signals, cross-host multi-bridge
coordination, and a live server process with a real network transport.

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Cite

If you use this software, please cite the accompanying paper,
*Audit-First Rollback Semantics for Safety-Critical Deployment Pipelines*.
