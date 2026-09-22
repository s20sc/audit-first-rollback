# Production-runtime experiments

This directory holds the fault-injection experiments that the paper reports
on the production deployment runtime. The runtime itself is not public. The
files here show exactly what was run and let every reported number be
recomputed from the raw trial records.

| Path | Content |
|------|---------|
| `harness/runtime_chaos_grid.py` | The 12-cell crash grid (A1-A4, B1-B4, C1-C4) under the audit-first and fail-open postures, driving the runtime's HTTP interface in-process |
| `harness/runtime_site_sweep.py` | The exhaustive sweep: one injected fault at each of 23 enumerated failure sites of a canary job, plus a fault-free control (24 sites) |
| `guard-scope.diff` | The two changes the paper reports, in one file (58 lines added, 32 removed): the rollback guard opens before the live-state switch, and a flagged FAILED write that fails re-raises with the flag so the job wrapper's fallback record keeps it |
| `data/site_sweep/<runtime>/<posture>.jsonl` | One record per trial, 50 trials per site and posture |
| `data/chaos_grid/<runtime>/runNN/raw.jsonl` | One record per trial, 1,200 trials per run |
| `data/chaos_grid_reversed/guard-fix/runNN/raw.jsonl` | The same grid with the fail-open cells run first, five runs |
| `data/environment.txt` | Python packages of the experiment environment |
| `run_experiments.sh` | The exact launcher of the reported runs (sweeps on both runtimes, grid and reversed-order runs on `guard-fix`), with the seeds |
| `MANIFEST.sha256` | Checksums of the harnesses, the diff, the launcher and every data file, written when the data were staged |
| `analysis/analyze.py` | Recomputes every number the paper reports; it recomputes the stored verdicts from the oracle fields and refuses incomplete inputs |
| `results/numbers.json` | Output of `analyze.py` on the committed data |

`<runtime>` is `base`, the runtime revision whose canary guard the paper
first evaluated (commit `8df4344` of the private runtime repository), or `guard-fix`, the same revision with `guard-scope.diff`
applied. The fail-open posture replaces the canary stage with its own copy
and is therefore the same under both.

## How the runs were made

- One workstation: Intel Core i9-11900K (8 cores), 128 GB memory, NVMe SSD,
  Ubuntu 24.04.5 LTS, Linux 6.8, Python 3.11.16, in an isolated virtual
  environment (`data/environment.txt`).
- Every run started a fresh process with an empty state directory, so no job
  record, audit row or live-state entry carried over between runs. Runs
  executed one at a time.
- The canary window was 0.3 s and the poll interval 0.05 s (the runtime's
  defaults are 30 s and 5 s); each soak therefore performs six polls.
- Seeds: 112026 for the site sweep, 112027 to 112036 for the ten grid runs,
  112037 to 112041 for the five reversed-order runs.
- The harness files are the deployed copies, kept byte-identical to what ran
  (`MANIFEST.sha256`). Three comments in them are stale and left as they
  ran. In `runtime_site_sweep.py`, the timing note says the terminal status
  is "durably written", whereas the evaluated runtime keeps job records in
  memory (the paper's Section 3.7); the measured instant is the return of
  the write. In `runtime_chaos_grid.py`, the Class-C header says C2 raises
  at the PROMOTED status write, whereas the patch it installs fires at the
  first of the CANARY_PROMOTED and PROMOTED writes, which is CANARY_PROMOTED
  (the paper's Table 4 names the cell by what fired); and the note above
  its C1-C3 patches says they are not removed after the trial, whereas the
  `finally` block at the end of each trial restores them (next bullet).
- Both harnesses restore every patch they install on process-wide objects
  after each trial, in installation order. Every trial installs at most one
  patch per attribute, which is why that order is safe here; a harness that
  stacked two patches on one attribute would have to restore them in reverse
  order, and the next revision of these harnesses will. An earlier version of the grid left its C1-C3 job-store
  patches attached; each fired inside its own trial in the order the grid
  runs, but a fail-open patch that never fires could reach a later trial when
  the posture order was reversed, which is what prompted the change.

From the root of a runtime checkout, with both harness files copied into its
`scripts/` directory:

```bash
python scripts/runtime_site_sweep.py --posture audit-first --trials 50 \
    --seed 112026 --output runs/site_sweep/audit-first.jsonl
python scripts/runtime_chaos_grid.py --trials 50 --seed 112027 --quiet \
    --output runs/chaos_grid/run01/raw.jsonl
python scripts/runtime_chaos_grid.py --trials 50 --seed 112037 --quiet \
    --postures fail-open,audit-first \
    --output runs/chaos_grid_reversed/run01/raw.jsonl
```

## Record fields

Site sweep (`site_sweep/*.jsonl`): `site`, `site_kind`, `site_desc`,
`posture`, `trial`, `from_version`, `to_version`, `fault_fired`,
`flipped_at_fault` (live state already switched when the fault fired),
`final_status`, `final_live`, `expected_status`, `expected_live` (derived from
the specification and the two facts above, never from the observed outcome),
`coherent`, `end_to_end_ms`, `fault_to_terminal_ms`, `canary_started_at`,
`canary_completed_at`, `error`.

Chaos grid (`chaos_grid/*/raw.jsonl`): `trial_id`, `injection`, `posture`,
`capability`, `from_version`, `to_version`, `expected_live_value`,
`final_job_status`, `final_live_value`, `audit_chain_last_record`,
`consistent`, `wall_clock_ms`, `rollback_path_ms`, and, when present,
`rollback_reason` and `error_text`.

Two fields of the grid harness are legacy and the paper does not use them.
`rollback_path_ms` is the trial's end-to-end time when an audit-first trial
ends ROLLED_BACK or FAILED and zero otherwise, so it is neither a measured
rollback duration nor a fail-open measurement; the paper's recovery
percentiles come from `fault_to_terminal_ms` in the site sweep. The
`sign_check.json` that the harness writes applies its own older pass
criterion (a median check on `rollback_path_ms`); the claims in the paper
are the ones `analysis/analyze.py` recomputes.

## Recompute

```bash
python production/analysis/analyze.py      # writes production/out/
diff <(python -m json.tool production/out/numbers.json) \
     <(python -m json.tool production/results/numbers.json)
```

`analyze.py` uses only the standard library and never writes into `data/` or
`results/`.
