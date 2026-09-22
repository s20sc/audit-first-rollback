# Provenance

| Records | Runtime | Revision of the private runtime | Pass |
|---|---|---|---|
| `site_sweep/base/*` | base | `8df4344` | 2026-09-22, `run_experiments.sh` sweep phase (`launch.log`) |
| `site_sweep/guard-fix/*` | guard-fix | `8df4344` + `guard-scope.diff` | 2026-09-22 (`launch.log`) |
| `chaos_grid/guard-fix/run01-10` | guard-fix | as above | 2026-09-22 (`launch.log`) |
| `chaos_grid_reversed/guard-fix/run01-05` | guard-fix | as above | 2026-09-22 (`launch.log`) |
| `chaos_grid/base/run01-10` | base | `8df4344` | 2026-09-20 pass, retained: the base runtime is unchanged and `run_experiments.sh` does not repeat its grid |
