# Production-runtime experiment tables

## Exhaustive site sweep (spec / strict coherent out of 50)

| Site | Region | Expected | AF base | AF guard-fix | FO base | FO guard-fix |
|---|---|---|---|---|---|---|
| S00 no fault | no fault | promoted/to | 50/50 | 50/50 | 50/50 | 50/50 |
| S01 VALIDATING status write | pre-flip | failed/from | 50/50 | 50/50 | 50/50 | 50/50 |
| S02 SHADOW_RUNNING status write | pre-flip | failed/from | 50/50 | 50/50 | 50/50 | 50/50 |
| S03 shadow summary write | pre-flip | failed/from | 50/50 | 50/50 | 50/50 | 50/50 |
| S04 SHADOW_PASSED status write | pre-flip | failed/from | 50/50 | 50/50 | 50/50 | 50/50 |
| S05 upgrade audit mirror (after live flip) | flip boundary | rolled_back/from | 0/0 | 50/50 | 0/0 | 0/0 |
| S06 CANARY_RUNNING status write | flip boundary | rolled_back/from | 0/0 | 50/50 | 0/0 | 0/0 |
| S07 canary poll-1 status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S08 canary poll-2 status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S09 canary poll-3 status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S10 canary poll-4 status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S11 canary poll-5 status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S12 canary poll-6 status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S13 end-of-window summary write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S14 CANARY_PROMOTED status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S15 PROMOTED status write | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S16a metric provider raises at poll 1 | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S16b metric provider raises at poll 3 | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S16c metric provider raises at final poll | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S17 post-poll success-rate compute raises | provisional | rolled_back/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S18 rollback closure raises before revert | rollback path | failed/to | 50/50 | 50/50 | 50/0 | 50/0 |
| S19 rollback audit mirror raises after revert | rollback path | failed/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S20 ROLLED_BACK status write raises | rollback path | failed/from | 50/50 | 50/50 | 0/0 | 0/0 |
| S21 flagged FAILED status write raises after the closure raised | rollback path | failed/to | 50/0 | 50/50 | 50/0 | 50/0 |

Totals: {"base|audit-first": {"n": 1200, "spec": 1100, "strict": 1050, "sites_with_mixed_outcomes": 0}, "base|fail-open": {"n": 1200, "spec": 350, "strict": 250, "sites_with_mixed_outcomes": 0}, "guard-fix|audit-first": {"n": 1200, "spec": 1200, "strict": 1200, "sites_with_mixed_outcomes": 0}, "guard-fix|fail-open": {"n": 1200, "spec": 350, "strict": 250, "sites_with_mixed_outcomes": 0}}

## Chaos grid per-run totals

### base (10 runs)
- run 1: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 2: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 3: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 4: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 5: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 6: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 7: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 8: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 9: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 10: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- latency audit-first: {"n": 6000, "p50": 97, "p95": 358, "p99": 366, "max": 433, "mean": 154.10666666666665, "p50_ci": [97, 98], "p95_ci": [357, 358], "p95_per_run_range": [357, 358], "cell_p95_min": 78, "cell_p95_max": 368, "cells_over_500ms_p95": 0}
- latency fail-open: {"n": 6000, "p50": 77, "p95": 330, "p99": 331, "max": 358, "mean": 137.80633333333333, "p50_ci": [77, 77], "p95_ci": [330, 330], "p95_per_run_range": [330, 330], "cell_p95_min": 77, "cell_p95_max": 334, "cells_over_500ms_p95": 0}
### guard-fix (10 runs)
- run 1: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 2: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 3: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 4: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 5: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 6: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 7: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 8: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 9: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- run 10: audit-first spec 600/600 strict 600/600, fail-open spec 200/600 strict 0/600
- latency audit-first: {"n": 6000, "p50": 88, "p95": 349, "p99": 356, "max": 381, "mean": 145.65583333333333, "p50_ci": [88, 89], "p95_ci": [348, 350], "p95_per_run_range": [348, 353], "cell_p95_min": 72, "cell_p95_max": 357, "cells_over_500ms_p95": 0}
- latency fail-open: {"n": 6000, "p50": 73, "p95": 322, "p99": 330, "max": 351, "mean": 131.38733333333334, "p50_ci": [71, 77], "p95_ci": [322, 323], "p95_per_run_range": [322, 330], "cell_p95_min": 77, "cell_p95_max": 330, "cells_over_500ms_p95": 0}
