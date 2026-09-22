#!/usr/bin/env bash
# Option (b) rerun: site S21 added to the sweep and the flag carried through the
# wrapper (guard-fix @ e1392cb). Sweeps on both runtimes; grid + reversed on
# guard-fix only (the original runtime is unchanged since runs2).
set -euo pipefail
ROOT=${ROOT:-$PWD}   # a directory holding base/ and guard-fix/ runtime checkouts
PY=$ROOT/.venv/bin/python
RUNS=$ROOT/runs3
export HOME=$ROOT/home
log() { echo "[$(date "+%F %T")] $*"; }
fresh() { rm -rf "$HOME/.aeros"; }
mkdir -p $RUNS/X1_sites/base $RUNS/X1_sites/guard-fix $RUNS/X2_grid/guard-fix $RUNS/X3_order/guard-fix
for rt in base guard-fix; do
  for posture in audit-first fail-open; do
    fresh; log "sweep $rt $posture"
    (cd $ROOT/$rt && nice -n 10 $PY scripts/runtime_site_sweep.py --posture $posture \
        --trials 50 --seed 112026 --output $RUNS/X1_sites/$rt/$posture.jsonl) | tail -1
  done
done
for rep in $(seq 1 10); do
  fresh; seed=$((112026 + rep)); log "grid guard-fix rep$rep seed=$seed"
  (cd $ROOT/guard-fix && nice -n 10 $PY scripts/runtime_chaos_grid.py --trials 50 \
      --seed $seed --quiet --output $RUNS/X2_grid/guard-fix/rep$(printf %02d $rep)/raw.jsonl) | tail -1
done
for rep in $(seq 1 5); do
  fresh; seed=$((112036 + rep)); log "reversed rep$rep seed=$seed"
  (cd $ROOT/guard-fix && nice -n 10 $PY scripts/runtime_chaos_grid.py --trials 50 \
      --seed $seed --quiet --postures fail-open,audit-first \
      --output $RUNS/X3_order/guard-fix/rep$(printf %02d $rep)/raw.jsonl) | tail -1
done
log "ALL DONE"
