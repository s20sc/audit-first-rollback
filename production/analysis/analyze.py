#!/usr/bin/env python3
"""Aggregate the production-runtime experiments into the numbers the paper
quotes.

Inputs (production/data/):
  site_sweep/<runtime>/<posture>.jsonl        24-site sweep (23 fault sites + control)
  chaos_grid/<runtime>/runNN/raw.jsonl        12-cell grid, independent runs
  chaos_grid_reversed/guard-fix/runNN/raw.jsonl
                                              the same grid, fail-open cells first
where <runtime> is ``base`` (the evaluated revision) or ``guard-fix`` (base
plus production/guard-scope.diff).

Outputs (production/out/ by default; --outdir to change):
  numbers.json    every number the paper quotes
  tables.md       human-readable per-site and per-run tables

Two coherence checks are reported.
  spec    : (terminal status, live version) equals the outcome the
            audit-first specification prescribes for the fault that fired
            (the original harness oracle).
  strict  : spec, and whenever the prescribed outcome follows a closure
            that raised (S18: the revert did not happen and the new version
            is still live; S19: the closure raised after reverting; S21: the
            flagged write itself failed once), the record also carries the
            divergence marker the specification requires for that outcome
            ("rollback failure" in the job error); on every rollback-path
            site the error must also carry the injected target's own marker.
"""
from __future__ import annotations

import json
import math
import random
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data"
OUTDIR = HERE.parent / "out"
RUNTIMES = ("base", "guard-fix")
POSTURES = ("audit-first", "fail-open")
MARKER = "rollback failure"
# The harness stamps the target fault of each rollback-path site with its own
# marker (distinct from the trigger's "injected <kind> trigger" text); the
# strict check requires it, so a trigger that fired without reaching the
# target cannot pass.
TARGET_MARKER = {"rb_closure": "injected rollback-closure fault",
                 "rb_mirror": "injected rb_mirror fault",
                 "rb_status": "injected rb_status fault",
                 "rb_flagged_status": "injected rb_flagged_status fault"}
WINDOW_S = 0.3
BOOT = 2000
SEED = 20260919

REGION = {
    "S00": "no fault",
    **{s: "pre-flip" for s in ("S01", "S02", "S03", "S04")},
    **{s: "flip boundary" for s in ("S05", "S06")},
    **{s: "provisional" for s in ("S07", "S08", "S09", "S10", "S11", "S12",
                                  "S13", "S14", "S15", "S16a", "S16b",
                                  "S16c", "S17")},
    **{s: "rollback path" for s in ("S18", "S19", "S20", "S21")},
}
CELL_FAILED_TO = {"B1", "B2", "B3", "B4"}
# The grid harness's ideal audit-first outcome per cell (its _AUDIT_FIRST_IDEAL
# table), replicated here so the stored verdict can be recomputed: status and
# whether the live version must be the from- or the to-version.
CELL_IDEAL = {**{c: ("rolled_back", "from") for c in ("A1", "A2", "A3", "A4", "C1", "C2", "C4")},
              **{c: ("failed", "to") for c in ("B1", "B2", "B3", "B4")},
              "C3": ("failed", "from")}


# --------------------------------------------------------------------------- #
# Record validation: the analysis never trusts a stored verdict. The oracle   #
# is re-derived from the two facts recorded at fault time, the stored         #
# expectation and the stored verdict must both agree with it, and every file  #
# must have the shape the experiment declares.                                #
# --------------------------------------------------------------------------- #
SWEEP_FIELDS = ("site", "site_kind", "trial", "posture", "from_version",
                "to_version", "fault_fired", "flipped_at_fault", "final_status",
                "final_live", "expected_status", "expected_live", "coherent",
                "fault_to_terminal_ms", "error", "site_desc",
                "canary_started_at", "canary_completed_at", "end_to_end_ms")
STATUSES = ("promoted", "failed", "rolled_back")
GRID_FIELDS = ("trial_id", "injection", "posture", "from_version", "to_version",
               "final_job_status", "final_live_value", "expected_live_value",
               "consistent", "wall_clock_ms", "error_text")
TRIALS_PER_SITE = 50
TRIALS_PER_CELL = 50
# The declared experiment: site id -> fault kind and description, copied from
# the SITES table of the frozen sweep harness (runtime_site_sweep.py). A
# record's own site_kind is checked against this table before any expectation
# is derived from it, so relabelling a site cannot change what it must satisfy.
SITE_KIND = {
    'S00': 'none',
    'S01': 'store_nth',
    'S02': 'store_nth',
    'S03': 'store_nth',
    'S04': 'store_nth',
    'S05': 'mirror',
    'S06': 'store_nth',
    'S07': 'store_nth',
    'S08': 'store_nth',
    'S09': 'store_nth',
    'S10': 'store_nth',
    'S11': 'store_nth',
    'S12': 'store_nth',
    'S13': 'store_nth',
    'S14': 'store_status',
    'S15': 'store_status',
    'S16a': 'metric_k',
    'S16b': 'metric_k',
    'S16c': 'metric_k',
    'S17': 'compute_k',
    'S18': 'rb_closure',
    'S19': 'rb_mirror',
    'S20': 'rb_status',
    'S21': 'rb_flagged_status',
}
SITE_DESC = {
    'S00': 'no fault',
    'S01': 'VALIDATING status write',
    'S02': 'SHADOW_RUNNING status write',
    'S03': 'shadow summary write',
    'S04': 'SHADOW_PASSED status write',
    'S05': 'upgrade audit mirror (after live flip)',
    'S06': 'CANARY_RUNNING status write',
    'S07': 'canary poll-1 status write',
    'S08': 'canary poll-2 status write',
    'S09': 'canary poll-3 status write',
    'S10': 'canary poll-4 status write',
    'S11': 'canary poll-5 status write',
    'S12': 'canary poll-6 status write',
    'S13': 'end-of-window summary write',
    'S14': 'CANARY_PROMOTED status write',
    'S15': 'PROMOTED status write',
    'S16a': 'metric provider raises at poll 1',
    'S16b': 'metric provider raises at poll 3',
    'S16c': 'metric provider raises at final poll',
    'S17': 'post-poll success-rate compute raises',
    'S18': 'rollback closure raises before revert',
    'S19': 'rollback audit mirror raises after revert',
    'S20': 'ROLLED_BACK status write raises',
    'S21': 'flagged FAILED status write raises after the closure raised',
}

PARTIAL = [False]   # set from --partial; exploratory runs skip the count gates


def oracle(site_kind: str, fault_fired: bool, flipped_at_fault: bool) -> tuple:
    """The harness's expected_outcome, re-derived from the facts it records
    at fault time. It never reads the observed outcome."""
    if not fault_fired:
        return ("promoted", "to")
    if not flipped_at_fault:
        return ("failed", "from")
    if site_kind in ("rb_closure", "rb_flagged_status"):
        return ("failed", "to")
    if site_kind in ("rb_mirror", "rb_status"):
        return ("failed", "from")
    return ("rolled_back", "from")


def _fail(msg: str) -> None:
    raise SystemExit(msg)


def _is_ms(v) -> bool:
    """A measured time: a finite, non-negative int or float. JSON booleans
    are ints in Python and are refused explicitly."""
    return (isinstance(v, (int, float)) and not isinstance(v, bool)
            and math.isfinite(v) and v >= 0)


def _stamp(path, r: dict, field: str, required: bool):
    """Parse an ISO-8601 timestamp field; it must carry a UTC offset. A
    missing value is allowed only where ``required`` is False."""
    v = r.get(field)
    if v is None:
        if required:
            _fail(f"{path}: {r['site']} trial {r['trial']} has no {field}")
        return None
    try:
        ts = datetime.fromisoformat(v)
    except (TypeError, ValueError):
        _fail(f"{path}: {r['site']} trial {r['trial']} has an unparseable {field} {v!r}")
    if ts.tzinfo is None:
        _fail(f"{path}: {r['site']} trial {r['trial']} has a naive {field} {v!r}")
    return ts


def validate_sweep(path, rows: list, posture: str | None = None) -> dict:
    """Validate one sweep file; return its rows grouped by site."""
    if not rows:
        _fail(f"{path}: no records")
    missing = sorted({f for r in rows for f in SWEEP_FIELDS if f not in r})
    if missing:
        _fail(f"{path}: records lack fields {missing}")
    ids = [(r["site"], r["trial"]) for r in rows]
    if len(ids) != len(set(ids)):
        _fail(f"{path}: duplicate (site, trial) ids")
    postures = {r["posture"] for r in rows}
    if len(postures) != 1:
        _fail(f"{path}: mixed postures {sorted(postures)}")
    if posture is not None and postures != {posture}:
        _fail(f"{path}: records say posture {sorted(postures)}, the file is labelled {posture}")
    for r in rows:
        for f in ("fault_fired", "coherent"):
            if not isinstance(r[f], bool):
                _fail(f"{path}: {f} must be a boolean at {r['site']} trial {r['trial']}, got {r[f]!r}")
        # The switch snapshot exists only where a fault fired; a control has none.
        if r["fault_fired"] and not isinstance(r["flipped_at_fault"], bool):
            _fail(f"{path}: flipped_at_fault must be a boolean where the fault fired "
                  f"({r['site']} trial {r['trial']}, got {r['flipped_at_fault']!r})")
        if not r["fault_fired"] and r["flipped_at_fault"] not in (None, False):
            _fail(f"{path}: {r['site']} trial {r['trial']} records a switch snapshot without a fault")
        # Evidence schema: a trial is evidence only if it names a real version
        # change, a status from the declared set and a textual error payload.
        for f in ("from_version", "to_version", "expected_live", "final_live"):
            if not (isinstance(r[f], str) and r[f]):
                _fail(f"{path}: {f} must be a non-empty string at {r['site']} trial "
                      f"{r['trial']}, got {r[f]!r}")
        if r["from_version"] == r["to_version"]:
            _fail(f"{path}: {r['site']} trial {r['trial']} names no version change "
                  f"({r['from_version']!r})")
        for f in ("expected_status", "final_status"):
            if r[f] not in STATUSES:
                _fail(f"{path}: {f} must be one of {STATUSES} at {r['site']} trial "
                      f"{r['trial']}, got {r[f]!r}")
        if r["error"] is not None and not isinstance(r["error"], str):
            _fail(f"{path}: error must be text or null at {r['site']} trial {r['trial']}, "
                  f"got {type(r['error']).__name__}")
        if not _is_ms(r["end_to_end_ms"]):
            _fail(f"{path}: invalid end_to_end_ms {r['end_to_end_ms']!r} at {r['site']} "
                  f"trial {r['trial']}")
        v = r["fault_to_terminal_ms"]
        if v is not None and not _is_ms(v):
            _fail(f"{path}: invalid fault_to_terminal_ms {v!r} at {r['site']} trial {r['trial']}")
        # A control runs its whole soak, so it carries both window stamps and
        # they are ordered; the soak-overrun statistic is computed from them
        # and must not shrink because a stamp went missing. Elsewhere a stamp
        # may be absent (the fault fired before or inside the window) but
        # must parse when present.
        control = r["site"] == "S00"
        t0 = _stamp(path, r, "canary_started_at", required=control and not PARTIAL[0])
        t1 = _stamp(path, r, "canary_completed_at", required=control and not PARTIAL[0])
        if t0 is not None and t1 is not None and t1 < t0:
            _fail(f"{path}: {r['site']} trial {r['trial']} completed its window "
                  "before starting it")
        # Every fault-injected trial reaches a terminal and the harness stamps
        # its fault-to-terminal time; a missing time would silently drop the
        # trial from the recovery cohort, so it is refused, not skipped. A
        # control has no fault instant and must carry none.
        if r["fault_fired"] and v is None and not PARTIAL[0]:
            _fail(f"{path}: {r['site']} trial {r['trial']} fired its fault but "
                  "has no fault_to_terminal_ms")
        if not r["fault_fired"] and v is not None:
            _fail(f"{path}: {r['site']} trial {r['trial']} records a "
                  "fault-to-terminal time without a fault")
        if r["site"] not in SITE_KIND:
            _fail(f"{path}: unknown site {r['site']!r} at trial {r['trial']}")
        if (r["site_kind"], r["site_desc"]) != (SITE_KIND[r["site"]], SITE_DESC[r["site"]]):
            _fail(f"{path}: {r['site']} trial {r['trial']} is labelled "
                  f"{r['site_kind']!r} / {r['site_desc']!r}; the declared site is "
                  f"{SITE_KIND[r['site']]!r} / {SITE_DESC[r['site']]!r}")
        # The switch snapshot is the oracle's second input and is read by the
        # harness's probe inside the faulted process; it is cross-checked here
        # against the declared position of the site, which fixes it: a site
        # before the switch cannot see the new version live, a site at or after
        # it always does.
        if r["fault_fired"] and r["flipped_at_fault"] != (REGION[r["site"]] != "pre-flip"):
            _fail(f"{path}: {r['site']} trial {r['trial']} records flipped_at_fault="
                  f"{r['flipped_at_fault']!r}, which contradicts its position "
                  f"({REGION[r['site']]}) relative to the switch")
    for r in rows:
        exp_status, exp_side = oracle(r["site_kind"], bool(r["fault_fired"]),
                                      bool(r["flipped_at_fault"]))
        exp_live = r["to_version"] if exp_side == "to" else r["from_version"]
        if (r["expected_status"], r["expected_live"]) != (exp_status, exp_live):
            _fail(f"{path}: stored expectation disagrees with the oracle at "
                  f"{r['site']} trial {r['trial']}")
        want = (r["final_status"] == exp_status and r["final_live"] == exp_live)
        if bool(r["coherent"]) != want:
            _fail(f"{path}: stored verdict disagrees with the oracle at "
                  f"{r['site']} trial {r['trial']}")
        if r["site"] == "S00" and r["fault_fired"]:
            _fail(f"{path}: the control fired a fault (trial {r['trial']})")
        if r["site"] != "S00" and not r["fault_fired"]:
            _fail(f"{path}: {r['site']} trial {r['trial']} never reached its "
                  f"injection point; it would be scored as a control")
    by: dict = defaultdict(list)
    for r in rows:
        by[r["site"]].append(r)
    if set(by) != set(REGION):
        _fail(f"{path}: sites {sorted(set(by) ^ set(REGION))} missing or unexpected")
    bad = {s: len(rs) for s, rs in by.items() if len(rs) != TRIALS_PER_SITE}
    if bad and not PARTIAL[0]:
        _fail(f"{path}: trials per site must be {TRIALS_PER_SITE}, got {bad}")
    return by


def validate_grid_run(path, rows: list, trials_per_cell: int = TRIALS_PER_CELL,
                      order: tuple | None = None) -> None:
    """Validate one grid run. ``order`` is the posture sequence the run was
    declared to use (main runs: audit-first then fail-open; the reversed
    runs: the opposite); the records must lie in that order, with increasing
    trial ids and each cell x posture as one contiguous block, so that a
    main-order file staged under the reversed path is refused."""
    if not rows:
        _fail(f"{path}: no records")
    missing = sorted({f for r in rows for f in GRID_FIELDS if f not in r})
    if missing:
        _fail(f"{path}: records lack fields {missing}")
    ids = [r["trial_id"] for r in rows]
    if len(ids) != len(set(ids)):
        _fail(f"{path}: duplicate trial ids")
    if order is not None:
        seq = [r["posture"] for r in rows]
        k = seq.count(order[0])
        if k in (0, len(seq)) or seq != [order[0]] * k + [order[1]] * (len(seq) - k):
            _fail(f"{path}: records are not in the declared posture order "
                  f"({order[0]} then {order[1]})")
        ids_ = [r["trial_id"] for r in rows]
        if any(not isinstance(i, int) or isinstance(i, bool) for i in ids_):
            _fail(f"{path}: trial ids must be integers (not floats, strings or booleans)")
        if any(b <= a for a, b in zip(ids_, ids_[1:], strict=False)):
            _fail(f"{path}: trial ids do not strictly increase in record order")
        keys = [(r["injection"], r["posture"]) for r in rows]
        blocks = [k_ for i, k_ in enumerate(keys) if i == 0 or k_ != keys[i - 1]]
        if len(blocks) != len(set(blocks)):
            _fail(f"{path}: a cell x posture block is split in record order")
    for r in rows:
        if not isinstance(r["consistent"], bool):
            _fail(f"{path}: consistent must be a boolean at {r['trial_id']}")
        for f in ("from_version", "to_version", "expected_live_value", "final_live_value"):
            if not (isinstance(r[f], str) and r[f]):
                _fail(f"{path}: {f} must be a non-empty string at {r['trial_id']}, got {r[f]!r}")
        if r["from_version"] == r["to_version"]:
            _fail(f"{path}: {r['trial_id']} names no version change ({r['from_version']!r})")
        if r["final_job_status"] not in STATUSES:
            _fail(f"{path}: final_job_status must be one of {STATUSES} at {r['trial_id']}, "
                  f"got {r['final_job_status']!r}")
        if r["error_text"] is not None and not isinstance(r["error_text"], str):
            _fail(f"{path}: error_text must be text or null at {r['trial_id']}, "
                  f"got {type(r['error_text']).__name__}")
        for f in ("wall_clock_ms", "rollback_path_ms"):
            v = r.get(f)
            if not _is_ms(v):
                _fail(f"{path}: invalid {f} {v!r} at {r['trial_id']}")
        if r["injection"] not in CELL_IDEAL:
            _fail(f"{path}: unknown cell {r['injection']}")
        exp_status, exp_side = CELL_IDEAL[r["injection"]]
        exp_version = r["from_version"] if exp_side == "from" else r["to_version"]
        if r["expected_live_value"] != exp_version:
            _fail(f"{path}: stored expected live version disagrees with the "
                  f"cell table at {r['trial_id']}")
        want = (r["final_job_status"] == exp_status
                and r["final_live_value"] == exp_version)
        if bool(r["consistent"]) != want:
            _fail(f"{path}: stored verdict disagrees with the oracle at {r['trial_id']}")
        if r["injection"] == "C4" and r.get("second_upgrade_http_status") != 409:
            # C4 is a concurrency cell: the second request must have been
            # rejected, or the first job's outcome proves nothing about it.
            _fail(f"{path}: C4 second request not rejected at {r['trial_id']} "
                  f"(status {r.get('second_upgrade_http_status')!r})")
    shape: dict = defaultdict(int)
    for r in rows:
        shape[(r["injection"], r["posture"])] += 1
    if set(shape) != {(c, p) for c in CELL_IDEAL for p in POSTURES}:
        _fail(f"{path}: cells x postures incomplete: {sorted(shape)}")
    counts = set(shape.values())
    if counts != {trials_per_cell} and not (PARTIAL[0] and len(counts) == 1):
        _fail(f"{path}: every cell x posture must have {trials_per_cell} trials, got {dict(shape)}")


def load(path: Path) -> list[dict]:
    return [json.loads(ln) for ln in path.open() if ln.strip()]


def pct(xs: list[float], q: float) -> float:
    """Nearest-rank percentile: the smallest value at or above rank
    ceil(q/100 * n), 1-indexed (q in [0, 100]). Empty input is an error."""
    if not xs:
        raise ValueError("percentile of an empty sample")
    s = sorted(xs)
    k = max(0, min(len(s) - 1, math.ceil(q / 100 * len(s)) - 1))
    return s[k]


def cluster_boot(groups: list[list[float]], q: float) -> tuple[float, float]:
    """95% interval for the pooled q-th percentile, resampling whole runs."""
    rng = random.Random(SEED)
    stats = []
    for _ in range(BOOT):
        pick = [x for _ in groups for x in rng.choice(groups)]
        stats.append(pct(pick, q))
    return pct(stats, 2.5), pct(stats, 97.5)


def summary(xs: list[float]) -> dict:
    return {"n": len(xs), "p50": pct(xs, 50), "p95": pct(xs, 95),
            "p99": pct(xs, 99), "max": max(xs), "mean": statistics.fmean(xs)}


# --------------------------------------------------------------------------- #
# Exhaustive site sweep                                                       #
# --------------------------------------------------------------------------- #
def strict_x1(r: dict) -> bool:
    """spec conformance, plus the divergence flag of Table 2 whenever the
    specified outcome follows a closure that raised (rb_closure: the revert
    did not happen; rb_mirror: the closure raised after reverting)."""
    if not r["coherent"]:
        return False
    err = r.get("error") or ""
    # The recorded error must carry the injected target's own marker: the
    # trigger fired is not enough on the rollback path.
    if (r["site_kind"] in ("rb_closure", "rb_mirror", "rb_status", "rb_flagged_status")
            and r["fault_fired"] and TARGET_MARKER[r["site_kind"]] not in err):
        return False
    if (r["site_kind"] in ("rb_closure", "rb_mirror", "rb_flagged_status")
            and r["fault_fired"]):
        # S21: the flag must have survived the job wrapper's fallback write
        return MARKER in err
    return True


def analyze_x1() -> dict:
    out: dict = {"sites": {}, "totals": {}}
    for rt in RUNTIMES:
        for posture in POSTURES:
            path = DATA / "site_sweep" / rt / f"{posture}.jsonl"
            if not path.exists():
                continue
            rows = load(path)
            missed = [r for r in rows
                      if r["site_kind"] != "none" and not r["fault_fired"]]
            if missed:
                raise SystemExit(
                    f"{path}: {len(missed)} trials never reached their "
                    f"injection point; they would be scored as controls")
            by = validate_sweep(path, rows, posture)
            for site, rs in by.items():
                outcomes = {(r["final_status"],
                             "to" if r["final_live"] == r["to_version"] else "from")
                            for r in rs}
                exp = {(r["expected_status"],
                        "to" if r["expected_live"] == r["to_version"] else "from")
                       for r in rs}
                ent = out["sites"].setdefault(site, {
                    "desc": rs[0]["site_desc"], "region": REGION[site],
                    "site_kind": rs[0]["site_kind"],
                    # The specification requires a divergence flag exactly
                    # where the rollback closure itself raised.
                    "flagged": rs[0]["site_kind"] in ("rb_closure", "rb_mirror", "rb_flagged_status"),
                    "expected": sorted("/".join(e) for e in exp)})
                ent[f"{rt}|{posture}"] = {
                    "n": len(rs), "fired": sum(r["fault_fired"] for r in rs),
                    "spec": sum(r["coherent"] for r in rs),
                    "strict": sum(strict_x1(r) for r in rs),
                    "observed": sorted("/".join(o) for o in outcomes),
                    "distinct_outcomes": len(outcomes)}
            out["totals"][f"{rt}|{posture}"] = {
                "n": len(rows), "spec": sum(r["coherent"] for r in rows),
                "strict": sum(strict_x1(r) for r in rows),
                "sites_with_mixed_outcomes": sum(
                    1 for rs in by.values()
                    if len({(r["final_status"], r["final_live"] == r["to_version"])
                            for r in rs}) > 1)}
    out["latency"] = x1_latency()
    return out


def x1_latency() -> dict:
    """Fault-to-terminal recovery latency and soak-window overshoot."""
    res: dict = {}
    for rt in RUNTIMES:
        path = DATA / "site_sweep" / rt / "audit-first.jsonl"
        if not path.exists():
            continue
        rows = load(path)
        eligible = [r for r in rows
                    if REGION[r["site"]] in ("provisional", "flip boundary")
                    and r["final_status"] == "rolled_back"]
        rec_rows = [r for r in eligible if r["fault_to_terminal_ms"] is not None]
        if len(rec_rows) != len(eligible) and not PARTIAL[0]:
            _fail(f"{path}: {len(eligible) - len(rec_rows)} recovery-eligible "
                  "trials have no fault-to-terminal time")
        rec = [r["fault_to_terminal_ms"] for r in rec_rows]
        # The sweep is one pass, so the repeated unit is the site, not the
        # run: resample whole sites to put an interval on the percentiles.
        by_site: dict = defaultdict(list)
        for r in rec_rows:
            by_site[r["site"]].append(r["fault_to_terminal_ms"])
        allf = [r["fault_to_terminal_ms"] for r in rows
                if r["fault_to_terminal_ms"] is not None]
        over = []
        n_controls = 0
        for r in rows:
            if r["site"] != "S00":
                continue
            n_controls += 1
            if r["canary_completed_at"] and r["canary_started_at"]:
                t0 = datetime.fromisoformat(r["canary_started_at"])
                t1 = datetime.fromisoformat(r["canary_completed_at"])
                over.append(1000 * ((t1 - t0).total_seconds() - WINDOW_S))
        if len(over) != n_controls and not PARTIAL[0]:
            _fail(f"{path}: {n_controls - len(over)} of {n_controls} controls have no "
                  "soak-window stamps; the overrun cohort would shrink")
        rec_sum = summary(rec) if rec else None
        if rec_sum is not None and len(by_site) > 1:
            groups = list(by_site.values())
            rec_sum["sites"] = len(groups)
            rec_sum["p50_ci"] = cluster_boot(groups, 50)
            rec_sum["p99_ci"] = cluster_boot(groups, 99)
        res[rt] = {
            "fault_to_rolled_back_ms": rec_sum,
            "fault_to_any_terminal_ms": summary(allf) if allf else None,
            "soak_overshoot_ms": summary(over) if over else None,
        }
    return res


# --------------------------------------------------------------------------- #
# Independent runs of the 12-cell grid                                        #
# --------------------------------------------------------------------------- #
def strict_x2(r: dict) -> bool:
    if not r["consistent"]:
        return False
    if r["injection"] in CELL_FAILED_TO:
        return MARKER in (r.get("error_text") or "")
    return True


def analyze_x2() -> dict:
    out: dict = {}
    for rt in RUNTIMES:
        reps = sorted((DATA / "chaos_grid" / rt).glob("run*/raw.jsonl"))
        if not reps:
            continue
        runs = [load(p) for p in reps]
        for rep_path, rows in zip(reps, runs, strict=True):
            validate_grid_run(rep_path, rows, order=POSTURES)
        ent: dict = {"runs": len(runs), "per_run": [], "cells": {}}
        for rows in runs:
            ent["per_run"].append({
                p: {"n": sum(r["posture"] == p for r in rows),
                    "spec": sum(r["consistent"] for r in rows if r["posture"] == p),
                    "strict": sum(strict_x2(r) for r in rows if r["posture"] == p)}
                for p in POSTURES})
        cells = defaultdict(list)
        for rows in runs:
            for r in rows:
                cells[(r["injection"], r["posture"])].append(r)
        for (cell, p), rs in sorted(cells.items()):
            ent["cells"][f"{cell}|{p}"] = {
                "n": len(rs), "spec": sum(r["consistent"] for r in rs),
                "strict": sum(strict_x2(r) for r in rs),
                "distinct_outcomes": len({(r["final_job_status"],
                                           r["final_live_value"] == r["to_version"])
                                          for r in rs}),
                "p95_ms": pct([r["wall_clock_ms"] for r in rs], 95)}
        for p in POSTURES:
            groups = [[r["wall_clock_ms"] for r in rows if r["posture"] == p]
                      for rows in runs]
            pooled = [x for g in groups for x in g]
            s = summary(pooled)
            s["p50_ci"] = cluster_boot(groups, 50)
            s["p95_ci"] = cluster_boot(groups, 95)
            s["p95_per_run_range"] = (min(pct(g, 95) for g in groups),
                                      max(pct(g, 95) for g in groups))
            cell_p95 = {c.split("|")[0]: v["p95_ms"]
                        for c, v in ent["cells"].items() if c.endswith("|" + p)}
            s["cell_p95_min"] = min(cell_p95.values())
            s["cell_p95_max"] = max(cell_p95.values())
            s["cells_over_500ms_p95"] = sum(v > 500 for v in cell_p95.values())
            ent[f"latency|{p}"] = s
        out[rt] = ent
    return out


# --------------------------------------------------------------------------- #
def analyze_reversed() -> dict:
    """Reversed posture order: the same grid with fail-open cells first
    (chaos_grid_reversed/guard-fix/runNN/raw.jsonl)."""
    reps = sorted((DATA / "chaos_grid_reversed" / "guard-fix").glob("run*/raw.jsonl"))
    if not reps:
        return {}
    runs = [load(p) for p in reps]
    for rep_path, rows in zip(reps, runs, strict=True):
        validate_grid_run(rep_path, rows, order=tuple(reversed(POSTURES)))
    out: dict = {"runs": len(runs), "per_run": []}
    for rows in runs:
        out["per_run"].append({
            p: {"n": sum(r["posture"] == p for r in rows),
                "spec": sum(r["consistent"] for r in rows if r["posture"] == p),
                "strict": sum(strict_x2(r) for r in rows if r["posture"] == p)}
            for p in POSTURES})
    for p_ in POSTURES:
        groups = [[r["wall_clock_ms"] for r in rows if r["posture"] == p_]
                  for rows in runs]
        pooled = [x for g in groups for x in g]
        s_ = summary(pooled)
        s_["p95_ci"] = cluster_boot(groups, 95)
        out[f"latency|{p_}"] = s_
    return out


def tables(x1: dict, x2: dict) -> str:
    lines = ["# Production-runtime experiment tables", "",
             "## Exhaustive site sweep (spec / strict coherent out of 50)", "",
             "| Site | Region | Expected | AF base | AF guard-fix | FO base | FO guard-fix |",
             "|---|---|---|---|---|---|---|"]
    for site, e in x1["sites"].items():
        def cell(k: str, ent: dict = e) -> str:
            v = ent.get(k)
            return f"{v['spec']}/{v['strict']}" if v else "-"
        lines.append(f"| {site} {e['desc']} | {e['region']} | {', '.join(e['expected'])} | "
                     f"{cell('base|audit-first')} | {cell('guard-fix|audit-first')} | "
                     f"{cell('base|fail-open')} | {cell('guard-fix|fail-open')} |")
    lines += ["", "Totals: " + json.dumps(x1["totals"]), "",
              "## Chaos grid per-run totals", ""]
    for rt, e in x2.items():
        lines.append(f"### {rt} ({e['runs']} runs)")
        for i, pr in enumerate(e["per_run"], 1):
            lines.append(f"- run {i}: " + ", ".join(
                f"{p} spec {v['spec']}/{v['n']} strict {v['strict']}/{v['n']}"
                for p, v in pr.items()))
        for p in POSTURES:
            lines.append(f"- latency {p}: " + json.dumps(e[f"latency|{p}"]))
    return "\n".join(lines) + "\n"



# The published experiment: anything short of this is an exploratory run and
# must be analysed with --partial, never reported.
INVENTORY = {"sweep_files": 4, "grid_runs": {"base": 10, "guard-fix": 10},
             "reversed_runs": 5}


def check_inventory(x1: dict, x2: dict, x3: dict) -> None:
    n_sweep = len(x1.get("totals", {}))
    if n_sweep != INVENTORY["sweep_files"]:
        _fail(f"inventory: {n_sweep} sweep files, expected {INVENTORY['sweep_files']}")
    for rt, n in INVENTORY["grid_runs"].items():
        got = (x2.get(rt) or {}).get("runs", 0)
        if got != n:
            _fail(f"inventory: {got} grid runs for {rt}, expected {n}")
    if x3.get("runs", 0) != INVENTORY["reversed_runs"]:
        _fail(f"inventory: {x3.get('runs', 0)} reversed runs, expected {INVENTORY['reversed_runs']}")

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--outdir", type=Path, default=OUTDIR)
    ap.add_argument("--partial", action="store_true",
                    help="exploratory run: accept any uniform trial count and skip "
                         "the inventory gate; the output is labelled exploratory")
    args = ap.parse_args()
    outdir = args.outdir
    PARTIAL[0] = args.partial
    outdir.mkdir(parents=True, exist_ok=True)
    x1 = analyze_x1()
    x2 = analyze_x2()
    if not x1["sites"] or not x2:
        raise SystemExit(
            f"no experiment records under {DATA}: nothing to analyse")
    x3 = analyze_reversed()
    if not PARTIAL[0]:
        check_inventory(x1, x2, x3)
    numbers = {"exploratory": PARTIAL[0], "x1": x1, "x2": x2, "x3": x3}
    (outdir / "numbers.json").write_text(json.dumps(numbers, indent=1, default=list))
    (outdir / "tables.md").write_text(tables(x1, x2))
    print(json.dumps(x1["totals"], indent=1))
    for rt, e in x2.items():
        print(rt, e["runs"], "runs", [pr for pr in e["per_run"]][:1])


if __name__ == "__main__":
    main()
