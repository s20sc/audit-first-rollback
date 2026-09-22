"""Mutation probes for the analysis gates: each corruption of the records
must make the analysis refuse, not report a number."""
import importlib.util
import pathlib

import pytest

HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("analyze", HERE.parent / "production" / "analysis" / "analyze.py")
A = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(A)
DATA = HERE.parent / "production" / "data"


def _sweep():
    return A.load(DATA / "site_sweep" / "guard-fix" / "audit-first.jsonl")


def _grid():
    return A.load(DATA / "chaos_grid" / "guard-fix" / "run01" / "raw.jsonl")


def _reversed():
    return A.load(DATA / "chaos_grid_reversed" / "guard-fix" / "run01" / "raw.jsonl")


def test_real_records_pass():
    A.validate_sweep("sweep", _sweep())
    A.validate_grid_run("grid", _grid())


def test_erased_fault_snapshot_is_refused():
    rows = _sweep()
    for r in rows:
        r.pop("flipped_at_fault")
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_expected_rewritten_to_observed_is_refused():
    rows = _sweep()
    r = next(x for x in rows if x["site"] == "S18")
    r["final_status"], r["final_live"] = "rolled_back", r["from_version"]
    r["expected_status"], r["expected_live"] = r["final_status"], r["final_live"]
    r["coherent"] = True
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_missing_site_is_refused():
    rows = [r for r in _sweep() if r["site"] != "S21"]
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_duplicate_grid_ids_are_refused():
    rows = _grid()
    for r in rows:
        r["trial_id"] = "1"
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_forged_grid_verdict_is_refused():
    rows = _grid()
    r = next(x for x in rows if x["posture"] == "fail-open" and x["injection"] == "A1")
    r["consistent"] = True
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_unrejected_second_request_is_refused():
    rows = _grid()
    r = next(x for x in rows if x["injection"] == "C4")
    r["second_upgrade_http_status"] = 200
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_oracle_matches_every_stored_expectation():
    for r in _sweep():
        status, side = A.oracle(r["site_kind"], bool(r["fault_fired"]), bool(r["flipped_at_fault"]))
        live = r["to_version"] if side == "to" else r["from_version"]
        assert (r["expected_status"], r["expected_live"]) == (status, live)

def test_uniformly_shortened_run_is_refused():
    rows = _grid()
    seen = {}
    short = [r for r in rows if seen.setdefault((r["injection"], r["posture"]), 0) < 25 and not seen.__setitem__((r["injection"], r["posture"]), seen[(r["injection"], r["posture"])] + 1)]
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", short)


def test_wrong_file_posture_is_refused():
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", _sweep(), posture="fail-open")


def test_null_snapshot_is_refused():
    rows = _sweep()
    r = next(x for x in rows if x["site"] == "S01")
    r["flipped_at_fault"] = None
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_non_finite_timing_is_refused():
    rows = _grid()
    rows[0]["wall_clock_ms"] = float("nan")
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_missing_recovery_timing_is_refused():
    """Erasing the time of the slowest recovery-eligible trials must not
    quietly shrink the cohort and improve the percentiles."""
    rows = _sweep()
    eligible = sorted((r for r in rows
                       if A.REGION[r["site"]] in ("provisional", "flip boundary")
                       and r["final_status"] == "rolled_back"),
                      key=lambda r: r["fault_to_terminal_ms"], reverse=True)
    for r in eligible[:100]:
        r["fault_to_terminal_ms"] = None
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_timing_on_a_control_is_refused():
    rows = _sweep()
    next(r for r in rows if r["site"] == "S00")["fault_to_terminal_ms"] = 1.0
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_relabelled_site_kind_is_refused():
    """A site's expectation comes from the declared table, not from the
    record's own label: relabelling S19 as a plain status-write site would
    otherwise drop its divergence-flag requirement."""
    rows = _sweep()
    for r in rows:
        if r["site"] == "S19":
            r["site_kind"], r["error"] = "rb_status", "injected rb_status fault"
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_relabelled_site_description_is_refused():
    rows = _sweep()
    for r in rows:
        if r["site"] == "S05":
            r["site_desc"] = "upgrade audit mirror"
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_boolean_grid_timing_is_refused():
    """JSON true/false are ints to Python; a boolean must not pass as a
    measured millisecond value in either grid timing field."""
    for field in ("wall_clock_ms", "rollback_path_ms"):
        for value in (True, False):
            rows = _grid()
            rows[0][field] = value
            with pytest.raises(SystemExit):
                A.validate_grid_run("grid", rows)


def test_boolean_timing_in_a_reversed_run_is_refused():
    rows = _reversed()
    rows[-1]["wall_clock_ms"] = True
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_missing_control_completion_is_refused():
    """Nulling the completion stamp of some controls would shrink the
    soak-overrun cohort without a refusal."""
    rows = _sweep()
    for r in [x for x in rows if x["site"] == "S00"][:10]:
        r["canary_completed_at"] = None
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_all_control_stamps_missing_is_refused():
    rows = _sweep()
    for r in rows:
        if r["site"] == "S00":
            r["canary_started_at"] = r["canary_completed_at"] = None
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_control_completed_before_start_is_refused():
    rows = _sweep()
    r = next(x for x in rows if x["site"] == "S00")
    r["canary_started_at"], r["canary_completed_at"] = r["canary_completed_at"], r["canary_started_at"]
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_naive_control_stamp_is_refused():
    rows = _sweep()
    r = next(x for x in rows if x["site"] == "S00")
    r["canary_completed_at"] = r["canary_completed_at"].replace("+00:00", "")
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_snapshot_contradicting_the_site_position_is_refused():
    """The switch snapshot is self-reported by the probe; it must agree with
    where the declared site sits relative to the switch."""
    rows = _sweep()
    next(r for r in rows if r["site"] == "S02")["flipped_at_fault"] = True
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)
    rows = _sweep()
    r = next(r for r in rows if r["site"] == "S09")
    # keep the stored expectation consistent with the forged snapshot, so
    # that only the position check can catch it
    r["flipped_at_fault"] = False
    r["expected_status"], r["expected_live"] = "failed", r["from_version"]
    r["coherent"] = (r["final_status"] == "failed" and r["final_live"] == r["from_version"])
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_real_runs_pass_their_declared_order():
    A.validate_grid_run("grid", _grid(), order=A.POSTURES)
    A.validate_grid_run("grid", _reversed(), order=tuple(reversed(A.POSTURES)))


def test_main_order_run_under_the_reversed_path_is_refused():
    """A main-order file staged where a reversed run belongs (or the other
    way round) must be refused; the shape of the two is identical."""
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", _grid(), order=tuple(reversed(A.POSTURES)))
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", _reversed(), order=A.POSTURES)


def test_reordered_records_are_refused():
    rows = _grid()
    rows.reverse()
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows, order=A.POSTURES)


def test_null_versions_are_refused():
    rows = _sweep()
    for f in ("from_version", "to_version", "expected_live", "final_live"):
        rows[0][f] = None
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)
    rows = _grid()
    for f in ("from_version", "to_version", "expected_live_value", "final_live_value"):
        rows[0][f] = None
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_coalesced_versions_are_refused():
    rows = _sweep()
    r = rows[0]
    r["to_version"] = r["expected_live"] = r["final_live"] = r["from_version"]
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_non_text_error_is_refused():
    """The strict check searches the error text; a list that contains the
    markers is not the text the harness records."""
    rows = _sweep()
    for r in rows:
        if r["site"] == "S19":
            r["error"] = ["rollback failure", "injected rb_mirror fault"]
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)
    rows = _grid()
    rows[0]["error_text"] = {"text": rows[0]["error_text"]}
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows)


def test_boolean_end_to_end_time_is_refused():
    rows = _sweep()
    rows[0]["end_to_end_ms"] = True
    with pytest.raises(SystemExit):
        A.validate_sweep("sweep", rows)


def test_non_integer_or_non_increasing_ids_are_refused():
    """Ids must be genuine integers that strictly increase: fractional ids
    that coerce to the same integer, numeric strings and booleans are not
    the identifiers the harness emits."""
    for bad in ([1.9, 1.1], ["1", "2"], [True, 2]):
        rows = _grid()
        rows[0]["trial_id"], rows[1]["trial_id"] = bad
        with pytest.raises(SystemExit):
            A.validate_grid_run("grid", rows, order=A.POSTURES)
    rows = _grid()
    rows[1]["trial_id"] = rows[0]["trial_id"]  # equal adjacent ids
    with pytest.raises(SystemExit):
        A.validate_grid_run("grid", rows, order=A.POSTURES)
