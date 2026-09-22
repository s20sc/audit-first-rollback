"""Twelve-cell chaos-grid driver for audit-first vs fail-open.

Three failure classes, four cells each, two postures:

  * Class A (metric-side raise): the canary's metric poll raises after
    the provisional promote. A1-A4 vary *when* it fires (first poll,
    third poll, final poll, post-poll compute).
  * Class B (rollback-internal raise): the metric raises and the
    rollback closure itself then raises. B1-B4 vary the exception type
    (KeyError, RuntimeError, TimeoutError, a cancellation-like error).
  * Class C (audit-write-side raise): a status write raises. C1 fires
    mid-canary, C2 on the success-path write, C3 on the ROLLED_BACK
    write after the closure already reverted live state, C4 is a
    metric raise while a second upgrade request for the same capability
    arrives during the soak (it must be rejected as busy).

For every cell the *audit-first ideal* terminal outcome is fixed by the
construction: a trial is consistent iff its (terminal status, live
version) equals that ideal, i.e. the audit chain truthfully describes
live state. Audit-first reaches its ideal in every cell; fail-open,
which omits the rollback closure, reaches it only where omitting
rollback could not have changed the outcome (the four Class-B cells,
whose rollback would have raised anyway).

Latency is modelled by sleeping one poll interval per canary poll, so a
cell that fails early is fast and one that fails late saturates the
canary window. The numbers are emergent, not fitted: the driver runs
the same 0.3 s window / 0.05 s poll interval as the reference
deployment and reports whatever the soak produces.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass

from src.audit_chain import InMemoryAuditChain
from src.audit_first import CanaryJob, CapabilityBusy, run_audit_first, run_fail_open
from src.job_store import Job, JobStore
from src.rollback_closure import make_rollback_closure
from src.status import JobStatus

from .mocks import ActiveVersionMap, ComputeRaise, MetricRaise, StatusWriter

# Reference-deployment canary config (window 0.3 s, poll every 0.05 s -> 6
# polls). Latency emerges from the per-poll sleep; do not tune to a target.
WINDOW_SECONDS = 0.3
POLL_INTERVAL_SECONDS = 0.05
MAX_POLLS = round(WINDOW_SECONDS / POLL_INTERVAL_SECONDS)  # 6

PRE_VERSION = "1.0.0"
TO_VERSION = "2.0.0"

# Trigger kinds for where a cell's failure originates.
_BODY = "body"               # metric / mid-body raise -> body fails
_COMPUTE = "compute"         # post-poll success-rate compute raises (A4)
_SUCCESS_WRITE = "success"   # body succeeds, the promote write raises (C2)


@dataclass(frozen=True)
class Cell:
    """One cell of the grid: where, when, and how the failure fires."""

    name: str
    failure_class: str          # "A" | "B" | "C"
    capability: str
    polls: int                  # canary polls before the failure (latency)
    trigger: str                # _BODY | _COMPUTE | _SUCCESS_WRITE
    rollback_exc: BaseException | None = None   # Class B
    audit_write_raises: bool = False            # Class C3
    concurrent_upgrade: bool = False            # Class C4: second request in the soak
    ideal_status: str = ""      # audit-first ideal terminal status
    ideal_live: str = ""        # audit-first ideal live version


def _b(name: str, exc: BaseException) -> Cell:
    return Cell(name=name, failure_class="B", capability=f"cap.{name}", polls=1,
                trigger=_BODY, rollback_exc=exc,
                ideal_status="FAILED", ideal_live=TO_VERSION)


CELLS: list[Cell] = [
    # Class A — metric-side raise; rollback succeeds -> ROLLED_BACK/from.
    Cell("A1", "A", "cap.A1", 1, _BODY, ideal_status="ROLLED_BACK",
         ideal_live=PRE_VERSION),
    Cell("A2", "A", "cap.A2", 3, _BODY, ideal_status="ROLLED_BACK",
         ideal_live=PRE_VERSION),
    Cell("A3", "A", "cap.A3", MAX_POLLS, _BODY, ideal_status="ROLLED_BACK",
         ideal_live=PRE_VERSION),
    Cell("A4", "A", "cap.A4", 3, _COMPUTE, ideal_status="ROLLED_BACK",
         ideal_live=PRE_VERSION),
    # Class B — rollback closure itself raises -> FAILED/to (cannot revert).
    _b("B1", KeyError("rollback raised KeyError")),
    _b("B2", RuntimeError("rollback raised RuntimeError")),
    _b("B3", TimeoutError("rollback raised TimeoutError")),
    _b("B4", RuntimeError("rollback cancelled (CancelledError, rerouted)")),
    # Class C — audit-write-side raise.
    Cell("C1", "C", "cap.C1", 2, _BODY, ideal_status="ROLLED_BACK",
         ideal_live=PRE_VERSION),
    Cell("C2", "C", "cap.C2", MAX_POLLS, _SUCCESS_WRITE,
         ideal_status="ROLLED_BACK", ideal_live=PRE_VERSION),
    Cell("C3", "C", "cap.C3", MAX_POLLS, _BODY, audit_write_raises=True,
         ideal_status="FAILED", ideal_live=PRE_VERSION),
    Cell("C4", "C", "cap.C4", 1, _BODY, concurrent_upgrade=True,
         ideal_status="ROLLED_BACK", ideal_live=PRE_VERSION),
]

ALL_CELLS = [c.name for c in CELLS]
ALL_POSTURES = ["audit-first", "fail-open"]


@dataclass
class TrialRecord:
    trial_id: str
    posture: str
    cell: str
    failure_class: str
    capability: str
    from_version: str
    to_version: str
    final_job_status: str
    final_live_value: str | None
    expected_status: str
    expected_live_value: str
    consistent: bool
    audit_chain_last_record: str
    wall_clock_ms: int
    rollback_path_ms: int
    error_text: str
    concurrent_request: str = ""   # C4: "rejected" | "accepted" | ""


class _WriterBackedChain(InMemoryAuditChain):
    """Audit chain whose terminal append also goes through the mocked
    status writer, so an armed status-write fault fires on the write the
    guard performs."""

    def __init__(self, writer: StatusWriter) -> None:
        super().__init__()
        self._writer = writer

    def append_terminal(self, job_id: str, status: JobStatus) -> None:
        self._writer.write(status.value)
        super().append_terminal(job_id, status)


async def run_trial(*, posture: str, cell: Cell, trial_index: int,
                    sleep: bool = True) -> TrialRecord:
    """Run one chaos trial of ``cell`` under ``posture``.

    ``audit-first`` runs the rollback closure on any provisional-region
    failure before writing the terminal record; ``fail-open`` omits the
    rollback closure and records FAILED directly, leaving live state
    wherever the failure left it. Both share the same mocks and timing.
    """

    cap = cell.capability
    active = ActiveVersionMap()
    active.promote(cap, PRE_VERSION)
    writer = StatusWriter()
    # Terminal appends go through the status writer, so a cell that arms a
    # status-write fault (C3) reaches the write the guard actually makes.
    audit = _WriterBackedChain(writer)
    store = JobStore()
    job_id = f"{cell.name}:{posture}:{trial_index}"
    store.insert(Job(job_id=job_id, capability=cap,
                     from_version=PRE_VERSION, to_version=TO_VERSION))

    # Rollback closure captures the pre-flip snapshot (initiation
    # obligation). Class B arms the revert to raise.
    if cell.rollback_exc is not None:
        active.arm_revert_raise(cell.rollback_exc)
    if cell.audit_write_raises:
        writer.arm_raise_on("ROLLED_BACK")
    if cell.trigger == _SUCCESS_WRITE:
        writer.arm_raise_on("PROMOTED")
    rb = make_rollback_closure(
        capability=cap, pre_version=PRE_VERSION,
        revert_fn=lambda snap: active.revert(snap.capability, snap.pre_version),
    )

    t0 = time.monotonic()
    rollback_start = 0.0
    error_text = ""
    concurrent = {"outcome": ""}

    async def second_request() -> None:
        # C4: a second upgrade of the same capability arrives while the
        # first job holds the capability lock. The reference runner must
        # reject it before touching anything.
        job2 = f"{job_id}:concurrent"
        store.insert(Job(job_id=job2, capability=cap,
                         from_version=PRE_VERSION, to_version="3.0.0"))
        cjob2 = CanaryJob(job_id=job2, capability=cap,
                          from_version=PRE_VERSION, to_version="3.0.0")
        try:
            await guard(job=cjob2, flip=lambda: active.promote(cap, "3.0.0"),
                        body=_never, rollback=rb, store=store, audit=audit)
        except CapabilityBusy:
            concurrent["outcome"] = "rejected"
        else:
            concurrent["outcome"] = "accepted"

    # Canary soak: one poll per interval (latency is emergent).
    async def soak() -> None:
        for i in range(cell.polls):
            if i == 0 and cell.concurrent_upgrade:
                await second_request()
            if sleep:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
            # Class A/B/C1/C3/C4 raise from the metric poll at the last
            # scheduled poll; A4 raises from the post-poll compute step.
            if i == cell.polls - 1 and cell.trigger == _BODY:
                raise MetricRaise(f"{cell.name}: metric poll raised")
        if cell.trigger == _COMPUTE:
            raise ComputeRaise(f"{cell.name}: post-poll compute raised")

    async def body():
        await soak()
        # The runner writes the PROMOTED terminal after a passed soak; the
        # Class C2 cell arms that write to raise (see the writer below).
        return "promoted"

    def flip() -> None:
        active.promote(cap, TO_VERSION)

    # Both postures run the guard from src/audit_first.py: the grid is a
    # regression test of the reference implementation, not a second copy
    # of it. The postures differ only in which entry point is called.
    guard = run_audit_first if posture == "audit-first" else run_fail_open
    cjob = CanaryJob(job_id=job_id, capability=cap,
                     from_version=PRE_VERSION, to_version=TO_VERSION)
    rollback_start = time.monotonic()
    await guard(job=cjob, flip=flip, body=body, rollback=rb, store=store,
                audit=audit)
    job_now = await store.fetch_or_raise(job_id)
    final_status = job_now.status
    error_text = job_now.error or ""

    t1 = time.monotonic()
    await store.update(job_id, status=final_status)

    final_live = active.view(cap)
    audit_last = audit.tail(1)[0].status.value if audit.tail(1) else "NONE"
    consistent = (final_status.value == cell.ideal_status
                  and final_live == cell.ideal_live
                  and (not cell.concurrent_upgrade
                       or concurrent["outcome"] == "rejected"))
    wall_ms = max(1, round((t1 - t0) * 1000))
    rb_ms = (max(1, round((t1 - rollback_start) * 1000))
             if rollback_start else 0)

    return TrialRecord(
        trial_id=job_id, posture=posture, cell=cell.name,
        failure_class=cell.failure_class, capability=cap,
        from_version=PRE_VERSION, to_version=TO_VERSION,
        final_job_status=final_status.value, final_live_value=final_live,
        expected_status=cell.ideal_status, expected_live_value=cell.ideal_live,
        consistent=consistent, audit_chain_last_record=audit_last,
        wall_clock_ms=wall_ms, rollback_path_ms=rb_ms, error_text=error_text,
        concurrent_request=concurrent["outcome"],
    )


async def _never() -> None:
    raise AssertionError("a rejected second request must not run its body")


def store_write_failed(writer: StatusWriter, audit: InMemoryAuditChain,
                       job_id: str) -> None:
    """Write a FAILED terminal (rollback-itself-raised path)."""
    writer.write(JobStatus.FAILED.value)
    audit.append_terminal(job_id, JobStatus.FAILED)


def cell_by_name(name: str) -> Cell:
    for c in CELLS:
        if c.name == name:
            return c
    raise KeyError(f"unknown cell: {name}")


def trial_record_to_dict(rec: TrialRecord) -> dict:
    return asdict(rec)


__all__ = [
    "ALL_CELLS", "ALL_POSTURES", "CELLS", "Cell", "TrialRecord",
    "cell_by_name", "run_trial", "trial_record_to_dict",
    "WINDOW_SECONDS", "POLL_INTERVAL_SECONDS", "MAX_POLLS",
    "PRE_VERSION", "TO_VERSION",
]
