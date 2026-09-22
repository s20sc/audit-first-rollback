"""Smoke tests for the audit-first guard.

These tests verify the three structural invariants from the paper
hold against the reference implementation:

  T1. The guard opens before the provisional flip, so a flip that fails
      after changing live state is rolled back like any other failure.
  T2. The rollback closure runs before the store update on the exception
      path.
  T3. The FAILED branch's error string preserves both the original
      exception and the rollback exception.
"""

from __future__ import annotations

import asyncio

import pytest

from src.audit_chain import InMemoryAuditChain
from src.audit_first import CanaryJob, CapabilityBusy, JobStatus, run_audit_first, run_fail_open
from src.job_store import Job, JobStore
from src.rollback_closure import make_rollback_closure


def _setup(rollback_raises: bool = False, flip_raises: str | None = None):
    """Build a fresh harness for one trial."""
    store = JobStore()
    audit = InMemoryAuditChain()
    cap, pre, new = "skill.demo", "v0", "v1"
    live = {"version": pre}

    job_record = Job(job_id="j1", capability=cap, from_version=pre, to_version=new)
    store.insert(job_record)

    cjob = CanaryJob(job_id="j1", capability=cap, from_version=pre, to_version=new)

    def flip() -> None:
        # flip_raises: "before" fails ahead of the live assignment,
        # "after" fails in the flip's own audit write once live state
        # has already changed (a non-atomic flip).
        if flip_raises == "before":
            raise OSError("flip failed before assignment")
        live["version"] = new
        if flip_raises == "after":
            raise OSError("flip audit write failed")

    def revert(snap) -> None:
        if rollback_raises:
            raise RuntimeError("simulated B")
        live["version"] = snap.pre_version

    rb = make_rollback_closure(capability=cap, pre_version=pre, revert_fn=revert)
    return cjob, flip, rb, store, audit, live


def test_success_path_promotes_live_state() -> None:
    cjob, flip, rb, store, audit, live = _setup()

    async def body() -> str:
        return "ok"

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v1"  # body succeeded, no rollback
    assert not rb.has_run


def test_failure_path_rolls_back_and_audits() -> None:
    """T2: rollback closure runs BEFORE the audit terminal append."""
    cjob, flip, rb, store, audit, live = _setup()

    async def body() -> str:
        raise ValueError("simulated A")

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v0"  # rolled back successfully
    assert rb.has_run
    tail = audit.tail(1)
    assert tail and tail[0].status is JobStatus.ROLLED_BACK


def test_failure_with_rollback_raise_records_failed() -> None:
    """T3: FAILED branch preserves both exceptions in the error string."""
    cjob, flip, rb, store, audit, live = _setup(rollback_raises=True)

    async def body() -> str:
        raise ValueError("simulated A")

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    tail = audit.tail(1)
    assert tail and tail[0].status is JobStatus.FAILED
    job_now = asyncio.run(store.fetch_or_raise("j1"))
    assert "simulated A" in (job_now.error or "")
    assert "simulated B" in (job_now.error or "")


def test_body_sees_flipped_state() -> None:
    """The body runs against the flipped state, and a body failure reverts it."""
    cjob, flip, rb, store, audit, live = _setup()

    async def body() -> str:
        # Inspecting state inside body should see the post-flip value.
        assert live["version"] == "v1"
        raise ValueError("trigger rollback")

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v0"  # rolled back


def test_flip_failure_after_assignment_rolls_back() -> None:
    """T1: a non-atomic flip that fails after changing live state must end
    ROLLED_BACK with the old version live, not FAILED with the new one."""
    cjob, flip, rb, store, audit, live = _setup(flip_raises="after")

    async def body() -> str:
        return "unreachable"

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v0"
    assert rb.has_run
    tail = audit.tail(1)
    assert tail and tail[0].status is JobStatus.ROLLED_BACK


def test_flip_failure_before_assignment_is_harmless() -> None:
    """T1: the closure is idempotent, so wrapping a flip that never landed
    leaves the old version live and records ROLLED_BACK."""
    cjob, flip, rb, store, audit, live = _setup(flip_raises="before")

    async def body() -> str:
        return "unreachable"

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v0"
    tail = audit.tail(1)
    assert tail and tail[0].status is JobStatus.ROLLED_BACK


def test_terminal_write_failure_after_revert_records_failed() -> None:
    """A revert that completed, followed by a ROLLED_BACK write that raises,
    ends FAILED with the old version live (Table 2, FAILED unflagged)."""
    cjob, flip, rb, store, audit, live = _setup()

    class RaiseOnRolledBack(InMemoryAuditChain):
        def append_terminal(self, job_id, status):
            if status is JobStatus.ROLLED_BACK:
                raise OSError("terminal write failed")
            super().append_terminal(job_id, status)

    audit = RaiseOnRolledBack()

    async def body() -> str:
        raise ValueError("simulated A")

    asyncio.run(
        run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v0"          # the revert did happen
    tail = audit.tail(1)
    assert tail and tail[0].status is JobStatus.FAILED
    job_now = asyncio.run(store.fetch_or_raise("j1"))
    assert "terminal write failed" in (job_now.error or "")
    assert "rollback failure" not in (job_now.error or "")  # not the flagged case


def test_closure_retries_after_a_failed_revert() -> None:
    """A revert that raised leaves the closure armed, so a retry runs it
    again; a revert that returned is not repeated."""
    from src.rollback_closure import make_rollback_closure

    live = {"version": "v1"}
    calls = {"n": 0}

    def revert(snap) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient")
        live["version"] = snap.pre_version

    rb = make_rollback_closure(capability="c", pre_version="v0", revert_fn=revert)
    with pytest.raises(OSError):
        rb.run()
    assert not rb.has_run and rb.attempts == 1 and live["version"] == "v1"
    rb.run()
    assert rb.has_run and rb.attempts == 2 and live["version"] == "v0"
    rb.run()
    assert rb.attempts == 2                 # no repeat after success


def test_fail_open_keeps_the_new_version_live() -> None:
    """The comparator shares the boundary and body and only omits the
    rollback call."""
    cjob, flip, rb, store, audit, live = _setup()

    async def body() -> str:
        raise ValueError("simulated A")

    asyncio.run(
        run_fail_open(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit
        )
    )
    assert live["version"] == "v1"          # not reverted
    assert not rb.has_run
    tail = audit.tail(1)
    assert tail and tail[0].status is JobStatus.FAILED


def test_grid_runs_the_reference_guard() -> None:
    """The twelve-cell grid must exercise src.audit_first, not a copy of it:
    breaking the guard has to break the grid."""
    import harness.chaos_grid as grid

    called = {"n": 0}
    original = grid.run_audit_first

    async def counting(**kwargs):
        called["n"] += 1
        return await original(**kwargs)

    grid.run_audit_first = counting
    try:
        rec = asyncio.run(grid.run_trial(posture="audit-first",
                                         cell=grid.cell_by_name("A1"),
                                         trial_index=0, sleep=False))
    finally:
        grid.run_audit_first = original
    assert called["n"] == 1
    assert rec.consistent


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_second_request_for_a_running_capability_is_rejected() -> None:
    """While a job holds the capability lock (from before the flip to after
    the terminal record), a second request for the same capability is
    rejected before it touches anything."""
    cjob, flip, rb, store, audit, live = _setup()
    gate = asyncio.Event()

    async def body() -> str:
        await gate.wait()
        audit.append_terminal("j1", JobStatus.PROMOTED)
        return "promoted"

    async def scenario():
        first = asyncio.create_task(run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
        await asyncio.sleep(0)                      # first job is in its soak
        assert live["version"] == "v1"
        store.insert(Job(job_id="j2", capability=cjob.capability,
                         from_version="v0", to_version="v2"))
        cjob2 = CanaryJob(job_id="j2", capability=cjob.capability,
                          from_version="v0", to_version="v2")
        touched = []
        with pytest.raises(CapabilityBusy):
            await asyncio.wait_for(run_audit_first(
                job=cjob2, flip=lambda: touched.append("flip"), body=body,
                rollback=rb, store=store, audit=audit), timeout=1.0)
        assert touched == []                         # nothing was touched
        gate.set()
        await first
        assert live["version"] == "v1"

    asyncio.run(scenario())


def test_store_update_does_not_take_the_capability_lock() -> None:
    """The runner holds the capability lock for the whole job and calls
    store.update inside it; update must therefore not re-take the lock."""
    cjob, flip, rb, store, audit, live = _setup()

    async def scenario():
        async with store.lock_for(cjob.capability):
            await asyncio.wait_for(
                store.update("j1", status=JobStatus.ROLLED_BACK), timeout=1.0)

    asyncio.run(scenario())
    assert asyncio.run(store.fetch_or_raise("j1")).status is JobStatus.ROLLED_BACK


def test_flagged_write_failure_keeps_the_flag() -> None:
    """When the closure raised and the flagged FAILED write itself fails
    once, the fallback record still carries the divergence flag
    (paper: site S21)."""
    cjob, flip, rb, store, audit, live = _setup(rollback_raises=True)
    calls = {"failed_writes": 0}
    real_update = store.update

    async def update(job_id, **kw):
        if kw.get("status") is JobStatus.FAILED:
            calls["failed_writes"] += 1
            if calls["failed_writes"] == 1:
                raise OSError("flagged FAILED write unavailable once")
        return await real_update(job_id, **kw)

    store.update = update  # type: ignore[method-assign]

    async def body() -> str:
        raise ValueError("simulated A")

    asyncio.run(run_audit_first(
        job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
    assert live["version"] == "v1"                       # the revert did not happen
    job_now = asyncio.run(store.fetch_or_raise("j1"))
    assert job_now.status is JobStatus.FAILED
    assert "rollback failure" in (job_now.error or "")   # the flag survived
    assert "unavailable once" in (job_now.error or "")   # and the write failure is recorded
    assert calls["failed_writes"] == 2


def test_success_terminal_is_written_by_the_runner() -> None:
    """A passed soak ends PROMOTED in both the store and the audit chain, and
    that write is the runner's, inside the guard."""
    cjob, flip, rb, store, audit, live = _setup()

    async def body() -> str:
        return "ok"

    out = asyncio.run(run_audit_first(
        job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
    assert out == "ok"
    assert asyncio.run(store.fetch_or_raise("j1")).status is JobStatus.PROMOTED
    assert audit.tail(1)[0].status is JobStatus.PROMOTED
    assert live["version"] == "v1"


def test_failed_promoted_write_is_rolled_back() -> None:
    """The success terminal is written inside the guard: if that write
    raises, the closure runs and the job ends ROLLED_BACK (grid cell C2)."""
    cjob, flip, rb, store, audit, live = _setup()

    class RaiseOnPromoted(InMemoryAuditChain):
        def append_terminal(self, job_id, status):
            if status is JobStatus.PROMOTED:
                raise OSError("PROMOTED write failed")
            super().append_terminal(job_id, status)

    audit = RaiseOnPromoted()

    async def body() -> str:
        return "ok"

    asyncio.run(run_audit_first(
        job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
    assert live["version"] == "v0"
    job_now = asyncio.run(store.fetch_or_raise("j1"))
    assert job_now.status is JobStatus.ROLLED_BACK
    assert "PROMOTED write failed" in (job_now.error or "")


def _store_refusing_terminals(store: JobStore) -> None:
    """Make every terminal write raise: the paper's terminal-write
    availability assumption, violated on purpose."""
    real_update = store.update

    async def update(job_id, **kw):
        if kw.get("status") in (JobStatus.PROMOTED, JobStatus.ROLLED_BACK, JobStatus.FAILED):
            raise OSError("terminal store unavailable")
        return await real_update(job_id, **kw)

    store.update = update  # type: ignore[method-assign]


def test_no_terminal_write_after_a_completed_revert_raises() -> None:
    """When the ROLLED_BACK write and the FAILED fallback both raise, the
    revert has still happened but no terminal record exists; the runner
    raises rather than return the non-terminal job as a terminal one."""
    from src.audit_first import TerminalWriteUnavailable

    cjob, flip, rb, store, audit, live = _setup()
    _store_refusing_terminals(store)

    async def body() -> str:
        raise ValueError("simulated A")

    with pytest.raises(TerminalWriteUnavailable) as info:
        asyncio.run(run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
    assert live["version"] == "v0"                       # the revert did happen
    assert audit.tail(1) == []                           # and no terminal row was written
    job_now = asyncio.run(store.fetch_or_raise("j1"))
    assert job_now.status not in (JobStatus.ROLLED_BACK, JobStatus.FAILED)
    assert "holds v0" in str(info.value)


def test_no_terminal_write_after_a_failed_revert_raises() -> None:
    """The flagged FAILED write raising twice leaves the new version live
    and no record that says so; the runner raises and names the version."""
    from src.audit_first import TerminalWriteUnavailable

    cjob, flip, rb, store, audit, live = _setup(rollback_raises=True)
    _store_refusing_terminals(store)

    async def body() -> str:
        raise ValueError("simulated A")

    with pytest.raises(TerminalWriteUnavailable) as info:
        asyncio.run(run_audit_first(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
    assert live["version"] == "v1"                       # the revert did not happen
    assert audit.tail(1) == []
    assert "may still hold v1" in str(info.value)


def test_fail_open_without_a_terminal_write_raises() -> None:
    from src.audit_first import TerminalWriteUnavailable

    cjob, flip, rb, store, audit, live = _setup()
    _store_refusing_terminals(store)

    async def body() -> str:
        raise ValueError("simulated A")

    with pytest.raises(TerminalWriteUnavailable):
        asyncio.run(run_fail_open(
            job=cjob, flip=flip, body=body, rollback=rb, store=store, audit=audit))
    assert live["version"] == "v1" and audit.tail(1) == []
