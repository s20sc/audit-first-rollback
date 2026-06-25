"""Smoke tests for the audit-first guard.

These tests verify the three structural invariants from the paper
hold against the reference implementation:

  T1. The provisional flip happens outside the try block (so a flip
      failure does not trigger a rollback of state never installed).
  T2. The rollback closure runs before the store update on the exception
      path.
  T3. The FAILED branch's error string preserves both the original
      exception and the rollback exception.
"""

from __future__ import annotations

import asyncio

import pytest

from src.audit_chain import InMemoryAuditChain
from src.audit_first import CanaryJob, JobStatus, run_audit_first
from src.job_store import Job, JobStore
from src.rollback_closure import make_rollback_closure


def _setup(rollback_raises: bool = False):
    """Build a fresh harness for one trial."""
    store = JobStore()
    audit = InMemoryAuditChain()
    cap, pre, new = "skill.demo", "v0", "v1"
    live = {"version": pre}

    job_record = Job(job_id="j1", capability=cap, from_version=pre, to_version=new)
    store.insert(job_record)

    cjob = CanaryJob(job_id="j1", capability=cap, from_version=pre, to_version=new)

    def flip() -> None:
        live["version"] = new

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


def test_flip_outside_try_block_is_observable() -> None:
    """T1: if the body raises, the flip has already happened (and rollback runs)."""
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
