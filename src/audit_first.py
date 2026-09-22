"""The audit-first rollback guard pattern.

This module is the **canonical reference** for the audit-first rollback
rule named in the paper. The runnable shape below follows the same guard
structure as the deployment-pipeline canary guard described there; it is
not that runtime, and its contracts are stated here explicitly:

  * ``flip`` may raise before or after it has changed live state; either
    way it raises inside the guard, so the closure runs and the job ends
    ROLLED_BACK (the closure is idempotent). A failure *before* the guard
    opens, which the production sweep calls a before-switch site, is not
    this function's concern: the caller records it as FAILED, unflagged.
  * ``body`` returns on a passed soak and raises on a failed one; the
    runner, not the body, writes the PROMOTED terminal, inside the guard.
  * ``store.update`` and ``audit.append_terminal`` either take effect or
    raise without taking effect. An append that lands and then raises is
    outside this contract; a store that cannot promise it needs per-job
    reconciliation of the terminal record.

The guard's three invariants:

  1. The guard opens *before* the provisional flip and closes only after
     the terminal record, so every write between the two lies inside it.
     A flip is often not atomic (it assigns live state and then writes
     its own audit row); a failure after the assignment must still be
     rolled back. Wrapping the flip is safe because the rollback closure
     is idempotent and restores a snapshot captured before the flip, so
     it changes nothing if the flip never landed.
  2. The rollback closure is called *before* the store update, so the
     terminal record reflects the rollback's outcome rather than
     predicting it.
  3. The FAILED branch's error string preserves both the original
     exception and the rollback exception so the audit chain carries
     the full causal chain. A terminal write that fails after a completed
     revert also ends FAILED, unflagged: live state holds the old version,
     and the record does not claim a rollback it could not write. A
     flagged FAILED write that fails is written once more with the flag
     kept, so the fallback record still says live state may be diverged.
     When a terminal write and its one fallback both raise, no terminal
     record exists; the runner then raises ``TerminalWriteUnavailable``
     rather than return a non-terminal record as if it were terminal.
     That is the case the paper's terminal-write availability assumption
     (its Section 3.6) excludes, and the caller reconciles the record.

The guard distinguishes ``ROLLED_BACK`` (rollback closure ran without
raising) from ``FAILED`` (rollback closure itself raised). See the paper
for the formal statement.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from .audit_chain import AuditChain
from .job_store import Job, JobStore
from .rollback_closure import RollbackClosure
from .status import JobStatus

T = TypeVar("T")


class CapabilityBusy(RuntimeError):
    """A second deployment job asked for a capability whose job is still
    running. Raised before anything is touched: no flip, no record."""


class TerminalWriteUnavailable(RuntimeError):
    """No terminal record could be written: a terminal write and its one
    fallback both raised. This is the case the paper's terminal-write
    availability assumption (Section 3.6) excludes. The job record is left
    as the last successful write left it, which may be non-terminal, and
    live state is wherever the rollback path left it: the old version when
    the revert completed, possibly the new one when it did not. The message
    says which. The caller reconciles the record; the runner does not hand
    back a non-terminal job as a terminal one."""


@dataclass(frozen=True)
class CanaryJob:
    """A canary deployment job.

    The job carries an immutable identifier and the capability /
    version pair being promoted. The mutable state of the job (status,
    error, rollback_reason) lives in the :class:`JobStore`.
    """

    job_id: str
    capability: str
    from_version: str
    to_version: str


async def run_audit_first(
    *,
    job: CanaryJob,
    flip: Callable[[], None],
    body: Callable[[], Awaitable[T]],
    rollback: RollbackClosure,
    store: JobStore,
    audit: AuditChain,
) -> T | Job:
    """Run a canary job under audit-first rollback semantics.

    This is the schematic from the paper written in its plainest
    standalone form. The five callables / objects bind to:

    * ``flip``      — the provisional live-state mutation (paper
                      ``self._promote(...)``); it may raise after it has
                      changed live state.
    * ``body``      — the bounded canary-soak coroutine that may raise
                      (paper ``self._run_canary_body(...)``).
    * ``rollback``  — the rollback closure constructed before ``flip``
                      ran (paper ``self._rollback(...)``).
    * ``store``     — the job store; its per-capability lock is held here
                      for the whole job, from before the flip to after the
                      terminal record, so two jobs on one capability
                      cannot interleave.
    * ``audit``     — the audit chain.

    Returns the body's return value on success, or the job record (with
    its terminal status, error and rollback reason) on either rollback
    path. Never propagates the original exception; the audit chain is the
    channel by which failure is communicated to the operator. Two
    exceptions reach the caller: :class:`CapabilityBusy`, before anything
    is touched, when the capability's job is still running; and
    :class:`TerminalWriteUnavailable` when a terminal write and its one
    fallback both raise, so that no terminal record exists. The latter is
    the terminal-write availability precondition of the paper's
    Section 3.6 being violated; the runner never returns a non-terminal
    record in its place.
    """

    lock = store.lock_for(job.capability)
    if lock.locked():
        raise CapabilityBusy(f"{job.capability}: a job is still running")
    async with lock:
        return await _guarded(job=job, flip=flip, body=body,
                              rollback=rollback, store=store, audit=audit)


async def _guarded(
    *,
    job: CanaryJob,
    flip: Callable[[], None],
    body: Callable[[], Awaitable[T]],
    rollback: RollbackClosure,
    store: JobStore,
    audit: AuditChain,
) -> T | Job:
    # The guard opens before the provisional flip (invariant #1) and closes
    # only after the terminal record: the success terminal is written here,
    # inside the guard, so a failing PROMOTED write is rolled back too.
    try:
        flip()
        result = await body()
        await store.update(job.job_id, status=JobStatus.PROMOTED)
        audit.append_terminal(job.job_id, JobStatus.PROMOTED)
        return result
    except Exception as exc:
        # Rollback closure runs BEFORE the store update (invariant #2).
        try:
            rollback.run()
        except Exception as rb_exc:
            # The revert did not complete: FAILED, flagged, so the record
            # says live state may still hold the new version.
            flagged = f"canary crash + rollback failure: {exc!r}; rb={rb_exc!r}"
            try:
                await store.update(job.job_id, status=JobStatus.FAILED,
                                   error=flagged)
                audit.append_terminal(job.job_id, JobStatus.FAILED)
            except Exception as w_exc:
                # The flagged record did not land. The fallback record must
                # keep the flag (paper: site S21); it is written once more,
                # with the write failure appended, and if that write fails
                # too there is no authoritative terminal record: the runner
                # says so instead of returning the non-terminal job.
                try:
                    await store.update(
                        job.job_id, status=JobStatus.FAILED,
                        error=f"{flagged}; flagged write raised {w_exc!r}")
                    audit.append_terminal(job.job_id, JobStatus.FAILED)
                except Exception as w2_exc:
                    raise TerminalWriteUnavailable(
                        f"{job.job_id}: no terminal record; the flagged FAILED "
                        f"write raised twice ({w_exc!r}; {w2_exc!r}); live state "
                        f"may still hold {job.to_version}") from w2_exc
            return await store.fetch_or_raise(job.job_id)

        try:
            await store.update(
                job.job_id,
                status=JobStatus.ROLLED_BACK,
                rollback_reason=str(exc),
                error=repr(exc),
            )
            audit.append_terminal(job.job_id, JobStatus.ROLLED_BACK)
        except Exception as w_exc:
            # The revert completed but its record did not land. The job
            # wrapper of the runtime writes an unflagged FAILED here: live
            # state holds the old version, and the record must not claim a
            # rollback it could not write. If that write fails too, no
            # terminal record exists and the runner says so.
            try:
                await store.update(
                    job.job_id,
                    status=JobStatus.FAILED,
                    error=f"rolled back, terminal write failed: {exc!r}; w={w_exc!r}",
                )
                audit.append_terminal(job.job_id, JobStatus.FAILED)
            except Exception as w2_exc:
                raise TerminalWriteUnavailable(
                    f"{job.job_id}: no terminal record; the ROLLED_BACK write "
                    f"raised ({w_exc!r}) and the FAILED fallback raised "
                    f"({w2_exc!r}); live state holds {job.from_version}") from w2_exc
        return await store.fetch_or_raise(job.job_id)


async def run_fail_open(
    *,
    job: CanaryJob,
    flip: Callable[[], None],
    body: Callable[[], Awaitable[T]],
    rollback: RollbackClosure,
    store: JobStore,
    audit: AuditChain,
) -> T | Job:
    """The fail-open comparator: the same boundary and body, with the
    rollback call omitted from the except branch. It keeps live state
    wherever the failure left it and records FAILED directly. The signature
    takes ``rollback`` so that both postures are called identically; the
    closure is never run. It takes the same capability lock as the
    audit-first runner, so the two postures are compared under one
    concurrency discipline, and it raises :class:`TerminalWriteUnavailable`
    under the same condition, a FAILED write that raises, since fail-open
    has no fallback record."""

    lock = store.lock_for(job.capability)
    if lock.locked():
        raise CapabilityBusy(f"{job.capability}: a job is still running")
    async with lock:
        return await _fail_open(job=job, flip=flip, body=body, store=store,
                                audit=audit)


async def _fail_open(
    *,
    job: CanaryJob,
    flip: Callable[[], None],
    body: Callable[[], Awaitable[T]],
    store: JobStore,
    audit: AuditChain,
) -> T | Job:
    try:
        flip()
        result = await body()
        await store.update(job.job_id, status=JobStatus.PROMOTED)
        audit.append_terminal(job.job_id, JobStatus.PROMOTED)
        return result
    except Exception as exc:
        try:
            await store.update(
                job.job_id,
                status=JobStatus.FAILED,
                error=f"canary crash, no rollback: {exc!r}",
            )
            audit.append_terminal(job.job_id, JobStatus.FAILED)
        except Exception as w_exc:
            # No fallback record in this posture: a FAILED write that raises
            # leaves no terminal record, and the comparator says so.
            raise TerminalWriteUnavailable(
                f"{job.job_id}: no terminal record; the FAILED write raised "
                f"({w_exc!r})") from w_exc
        return await store.fetch_or_raise(job.job_id)


__all__ = ["CanaryJob", "CapabilityBusy", "JobStatus", "TerminalWriteUnavailable",
           "run_audit_first", "run_fail_open"]  # JobStatus re-exported for convenience
