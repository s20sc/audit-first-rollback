"""The audit-first rollback guard pattern.

This module is the **canonical reference** for the audit-first rollback
rule named in the paper. The runnable shape below is behaviourally
equivalent to the deployment-pipeline canary guard described there.

The guard's three invariants ("three properties of this pattern matter
for auditability"):

  1. The provisional flip happens *outside* the try block, so a failure
     in the flip itself cannot trigger a rollback of state that was
     never installed.
  2. The rollback closure is called *before* the store update, so the
     terminal record reflects the rollback's outcome rather than
     predicting it.
  3. The FAILED branch's error string preserves both the original
     exception and the rollback exception so the audit chain carries
     the full causal chain.

The guard distinguishes ``ROLLED_BACK`` (rollback closure ran without
raising) from ``FAILED`` (rollback closure itself raised). See the paper
for the formal statement.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

from .audit_chain import AuditChain
from .job_store import JobStore
from .rollback_closure import RollbackClosure
from .status import JobStatus

T = TypeVar("T")


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
) -> T | None:
    """Run a canary job under audit-first rollback semantics.

    This is the schematic from the paper written in its plainest
    standalone form. The five callables / objects bind to:

    * ``flip``      — the provisional live-state mutation (paper
                      ``self._promote(...)``).
    * ``body``      — the bounded canary-soak coroutine that may raise
                      (paper ``self._run_canary_body(...)``).
    * ``rollback``  — the rollback closure constructed before ``flip``
                      ran (paper ``self._rollback(...)``).
    * ``store``     — the per-capability-locked job store.
    * ``audit``     — the audit chain.

    Returns the body's return value on success, or whatever the store
    returns for the terminal fetch on either rollback path. Never
    propagates the original exception; the audit chain is the channel
    by which failure is communicated to the operator.
    """

    # Provisional flip lives OUTSIDE the try block (invariant #1).
    flip()
    try:
        return await body()
    except Exception as exc:
        # Rollback closure runs BEFORE the store update (invariant #2).
        try:
            rollback.run()
        except Exception as rb_exc:
            await store.update(
                job.job_id,
                status=JobStatus.FAILED,
                error=(
                    f"canary crash + rollback failure: "
                    f"{exc!r}; rb={rb_exc!r}"
                ),
            )
            audit.append_terminal(job.job_id, JobStatus.FAILED)
            return await store.fetch_or_raise(job.job_id)

        await store.update(
            job.job_id,
            status=JobStatus.ROLLED_BACK,
            rollback_reason=str(exc),
        )
        audit.append_terminal(job.job_id, JobStatus.ROLLED_BACK)
        return await store.fetch_or_raise(job.job_id)


__all__ = ["CanaryJob", "JobStatus", "run_audit_first"]  # JobStatus re-exported for convenience
