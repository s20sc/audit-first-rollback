"""Job records and per-capability locks.

The store holds one mutable :class:`Job` record per deployment job and
hands out one ``asyncio.Lock`` per capability. It does not serialise jobs
by itself: the runner (:func:`audit_first.run_audit_first`) takes the
capability's lock before the provisional flip and releases it after the
terminal record, so two jobs on the same capability cannot interleave, and
a second request that arrives while the lock is held is rejected with
:class:`audit_first.CapabilityBusy`. Field updates in :meth:`JobStore.update`
contain no ``await`` and are therefore atomic under asyncio; they take no
lock, so the runner can call them while it holds the capability lock.

A production runtime may back the same interface against a durable
journal and take the same per-capability lock around the whole job. The
runtime evaluated in the paper keeps its job records in memory; the harness
uses this in-memory variant so that the chaos grid runs in a single Python
process.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from .status import JobStatus


@dataclass
class Job:
    """A mutable canary-job record.

    Mutated by :meth:`JobStore.update` along the rollback / promote
    paths. Distinct from the immutable :class:`audit_first.CanaryJob`,
    which captures only the identifying tuple.
    """

    job_id: str
    capability: str
    from_version: str
    to_version: str
    status: JobStatus | None = None
    error: str | None = None
    rollback_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class JobStore:
    """In-memory job store; per-capability locks are held by the runner."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def lock_for(self, capability: str) -> asyncio.Lock:
        """Return (creating if missing) the per-capability lock."""
        if capability not in self._locks:
            self._locks[capability] = asyncio.Lock()
        return self._locks[capability]

    def insert(self, job: Job) -> None:
        """Insert a new job record (no concurrency check at this layer)."""
        self._jobs[job.job_id] = job

    async def update(
        self,
        job_id: str,
        *,
        status: JobStatus | None = None,
        error: str | None = None,
        rollback_reason: str | None = None,
    ) -> Job:
        """Apply a status mutation to the job record.

        Called by the guard on the success path (``status=PROMOTED``) and
        on the rollback paths (``ROLLED_BACK`` or ``FAILED``). The update
        has no suspension point, so it is atomic under asyncio, and it
        takes no lock: the caller already holds the capability lock for
        the whole job (see :func:`audit_first.run_audit_first`).
        """

        job = self._jobs[job_id]
        if status is not None:
            job.status = status
        if error is not None:
            job.error = error
        if rollback_reason is not None:
            job.rollback_reason = rollback_reason
        return job

    async def fetch_or_raise(self, job_id: str) -> Job:
        """Return the current job record; raise ``KeyError`` if absent."""
        if job_id not in self._jobs:
            raise KeyError(f"unknown job: {job_id}")
        return self._jobs[job_id]


__all__ = ["Job", "JobStore"]
