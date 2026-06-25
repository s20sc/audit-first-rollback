"""Per-capability locked job store.

The store mediates concurrent upgrades to the same capability. Two
concurrent canary jobs that affect overlapping capabilities must
serialise: while job A holds the lock for capability ``c``, job B
either waits for A's terminal state or is rejected.

This module provides an in-memory :class:`JobStore` with explicit
per-capability ``asyncio.Lock``s and a ``Job`` data record. A
production runtime backs the same interface against a durable
journal; the harness substitutes this in-memory variant so that the
chaos-grid runs in a single Python process.
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
    """In-memory job store with per-capability locks."""

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
        """Apply terminal-state mutations atomically inside the
        per-capability lock.

        Called by the audit-first guard on both the success path
        (``status=PROMOTED``) and the rollback paths (``ROLLED_BACK``
        or ``FAILED``).
        """

        job = self._jobs[job_id]
        async with self.lock_for(job.capability):
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
