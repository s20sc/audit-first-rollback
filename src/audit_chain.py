"""Audit chain interface.

The audit chain is an append-only log of pipeline events. The
audit-first rule treats the chain as the source of truth: live state
is required to converge to whatever the chain has most recently
committed.

This module defines:

  * :class:`AuditChain` — a structural ``Protocol`` describing the
    minimal interface the audit-first guard requires.
  * :class:`InMemoryAuditChain` — a reference implementation backed
    by a Python list, suitable for the chaos-grid harness.

The chain entry here carries only the job id, the terminal status and a
timestamp. On its own that is not the paper's terminal record: the
divergence flag, the error text and the rollback reason live in the job
record of :class:`job_store.JobStore`, and the terminal record of a job
is the pair (job record, chain entry). A deployment that wants the chain
alone to be the terminal record has to put the from/to versions and the
divergence flag into the appended payload; this reference does not.
A production deployment may back the chain with a durable append-only
store and add fields (signer, content hash, sequence number) without
changing the guard.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from .status import JobStatus


@dataclass(frozen=True)
class AuditRecord:
    """One row in the audit chain.

    The record is deliberately minimal: job id, terminal status and time.
    It identifies the terminal record; the divergence flag, error text and
    rollback reason are on the job record (see the module docstring).
    """

    job_id: str
    status: JobStatus
    timestamp_ns: int = field(default_factory=time.monotonic_ns)


@runtime_checkable
class AuditChain(Protocol):
    """Minimal audit-chain interface required by the audit-first guard.

    The guard only ever calls :meth:`append_terminal` (and inspects
    the result of :meth:`tail` in test paths). Any object satisfying
    this Protocol — including a durable production chain —
    can drop in as the ``audit`` argument to ``run_audit_first``.
    """

    def append_terminal(self, job_id: str, status: JobStatus) -> AuditRecord:
        """Append a terminal record (``PROMOTED`` / ``ROLLED_BACK`` /
        ``FAILED``) for ``job_id`` and return the appended record.

        Implementations MUST be idempotent on repeated appends for the
        same ``(job_id, status)`` pair: re-running the rollback path
        after a partial crash should not produce duplicate terminal
        records.
        """
        ...

    def tail(self, n: int = 1) -> list[AuditRecord]:
        """Return the most recent ``n`` records (most recent last).

        Used by the sign-check verifier to confirm that every chaos
        trial has a single terminal record per job.
        """
        ...


class InMemoryAuditChain:
    """Reference :class:`AuditChain` implementation backed by a list."""

    def __init__(self) -> None:
        self._records: list[AuditRecord] = []

    def append_terminal(self, job_id: str, status: JobStatus) -> AuditRecord:
        existing = [r for r in self._records if r.job_id == job_id]
        if existing and existing[-1].status == status:
            return existing[-1]
        record = AuditRecord(job_id=job_id, status=status)
        self._records.append(record)
        return record

    def tail(self, n: int = 1) -> list[AuditRecord]:
        return self._records[-n:]

    def all_records(self) -> list[AuditRecord]:
        """Return a copy of the full chain (for inspection by tests)."""
        return list(self._records)


__all__ = ["AuditChain", "AuditRecord", "InMemoryAuditChain"]
