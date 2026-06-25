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

Production deployments will substitute a durable append-only store
(SQLite, Postgres WAL, or a content-addressed object store). The
:class:`AuditChain` protocol is exactly what the guard needs and
nothing more; a durable chain may carry additional fields (signer
public key, content hash, sequence number) without affecting the
audit-first rule.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from .status import JobStatus


@dataclass(frozen=True)
class AuditRecord:
    """One row in the audit chain.

    The minimal record carries enough information for the audit-first
    rule's correctness argument: the job's identity, the terminal
    status that was finally written, and a monotonic timestamp.
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
