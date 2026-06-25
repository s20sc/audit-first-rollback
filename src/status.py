"""Job-status enum shared by the audit chain, job store, and guard.

Lives in its own module so that :mod:`src.audit_chain` and
:mod:`src.job_store` can import :class:`JobStatus` without circularly
importing :mod:`src.audit_first` (which itself imports the chain and
store).
"""

from __future__ import annotations

from enum import Enum


class JobStatus(str, Enum):
    """Terminal states a canary job can reach."""

    PROMOTED = "PROMOTED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


__all__ = ["JobStatus"]
