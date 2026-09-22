"""Job-status enum shared by the audit chain, job store, and guard.

Lives in its own module so that :mod:`src.audit_chain` and
:mod:`src.job_store` can import :class:`JobStatus` without circularly
importing :mod:`src.audit_first` (which itself imports the chain and
store).
"""

from __future__ import annotations

from enum import Enum


class JobStatus(str, Enum):  # noqa: UP042 -- StrEnum needs 3.11; the harness runs on 3.10 too
    """Terminal states a canary job can reach."""

    PROMOTED = "PROMOTED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


__all__ = ["JobStatus"]
