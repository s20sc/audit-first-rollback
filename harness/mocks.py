"""In-process mocks for the chaos-grid harness.

These mocks isolate the audit-first rollback rule from production
plumbing so the twelve-cell grid runs in a single Python process with
no external dependencies. They model exactly three failure surfaces:

  * the metrics provider (the canary success-rate source),
  * the live-state active-version map (and its rollback closure), and
  * the audit / status writer.

A reference deployment substitutes a durable runtime for all three; the
audit-first rule and the per-cell consistency outcomes are independent
of that substitution, which is the whole point of the standalone model.
"""

from __future__ import annotations


class MetricRaise(RuntimeError):
    """Raised by the mock metrics provider to model a Class-A failure."""


class ComputeRaise(RuntimeError):
    """Raised by the post-poll success-rate compute step (Class A4)."""


class ActiveVersionMap:
    """In-memory live-state surface: capability -> active version.

    ``promote`` is the provisional flip; ``revert`` is what the rollback
    closure calls. A Class-B cell arms ``revert`` to raise.
    """

    def __init__(self) -> None:
        self._versions: dict[str, str] = {}
        self._revert_exc: BaseException | None = None

    def arm_revert_raise(self, exc: BaseException) -> None:
        self._revert_exc = exc

    def promote(self, capability: str, version: str) -> None:
        self._versions[capability] = version

    def revert(self, capability: str, version: str) -> None:
        if self._revert_exc is not None:
            raise self._revert_exc
        self._versions[capability] = version

    def view(self, capability: str) -> str | None:
        return self._versions.get(capability)


class StatusWriter:
    """Mock status/audit writer that can raise on a chosen terminal.

    A Class-C3 cell arms it to raise when the ROLLED_BACK status is
    written (after the rollback closure has already reverted live
    state), modelling an audit-write-side fault on the success path.
    """

    def __init__(self) -> None:
        self._raise_on: str | None = None
        self.writes: list[str] = []

    def arm_raise_on(self, status: str) -> None:
        self._raise_on = status

    def write(self, status: str) -> None:
        if self._raise_on is not None and status == self._raise_on:
            # One-shot: the fallback FAILED write must still succeed.
            self._raise_on = None
            raise OSError(f"status-write fault on {status}")
        self.writes.append(status)


__all__ = ["ActiveVersionMap", "StatusWriter", "MetricRaise", "ComputeRaise"]
