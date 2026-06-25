"""Rollback closure factory.

A rollback closure is a no-argument callable that reverts live state
to the most recent committed state's snapshot. The factory constructs
the closure *before* the provisional flip runs, capturing the
pre-transition snapshot it will need. The closure is then attached to
the job and runs synchronously on any caught exception.

The closure is intentionally minimal: one bound method, one snapshot
dict, no I/O on construction. This keeps the construction cheap (it
runs in the hot path of every canary deployment) and the runtime
behaviour predictable (no surprise external dependencies).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class Snapshot:
    """The captured pre-transition view of live state.

    For the canary scenario in the paper, this is the
    capability's active version before the provisional promote runs.
    """

    capability: str
    pre_version: str


class RollbackClosure:
    """A bound rollback closure.

    Constructed *before* the provisional flip mutates live state, so
    that the closure carries the pre-flip snapshot it needs even if
    everything afterwards goes wrong.

    The closure exposes a single :meth:`run` method which the
    audit-first guard invokes synchronously on any caught exception.
    The closure is best-effort: it may itself raise, in which case the
    guard records ``FAILED`` rather than ``ROLLED_BACK``.
    """

    def __init__(
        self,
        snapshot: Snapshot,
        revert_fn: Callable[[Snapshot], None],
    ) -> None:
        """Bind ``snapshot`` and the unparameterised ``revert_fn``.

        ``revert_fn`` should take the snapshot and apply whatever
        live-state mutations are needed to restore the pre-flip view.
        In the harness, that means writing the pre-version back to the
        in-memory active-version map; in a production runtime, it
        means the same write against the durable store.
        """

        self._snapshot = snapshot
        self._revert_fn = revert_fn
        self._has_run = False

    @property
    def snapshot(self) -> Snapshot:
        return self._snapshot

    @property
    def has_run(self) -> bool:
        return self._has_run

    def run(self) -> None:
        """Invoke the bound revert function with the captured snapshot.

        Idempotent on repeat invocations: a second :meth:`run` call
        becomes a no-op. This is defensive — the audit-first guard
        only calls ``run`` once per exception, but a crash between the
        rollback call and the store update could see the same closure
        re-attached and re-run on restart.
        """

        if self._has_run:
            return
        self._has_run = True
        self._revert_fn(self._snapshot)


def make_rollback_closure(
    capability: str,
    pre_version: str,
    revert_fn: Callable[[Snapshot], None],
) -> RollbackClosure:
    """Construct a :class:`RollbackClosure` from a (capability, version)
    pair plus the bound revert function. Convenience wrapper for the
    harness; production code may bypass this and construct directly.
    """

    snapshot = Snapshot(capability=capability, pre_version=pre_version)
    return RollbackClosure(snapshot=snapshot, revert_fn=revert_fn)


__all__ = ["RollbackClosure", "Snapshot", "make_rollback_closure"]
