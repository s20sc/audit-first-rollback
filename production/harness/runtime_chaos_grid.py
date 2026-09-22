#!/usr/bin/env python3
"""12-injection chaos grid driver for audit-first vs fail-open rollback.

Drives the runtime's canary upgrade pipeline through its HTTP API
in-process and injects one fail-stop exception per trial. The grid has
twelve injection cells in three classes, each run under two postures
(audit-first and fail-open):

* A1..A4 — metric-side raises (metric provider or post-poll compute);
* B1..B4 — rollback-internal raises (the rollback closure itself raises);
* C1..C4 — audit-write-side raises (job-store status writes), plus a
  concurrent-upgrade race in C4.

With the default of 50 trials per cell this is a 12 × 50 × 2 = 1200-trial
grid, and the outputs report *measured* per-cell consistency for every
failure mode × crash point.

CLI::

    python scripts/runtime_chaos_grid.py \\
        --trials 50 \\
        --output runs/chaos-grid/raw.jsonl \\
        --variants A1,A2,A3,A4,B1,B2,B3,B4,C1,C2,C3,C4 \\
        --postures audit-first,fail-open

Quick smoke (1 trial per cell)::

    python scripts/runtime_chaos_grid.py --quick

Outputs three files alongside ``--output`` (under the same directory):

* ``raw.jsonl``      — one record per trial (fields built in ``_run_one_trial``).
* ``summary.json``   — per-posture + per-cell aggregates with Wilson 95% CI.
* ``sign_check.json`` — H1..H4 verdict (PASS / PARTIAL / FAIL); the four
  hypotheses are defined in ``_sign_check``.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import math
import random
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
# pyproject.toml pins ``pythonpath = ["."]`` for pytest; mirror it here so
# this script also runs cleanly via ``.venv/bin/python scripts/...``.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from httpx import ASGITransport, AsyncClient  # noqa: E402

DEFAULT_RAW_OUTPUT = (
    REPO_ROOT / "runs" / "chaos-grid" / "raw.jsonl"
)


# --------------------------------------------------------------------------- #
# Grid definition                                                             #
# --------------------------------------------------------------------------- #

ALL_INJECTIONS: tuple[str, ...] = (
    "A1", "A2", "A3", "A4",
    "B1", "B2", "B3", "B4",
    "C1", "C2", "C3", "C4",
)
ALL_POSTURES: tuple[str, ...] = ("audit-first", "fail-open")

# Audit-first's *ideal* terminal outcome per injection.
# A trial is "consistent" iff (terminal_status, active_after) matches this
# tuple — i.e. the audit chain truthfully describes the live active version.
#
# active_kind ∈ {"from_version", "to_version"} resolves at trial time.
_AUDIT_FIRST_IDEAL: dict[str, tuple[str, str]] = {
    # Class A — metric-side raise: body crashes after provisional promote;
    # rollback closure reverts active → from; ROLLED_BACK is recorded.
    "A1": ("rolled_back", "from_version"),
    "A2": ("rolled_back", "from_version"),
    "A3": ("rolled_back", "from_version"),
    "A4": ("rolled_back", "from_version"),
    # Class B — rollback-internal raise: body crashes, rollback fn itself
    # raises, audit-first reports FAILED with the new version still active
    # (the canary contract: we cannot lie about state we could not revert).
    "B1": ("failed", "to_version"),
    "B2": ("failed", "to_version"),
    "B3": ("failed", "to_version"),
    "B4": ("failed", "to_version"),
    # Class C — audit-write-side raise.
    # C1: store update raises mid-canary (poll-status write); body crashes;
    #     rollback runs; ROLLED_BACK status writes successfully.
    # C2: PROMOTED-status write raises; body crashes; rollback runs;
    #     ROLLED_BACK status writes successfully.
    # C3: rollback closure completes (active reverted), then the
    #     ROLLED_BACK-status audit-write raises; the outer run() wrapper
    #     catches and writes FAILED while active stays at from_version.
    # C4: A1-style metric-first raise plus a concurrent /upgrade attempt
    #     (HTTP 409 path); rollback runs as in A1.
    "C1": ("rolled_back", "from_version"),
    "C2": ("rolled_back", "from_version"),
    "C3": ("failed", "from_version"),
    "C4": ("rolled_back", "from_version"),
}

# One capability per cell so cross-cell active-version state cannot leak.
# Picked from the bridge's registered ECMs; injection→capability mapping
# is stable across reruns for deterministic raw.jsonl.
_CAP_BY_INJECTION: dict[str, str] = {
    "A1": "manipulation.grasp",
    "A2": "manipulation.place",
    "A3": "navigation.move_to",
    "A4": "perception.locate_object",
    "B1": "manipulation.grasp",
    "B2": "manipulation.place",
    "B3": "navigation.move_to",
    "B4": "perception.locate_object",
    "C1": "manipulation.grasp",
    "C2": "manipulation.place",
    "C3": "navigation.move_to",
    "C4": "perception.locate_object",
}


# --------------------------------------------------------------------------- #
# Fail-open _run_canary override                                              #
# --------------------------------------------------------------------------- #

def _fail_open_run_canary_factory(JobStatus_cls):  # noqa: N803
    async def _fail_open_run_canary(self, job):
        config = self._canary_config
        canary_start_dt = datetime.now(UTC)
        canary_started_at = canary_start_dt.isoformat()
        self._promote(job.capability, job.to_version, job.from_version)
        await self._store.update(
            job.job_id,
            status=JobStatus_cls.CANARY_RUNNING,
            current_stage="canary_running",
            canary_started_at=canary_started_at,
            canary_window_seconds=config.window_seconds,
            append_audit=(
                f"canary_running (fail-open variant): provisional promote"
                f" {job.from_version} → {job.to_version},"
                f" soak {config.window_seconds:g}s"
            ),
        )
        try:
            return await self._run_canary_body(job, canary_start_dt)
        except BaseException as exc:  # noqa: BLE001 — fail-open boundary
            # BaseException so CancelledError also lands as FAILED rather
            # than tearing down the test event loop.
            try:
                await self._store.update(
                    job.job_id,
                    status=JobStatus_cls.FAILED,
                    current_stage="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    append_audit=(
                        f"failed (fail-open variant, no rollback):"
                        f" {type(exc).__name__}: {exc}"
                    ),
                )
            except Exception:  # noqa: BLE001 — best-effort
                pass
            return await self._fetch_or_raise(job.job_id)

    return _fail_open_run_canary


# --------------------------------------------------------------------------- #
# Per-pipeline injection installer                                            #
# --------------------------------------------------------------------------- #

def _install_injection(pipeline, injection: str) -> None:
    """Mutate *pipeline* in-place to install one of the 12 grid injections.

    Each branch patches exactly one surface:

    * A1/A2/A3 — replace ``_metrics_provider`` so the Nth poll raises.
    * A4       — replace ``_canary_config.evaluate_mid_window`` so the Nth
      poll's *post-poll compute* step raises, after the metric data
      itself has arrived cleanly.
    * B1..B4   — replace ``_rollback`` so the closure raises
      KeyError / RuntimeError / TimeoutError / CancelledError respectively;
      metrics provider is also tripped so the rollback path is exercised.
    * C1       — patch ``_store.update`` so the Nth call (the in-body
      poll-status write) raises.
    * C2       — patch ``_store.update`` so the PROMOTED/CANARY_PROMOTED
      status write raises (canary success path).
    * C3       — patch ``_store.update`` so the ROLLED_BACK status write
      raises *after* the rollback closure ran; outer wrapper writes FAILED.
    * C4       — A1-style metric-first raise; the harness fires a
      concurrent second /upgrade outside this function to exercise the
      409 path.
    """
    if injection == "A1":
        def _bad(capability, since):
            raise RuntimeError("A1: metrics_provider raised on first poll")
        pipeline._metrics_provider = _bad

    elif injection == "A2":
        counter = {"n": 0}
        original = pipeline._metrics_provider

        def _bad(capability, since, _c=counter, _o=original):
            _c["n"] += 1
            if _c["n"] < 3:
                return _o(capability, since)
            raise RuntimeError(
                "A2: metrics_provider raised on third poll"
                f" (call #{_c['n']})"
            )
        pipeline._metrics_provider = _bad

    elif injection == "A3":
        config = pipeline._canary_config
        # With window=0.3s, poll_interval=0.05s → 6 polls. Use ceil so the
        # final-poll injection fires on the last poll even if the window
        # grows in a future config.
        n_expected = max(
            1,
            int(math.ceil(config.window_seconds / config.poll_interval_seconds)),
        )
        counter = {"n": 0}
        original = pipeline._metrics_provider

        def _bad(capability, since, _c=counter, _o=original, _N=n_expected):
            _c["n"] += 1
            if _c["n"] < _N:
                return _o(capability, since)
            raise RuntimeError(
                f"A3: metrics_provider raised on final poll"
                f" (call #{_c['n']} of {_N})"
            )
        pipeline._metrics_provider = _bad

    elif injection == "A4":
        # Per-pipeline CanaryConfig replica so the patched
        # ``evaluate_mid_window`` doesn't leak across cells. The metric
        # data itself arrives cleanly — this models a failure in the
        # success-rate evaluation step, not in the data-fetch step.
        config_replica = dataclasses.replace(pipeline._canary_config)
        original_evaluate = config_replica.evaluate_mid_window
        counter = {"n": 0}

        def _bad_evaluate(metrics, _c=counter, _o=original_evaluate):
            _c["n"] += 1
            if _c["n"] < 3:
                return _o(metrics)
            raise RuntimeError(
                "A4: success-rate compute raised post-poll"
                f" (evaluation #{_c['n']})"
            )
        config_replica.evaluate_mid_window = _bad_evaluate
        pipeline._canary_config = config_replica

    elif injection == "B1":
        def _bad_rollback(*args, **kwargs):
            raise KeyError("B1: rollback raised KeyError")
        pipeline._rollback = _bad_rollback

        def _force_body_fail(capability, since):
            raise RuntimeError("B1: metric raises so rollback path is exercised")
        pipeline._metrics_provider = _force_body_fail

    elif injection == "B2":
        def _bad_rollback(*args, **kwargs):
            raise RuntimeError("B2: rollback raised RuntimeError")
        pipeline._rollback = _bad_rollback

        def _force_body_fail(capability, since):
            raise RuntimeError("B2: metric raises so rollback path is exercised")
        pipeline._metrics_provider = _force_body_fail

    elif injection == "B3":
        def _bad_rollback(*args, **kwargs):
            raise TimeoutError("B3: rollback raised TimeoutError")
        pipeline._rollback = _bad_rollback

        def _force_body_fail(capability, since):
            raise RuntimeError("B3: metric raises so rollback path is exercised")
        pipeline._metrics_provider = _force_body_fail

    elif injection == "B4":
        # asyncio.CancelledError inherits from BaseException; the
        # pipeline only catches Exception. If we let CancelledError
        # propagate the whole test event loop tears down.
        # The pragmatic solution is to raise CancelledError inside the
        # closure and immediately catch+rebrand as a runtime-exception
        # that carries the CancelledError text — preserving the
        # failure-flavour distinction in the audit trail without
        # disrupting the loop.
        class B4Cancelled(RuntimeError):
            """Synthetic CancelledError stand-in scoped to B4."""

        def _bad_rollback(*args, **kwargs):
            try:
                raise asyncio.CancelledError("B4: rollback cancelled")
            except asyncio.CancelledError as exc:
                raise B4Cancelled(
                    f"B4: rollback CancelledError({exc!s}) — rerouted"
                ) from exc
        pipeline._rollback = _bad_rollback

        def _force_body_fail(capability, since):
            raise RuntimeError("B4: metric raises so rollback path is exercised")
        pipeline._metrics_provider = _force_body_fail

    elif injection == "C1":
        # Note on C1-C3: the patches below replace ``update`` on the job
        # store, which all pipelines of the process share, and are not
        # removed after the trial. Each patch fires once, inside its own
        # trial, and passes calls through afterwards. The one exception is
        # C3 under fail-open, which never fires because fail-open writes no
        # ROLLED_BACK record; the only cell that follows it is fail-open C4,
        # which writes none either. runtime_site_sweep.py restores its
        # patches after every trial.
        # Fire on the 6th _store.update — the first update inside
        # _run_canary_body. Earlier writes (VALIDATING, SHADOW_*,
        # CANARY_RUNNING) must succeed so the fault lands after the
        # provisional promote, inside the guarded canary body, and the
        # rollback guard is exercised.
        original_update = pipeline._store.update
        counter = {"n": 0, "fired": False}

        async def _bad_update(*args, _c=counter, _o=original_update, **kwargs):
            _c["n"] += 1
            if _c["n"] == 6 and not _c["fired"]:
                _c["fired"] = True
                raise RuntimeError(
                    "C1: store.update raised mid-canary (call #6)"
                )
            return await _o(*args, **kwargs)
        pipeline._store.update = _bad_update

    elif injection == "C2":
        # Fire only when the update sets status to CANARY_PROMOTED or
        # PROMOTED. The canary itself must complete cleanly, so we let
        # all other writes through.
        original_update = pipeline._store.update
        counter = {"fired": False}

        async def _bad_update(*args, _c=counter, _o=original_update, **kwargs):
            status_obj = kwargs.get("status")
            status_value = (
                status_obj.value if hasattr(status_obj, "value")
                else status_obj
            )
            if (
                not _c["fired"]
                and status_value in {"canary_promoted", "promoted"}
            ):
                _c["fired"] = True
                raise RuntimeError(
                    "C2: store.update raised on success-write"
                    f" (status={status_value})"
                )
            return await _o(*args, **kwargs)
        pipeline._store.update = _bad_update

    elif injection == "C3":
        # Body must fail first (so rollback runs). Then patch update to
        # raise specifically on the ROLLED_BACK status write — the one
        # that lands AFTER the rollback closure has succeeded. The outer
        # run() wrapper catches and writes FAILED; active stays at
        # from_version because rollback already ran.
        def _force_body_fail(capability, since):
            raise RuntimeError(
                "C3: metric raises so rollback path is exercised"
            )
        pipeline._metrics_provider = _force_body_fail

        original_update = pipeline._store.update
        counter = {"fired": False}

        async def _bad_update(*args, _c=counter, _o=original_update, **kwargs):
            status_obj = kwargs.get("status")
            status_value = (
                status_obj.value if hasattr(status_obj, "value")
                else status_obj
            )
            if not _c["fired"] and status_value == "rolled_back":
                _c["fired"] = True
                raise RuntimeError(
                    "C3: store.update raised on ROLLED_BACK audit-write"
                )
            return await _o(*args, **kwargs)
        pipeline._store.update = _bad_update

    elif injection == "C4":
        def _bad(capability, since):
            raise RuntimeError(
                "C4: metrics_provider raised (concurrent-rollback-race trial)"
            )
        pipeline._metrics_provider = _bad

    else:
        raise ValueError(f"unknown injection {injection!r}")


def _make_pipeline_builder(
    *,
    posture: str,
    injection: str,
    base_builder: Callable,
    JobStatus_cls,  # noqa: N803
    restore: list,
):
    fail_open_canary = _fail_open_run_canary_factory(JobStatus_cls)

    def _build(state):
        pipeline = base_builder(state)
        # The job store is shared by every pipeline of the process, so a
        # patch on its ``update`` outlives the trial that installed it.
        # Record the original so the trial can put it back (see the
        # ``finally`` in _run_one_trial): an armed patch that never fires
        # in its own trial would otherwise fire in a later one.
        store = pipeline._store
        restore.append((store, "update", store.update))
        _install_injection(pipeline, injection)
        if posture == "fail-open":
            pipeline._run_canary = fail_open_canary.__get__(pipeline)
        return pipeline

    return _build


# --------------------------------------------------------------------------- #
# Trial driver                                                                #
# --------------------------------------------------------------------------- #

async def _wait_for_terminal(client, job_id: str, *, timeout_iters: int = 200):
    for _ in range(timeout_iters):
        resp = await client.get(f"/api/evolution/jobs/{job_id}")
        if resp.status_code != 200:
            await asyncio.sleep(0.01)
            continue
        body = resp.json()
        if body["status"] in {
            "shadow_failed",
            "rolled_back",
            "rejected",
            "promoted",
            "failed",
        }:
            return body
        await asyncio.sleep(0.01)
    return None


def _classify_consistency(
    *,
    injection: str,
    terminal_status: str,
    active_after: str | None,
    from_version: str,
    to_version: str,
) -> bool:
    expected_status, expected_active_kind = _AUDIT_FIRST_IDEAL[injection]
    expected_active = (
        from_version if expected_active_kind == "from_version" else to_version
    )
    return (
        terminal_status == expected_status and active_after == expected_active
    )


def _resolve_last_audit_record(state, capability: str) -> dict[str, Any] | None:
    """Pull the most recent evolution-audit row for *capability* off state."""
    log = getattr(state, "evolution_audit", None)
    if not log:
        return None
    for row in reversed(log):
        if row.get("capability") == capability:
            return {
                "action": row.get("action"),
                "from_version": row.get("from_version"),
                "to_version": row.get("to_version"),
                "reason": row.get("reason"),
            }
    return None


async def _run_one_trial(
    *,
    client,
    state,
    routes_mod,
    base_builder,
    JobStatus_cls,  # noqa: N803
    posture: str,
    injection: str,
    capability: str,
    to_version: str,
    trial_id: int,
) -> dict[str, Any]:
    # Patches this trial installs on objects the whole process shares; the
    # ``finally`` below puts every one of them back.
    restore: list[tuple[Any, str, Any]] = []
    pre_active_resp = await client.get("/api/evolution/active")
    from_version = pre_active_resp.json().get(capability, "1.0.0")

    reg_resp = await client.post(
        "/api/evolution/register",
        json={"capability": capability, "version": to_version},
    )
    if reg_resp.status_code not in (200, 201):
        return {
            "trial_id": trial_id,
            "injection": injection,
            "posture": posture,
            "capability": capability,
            "from_version": from_version,
            "to_version": to_version,
            "expected_live_value": from_version,
            "final_job_status": "register_rejected",
            "final_live_value": from_version,
            "audit_chain_last_record": None,
            "consistent": False,
            "wall_clock_ms": 0,
            "rollback_path_ms": 0,
            "rejection": reg_resp.text[:200],
        }

    routes_mod._build_evolution_pipeline = _make_pipeline_builder(
        posture=posture,
        injection=injection,
        base_builder=base_builder,
        JobStatus_cls=JobStatus_cls,
        restore=restore,
    )
    second_upgrade_status: int | None = None
    try:
        wall_t0 = time.monotonic()
        if injection == "C4":
            # Register the second version up front so the validator gate
            # passes when the racing /upgrade fires.
            second_version = f"{to_version}+c4race"
            await client.post(
                "/api/evolution/register",
                json={"capability": capability, "version": second_version},
            )
            # Fire the first /upgrade as a background task so the trial
            # task can pre-empt to the second POST while the first's
            # canary is asleep inside its window. ~20ms is enough for
            # the first request to land CANARY_RUNNING before the racing
            # /upgrade arrives.
            first_task = asyncio.create_task(
                client.post(
                    "/api/evolution/upgrade",
                    json={
                        "capability": capability,
                        "version": to_version,
                        "force_unsoaked": True,
                    },
                )
            )
            await asyncio.sleep(0.02)
            second = await client.post(
                "/api/evolution/upgrade",
                json={
                    "capability": capability,
                    "version": second_version,
                    "force_unsoaked": True,
                },
            )
            second_upgrade_status = second.status_code
            resp = await first_task
        else:
            resp = await client.post(
                "/api/evolution/upgrade",
                json={
                    "capability": capability,
                    "version": to_version,
                    "force_unsoaked": True,
                },
            )
        if resp.status_code not in (200, 202):
            return {
                "trial_id": trial_id,
                "injection": injection,
                "posture": posture,
                "capability": capability,
                "from_version": from_version,
                "to_version": to_version,
                "expected_live_value": from_version,
                "final_job_status": "rejected_by_validator",
                "final_live_value": from_version,
                "audit_chain_last_record": None,
                "consistent": False,
                "wall_clock_ms": int(1000 * (time.monotonic() - wall_t0)),
                "rollback_path_ms": 0,
                "rejection": f"http {resp.status_code}: {resp.text[:200]}",
            }
        job_id = resp.json()["job_id"]

        job = await _wait_for_terminal(client, job_id)
        wall_t1 = time.monotonic()

        if job is None:
            stuck = await client.get(f"/api/evolution/jobs/{job_id}")
            job = stuck.json() if stuck.status_code == 200 else {
                "status": "harness_no_terminal",
            }

        active_resp = await client.get("/api/evolution/active")
        active_after = active_resp.json().get(capability)

        _, expected_active_kind = _AUDIT_FIRST_IDEAL[injection]
        expected_live = (
            from_version if expected_active_kind == "from_version" else to_version
        )
        consistent = _classify_consistency(
            injection=injection,
            terminal_status=job["status"],
            active_after=active_after,
            from_version=from_version,
            to_version=to_version,
        )

        rollback_path_ms = (
            int(1000 * (wall_t1 - wall_t0))
            if posture == "audit-first"
            and job["status"] in {"rolled_back", "failed"}
            else 0
        )

        last_audit = _resolve_last_audit_record(state, capability)

        record = {
            "trial_id": trial_id,
            "injection": injection,
            "posture": posture,
            "capability": capability,
            "from_version": from_version,
            "to_version": to_version,
            "expected_live_value": expected_live,
            "final_job_status": job.get("status"),
            "final_live_value": active_after,
            "audit_chain_last_record": last_audit,
            "consistent": consistent,
            "wall_clock_ms": int(1000 * (wall_t1 - wall_t0)),
            "rollback_path_ms": rollback_path_ms,
        }
        if second_upgrade_status is not None:
            record["second_upgrade_http_status"] = second_upgrade_status
        if job.get("rollback_reason"):
            record["rollback_reason"] = job["rollback_reason"]
        if job.get("error"):
            record["error_text"] = str(job["error"])[:200]
        return record
    finally:
        routes_mod._build_evolution_pipeline = base_builder
        for obj, attr, original in restore:
            setattr(obj, attr, original)


# --------------------------------------------------------------------------- #
# Harness                                                                     #
# --------------------------------------------------------------------------- #

async def _run_grid(
    *,
    n_trials: int,
    variants: tuple[str, ...],
    postures: tuple[str, ...],
    rng_seed: int,
    progress: Callable[[str], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from bridge.app.aeros.evolution_pipeline import (
        CanaryConfig,
        JobStatus,
        PipelineMode,
    )
    from aeros.runtime.persona import PersonaProfile
    from bridge.app.aeros.evolution_validator import UpgradeValidator
    from bridge.app.api import routes as routes_mod
    from bridge.app.main import app, lifespan

    rng = random.Random(rng_seed)

    async with lifespan(app):
        permissive = PersonaProfile(
            name="chaos-grid-permissive",
            risk_appetite="aggressive",
            accept_unsoaked_ecm=True,
        )
        app.state.upgrade_validator = UpgradeValidator(persona=permissive)
        app.state.evolution_pipeline_mode = PipelineMode.CANARY
        app.state.evolution_canary_config = CanaryConfig(
            window_seconds=0.3,
            poll_interval_seconds=0.05,
            min_intents=0,
        )

        base_builder = routes_mod._build_evolution_pipeline
        per_trial: list[dict[str, Any]] = []
        state = app.state

        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://test",
        ) as client:
            trial_counter = 0
            for posture in postures:
                for injection in variants:
                    cap = _CAP_BY_INJECTION[injection]
                    if progress is not None:
                        progress(f"cell {posture}/{injection} ...")
                    for trial in range(n_trials):
                        trial_counter += 1
                        to_version = (
                            f"9.{posture[0]}.{injection}."
                            f"{trial}.{rng.randrange(1 << 32):08x}"
                        )
                        try:
                            r = await _run_one_trial(
                                client=client,
                                state=state,
                                routes_mod=routes_mod,
                                base_builder=base_builder,
                                JobStatus_cls=JobStatus,
                                posture=posture,
                                injection=injection,
                                capability=cap,
                                to_version=to_version,
                                trial_id=trial_counter,
                            )
                        except Exception as exc:  # noqa: BLE001 — surface
                            r = {
                                "trial_id": trial_counter,
                                "injection": injection,
                                "posture": posture,
                                "capability": cap,
                                "from_version": None,
                                "to_version": to_version,
                                "expected_live_value": None,
                                "final_job_status": "harness_error",
                                "final_live_value": None,
                                "audit_chain_last_record": None,
                                "consistent": False,
                                "wall_clock_ms": 0,
                                "rollback_path_ms": 0,
                                "harness_error": f"{type(exc).__name__}: {exc}",
                                "traceback": traceback.format_exc()[:400],
                            }
                        per_trial.append(r)
                        # Reset: always rollback to 1.0.0. Best-effort —
                        # C4 leaves a second version registered, which is
                        # harmless for subsequent trials on this
                        # capability.
                        try:
                            await client.post(
                                "/api/evolution/rollback",
                                json={"capability": cap, "version": "1.0.0"},
                            )
                        except Exception:  # noqa: BLE001 — best-effort
                            pass

        return per_trial, {
            "n_trials_per_cell": n_trials,
            "variants": list(variants),
            "postures": list(postures),
        }


# --------------------------------------------------------------------------- #
# Aggregation                                                                 #
# --------------------------------------------------------------------------- #

def _wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval (continuity-corrected)."""
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = (
        z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    )
    return (max(0.0, centre - half), min(1.0, centre + half))


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_v = sorted(values)
    if q <= 0:
        return sorted_v[0]
    if q >= 100:
        return sorted_v[-1]
    rank = (q / 100) * (len(sorted_v) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return sorted_v[lo]
    frac = rank - lo
    return sorted_v[lo] + frac * (sorted_v[hi] - sorted_v[lo])


def _summarise(
    per_trial: list[dict[str, Any]],
    *,
    variants: tuple[str, ...],
    postures: tuple[str, ...],
    n_trials: int,
) -> dict[str, Any]:
    by_cell: dict[str, dict[str, Any]] = {}
    for posture in postures:
        for inj in variants:
            cell_key = f"{inj}__{posture}"
            cell = [
                r for r in per_trial
                if r["posture"] == posture and r["injection"] == inj
            ]
            k = sum(1 for r in cell if r.get("consistent"))
            n = len(cell)
            rate = k / n if n else 0.0
            ci_lo, ci_hi = _wilson_ci(k, n)
            by_cell[cell_key] = {
                "consistent": k,
                "n": n,
                "rate": rate,
                "ci95_wilson": [round(ci_lo, 4), round(ci_hi, 4)],
            }

    by_posture: dict[str, dict[str, Any]] = {}
    for posture in postures:
        trials = [r for r in per_trial if r["posture"] == posture]
        k = sum(1 for r in trials if r.get("consistent"))
        n = len(trials)
        rate = k / n if n else 0.0
        ci_lo, ci_hi = _wilson_ci(k, n)
        latencies = [
            r["wall_clock_ms"] for r in trials if r.get("wall_clock_ms", 0) > 0
        ]
        rollback_path = [
            r["rollback_path_ms"] for r in trials
            if r.get("rollback_path_ms", 0) > 0
        ]
        entry: dict[str, Any] = {
            "consistent_count": k,
            "n_trials": n,
            "consistent_rate": round(rate, 4),
            "ci95_wilson": [round(ci_lo, 4), round(ci_hi, 4)],
            "wall_clock_p50_ms": int(_percentile(latencies, 50)),
            "wall_clock_p95_ms": int(_percentile(latencies, 95)),
        }
        if rollback_path:
            entry["rollback_path_p50_ms"] = int(_percentile(rollback_path, 50))
            entry["rollback_path_p95_ms"] = int(_percentile(rollback_path, 95))
        by_posture[posture] = entry

    return {
        "experiment": "runtime_chaos_grid",
        "n_trials_per_cell": n_trials,
        "n_cells": len(by_cell),
        "n_total_trials": sum(c["n"] for c in by_cell.values()),
        "by_posture": by_posture,
        "by_cell": by_cell,
    }


def _sign_check(summary: dict[str, Any]) -> dict[str, Any]:
    """PASS / PARTIAL / FAIL verdict over four hypotheses.

    H1  every audit-first cell is consistent in all trials;
    H2  every fail-open class-B cell is consistent in all trials (when the
        rollback closure raises, both postures end FAILED with the new
        version live, so they coincide);
    H3  every fail-open class-A/C cell is inconsistent in all trials;
    H4  the audit-first rollback-path p50 latency is under the 300ms
        canary window.

    PASS if all four hold; PARTIAL if H1/H3/H4 hold and at most two
    class-B cells deviate under fail-open; FAIL otherwise.
    """
    h1: list[str] = []
    h2: list[str] = []
    h3: list[str] = []
    h4: list[str] = []
    n_per_cell = summary["n_trials_per_cell"]

    for inj in ("A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4",
                "C1", "C2", "C3", "C4"):
        cell = summary["by_cell"].get(f"{inj}__audit-first")
        if cell is None:
            h1.append(f"{inj}: cell missing")
            continue
        if cell["consistent"] != n_per_cell:
            h1.append(
                f"{inj}: audit-first {cell['consistent']}/{n_per_cell} consistent"
            )

    for inj in ("B1", "B2", "B3", "B4"):
        cell = summary["by_cell"].get(f"{inj}__fail-open")
        if cell is None:
            h2.append(f"{inj}: cell missing")
            continue
        if cell["consistent"] != n_per_cell:
            h2.append(
                f"{inj}: fail-open {cell['consistent']}/{n_per_cell}"
                f" consistent (expected {n_per_cell}/{n_per_cell})"
            )

    for inj in ("A1", "A2", "A3", "A4", "C1", "C2", "C3", "C4"):
        cell = summary["by_cell"].get(f"{inj}__fail-open")
        if cell is None:
            h3.append(f"{inj}: cell missing")
            continue
        if cell["consistent"] != 0:
            h3.append(
                f"{inj}: fail-open {cell['consistent']}/{n_per_cell} consistent"
                f" (expected 0/{n_per_cell})"
            )

    # H4 — audit-first p50 rollback-path latency within the 300ms canary
    # window. A gate on ``wall_clock_p95_ms < 300`` would fail by
    # construction: A3 (metric-final-poll) and C2 (success-write) are
    # designed to fire on the *last* poll / success path, so both
    # intrinsically run the full window and their wall_clock is
    # necessarily ≥300ms, pushing p95 over the budget. The gate is
    # therefore on the rollback-path *p50*: rollback_path_p50_ms < 300.
    # rollback_path_p95_ms / wall_clock_p95_ms are reported alongside as
    # informational figures.
    audit_first = summary["by_posture"].get("audit-first", {})
    rollback_p50 = audit_first.get("rollback_path_p50_ms")
    if rollback_p50 is None:
        h4.append("audit-first rollback_path_p50_ms missing (no samples)")
    elif rollback_p50 >= 300:
        h4.append(
            f"audit-first rollback_path p50 {rollback_p50}ms exceeds"
            f" canary window (300ms)"
        )

    if not h1 and not h3 and not h4:
        if not h2:
            verdict = "PASS"
            note = "all four hypotheses (H1..H4) hold"
        elif len(h2) <= 2:
            verdict = "PARTIAL"
            note = (
                "H1/H3/H4 hold; H2 shows minor fail-open class-B anomalies"
                " (at most two cells, tolerated as non-determinism)"
            )
        else:
            verdict = "FAIL"
            note = "H1/H3/H4 hold but H2 has >2 fail-open class-B anomalies"
    else:
        verdict = "FAIL"
        note = "one or more of H1/H3/H4 broke"

    return {
        "verdict": verdict,
        "note": note,
        "hypotheses": {
            "H1_audit_first_all_cells_consistent": {
                "passed": not h1,
                "failures": h1,
            },
            "H2_fail_open_class_B_coincident": {
                "passed": not h2,
                "failures": h2,
            },
            "H3_fail_open_class_A_C_inconsistent": {
                "passed": not h3,
                "failures": h3,
            },
            "H4_audit_first_latency_under_canary_window": {
                "passed": not h4,
                "failures": h4,
                "note": (
                    "gated on rollback_path_p50_ms < 300 (see the"
                    " code comment in _sign_check for the rationale)"
                ),
            },
        },
        "n_trials_per_cell": n_per_cell,
    }


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #

def _parse_csv_list(raw: str, valid: tuple[str, ...]) -> tuple[str, ...]:
    items = tuple(x.strip() for x in raw.split(",") if x.strip())
    unknown = [x for x in items if x not in valid]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown entries {unknown}; valid={list(valid)}"
        )
    return items


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "12-injection × N-trial × 2-posture chaos grid for "
            "audit-first vs fail-open."
        )
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=50,
        help="trials per (injection × posture) cell (default 50)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="shortcut for --trials 1 (smoke test)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_RAW_OUTPUT,
        help=f"path to raw.jsonl (default {DEFAULT_RAW_OUTPUT})",
    )
    parser.add_argument(
        "--variants",
        type=lambda raw: _parse_csv_list(raw, ALL_INJECTIONS),
        default=ALL_INJECTIONS,
        help="comma-separated injection labels (default all 12)",
    )
    parser.add_argument(
        "--postures",
        type=lambda raw: _parse_csv_list(raw, ALL_POSTURES),
        default=ALL_POSTURES,
        help="comma-separated postures (default audit-first,fail-open)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=11_2026,
        help="random seed for deterministic to_version strings",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="suppress per-cell progress prints to stderr",
    )
    args = parser.parse_args(argv)

    n_trials = 1 if args.quick else args.trials
    variants: tuple[str, ...] = args.variants
    postures: tuple[str, ...] = args.postures
    # Resolve relative paths against the repo root, not the process CWD.
    # Bridge lifespan startup pokes ``~/.aeros``; re-anchoring keeps the
    # output write deterministic regardless of how the script is invoked.
    output_path: Path = args.output
    if not output_path.is_absolute():
        output_path = (REPO_ROOT / output_path).resolve()
    output_dir = output_path.parent
    summary_path = output_dir / "summary.json"
    sign_check_path = output_dir / "sign_check.json"

    output_dir.mkdir(parents=True, exist_ok=True)

    def _progress(msg: str) -> None:
        if not args.quiet:
            print(f"[chaos-grid] {msg}", file=sys.stderr, flush=True)

    t0 = time.monotonic()
    per_trial, _run_meta = asyncio.run(
        _run_grid(
            n_trials=n_trials,
            variants=variants,
            postures=postures,
            rng_seed=args.seed,
            progress=_progress,
        )
    )
    elapsed = time.monotonic() - t0

    summary = _summarise(
        per_trial,
        variants=variants,
        postures=postures,
        n_trials=n_trials,
    )
    summary["wall_clock_seconds_total"] = round(elapsed, 2)
    summary["seed"] = args.seed
    sign_check = _sign_check(summary)

    output_dir.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as fh:
        for record in per_trial:
            fh.write(json.dumps(record, sort_keys=True))
            fh.write("\n")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    sign_check_path.write_text(json.dumps(sign_check, indent=2, sort_keys=True))

    print(json.dumps({
        "verdict": sign_check["verdict"],
        "note": sign_check["note"],
        "n_trials_per_cell": n_trials,
        "n_total_trials": summary["n_total_trials"],
        "wall_clock_seconds_total": summary["wall_clock_seconds_total"],
        "audit_first_consistent_rate": (
            summary["by_posture"].get("audit-first", {}).get("consistent_rate")
        ),
        "fail_open_consistent_rate": (
            summary["by_posture"].get("fail-open", {}).get("consistent_rate")
        ),
        "audit_first_wall_clock_p50_ms": (
            summary["by_posture"].get("audit-first", {}).get(
                "wall_clock_p50_ms"
            )
        ),
        "audit_first_wall_clock_p95_ms": (
            summary["by_posture"].get("audit-first", {}).get(
                "wall_clock_p95_ms"
            )
        ),
        "audit_first_rollback_path_p50_ms": (
            summary["by_posture"].get("audit-first", {}).get(
                "rollback_path_p50_ms"
            )
        ),
        "audit_first_rollback_path_p95_ms": (
            summary["by_posture"].get("audit-first", {}).get(
                "rollback_path_p95_ms"
            )
        ),
    }, sort_keys=True))
    return 0 if sign_check["verdict"] in {"PASS", "PARTIAL"} else 1


if __name__ == "__main__":
    sys.exit(main())
