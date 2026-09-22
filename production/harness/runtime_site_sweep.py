#!/usr/bin/env python3
"""Exhaustive fault-site sweep over a canary upgrade job on the production runtime.

Enumerates every place a fail-stop exception can surface during a canary
upgrade job, rather than sampling the twelve structural cells of the crash
grid in runtime_chaos_grid.py:

  pre-flip job-store writes            S01-S04
  flip boundary                        S05 (episodic mirror of the "upgrade"
                                            audit row, after the live assignment)
                                       S06 (CANARY_RUNNING status write)
  provisional region (inside guard)    S07-S12 per-poll status writes,
                                       S13 end-of-window summary write,
                                       S14 CANARY_PROMOTED write, S15 PROMOTED write,
                                       S16 metric provider raises at poll k (k=1..6),
                                       S17 post-poll success-rate compute raises
  rollback path (trigger: metric fault at poll 1)
                                       S18 rollback closure raises before revert,
                                       S19 rollback audit mirror raises after revert,
                                       S20 ROLLED_BACK status write raises,
                                       S21 flagged FAILED status write raises after
                                           the closure raised (the flag must survive
                                           the job wrapper)
  no fault                             S00

The expected terminal outcome of every trial is derived from the
specification and from facts observed *at fault time* (did the fault fire,
had the live version already flipped), never from the measured terminal
outcome itself:

  no fault fired                          -> (promoted,    to_version)
  fault fired before the live flip        -> (failed,      from_version)
  post-flip fault, closure cannot revert  -> (failed,      to_version)    S18, S21
  post-flip fault, revert done, a later
    rollback-path write fails             -> (failed,      from_version)  S19, S20
  any other post-flip fault               -> (rolled_back, from_version)

A trial is coherent iff (terminal status, live version) equals that outcome.

Instrumentation adds two timestamps per trial (time.monotonic): the moment
the first injected fault fires and the moment a terminal status is durably
written to the job store, giving fault-to-terminal recovery latency in
addition to end-to-end job latency. All patches on app-level singletons are
restored after every trial.

Usage (from the runtime checkout root):
    python scripts/runtime_site_sweep.py --posture audit-first --trials 50 \
        --output runs/sites/<runtime>/audit-first.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import random
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from httpx import ASGITransport, AsyncClient  # noqa: E402

from runtime_chaos_grid import _fail_open_run_canary_factory  # noqa: E402

TERMINAL = {"shadow_failed", "rolled_back", "rejected", "promoted", "failed"}
TERMINAL_WRITE = {"rolled_back", "failed", "promoted", "shadow_failed", "rejected"}

# (site id, kind, parameter, description)
SITES: list[tuple[str, str, Any, str]] = [
    ("S00", "none", None, "no fault"),
    ("S01", "store_nth", 1, "VALIDATING status write"),
    ("S02", "store_nth", 2, "SHADOW_RUNNING status write"),
    ("S03", "store_nth", 3, "shadow summary write"),
    ("S04", "store_nth", 4, "SHADOW_PASSED status write"),
    ("S05", "mirror", "evolution.upgrade", "upgrade audit mirror (after live flip)"),
    ("S06", "store_nth", 5, "CANARY_RUNNING status write"),
    ("S07", "store_nth", 6, "canary poll-1 status write"),
    ("S08", "store_nth", 7, "canary poll-2 status write"),
    ("S09", "store_nth", 8, "canary poll-3 status write"),
    ("S10", "store_nth", 9, "canary poll-4 status write"),
    ("S11", "store_nth", 10, "canary poll-5 status write"),
    ("S12", "store_nth", 11, "canary poll-6 status write"),
    ("S13", "store_nth", 12, "end-of-window summary write"),
    ("S14", "store_status", "canary_promoted", "CANARY_PROMOTED status write"),
    ("S15", "store_status", "promoted", "PROMOTED status write"),
    ("S16a", "metric_k", 1, "metric provider raises at poll 1"),
    ("S16b", "metric_k", 3, "metric provider raises at poll 3"),
    ("S16c", "metric_k", 6, "metric provider raises at final poll"),
    ("S17", "compute_k", 3, "post-poll success-rate compute raises"),
    ("S18", "rb_closure", None, "rollback closure raises before revert"),
    ("S19", "rb_mirror", "evolution.rollback", "rollback audit mirror raises after revert"),
    ("S20", "rb_status", "rolled_back", "ROLLED_BACK status write raises"),
    ("S21", "rb_flagged_status", "failed",
     "flagged FAILED status write raises after the closure raised"),
]

EXC_TYPES = [RuntimeError, ValueError, KeyError, TypeError, OSError]


class Probe:
    """Per-trial instrumentation shared by all injected callables."""

    def __init__(self, state: Any, capability: str, to_version: str) -> None:
        self.state = state
        self.capability = capability
        self.to_version = to_version
        self.fault_t: float | None = None
        self.flipped_at_fault: bool | None = None
        self.terminal_t: float | None = None
        self.terminal_status: str | None = None
        self.n_updates = 0

    def live_is_to(self) -> bool:
        return self.state.active_ecm_versions.get(self.capability) == self.to_version

    def fire(self, exc: BaseException) -> BaseException:
        if self.fault_t is None:
            self.fault_t = time.monotonic()
            self.flipped_at_fault = self.live_is_to()
        return exc


def expected_outcome(site_kind: str, probe: Probe) -> tuple[str, str]:
    if probe.fault_t is None:
        return ("promoted", "to")
    if not probe.flipped_at_fault:
        return ("failed", "from")
    if site_kind in ("rb_closure", "rb_flagged_status"):
        return ("failed", "to")
    if site_kind in ("rb_mirror", "rb_status"):
        return ("failed", "from")
    return ("rolled_back", "from")


def install(pipeline: Any, state: Any, kind: str, param: Any, probe: Probe,
            rng: random.Random, restore: list) -> None:
    """Install the site's fault. Every patch on a shared object is recorded
    in *restore* so it can be undone after the trial."""
    exc_type = rng.choice(EXC_TYPES)
    store = pipeline._store
    orig_update = store.update
    restore.append((store, "update", orig_update))
    flagged_fired = {"done": False}

    async def update(*a: Any, **k: Any) -> Any:
        probe.n_updates += 1
        st = k.get("status")
        sv = getattr(st, "value", st)
        fault = False
        if kind == "store_nth" and probe.n_updates == param:
            fault = True
        if kind == "store_status" and sv == param and probe.fault_t is None:
            fault = True
        if kind == "rb_status" and sv == param and probe.fault_t is not None:
            # companion fault: the rollback-path terminal write
            raise exc_type(f"injected {kind} fault")
        if (kind == "rb_flagged_status" and sv == param
                and probe.fault_t is not None
                and "rollback failure" in str(k.get("error") or "")
                and not flagged_fired["done"]):
            # companion fault, one shot: the flagged FAILED write itself
            # fails; the job wrapper's fallback write must then land
            flagged_fired["done"] = True
            raise exc_type(f"injected {kind} fault")
        if fault:
            raise probe.fire(exc_type(f"injected {kind} fault"))
        result = await orig_update(*a, **k)
        if sv in TERMINAL_WRITE and probe.terminal_t is None:
            probe.terminal_t = time.monotonic()
            probe.terminal_status = sv
        return result

    store.update = update

    agent = getattr(state, "persistent_agent", None)
    if kind in ("mirror", "rb_mirror") and agent is not None:
        orig_record = agent.record
        restore.append((agent, "record", orig_record))

        def record(*a: Any, **kw: Any) -> Any:
            rec_kind = kw.get("kind", a[0] if a else None)
            if rec_kind == param:
                if kind == "mirror" and probe.fault_t is None:
                    raise probe.fire(exc_type(f"injected {kind} fault"))
                if kind == "rb_mirror" and probe.fault_t is not None:
                    raise exc_type(f"injected {kind} fault")
            return orig_record(*a, **kw)

        agent.record = record

    if kind == "metric_k" or kind.startswith("rb_"):
        k_fire = param if kind == "metric_k" else 1
        orig_metrics = pipeline._metrics_provider
        calls = {"n": 0}

        def metrics(capability: str, since: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == k_fire:
                raise probe.fire(exc_type(f"injected {kind} trigger at poll {k_fire}"))
            return orig_metrics(capability, since)

        pipeline._metrics_provider = metrics

    if kind == "compute_k":
        replica = dataclasses.replace(pipeline._canary_config)
        orig_eval = replica.evaluate_mid_window
        calls = {"n": 0}

        def evaluate(metrics_: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == param:
                raise probe.fire(exc_type("injected compute fault"))
            return orig_eval(metrics_)

        replica.evaluate_mid_window = evaluate
        pipeline._canary_config = replica

    if kind in ("rb_closure", "rb_flagged_status"):
        def bad_rollback(*a: Any, **k: Any) -> None:
            raise exc_type("injected rollback-closure fault")

        pipeline._rollback = bad_rollback


async def run_trial(client: Any, state: Any, routes_mod: Any, base: Any,
                    job_status_cls: Any, posture: str, site: tuple, idx: int,
                    rng: random.Random) -> dict[str, Any]:
    sid, kind, param, desc = site
    cap = "manipulation.grasp"
    to_v = f"9.{sid}.{idx}.{rng.randrange(1 << 32):08x}"
    from_v = (await client.get("/api/evolution/active")).json().get(cap, "1.0.0")
    await client.post("/api/evolution/register", json={"capability": cap, "version": to_v})
    probe = Probe(state, cap, to_v)
    restore: list = []
    fail_open = _fail_open_run_canary_factory(job_status_cls)

    def build(st: Any) -> Any:
        p = base(st)
        install(p, st, kind, param, probe, rng, restore)
        if posture == "fail-open":
            p._run_canary = fail_open.__get__(p)
        return p

    routes_mod._build_evolution_pipeline = build
    t0 = time.monotonic()
    try:
        resp = await client.post("/api/evolution/upgrade", json={
            "capability": cap, "version": to_v, "force_unsoaked": True})
        job_id = resp.json()["job_id"]
        job = None
        for _ in range(500):
            j = (await client.get(f"/api/evolution/jobs/{job_id}")).json()
            if j["status"] in TERMINAL:
                job = j
                break
            await asyncio.sleep(0.01)
        t1 = time.monotonic()
        live = (await client.get("/api/evolution/active")).json().get(cap)
    finally:
        routes_mod._build_evolution_pipeline = base
        for obj, attr, fn in restore:
            setattr(obj, attr, fn)

    exp_status, exp_live_kind = expected_outcome(kind, probe)
    exp_live = to_v if exp_live_kind == "to" else from_v
    status = job["status"] if job else "harness_no_terminal"
    rec = {
        "site": sid, "site_kind": kind, "site_desc": desc, "posture": posture,
        "trial": idx, "from_version": from_v, "to_version": to_v,
        "fault_fired": probe.fault_t is not None,
        "flipped_at_fault": probe.flipped_at_fault,
        "final_status": status, "final_live": live,
        "expected_status": exp_status, "expected_live": exp_live,
        "coherent": status == exp_status and live == exp_live,
        "end_to_end_ms": round(1000 * (t1 - t0), 3),
        "fault_to_terminal_ms": (round(1000 * (probe.terminal_t - probe.fault_t), 3)
                                 if probe.fault_t and probe.terminal_t else None),
        "canary_started_at": (job or {}).get("canary_started_at"),
        "canary_completed_at": (job or {}).get("canary_completed_at"),
        "error": ((job or {}).get("error") or "")[:240],
    }
    await client.post("/api/evolution/rollback", json={"capability": cap, "version": "1.0.0"})
    return rec


async def main_async(args: argparse.Namespace) -> list[dict[str, Any]]:
    from aeros.runtime.persona import PersonaProfile
    from bridge.app.aeros.evolution_pipeline import CanaryConfig, JobStatus, PipelineMode
    from bridge.app.aeros.evolution_validator import UpgradeValidator
    from bridge.app.api import routes as routes_mod
    from bridge.app.main import app, lifespan

    rng = random.Random(args.seed)
    sites = [s for s in SITES if not args.sites or s[0] in args.sites.split(",")]
    rows: list[dict[str, Any]] = []
    async with lifespan(app):
        app.state.upgrade_validator = UpgradeValidator(persona=PersonaProfile(
            name="site-sweep", risk_appetite="aggressive", accept_unsoaked_ecm=True))
        app.state.evolution_pipeline_mode = PipelineMode.CANARY
        app.state.evolution_canary_config = CanaryConfig(
            window_seconds=0.3, poll_interval_seconds=0.05, min_intents=0)
        base = routes_mod._build_evolution_pipeline
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            for site in sites:
                for i in range(args.trials):
                    rows.append(await run_trial(c, app.state, routes_mod, base,
                                                JobStatus, args.posture, site, i, rng))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--posture", choices=["audit-first", "fail-open"], required=True)
    ap.add_argument("--trials", type=int, default=50)
    ap.add_argument("--seed", type=int, default=112026)
    ap.add_argument("--sites", default="", help="comma-separated site ids (default all)")
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    rows = asyncio.run(main_async(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
    n_coh = sum(r["coherent"] for r in rows)
    print(json.dumps({"posture": args.posture, "n": len(rows), "coherent": n_coh,
                      "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
