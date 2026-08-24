"""Main measurement run.

Executes the big seeded chaos scenario (target: well over 100k events)
plus a three-level load sweep with identical topology, and writes
everything to results/sim_metrics.json.

Usage: .venv/bin/python scripts/run_sim.py [seed]
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from talkgroup.sim import ChaosSim, SimConfig  # noqa: E402


def run_one(name, cfg):
    t0 = time.perf_counter()
    sim = ChaosSim(cfg)
    rep = sim.run()
    wall = time.perf_counter() - t0
    rep["wall_seconds"] = round(wall, 3)
    rep["digest"] = ChaosSim.digest({k: v for k, v in rep.items()
                                     if k != "wall_seconds"})
    inv = rep["invariants"]
    print("[%s] events=%d wall=%.2fs util=%.3f" % (
        name, rep["events_processed"], wall, rep["channel_utilization"]))
    print("  invariants: double_speaker=%d emergency_bound=%d starvation=%d "
          "msg_loss=%d msg_dup=%d" % (
              inv["double_speaker"], inv["emergency_bound_violations"],
              inv["starvation"], inv["message_loss"],
              inv["message_duplicate_delivery"]))
    print("  emergency latency: %s" % rep["emergency_preempt_latency"])
    print("  routine grant latency: %s" % rep["grant_latency"]["routine"])
    return rep


def main():
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    out = {"seed": seed}

    # the headline chaos run
    out["main"] = run_one("main", SimConfig(seed=seed))

    # load sweep: same topology, rising press rate
    for name, rate in (("light", 0.004), ("moderate", 0.01), ("heavy", 0.02)):
        cfg = SimConfig(seed=seed, press_rate=rate, horizon=600_000,
                        messages=1200, storms=8, drops=150, late_joins=100)
        out["load_" + name] = run_one("load_" + name, cfg)

    path = os.path.join(os.path.dirname(__file__), "..", "results",
                        "sim_metrics.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    print("wrote", os.path.normpath(path))


if __name__ == "__main__":
    main()
