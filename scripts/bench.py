"""Throughput benchmark: events processed per second of wall time.

Runs the moderate chaos scenario three times and reports each run plus
the best rate. Single thread, pure Python. Writes results/bench.json.

Usage: .venv/bin/python scripts/bench.py [seed]
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from talkgroup.sim import ChaosSim, SimConfig  # noqa: E402


def main():
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    runs = []
    for i in range(3):
        cfg = SimConfig(seed=seed)
        sim = ChaosSim(cfg)
        t0 = time.perf_counter()
        sim.engine.run()
        wall = time.perf_counter() - t0
        ev = sim.engine.events_processed
        runs.append({"events": ev, "wall_seconds": round(wall, 4),
                     "events_per_sec": round(ev / wall)})
        print("run %d: %d events in %.3fs = %d events/sec"
              % (i + 1, ev, wall, ev / wall))
    best = max(r["events_per_sec"] for r in runs)
    result = {"seed": seed, "runs": runs, "best_events_per_sec": best,
              "note": "single thread, pure Python, engine.run() only "
                      "(event generation excluded)"}
    path = os.path.join(os.path.dirname(__file__), "..", "results",
                        "bench.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=1, sort_keys=True)
    print("best: %d events/sec" % best)
    print("wrote", os.path.normpath(path))


if __name__ == "__main__":
    main()
