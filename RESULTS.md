# Results

My benchmark and validation notes. Everything below came from runs on my machine: Apple silicon laptop, macOS, Python 3.9.6, single thread. Committed raw output is in `results/sim_metrics.json` and `results/bench.json`.

## Reproduce

```
python3 -m venv .venv && .venv/bin/pip install -U pip pytest pytest-cov
.venv/bin/pytest -q --cov=talkgroup
.venv/bin/python scripts/run_sim.py 1
.venv/bin/python scripts/bench.py 1
```

The sim is fully deterministic per (seed, config): the report digest for the main seed-1 run is `e909b253fde106f8...` and reruns reproduce it exactly (there is a pytest for this on a smaller config).

## Main chaos run (seed 1)

Config: 220 subscribers, 24 talkgroups, 8 channels, 1.2M tick horizon, 0.02 presses/tick background load plus 20 request storms, 2 percent emergency mix, 3,500 group messages over a network with 15 percent loss, 400 drop/rejoin cycles, 300 late joins.

* Events processed: 353,140 (0.66 s wall).
* Channel utilization: 98.9 percent, so the pool was saturated most of the run.
* Invariants, audited at drain: double-speaker grants 0, emergency bound violations 0, starved requests 0, lost messages 0, duplicate deliveries 0. The allocator additionally self-checked conservation 2,472 times without raising.

### Floor control and preemption

* 13,059 floor grants; 492 speaker preemptions, 285 channel preemptions (emergency stealing a channel from a lower-priority call), 10 preemptions of an in-flight grant.
* 479 regrants: preempted speakers got the floor back and finished their remaining talk time.
* Emergency grant latency (517 emergencies): p50 25, p99 25, max 694 ticks. The configured bound is 25 ticks (the normal grant processing delay). Every emergency slower than 25 was queued behind another emergency, which the bound check exempts by design; zero violations otherwise.
* 17 drop-offs happened mid-transmission and the floor recovered every time.

### Grant latency under load (same topology, 600k tick horizon)

| load | presses/tick | channel util | routine p50 | routine p99 | emergency p99 |
|---|---|---|---|---|---|
| light | 0.004 | 39.5% | 25 | 9,259 | 25 |
| moderate | 0.01 | 93.4% | 597 | 6,151 | 25 |
| heavy | 0.02 | 98.9% | 7,500 | 143,696 | 152 |

Notes: the light-load routine p99 (9,259) is dominated by the injected request storms, which are bursts by construction. Heavy load is roughly 2x the pool's service capacity, so routine waits blow up into the hundreds of thousands of ticks while emergency latency stays flat; the heavy emergency p99 of 152 comes from emergencies stacked behind other emergencies, which is exactly the traffic you cannot preempt. Max routine wait in the main run was 235,850 ticks, but starvation at drain was zero: every request was eventually granted or its subscriber dropped.

### Messaging (main run)

* 3,500 sends fanned out to 83,558 (message, recipient) pairs. 115,325 transmissions, 17,102 simulated data losses, 14,671 receipt losses, 31,773 retries, 2,814 mailbox stores with 2,814 flushes.
* Exactly-once audit: every expected pair delivered exactly once. 14,665 duplicate arrivals were suppressed by receiver-side dedupe.
* 83,552 of 83,558 receipts confirmed; the remaining 6 pairs were delivered but their receipt chain ended when the recipient went offline, and the mailbox path closed them out. Delivery still counted exactly once.

## Throughput

`scripts/bench.py 1`, three runs of the main scenario timing `engine.run()` only (event generation excluded):

* 575,909 / 561,895 / 558,864 events per second; best 575,910 as rounded in bench.json.
* Pure Python on one core. I did not profile or optimize; the heap and dict churn is where the time goes.

## Tests and coverage

`.venv/bin/pytest -q --cov=talkgroup`: 83 passed in about 1.1 s, 96 percent line coverage (engine.py 97 percent, messaging.py 95 percent). The suite covers the full 5x5 transition matrix (25 parametrized cases), allocator misuse and a 5,000-op seeded fuzz, preemption of speakers, pending grants and channels, exactly-once messaging under injected data and receipt loss, and determinism (same seed, same digest; different seed, different digest).

## Caveats

Tick-to-millisecond mapping is nominal; nothing here measures a real radio system. All latency numbers are virtual-time queueing results under my configured processing delays (grant 25, preempt grant 10, delivery 20, retry 250). Wall-clock numbers are machine-specific and single-threaded.
