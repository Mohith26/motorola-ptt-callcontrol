# talkgroup

I wanted to understand how trunked radio systems keep group voice traffic sane: hundreds of users, a couple dozen talkgroups, and only eight traffic channels to share between all of them. So I built the call-control layer as a deterministic event-driven simulator and then tried to break it with seeded chaos.

The rule that makes these systems work is brutal and simple: one speaker per talkgroup, ever, and an emergency call gets the floor fast no matter what else is happening. Everything in this repo exists to either enforce that rule or prove it held.

## What is actually in here

`talkgroup/` is a small pure-Python package (no runtime dependencies):

* **Floor control** (`model.py`, `engine.py`). Each subscriber's claim on a talkgroup moves through a five-state machine: idle, requesting, queued, granted, preempted. Every transition is checked against an explicit allow list and an illegal move raises immediately. Grants are not instant: arbitration schedules a grant event after a processing delay, and a grant that is already in flight can still be preempted by an emergency before it lands. Stale grants are fenced with a per-talkgroup epoch counter.
* **Channel pool** (`allocator.py`). A talkgroup needs a traffic channel from a shared pool before anyone in it can speak. The allocator self-checks conservation on every operation, so a run that finishes is itself proof no channel was leaked or double-assigned. When the pool is exhausted, talkgroups queue for channels by priority; an emergency call instead steals the channel from the lowest-priority active call.
* **Preemption semantics**. Only EMERGENCY preempts. HIGH sorts ahead of ROUTINE in queues but never rips the floor away from a live speaker. A preempted speaker is requeued automatically with its original request time and its remaining talk time, so preemption interrupts a transmission but never eats the tail of it.
* **Store-and-forward messaging** (`messaging.py`). Group messages ride a lossy network with per-recipient retry timers and delivery receipts. The receiver dedupes by message id, so the wire is at-least-once but the application layer sees each message exactly once. Offline recipients get a mailbox that flushes when they come back.
* **Chaos simulation** (`sim.py`). All external stimuli (press storms, emergency injections, mid-transmission drop-offs, late joins, messages, packet loss) come from one seeded RNG, so a (seed, config) pair reproduces a bit-identical multi-hundred-thousand-event history. The end-of-run audit checks five invariants: zero double-speaker grants, zero unexplained emergency latency bound violations, zero starved requests, zero lost messages, zero duplicate deliveries.

## Numbers from my machine

Apple silicon, single thread, Python 3.9. Seed 1, 220 subscribers, 24 talkgroups, 8 channels:

* 353,140 events processed, all five invariant counters at zero.
* Emergency floor-grant latency p99 of 25 ticks at 98.9 percent channel utilization; the only slower emergencies were ones queued behind another emergency, which the bound check deliberately exempts and the audit counts separately.
* Routine grant latency p50 went from 25 ticks under light load to 7,500 ticks under 2x overload, which is the queueing math doing exactly what it should.
* 83,558 message deliveries with 31,773 retries and 14,665 suppressed duplicates, and the exactly-once audit found zero losses and zero double deliveries.
* About 576k events/sec through the engine.

Full details plus reproduce commands live in [RESULTS.md](RESULTS.md).

## Running it

```
python3 -m venv .venv && .venv/bin/pip install -U pip pytest pytest-cov
.venv/bin/pytest -q                      # 83 tests
.venv/bin/python scripts/run_sim.py 1    # chaos run + load sweep -> results/sim_metrics.json
.venv/bin/python scripts/bench.py 1      # throughput -> results/bench.json
```

## Limitations

This is the control plane only, and a simplified one:

* No real trunking protocol details, no RF or audio modeling, no networking. Time is an abstract integer tick; delays are configured constants, not measured radio behavior.
* A talkgroup keeps its channel while its own queue is non-empty. That models conversation continuity but means one busy talkgroup can hold a channel for a long stretch under overload; fairness across talkgroups comes only from the priority wait queue and from calls eventually ending.
* The emergency latency bound is checked against a designed worst case (the slower of the normal and preempt grant delays). Emergencies queued behind other emergencies are excluded from the bound by design and reported as their own count, so the p99 in a heavy run can exceed the bound without a violation. That is a modeling choice, not a proof of a real system.
* Message loss is decided by a seeded coin flip and becomes zero after the chaos horizon so the drain phase converges. The exactly-once guarantee is against this model, not against arbitrary partitions.
* Everything is single-threaded by construction. Determinism comes from the event heap ordering, and concurrency is exactly the thing this simulator does not model.
