from talkgroup.engine import Engine
from talkgroup.model import Priority, ReqState


def make_engine(channels=2):
    e = Engine(num_channels=channels)
    e.create_talkgroup("tg")
    for s in ("alice", "bob", "carol"):
        e.add_subscriber(s)
        e.join(s, "tg")
    return e


def press(e, t, sub, prio=Priority.ROUTINE, hold=1000, tg="tg"):
    e.at(t, "ptt_request", sub=sub, tg=tg, prio=prio, hold=hold)


def test_emergency_preempts_routine_speaker():
    e = make_engine()
    press(e, 0, "alice", prio=Priority.ROUTINE, hold=5000)
    press(e, 1000, "bob", prio=Priority.EMERGENCY, hold=300)
    e.run(max_events=4)
    tg = e.tgs["tg"]
    assert tg.speaker == "bob"
    assert tg.state["alice"] == ReqState.QUEUED
    assert e.counters["speaker_preemptions"] == 1


def test_emergency_preempt_latency_is_fast_path():
    e = make_engine()
    press(e, 0, "alice", prio=Priority.ROUTINE, hold=5000)
    press(e, 1000, "bob", prio=Priority.EMERGENCY, hold=300)
    e.run()
    assert e.emergency_latency.values == [e.preempt_grant_delay]
    assert e.invariants["emergency_bound"] == 0


def test_preempted_speaker_resumes_with_remaining_hold():
    e = make_engine()
    press(e, 0, "alice", prio=Priority.ROUTINE, hold=5000)
    press(e, 1000, "bob", prio=Priority.EMERGENCY, hold=300)
    e.run()
    # alice talked 975 ticks before preemption (granted at t=25),
    # then finishes the remaining 4025 after bob's emergency call
    assert e.counters["regrants_after_preemption"] == 1
    assert sorted(e.talk_series.values) == [300, 975, 4025]
    assert e.audit()["starvation"] == 0


def test_emergency_preempts_pending_grant():
    e = make_engine()
    press(e, 0, "alice", prio=Priority.ROUTINE, hold=1000)
    # alice's grant is in flight (lands at t=25); emergency at t=5 beats it
    press(e, 5, "bob", prio=Priority.EMERGENCY, hold=200)
    e.run(max_events=4)
    tg = e.tgs["tg"]
    assert tg.speaker == "bob"
    assert e.counters["pending_preemptions"] == 1
    e.run()
    assert e.counters["stale_grants"] == 1  # alice's original grant fenced
    assert e.counters["floor_grants"] == 2  # both eventually spoke


def test_emergency_behind_emergency_queues():
    e = make_engine()
    press(e, 0, "alice", prio=Priority.EMERGENCY, hold=1000)
    press(e, 100, "bob", prio=Priority.EMERGENCY, hold=200)
    e.run(max_events=3)
    tg = e.tgs["tg"]
    assert tg.speaker == "alice"
    assert tg.state["bob"] == ReqState.QUEUED
    e.run()
    assert e.counters["speaker_preemptions"] == 0
    # bob's long wait is excluded from the bound check by design
    assert e.invariants["emergency_bound"] == 0


def two_group_engine(channels=1):
    e = Engine(num_channels=channels)
    for tg in ("tg_a", "tg_b"):
        e.create_talkgroup(tg)
    e.add_subscriber("alice")
    e.add_subscriber("bob")
    e.join("alice", "tg_a")
    e.join("bob", "tg_b")
    return e


def test_channel_preemption_on_exhaustion():
    e = two_group_engine(channels=1)
    e.at(0, "ptt_request", sub="alice", tg="tg_a",
         prio=Priority.ROUTINE, hold=10000)
    e.at(1000, "ptt_request", sub="bob", tg="tg_b",
         prio=Priority.EMERGENCY, hold=500)
    e.run(max_events=4)
    assert e.tgs["tg_b"].speaker == "bob"
    assert e.tgs["tg_a"].channel is None
    assert e.counters["channel_preemptions"] == 1


def test_channel_preemption_victim_recovers():
    e = two_group_engine(channels=1)
    e.at(0, "ptt_request", sub="alice", tg="tg_a",
         prio=Priority.ROUTINE, hold=10000)
    e.at(1000, "ptt_request", sub="bob", tg="tg_b",
         prio=Priority.EMERGENCY, hold=500)
    e.run()
    # alice regains a channel and finishes her remaining talk time
    assert e.counters["regrants_after_preemption"] == 1
    assert e.audit()["starvation"] == 0
    assert e.invariants["double_speaker"] == 0
    assert e.pool.busy == 0


def test_channel_preemption_picks_lowest_priority_victim():
    e = Engine(num_channels=2)
    for tg in ("tg_a", "tg_b", "tg_c"):
        e.create_talkgroup(tg)
    for s, tg in (("alice", "tg_a"), ("bob", "tg_b"), ("carol", "tg_c")):
        e.add_subscriber(s)
        e.join(s, tg)
    e.at(0, "ptt_request", sub="alice", tg="tg_a",
         prio=Priority.HIGH, hold=10000)
    e.at(10, "ptt_request", sub="bob", tg="tg_b",
         prio=Priority.ROUTINE, hold=10000)
    e.at(1000, "ptt_request", sub="carol", tg="tg_c",
         prio=Priority.EMERGENCY, hold=500)
    e.run(max_events=6)
    # bob (ROUTINE) loses his channel, alice (HIGH) keeps hers
    assert e.tgs["tg_b"].channel is None
    assert e.tgs["tg_a"].channel is not None
    assert e.tgs["tg_c"].speaker == "carol"


def test_no_channel_preemption_when_all_calls_emergency():
    e = two_group_engine(channels=1)
    e.at(0, "ptt_request", sub="alice", tg="tg_a",
         prio=Priority.EMERGENCY, hold=2000)
    e.at(100, "ptt_request", sub="bob", tg="tg_b",
         prio=Priority.EMERGENCY, hold=300)
    e.run(max_events=3)
    assert e.counters["channel_preemptions"] == 0
    assert e.tgs["tg_b"].channel is None
    e.run()
    # bob is served after alice finishes, and the wait is exempt from
    # the bound because he was behind another emergency
    assert e.counters["floor_grants"] == 2
    assert e.invariants["emergency_bound"] == 0


def test_emergency_bound_invariant_holds_in_mixed_load():
    e = make_engine(channels=1)
    for t in range(0, 20000, 900):
        press(e, t, "alice", prio=Priority.ROUTINE, hold=800)
    press(e, 5000, "bob", prio=Priority.EMERGENCY, hold=200)
    press(e, 12000, "carol", prio=Priority.EMERGENCY, hold=200)
    e.run()
    assert e.invariants["emergency_bound"] == 0
    assert e.emergency_latency.maximum() <= e.emergency_bound
