from talkgroup.engine import Engine
from talkgroup.model import Priority, ReqState


def make_engine(channels=2, **kw):
    e = Engine(num_channels=channels, **kw)
    e.create_talkgroup("tg")
    for s in ("alice", "bob", "carol", "dave"):
        e.add_subscriber(s)
        e.join(s, "tg")
    return e


def press(e, t, sub, prio=Priority.ROUTINE, hold=1000, tg="tg"):
    e.at(t, "ptt_request", sub=sub, tg=tg, prio=prio, hold=hold)


def test_single_request_granted_after_grant_delay():
    e = make_engine()
    press(e, 100, "alice")
    e.run()
    assert e.counters["floor_grants"] == 1
    assert e.grant_latency[Priority.ROUTINE].values == [e.grant_delay]


def test_speaker_state_is_granted_while_talking():
    e = make_engine()
    press(e, 0, "alice", hold=500)
    e.run(max_events=2)  # request + grant
    tg = e.tgs["tg"]
    assert tg.speaker == "alice"
    assert tg.state["alice"] == ReqState.GRANTED


def test_auto_release_after_hold():
    e = make_engine()
    press(e, 0, "alice", hold=700)
    e.run()
    assert e.counters["ptt_releases"] == 1
    assert e.talk_series.values == [700]
    assert e.tgs["tg"].speaker is None


def test_channel_released_when_call_ends():
    e = make_engine(channels=1)
    press(e, 0, "alice", hold=300)
    e.run()
    assert e.pool.busy == 0
    assert e.pool.free_count == 1


def test_second_request_queues_then_gets_floor():
    e = make_engine()
    press(e, 0, "alice", hold=1000)
    press(e, 100, "bob", hold=200)
    e.run()
    assert e.counters["floor_grants"] == 2
    # bob waited for alice's release at t=1025, granted at 1025+25
    assert e.grant_latency[Priority.ROUTINE].values[1] == (1025 + 25) - 100


def test_never_two_speakers():
    e = make_engine()
    for t, s in [(0, "alice"), (5, "bob"), (10, "carol"), (15, "dave")]:
        press(e, t, s, hold=400)
    e.run()
    assert e.invariants["double_speaker"] == 0
    assert e.counters["floor_grants"] == 4


def test_queue_orders_by_priority_then_fifo():
    e = make_engine()
    press(e, 0, "alice", hold=2000)
    press(e, 10, "bob", prio=Priority.ROUTINE, hold=100)
    press(e, 20, "carol", prio=Priority.HIGH, hold=100)
    press(e, 30, "dave", prio=Priority.ROUTINE, hold=100)
    e.run()
    assert e.tgs["tg"].speaker is None
    assert e.counters["floor_grants"] == 4
    # carol (HIGH) must have shorter wait than bob even though bob asked first
    bob_wait = e.grant_latency[Priority.ROUTINE].values[1]
    carol_wait = e.grant_latency[Priority.HIGH].values[0]
    assert carol_wait < bob_wait


def test_high_priority_does_not_preempt_speaker():
    e = make_engine()
    press(e, 0, "alice", prio=Priority.ROUTINE, hold=1000)
    press(e, 100, "bob", prio=Priority.HIGH, hold=100)
    e.run(max_events=3)
    tg = e.tgs["tg"]
    assert tg.speaker == "alice"
    assert tg.state["bob"] == ReqState.QUEUED
    e.run()
    assert e.counters["speaker_preemptions"] == 0


def test_duplicate_press_ignored():
    e = make_engine()
    press(e, 0, "alice", hold=1000)
    press(e, 50, "alice", hold=1000)
    e.run()
    assert e.counters["requests_ignored_duplicate"] == 1
    assert e.counters["floor_grants"] == 1


def test_offline_subscriber_request_ignored():
    e = make_engine()
    e.at(0, "drop", sub="alice")
    press(e, 10, "alice")
    e.run()
    assert e.counters["requests_ignored_offline"] == 1
    assert e.counters["floor_grants"] == 0


def test_non_member_request_ignored():
    e = make_engine()
    e.add_subscriber("eve")
    press(e, 0, "eve")
    e.run()
    assert e.counters["requests_ignored_offline"] == 1


def test_channel_exhaustion_queues_talkgroup():
    e = Engine(num_channels=1)
    for tg in ("tg_a", "tg_b"):
        e.create_talkgroup(tg)
    e.add_subscriber("alice")
    e.add_subscriber("bob")
    e.join("alice", "tg_a")
    e.join("bob", "tg_b")
    e.at(0, "ptt_request", sub="alice", tg="tg_a",
         prio=Priority.ROUTINE, hold=500)
    e.at(10, "ptt_request", sub="bob", tg="tg_b",
         prio=Priority.ROUTINE, hold=500)
    e.run(max_events=3)
    assert e.tgs["tg_b"].channel is None
    assert e.counters["channel_waits"] == 1
    e.run()
    # bob's talkgroup gets the channel once alice's call ends
    assert e.counters["floor_grants"] == 2
    assert e.invariants["double_speaker"] == 0


def test_drop_mid_transmission_promotes_next():
    e = make_engine()
    press(e, 0, "alice", hold=100000)
    press(e, 100, "bob", hold=200)
    e.at(500, "drop", sub="alice")
    e.run()
    assert e.counters["drops_mid_transmission"] == 1
    assert e.counters["floor_grants"] == 2  # bob still got the floor
    assert e.tgs["tg"].speaker is None


def test_drop_of_queued_subscriber_removes_request():
    e = make_engine()
    press(e, 0, "alice", hold=1000)
    press(e, 100, "bob", hold=200)
    e.at(200, "drop", sub="bob")
    e.run()
    assert e.counters["floor_grants"] == 1
    assert e.audit()["starvation"] == 0


def test_drop_of_pending_grant_cancels_it():
    e = make_engine()
    press(e, 0, "alice", hold=1000)
    e.at(10, "drop", sub="alice")  # grant lands at t=25, alice gone at 10
    e.run()
    assert e.counters["floor_grants"] == 0
    assert e.counters["stale_grants"] == 1
    assert e.pool.busy == 0


def test_late_entry_join_counted():
    e = make_engine()
    e.add_subscriber("eve")
    press(e, 0, "alice", hold=1000)
    e.at(500, "join", sub="eve", tg="tg")
    e.run()
    assert e.counters["late_entry_joins"] == 1
    assert "eve" in e.tgs["tg"].members


def test_stale_release_ignored():
    e = make_engine()
    press(e, 0, "alice", hold=1000)
    e.at(50, "ptt_release", sub="bob", tg="tg", grant_id=999)
    e.run()
    assert e.counters["stale_releases"] == 1
    assert e.counters["ptt_releases"] == 1


def test_utilization_between_zero_and_one():
    e = make_engine(channels=1)
    press(e, 0, "alice", hold=500)
    press(e, 2000, "bob", hold=500)
    e.run()
    u = e.channel_utilization()
    assert 0.0 < u < 1.0
