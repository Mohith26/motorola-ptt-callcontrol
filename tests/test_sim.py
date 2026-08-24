from talkgroup.sim import ChaosSim, SimConfig

SMALL = dict(subscribers=60, talkgroups=8, channels=4, horizon=120_000,
             press_rate=0.01, storms=4, messages=300, drops=40,
             late_joins=30)


def run_small(seed):
    return ChaosSim(SimConfig(seed=seed, **SMALL)).run()


def test_chaos_run_holds_all_invariants():
    rep = run_small(7)
    inv = rep["invariants"]
    assert inv["double_speaker"] == 0
    assert inv["emergency_bound_violations"] == 0
    assert inv["starvation"] == 0
    assert inv["message_loss"] == 0
    assert inv["message_duplicate_delivery"] == 0


def test_chaos_run_actually_exercises_chaos():
    rep = run_small(7)
    c = rep["counters"]
    assert c["speaker_preemptions"] > 0
    assert c["drops_mid_transmission"] > 0
    assert c["channel_waits"] > 0
    assert rep["messaging"]["retries"] > 0
    assert rep["messaging"]["duplicates_suppressed"] > 0
    assert rep["messaging"]["messages_stored"] > 0


def test_determinism_same_seed_same_digest():
    d1 = ChaosSim.digest(run_small(21))
    d2 = ChaosSim.digest(run_small(21))
    assert d1 == d2


def test_different_seeds_diverge():
    d1 = ChaosSim.digest(run_small(21))
    d2 = ChaosSim.digest(run_small(22))
    assert d1 != d2


def test_event_volume_scales_with_config():
    rep = run_small(7)
    assert rep["events_processed"] > 10_000


def test_utilization_is_a_ratio():
    rep = run_small(7)
    assert 0.0 <= rep["channel_utilization"] <= 1.0


def test_emergency_latency_measured():
    rep = run_small(7)
    s = rep["emergency_preempt_latency"]
    assert s["count"] > 0
    assert s["p50"] <= s["p99"] <= s["max"]


def test_fairness_no_request_left_behind():
    rep = run_small(9)
    # after drain, every request was either granted or its subscriber
    # dropped; nobody is still stuck in a queue
    assert rep["invariants"]["starvation"] == 0
    assert rep["fairness_wait"]["count"] > 0
    assert rep["fairness_wait"]["max"] < rep["virtual_ticks"]


def test_report_is_json_serializable():
    import json
    rep = run_small(7)
    json.dumps(rep, sort_keys=True)
