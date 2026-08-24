from talkgroup.engine import Engine


def make_engine(loss_fn=None):
    e = Engine(num_channels=2, loss_fn=loss_fn)
    e.create_talkgroup("tg")
    for s in ("alice", "bob", "carol"):
        e.add_subscriber(s)
        e.join(s, "tg")
    return e


def test_group_message_delivered_to_members_not_sender():
    e = make_engine()
    e.at(0, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.run()
    a = e.router.audit()
    assert a["expected_pairs"] == 2  # bob and carol, not alice
    assert a["message_loss"] == 0
    assert a["message_duplicate_delivery"] == 0
    assert e.router.stats["messages_delivered"] == 2


def test_receipts_confirmed_on_clean_network():
    e = make_engine()
    e.at(0, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.run()
    assert e.router.stats["receipts_confirmed"] == 2
    assert e.router.stats["retries"] == 0


def test_offline_member_gets_message_on_rejoin():
    e = make_engine()
    e.at(0, "drop", sub="bob")
    e.at(10, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.at(5000, "rejoin", sub="bob")
    e.run()
    a = e.router.audit()
    assert a["message_loss"] == 0
    assert e.router.stats["messages_stored"] == 1
    assert e.router.stats["mailbox_flushes"] == 1


def test_drop_between_send_and_delivery_goes_to_mailbox():
    e = make_engine()
    e.at(0, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.at(5, "drop", sub="bob")  # delivery would land at t=20
    e.at(1000, "rejoin", sub="bob")
    e.run()
    assert e.router.audit()["message_loss"] == 0
    assert e.router.stats["messages_stored"] == 1


def test_lost_delivery_is_retried_until_it_lands():
    calls = []

    def loss(kind, msg_id, rcpt, attempt):
        calls.append((kind, rcpt, attempt))
        return kind == "data" and attempt == 1  # first attempt always lost

    e = make_engine(loss_fn=loss)
    e.at(0, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.run()
    a = e.router.audit()
    assert a["message_loss"] == 0
    assert e.router.stats["retries"] == 2  # one retry per recipient
    assert e.router.stats["data_losses"] == 2


def test_lost_receipt_causes_retransmit_but_no_duplicate():
    def loss(kind, msg_id, rcpt, attempt):
        return kind == "receipt" and attempt == 1

    e = make_engine(loss_fn=loss)
    e.at(0, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.run()
    a = e.router.audit()
    assert a["message_loss"] == 0
    assert a["message_duplicate_delivery"] == 0
    # the retransmit arrived but was deduped by the receiver
    assert e.router.stats["duplicates_suppressed"] == 2
    assert e.router.stats["messages_delivered"] == 2


def test_multiple_messages_are_independent():
    e = make_engine()
    for i in range(5):
        e.at(i * 10, "msg_send", msg_id="m%d" % i, src="alice", tg="tg")
    e.run()
    a = e.router.audit()
    assert a["expected_pairs"] == 10
    assert a["message_loss"] == 0
    assert a["message_duplicate_delivery"] == 0


def test_exactly_once_audit_counts_are_per_pair():
    e = make_engine()
    e.at(0, "msg_send", msg_id="m1", src="alice", tg="tg")
    e.at(0, "msg_send", msg_id="m2", src="bob", tg="tg")
    e.run()
    for key, n in e.router.delivered_count.items():
        assert n == 1, key
