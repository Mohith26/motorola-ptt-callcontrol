"""Store-and-forward messaging with delivery receipts.

Semantics I wanted to nail down:
- At-least-once on the wire: a delivery attempt can be lost, and the
  sender keeps a retry timer per (message, recipient) until it sees a
  receipt.
- Exactly-once at the application layer: the receiver dedupes by
  message id, so retransmits caused by a lost receipt never surface
  twice. Suppressed duplicates are counted, not hidden.
- Store-and-forward: if the recipient is offline (at dispatch or at
  delivery time), the message parks in a mailbox and is flushed when
  the subscriber comes back.

The router never touches wall time. The engine owns the clock and
drives the router through scheduled events, which keeps the whole
thing deterministic and auditable.
"""
from collections import defaultdict


class MessageRouter:
    def __init__(self, engine, delivery_delay=20, retry_timeout=250, loss_fn=None):
        self.engine = engine
        self.delivery_delay = delivery_delay
        self.retry_timeout = retry_timeout
        # loss_fn(kind, msg_id, rcpt, attempt) -> True if that transmission
        # is lost. kind is "data" or "receipt". None means a perfect network.
        self.loss_fn = loss_fn
        self.outstanding = {}  # (msg_id, rcpt) -> {"attempts": n, "acked": bool}
        self.seen = defaultdict(set)        # rcpt -> {msg_id}
        self.mailbox = defaultdict(list)    # rcpt -> [msg_id]
        self.expected = defaultdict(set)    # msg_id -> {rcpt}
        self.delivered_count = defaultdict(int)  # (msg_id, rcpt) -> n
        self.stats = defaultdict(int)

    # -- sending ---------------------------------------------------------

    def send(self, msg_id, src, tg_id):
        tg = self.engine.tgs[tg_id]
        self.stats["messages_sent"] += 1
        for rcpt in sorted(tg.members):
            if rcpt == src:
                continue
            self.expected[msg_id].add(rcpt)
            self._dispatch(msg_id, rcpt)

    def _lost(self, kind, msg_id, rcpt, attempt):
        if self.loss_fn is None:
            return False
        return bool(self.loss_fn(kind, msg_id, rcpt, attempt))

    def _store(self, msg_id, rcpt):
        if msg_id in self.seen[rcpt]:
            return
        if msg_id not in self.mailbox[rcpt]:
            self.mailbox[rcpt].append(msg_id)
            self.stats["messages_stored"] += 1

    def _dispatch(self, msg_id, rcpt):
        key = (msg_id, rcpt)
        if rcpt not in self.engine.online:
            rec = self.outstanding.get(key)
            if rec is not None:
                rec["acked"] = True  # mailbox owns it now
            self._store(msg_id, rcpt)
            return
        rec = self.outstanding.setdefault(key, {"attempts": 0, "acked": False})
        rec["attempts"] += 1
        attempt = rec["attempts"]
        self.stats["transmissions"] += 1
        if self._lost("data", msg_id, rcpt, attempt):
            self.stats["data_losses"] += 1
        else:
            self.engine.schedule(self.delivery_delay, "msg_deliver",
                                 msg_id=msg_id, rcpt=rcpt)
        self.engine.schedule(self.retry_timeout, "msg_retry",
                             msg_id=msg_id, rcpt=rcpt, attempt=attempt)

    # -- event handlers (called by the engine) ---------------------------

    def on_deliver(self, msg_id, rcpt):
        key = (msg_id, rcpt)
        if rcpt not in self.engine.online:
            rec = self.outstanding.get(key)
            if rec is not None:
                rec["acked"] = True
            self._store(msg_id, rcpt)
            return
        if msg_id in self.seen[rcpt]:
            self.stats["duplicates_suppressed"] += 1
        else:
            self.seen[rcpt].add(msg_id)
            self.delivered_count[key] += 1
            self.stats["messages_delivered"] += 1
        # receipt travels back over the same lossy network
        rec = self.outstanding.setdefault(key, {"attempts": 1, "acked": False})
        if self._lost("receipt", msg_id, rcpt, rec["attempts"]):
            self.stats["receipt_losses"] += 1
        else:
            self.engine.schedule(self.delivery_delay, "msg_receipt",
                                 msg_id=msg_id, rcpt=rcpt)

    def on_receipt(self, msg_id, rcpt):
        rec = self.outstanding.get((msg_id, rcpt))
        if rec is not None and not rec["acked"]:
            rec["acked"] = True
            self.stats["receipts_confirmed"] += 1

    def on_retry(self, msg_id, rcpt, attempt):
        rec = self.outstanding.get((msg_id, rcpt))
        if rec is None or rec["acked"]:
            return
        if rec["attempts"] != attempt:
            return  # a newer attempt is already in flight
        self.stats["retries"] += 1
        if rcpt not in self.engine.online:
            rec["acked"] = True
            self._store(msg_id, rcpt)
            return
        self._dispatch(msg_id, rcpt)

    def flush(self, rcpt):
        """Deliver mailbox contents after a subscriber comes back online."""
        pending = self.mailbox.pop(rcpt, [])
        for msg_id in pending:
            if msg_id in self.seen[rcpt]:
                continue
            rec = self.outstanding.get((msg_id, rcpt))
            if rec is not None:
                rec["acked"] = False
            self.stats["mailbox_flushes"] += 1
            self._dispatch(msg_id, rcpt)

    # -- audit -----------------------------------------------------------

    def audit(self):
        """Exactly-once audit over every (message, recipient) pair."""
        loss = 0
        dup = 0
        for msg_id, rcpts in self.expected.items():
            for rcpt in rcpts:
                c = self.delivered_count[(msg_id, rcpt)]
                if c == 0:
                    loss += 1
                elif c > 1:
                    dup += 1
        return {
            "expected_pairs": sum(len(r) for r in self.expected.values()),
            "message_loss": loss,
            "message_duplicate_delivery": dup,
            "duplicates_suppressed": self.stats["duplicates_suppressed"],
            "retries": self.stats["retries"],
        }
