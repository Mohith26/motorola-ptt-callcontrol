"""Event-driven call-control engine.

Everything runs on a single virtual clock (integer ticks, think
milliseconds). Events live in a heap keyed by (time, sequence), so a
run is fully deterministic for a given input schedule. The engine owns:

- talkgroup registry and membership,
- per-talkgroup floor control (one speaker at a time),
- the shared channel pool with a priority wait queue,
- emergency preemption of speakers, pending grants, and channels,
- store-and-forward messaging via MessageRouter,
- invariant counters and latency series.

Design choices worth calling out:

- Grants are not instantaneous. When arbitration decides a subscriber
  should get the floor, a floor_grant event is scheduled after a
  processing delay. Emergency preemption uses a shorter delay. A
  pending grant can itself be preempted by an emergency before it
  lands; stale grants are fenced off with a per-talkgroup epoch counter.
- Only EMERGENCY preempts. HIGH sorts ahead of ROUTINE in queues but
  never rips the floor away from an active speaker.
- A preempted speaker is automatically requeued with its original
  request time and its remaining talk time, so preemption never loses
  the tail of a transmission.
- PTT presses carry a hold duration; the engine schedules the release
  itself once the grant lands. That makes "every queued request is
  eventually served" a checkable end-of-run invariant instead of a hope.
"""
import heapq
from collections import defaultdict

from .allocator import ChannelPool
from .messaging import MessageRouter
from .metrics import Series
from .model import Priority, ReqState, check_transition


class TalkGroup:
    def __init__(self, tg_id):
        self.tg_id = tg_id
        self.members = set()
        self.channel = None
        self.speaker = None
        self.speaker_prio = None
        self.speaker_since = None
        self.grant_id = None
        self.pending = None  # {"sub", "prio"} while a grant is in flight
        self.epoch = 0
        self.queue = []  # heap of (-prio, req_time, seq, sub)
        self.state = {}  # sub -> ReqState
        self.req = {}    # sub -> request info dict

    def queued_subs(self):
        return [e[3] for e in self.queue
                if self.state.get(e[3]) == ReqState.QUEUED]

    def effective_priority(self):
        """Highest priority with a live claim on this talkgroup's channel."""
        best = -1
        if self.speaker is not None:
            best = max(best, int(self.speaker_prio))
        if self.pending is not None:
            best = max(best, int(self.pending["prio"]))
        for sub in self.queued_subs():
            best = max(best, int(self.req[sub]["prio"]))
        return best

    def wants_channel(self):
        return (self.speaker is not None or self.pending is not None
                or bool(self.queued_subs()))


class Engine:
    def __init__(self, num_channels=8, grant_delay=25, preempt_grant_delay=10,
                 delivery_delay=20, retry_timeout=250, loss_fn=None,
                 emergency_bound=None):
        self.now = 0
        self._q = []
        self._seq = 0
        self._grant_counter = 0
        self.events_processed = 0
        self.grant_delay = grant_delay
        self.preempt_grant_delay = preempt_grant_delay
        self.emergency_bound = (emergency_bound if emergency_bound is not None
                                else max(grant_delay, preempt_grant_delay))
        self.pool = ChannelPool(num_channels)
        self.tgs = {}
        self.subs = set()
        self.online = set()
        self.channel_waitq = []  # heap of (-prio, time, seq, tg_id)
        self.router = MessageRouter(self, delivery_delay=delivery_delay,
                                    retry_timeout=retry_timeout,
                                    loss_fn=loss_fn)
        self.counters = defaultdict(int)
        self.grant_latency = {p: Series() for p in Priority}
        self.emergency_latency = Series()
        self.wait_series = Series()
        self.talk_series = Series()
        self.invariants = defaultdict(int)
        self._util_area = 0

    # -- registry --------------------------------------------------------

    def add_subscriber(self, sub, online=True):
        self.subs.add(sub)
        if online:
            self.online.add(sub)

    def create_talkgroup(self, tg_id):
        tg = TalkGroup(tg_id)
        self.tgs[tg_id] = tg
        return tg

    def join(self, sub, tg_id):
        tg = self.tgs[tg_id]
        tg.members.add(sub)
        tg.state.setdefault(sub, ReqState.IDLE)
        if tg.speaker is not None:
            self.counters["late_entry_joins"] += 1

    # -- clock and queue -------------------------------------------------

    def schedule(self, delay, kind, **data):
        self.at(self.now + delay, kind, **data)

    def at(self, t, kind, **data):
        heapq.heappush(self._q, (t, self._seq, kind, data))
        self._seq += 1

    def run(self, max_events=None):
        while self._q:
            t, _seq, kind, data = heapq.heappop(self._q)
            self._advance(t)
            self._dispatch(kind, data)
            self.events_processed += 1
            if max_events is not None and self.events_processed >= max_events:
                break

    def _advance(self, t):
        if t < self.now:
            raise RuntimeError("time went backwards")
        self._util_area += self.pool.busy * (t - self.now)
        self.now = t

    def channel_utilization(self):
        if self.now == 0:
            return 0.0
        return self._util_area / (self.pool.num_channels * self.now)

    def _dispatch(self, kind, d):
        if kind == "ptt_request":
            self._handle_request(d["sub"], d["tg"], d["prio"], d["hold"])
        elif kind == "floor_grant":
            self._handle_grant(d["tg"], d["sub"], d["epoch"])
        elif kind == "ptt_release":
            self._handle_release(d["sub"], d["tg"], d["grant_id"])
        elif kind == "drop":
            self._handle_drop(d["sub"])
        elif kind == "rejoin":
            self._handle_rejoin(d["sub"])
        elif kind == "join":
            self.join(d["sub"], d["tg"])
        elif kind == "msg_send":
            self.router.send(d["msg_id"], d["src"], d["tg"])
        elif kind == "msg_deliver":
            self.router.on_deliver(d["msg_id"], d["rcpt"])
        elif kind == "msg_receipt":
            self.router.on_receipt(d["msg_id"], d["rcpt"])
        elif kind == "msg_retry":
            self.router.on_retry(d["msg_id"], d["rcpt"], d["attempt"])
        else:
            raise ValueError("unknown event kind %r" % kind)

    # -- state helpers ---------------------------------------------------

    def _set_state(self, tg, sub, new):
        old = tg.state.get(sub, ReqState.IDLE)
        check_transition(old, new)
        tg.state[sub] = new

    def _push_queue(self, tg, sub):
        info = tg.req[sub]
        heapq.heappush(tg.queue,
                       (-int(info["prio"]), info["req_time"], self._seq, sub))
        self._seq += 1

    # -- PTT request path ------------------------------------------------

    def _handle_request(self, sub, tg_id, prio, hold):
        tg = self.tgs[tg_id]
        if sub not in self.online or sub not in tg.members:
            self.counters["requests_ignored_offline"] += 1
            return
        if tg.state.get(sub, ReqState.IDLE) != ReqState.IDLE:
            self.counters["requests_ignored_duplicate"] += 1
            return
        prio = Priority(prio)
        self.counters["ptt_requests"] += 1
        self._set_state(tg, sub, ReqState.REQUESTING)
        tg.req[sub] = {"prio": prio, "req_time": self.now, "hold": hold,
                       "behind_emergency": False, "granted_before": False}
        self._arbitrate(tg, sub, prio)

    def _arbitrate(self, tg, sub, prio):
        if tg.pending is not None:
            if prio == Priority.EMERGENCY and prio > tg.pending["prio"]:
                victim = tg.pending["sub"]
                tg.pending = None
                tg.epoch += 1
                self._set_state(tg, victim, ReqState.QUEUED)
                self._push_queue(tg, victim)
                self.counters["pending_preemptions"] += 1
                self._schedule_grant(tg, sub, preempt=True)
            else:
                if (prio == Priority.EMERGENCY
                        and tg.pending["prio"] == Priority.EMERGENCY):
                    tg.req[sub]["behind_emergency"] = True
                self._set_state(tg, sub, ReqState.QUEUED)
                self._push_queue(tg, sub)
            return
        if tg.speaker is not None:
            if prio == Priority.EMERGENCY and prio > tg.speaker_prio:
                self._preempt_speaker(tg)
                self._schedule_grant(tg, sub, preempt=True)
            else:
                if (prio == Priority.EMERGENCY
                        and tg.speaker_prio == Priority.EMERGENCY):
                    tg.req[sub]["behind_emergency"] = True
                self._set_state(tg, sub, ReqState.QUEUED)
                self._push_queue(tg, sub)
            return
        # floor is free
        if tg.channel is None:
            ch = self.pool.acquire(tg.tg_id)
            if ch is None and prio == Priority.EMERGENCY:
                ch = self._preempt_channel(tg)
                if ch is None:
                    tg.req[sub]["behind_emergency"] = True
            if ch is None:
                self._set_state(tg, sub, ReqState.QUEUED)
                self._push_queue(tg, sub)
                heapq.heappush(self.channel_waitq,
                               (-int(prio), self.now, self._seq, tg.tg_id))
                self._seq += 1
                self.counters["channel_waits"] += 1
                return
            tg.channel = ch
        self._schedule_grant(tg, sub, preempt=False)

    def _schedule_grant(self, tg, sub, preempt):
        delay = self.preempt_grant_delay if preempt else self.grant_delay
        tg.pending = {"sub": sub, "prio": tg.req[sub]["prio"]}
        self.schedule(delay, "floor_grant", tg=tg.tg_id, sub=sub,
                      epoch=tg.epoch)

    def _handle_grant(self, tg_id, sub, epoch):
        tg = self.tgs[tg_id]
        if (epoch != tg.epoch or tg.pending is None
                or tg.pending["sub"] != sub):
            self.counters["stale_grants"] += 1
            return
        if tg.speaker is not None:
            self.invariants["double_speaker"] += 1
        info = tg.req[sub]
        tg.pending = None
        self._set_state(tg, sub, ReqState.GRANTED)
        tg.speaker = sub
        tg.speaker_prio = info["prio"]
        tg.speaker_since = self.now
        self._grant_counter += 1
        tg.grant_id = self._grant_counter
        self.counters["floor_grants"] += 1
        if not info["granted_before"]:
            info["granted_before"] = True
            latency = self.now - info["req_time"]
            self.grant_latency[info["prio"]].record(latency)
            self.wait_series.record(latency)
            if info["prio"] == Priority.EMERGENCY:
                self.emergency_latency.record(latency)
                if (not info["behind_emergency"]
                        and latency > self.emergency_bound):
                    self.invariants["emergency_bound"] += 1
        else:
            self.counters["regrants_after_preemption"] += 1
        self.schedule(max(1, info["hold"]), "ptt_release", sub=sub,
                      tg=tg_id, grant_id=tg.grant_id)

    # -- release path ----------------------------------------------------

    def _handle_release(self, sub, tg_id, grant_id):
        tg = self.tgs[tg_id]
        if tg.speaker != sub or tg.grant_id != grant_id:
            self.counters["stale_releases"] += 1
            return
        self.talk_series.record(self.now - tg.speaker_since)
        self._clear_speaker(tg)
        self._set_state(tg, sub, ReqState.IDLE)
        tg.req.pop(sub, None)
        self.counters["ptt_releases"] += 1
        self._next(tg)

    def _clear_speaker(self, tg):
        tg.speaker = None
        tg.speaker_prio = None
        tg.speaker_since = None
        tg.grant_id = None

    def _next(self, tg):
        if tg.pending is not None:
            return
        while tg.queue:
            entry = heapq.heappop(tg.queue)
            sub = entry[3]
            if tg.state.get(sub) != ReqState.QUEUED:
                continue
            if tg.channel is None:
                # lost the channel while queued; put the entry back and
                # wait for the pool to serve this talkgroup again
                heapq.heappush(tg.queue, entry)
                return
            self._set_state(tg, sub, ReqState.REQUESTING)
            self._schedule_grant(tg, sub, preempt=False)
            return
        if tg.channel is not None:
            self.pool.release(tg.channel, tg.tg_id)
            tg.channel = None
            self._serve_waitq()

    def _serve_waitq(self):
        while self.pool.free_count > 0 and self.channel_waitq:
            _p, _t, _s, tg_id = heapq.heappop(self.channel_waitq)
            tg = self.tgs[tg_id]
            if tg.channel is not None or not tg.wants_channel():
                continue
            tg.channel = self.pool.acquire(tg_id)
            self._next(tg)

    # -- preemption ------------------------------------------------------

    def _preempt_speaker(self, tg):
        sub = tg.speaker
        info = tg.req[sub]
        elapsed = self.now - tg.speaker_since
        info["hold"] = max(1, info["hold"] - elapsed)
        self.talk_series.record(elapsed)
        self._set_state(tg, sub, ReqState.PREEMPTED)
        self._clear_speaker(tg)
        self._set_state(tg, sub, ReqState.QUEUED)
        self._push_queue(tg, sub)
        self.counters["speaker_preemptions"] += 1

    def _preempt_channel(self, requesting_tg):
        """Steal a channel from the lowest-priority active call."""
        victim = None
        victim_eff = None
        for tg_id in sorted(self.tgs):
            tg = self.tgs[tg_id]
            if tg is requesting_tg or tg.channel is None:
                continue
            eff = tg.effective_priority()
            if eff >= int(Priority.EMERGENCY):
                continue
            if victim is None or eff < victim_eff:
                victim = tg
                victim_eff = eff
        if victim is None:
            return None
        if victim.pending is not None:
            sub = victim.pending["sub"]
            victim.pending = None
            victim.epoch += 1
            self._set_state(victim, sub, ReqState.QUEUED)
            self._push_queue(victim, sub)
        if victim.speaker is not None:
            self._preempt_speaker(victim)
        ch = victim.channel
        self.pool.release(ch, victim.tg_id)
        victim.channel = None
        self.counters["channel_preemptions"] += 1
        if victim.wants_channel():
            best = victim.effective_priority()
            earliest = min((victim.req[s]["req_time"]
                            for s in victim.queued_subs()), default=self.now)
            heapq.heappush(self.channel_waitq,
                           (-best, earliest, self._seq, victim.tg_id))
            self._seq += 1
        got = self.pool.acquire(requesting_tg.tg_id)
        if got is None:
            raise RuntimeError("channel preemption failed to free a channel")
        return got

    # -- subscriber lifecycle --------------------------------------------

    def _handle_drop(self, sub):
        if sub not in self.online:
            return
        self.online.discard(sub)
        self.counters["drops"] += 1
        for tg in self.tgs.values():
            if sub not in tg.members:
                continue
            st = tg.state.get(sub, ReqState.IDLE)
            if st == ReqState.GRANTED:
                self.counters["drops_mid_transmission"] += 1
                self.talk_series.record(self.now - tg.speaker_since)
                self._clear_speaker(tg)
                self._set_state(tg, sub, ReqState.IDLE)
                tg.req.pop(sub, None)
                self._next(tg)
            elif st == ReqState.REQUESTING:
                if tg.pending is not None and tg.pending["sub"] == sub:
                    tg.pending = None
                    tg.epoch += 1
                self._set_state(tg, sub, ReqState.IDLE)
                tg.req.pop(sub, None)
                self._next(tg)
            elif st == ReqState.QUEUED:
                self._set_state(tg, sub, ReqState.IDLE)
                tg.req.pop(sub, None)

    def _handle_rejoin(self, sub):
        if sub in self.online:
            return
        self.online.add(sub)
        self.counters["rejoins"] += 1
        self.router.flush(sub)

    # -- audit -----------------------------------------------------------

    def audit(self):
        """End-of-run invariant audit. Call after the queue drains."""
        starvation = 0
        for tg in self.tgs.values():
            for sub, st in tg.state.items():
                if st in (ReqState.QUEUED, ReqState.REQUESTING,
                          ReqState.PREEMPTED) and sub in self.online:
                    starvation += 1
        result = {
            "double_speaker": self.invariants["double_speaker"],
            "emergency_bound_violations": self.invariants["emergency_bound"],
            "starvation": starvation,
            "allocator_checks_run": self.pool.checks_run,
        }
        result.update(self.router.audit())
        return result
