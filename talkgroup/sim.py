"""Seeded chaos simulation.

All external stimuli (PTT presses, emergency injections, request storms,
drop-offs, rejoins, late joins, group messages) are pre-generated from a
single seeded RNG and injected into the engine's event heap up front.
Message loss uses a second RNG derived from the same seed. Everything
after that is the deterministic engine, so a (seed, config) pair always
produces bit-identical metrics. That is what makes the invariant counts
trustworthy: rerunning the same seed reproduces the exact same 100k+
event history.

The horizon splits the run in two: chaos is injected only before the
horizon, and the network becomes lossless after it, so retries converge
and every subscriber is brought back online to flush mailboxes. The
audit then checks that queues drained and every message landed exactly
once.
"""
import hashlib
import json
import random

from .engine import Engine
from .model import Priority


class SimConfig:
    def __init__(self, seed=1, subscribers=220, talkgroups=24, channels=8,
                 horizon=1_200_000, press_rate=0.02, storms=20,
                 storm_size=30, storm_window=500, hold_min=300,
                 hold_max=1200, emergency_frac=0.02, high_frac=0.08,
                 messages=3500, drops=400, offline_min=5_000,
                 offline_max=40_000, late_joins=300, loss_rate=0.15,
                 grant_delay=25, preempt_grant_delay=10):
        self.__dict__.update(locals())
        del self.__dict__["self"]

    def as_dict(self):
        return dict(self.__dict__)


class ChaosSim:
    def __init__(self, cfg):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.loss_rng = random.Random(cfg.seed ^ 0x5EED_CAFE)
        self.engine = Engine(num_channels=cfg.channels,
                             grant_delay=cfg.grant_delay,
                             preempt_grant_delay=cfg.preempt_grant_delay,
                             loss_fn=self._loss)
        self._build()
        self._generate()

    def _loss(self, kind, msg_id, rcpt, attempt):
        # lossless after the horizon so the drain phase converges
        if self.engine.now >= self.cfg.horizon:
            return False
        return self.loss_rng.random() < self.cfg.loss_rate

    def _build(self):
        cfg = self.cfg
        e = self.engine
        for t in range(cfg.talkgroups):
            e.create_talkgroup("tg%03d" % t)
        tg_ids = sorted(e.tgs)
        self.memberships = {}
        for s in range(cfg.subscribers):
            sub = "sub%04d" % s
            e.add_subscriber(sub)
            n = self.rng.randint(1, 3)
            mine = self.rng.sample(tg_ids, n)
            self.memberships[sub] = mine
            for tg in mine:
                e.join(sub, tg)

    def _pick_prio(self):
        r = self.rng.random()
        if r < self.cfg.emergency_frac:
            return Priority.EMERGENCY
        if r < self.cfg.emergency_frac + self.cfg.high_frac:
            return Priority.HIGH
        return Priority.ROUTINE

    def _press(self, t, sub, tg=None, prio=None):
        if tg is None:
            tg = self.rng.choice(self.memberships[sub])
        if prio is None:
            prio = self._pick_prio()
        hold = self.rng.randint(self.cfg.hold_min, self.cfg.hold_max)
        self.engine.at(t, "ptt_request", sub=sub, tg=tg,
                       prio=prio, hold=hold)

    def _generate(self):
        cfg = self.cfg
        e = self.engine
        subs = sorted(self.memberships)
        tg_ids = sorted(e.tgs)
        # background press load
        n_presses = int(cfg.press_rate * cfg.horizon)
        for _ in range(n_presses):
            t = self.rng.randrange(cfg.horizon)
            self._press(t, self.rng.choice(subs))
        # request storms: many subscribers hammer one talkgroup at once
        for _ in range(cfg.storms):
            t0 = self.rng.randrange(cfg.horizon)
            tg = self.rng.choice(tg_ids)
            pressers = [s for s in subs if tg in self.memberships[s]]
            for _ in range(cfg.storm_size):
                if not pressers:
                    break
                s = self.rng.choice(pressers)
                self._press(t0 + self.rng.randrange(cfg.storm_window), s,
                            tg=tg)
        # dedicated emergency injections on top of the background mix
        for _ in range(max(1, n_presses // 50)):
            t = self.rng.randrange(cfg.horizon)
            s = self.rng.choice(subs)
            self._press(t, s, prio=Priority.EMERGENCY)
        # drops and rejoins
        for _ in range(cfg.drops):
            s = self.rng.choice(subs)
            t = self.rng.randrange(cfg.horizon)
            e.at(t, "drop", sub=s)
            back = t + self.rng.randint(cfg.offline_min, cfg.offline_max)
            e.at(back, "rejoin", sub=s)
        # late-entry joins into extra talkgroups
        for _ in range(cfg.late_joins):
            s = self.rng.choice(subs)
            tg = self.rng.choice(tg_ids)
            t = self.rng.randrange(cfg.horizon)
            e.at(t, "join", sub=s, tg=tg)
            if tg not in self.memberships[s]:
                self.memberships[s].append(tg)
        # group messages
        for i in range(cfg.messages):
            s = self.rng.choice(subs)
            tg = self.rng.choice(self.memberships[s])
            t = self.rng.randrange(cfg.horizon)
            e.at(t, "msg_send", msg_id="m%05d" % i, src=s, tg=tg)
        # end of chaos: bring everyone back so mailboxes can flush
        e.at(cfg.horizon, "rejoin", sub=subs[0])
        for s in subs:
            e.at(cfg.horizon + 1, "rejoin", sub=s)

    def run(self):
        self.engine.run()
        return self.report()

    def report(self):
        e = self.engine
        rep = {
            "config": self.cfg.as_dict(),
            "events_processed": e.events_processed,
            "virtual_ticks": e.now,
            "invariants": e.audit(),
            "grant_latency": {
                p.name.lower(): e.grant_latency[p].summary()
                for p in Priority
            },
            "emergency_preempt_latency": e.emergency_latency.summary(),
            "fairness_wait": e.wait_series.summary(),
            "talk_time": e.talk_series.summary(),
            "channel_utilization": round(e.channel_utilization(), 4),
            "counters": dict(sorted(e.counters.items())),
            "messaging": dict(sorted(e.router.stats.items())),
        }
        return rep

    @staticmethod
    def digest(report):
        blob = json.dumps(report, sort_keys=True).encode()
        return hashlib.sha256(blob).hexdigest()
