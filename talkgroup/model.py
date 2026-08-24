"""Core domain types for the talkgroup call-control engine.

Priorities and the per-request floor-control state machine live here.
The state machine is deliberately tiny and every transition is checked
against an explicit allow list, so an illegal transition is a bug that
fails loudly instead of silently corrupting call state.
"""
from enum import Enum, IntEnum


class Priority(IntEnum):
    ROUTINE = 0
    HIGH = 1
    EMERGENCY = 2


class ReqState(Enum):
    IDLE = "idle"
    REQUESTING = "requesting"
    QUEUED = "queued"
    GRANTED = "granted"
    PREEMPTED = "preempted"


# Allowed floor-control transitions for a single subscriber within one
# talkgroup. REQUESTING means "a grant is in flight for this subscriber".
ALLOWED_TRANSITIONS = {
    ReqState.IDLE: {ReqState.REQUESTING},
    ReqState.REQUESTING: {ReqState.GRANTED, ReqState.QUEUED, ReqState.IDLE},
    ReqState.QUEUED: {ReqState.REQUESTING, ReqState.IDLE},
    ReqState.GRANTED: {ReqState.IDLE, ReqState.PREEMPTED},
    ReqState.PREEMPTED: {ReqState.QUEUED, ReqState.IDLE},
}


class IllegalTransition(Exception):
    """Raised when the floor-control state machine is driven illegally."""


def check_transition(old: ReqState, new: ReqState) -> None:
    if new not in ALLOWED_TRANSITIONS[old]:
        raise IllegalTransition("%s -> %s" % (old.value, new.value))
