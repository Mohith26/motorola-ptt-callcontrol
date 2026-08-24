"""Trunked-radio-style group communication engine.

Talkgroups, floor control, channel allocation, emergency preemption,
and store-and-forward messaging as one deterministic event-driven
state machine.
"""
from .allocator import AllocatorError, ChannelPool
from .engine import Engine, TalkGroup
from .messaging import MessageRouter
from .model import (ALLOWED_TRANSITIONS, IllegalTransition, Priority,
                    ReqState, check_transition)
from .sim import ChaosSim, SimConfig

__all__ = [
    "AllocatorError", "ChannelPool", "Engine", "TalkGroup",
    "MessageRouter", "ALLOWED_TRANSITIONS", "IllegalTransition",
    "Priority", "ReqState", "check_transition", "ChaosSim", "SimConfig",
]
