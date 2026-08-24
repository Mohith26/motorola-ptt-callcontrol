import itertools

import pytest

from talkgroup.model import (ALLOWED_TRANSITIONS, IllegalTransition,
                             Priority, ReqState, check_transition)

ALL_PAIRS = list(itertools.product(list(ReqState), list(ReqState)))


@pytest.mark.parametrize("old,new", ALL_PAIRS,
                         ids=["%s_to_%s" % (a.value, b.value)
                              for a, b in ALL_PAIRS])
def test_transition_matrix(old, new):
    """Full 5x5 transition matrix: allowed pairs pass, all others raise."""
    if new in ALLOWED_TRANSITIONS[old]:
        check_transition(old, new)
    else:
        with pytest.raises(IllegalTransition):
            check_transition(old, new)


def test_every_state_has_transition_rules():
    assert set(ALLOWED_TRANSITIONS) == set(ReqState)


def test_idle_only_reaches_requesting():
    assert ALLOWED_TRANSITIONS[ReqState.IDLE] == {ReqState.REQUESTING}


def test_granted_cannot_jump_back_to_queued():
    assert ReqState.QUEUED not in ALLOWED_TRANSITIONS[ReqState.GRANTED]


def test_priority_ordering():
    assert Priority.EMERGENCY > Priority.HIGH > Priority.ROUTINE
