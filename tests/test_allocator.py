import random

import pytest

from talkgroup.allocator import AllocatorError, ChannelPool


def test_acquire_returns_lowest_free():
    p = ChannelPool(3)
    assert p.acquire("a") == 0
    assert p.acquire("b") == 1


def test_exhaustion_returns_none():
    p = ChannelPool(2)
    p.acquire("a")
    p.acquire("b")
    assert p.acquire("c") is None


def test_release_makes_channel_reusable():
    p = ChannelPool(1)
    ch = p.acquire("a")
    p.release(ch, "a")
    assert p.acquire("b") == ch


def test_release_wrong_owner_raises():
    p = ChannelPool(1)
    ch = p.acquire("a")
    with pytest.raises(AllocatorError):
        p.release(ch, "b")


def test_double_release_raises():
    p = ChannelPool(1)
    ch = p.acquire("a")
    p.release(ch, "a")
    with pytest.raises(AllocatorError):
        p.release(ch, "a")


def test_release_unallocated_raises():
    p = ChannelPool(2)
    with pytest.raises(AllocatorError):
        p.release(1, "a")


def test_owner_tracking():
    p = ChannelPool(2)
    ch = p.acquire("owner1")
    assert p.owner_of(ch) == "owner1"
    assert p.owner_of(1) is None


def test_counts_stay_consistent():
    p = ChannelPool(4)
    a = p.acquire("a")
    p.acquire("b")
    assert p.busy == 2 and p.free_count == 2
    p.release(a, "a")
    assert p.busy == 1 and p.free_count == 3


def test_conservation_under_random_ops():
    """Seeded fuzz: thousands of acquire/release cycles keep the pool sane."""
    rng = random.Random(42)
    p = ChannelPool(5)
    held = {}
    for i in range(5000):
        if held and rng.random() < 0.5:
            ch = rng.choice(sorted(held))
            p.release(ch, held.pop(ch))
        else:
            owner = "o%d" % i
            ch = p.acquire(owner)
            if ch is not None:
                held[ch] = owner
        assert p.busy + p.free_count == 5
    assert p.checks_run >= 5000 // 2
