"""Channel pool allocator.

A trunked system has a small pool of traffic channels shared by many
talkgroups. The allocator hands out the lowest free channel, tracks
ownership, and self-checks conservation (free + assigned == total,
no channel in both sets) on every operation. Misuse raises instead of
silently leaking, so a completed run is itself evidence the invariant
held for every allocation.
"""
import bisect


class AllocatorError(Exception):
    pass


class ChannelPool:
    def __init__(self, num_channels: int):
        if num_channels < 1:
            raise ValueError("need at least one channel")
        self.num_channels = num_channels
        self._free = list(range(num_channels))
        self._assigned = {}  # channel -> owner id
        self.checks_run = 0

    @property
    def busy(self) -> int:
        return len(self._assigned)

    @property
    def free_count(self) -> int:
        return len(self._free)

    def acquire(self, owner):
        """Assign the lowest free channel to owner, or None if exhausted."""
        if not self._free:
            return None
        ch = self._free.pop(0)
        if ch in self._assigned:
            raise AllocatorError("channel %d already assigned" % ch)
        self._assigned[ch] = owner
        self._check()
        return ch

    def release(self, channel, owner):
        if self._assigned.get(channel) != owner:
            raise AllocatorError(
                "release of channel %s by %r but owner is %r"
                % (channel, owner, self._assigned.get(channel))
            )
        del self._assigned[channel]
        bisect.insort(self._free, channel)
        self._check()

    def owner_of(self, channel):
        return self._assigned.get(channel)

    def _check(self):
        self.checks_run += 1
        if len(self._free) + len(self._assigned) != self.num_channels:
            raise AllocatorError("channel conservation violated")
        if set(self._free) & set(self._assigned):
            raise AllocatorError("channel in both free and assigned sets")
