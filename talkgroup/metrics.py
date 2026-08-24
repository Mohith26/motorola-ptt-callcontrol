"""Latency series and percentile helpers."""


class Series:
    """Append-only sample series with nearest-rank percentiles."""

    def __init__(self):
        self._v = []

    def record(self, x):
        self._v.append(x)

    @property
    def count(self):
        return len(self._v)

    @property
    def values(self):
        return list(self._v)

    def mean(self):
        if not self._v:
            return None
        return sum(self._v) / len(self._v)

    def maximum(self):
        if not self._v:
            return None
        return max(self._v)

    def percentile(self, p):
        """Nearest-rank percentile, p in (0, 100]."""
        if not self._v:
            return None
        s = sorted(self._v)
        rank = max(1, int(-(-p * len(s) // 100)))  # ceil(p/100 * n)
        return s[min(rank, len(s)) - 1]

    def summary(self):
        if not self._v:
            return {"count": 0}
        return {
            "count": self.count,
            "mean": round(self.mean(), 3),
            "p50": self.percentile(50),
            "p90": self.percentile(90),
            "p99": self.percentile(99),
            "max": self.maximum(),
        }
