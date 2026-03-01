import threading
import time
from typing import Dict, Any


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._counters = {
            "messages_produced": 0,
            "messages_fetched": 0,
        }
        # simple latency accumulators (ms)
        self._latency = {
            "produce_sum_ms": 0.0,
            "produce_count": 0,
            "fetch_sum_ms": 0.0,
            "fetch_count": 0,
        }

    def incr(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._counters.setdefault(key, 0)
            self._counters[key] += n

    def record_latency(self, kind: str, ms: float) -> None:
        with self._lock:
            if kind == "produce":
                self._latency["produce_sum_ms"] += ms
                self._latency["produce_count"] += 1
            elif kind == "fetch":
                self._latency["fetch_sum_ms"] += ms
                self._latency["fetch_count"] += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            snap = dict(self._counters)
            # compute averages
            snap["produce_avg_ms"] = (
                (self._latency["produce_sum_ms"] / self._latency["produce_count"]) if self._latency["produce_count"] else 0.0
            )
            snap["fetch_avg_ms"] = (
                (self._latency["fetch_sum_ms"] / self._latency["fetch_count"]) if self._latency["fetch_count"] else 0.0
            )
            snap["produce_count"] = self._latency["produce_count"]
            snap["fetch_count"] = self._latency["fetch_count"]
            return snap
