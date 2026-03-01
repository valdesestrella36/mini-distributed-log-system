import threading
import time
from typing import Dict, Any, Optional, Tuple
import statistics


def _tags_key(tags: Optional[Dict[str, str]]) -> Tuple[Tuple[str, str], ...]:
    if not tags:
        return ()
    return tuple(sorted((k, str(v)) for k, v in tags.items()))


class Metrics:
    """Thread-safe metrics collector with counters and simple histograms.

    Features:
    - counters via `incr`
    - observe_latency(kind, ms, tags=None) records samples for histogram
    - snapshot computes counters and latency summaries (avg + p50/p90/p99) per kind and tag-set
    """

    def __init__(self, max_hist_samples: int = 2000):
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {
            "messages_produced": 0,
            "messages_fetched": 0,
        }
        # simple histogram samples per (kind, tags_key)
        self._histograms: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], list[float]] = {}
        self._max_hist_samples = max_hist_samples

    def incr(self, key: str, n: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        with self._lock:
            self._counters.setdefault(key, 0)
            self._counters[key] += n

    def observe_latency(self, kind: str, ms: float, tags: Optional[Dict[str, str]] = None) -> None:
        key = (kind, _tags_key(tags))
        with self._lock:
            hist = self._histograms.setdefault(key, [])
            hist.append(ms)
            # keep bounded memory: simple reservoir trimming
            if len(hist) > self._max_hist_samples:
                # drop oldest samples
                del hist[0 : len(hist) - self._max_hist_samples]

    # backward-compatible alias used by existing code
    def record_latency(self, kind: str, ms: float, tags: Optional[Dict[str, str]] = None) -> None:
        self.observe_latency(kind, ms, tags=tags)

    def _summarize_hist(self, samples: list[float]) -> Dict[str, Any]:
        if not samples:
            return {"count": 0, "avg_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0}
        # compute basic stats
        cnt = len(samples)
        avg = float(sum(samples) / cnt)
        p50 = float(statistics.median(samples))
        # approximate p90/p99
        s = sorted(samples)
        def pct(p: float) -> float:
            idx = int((p / 100.0) * (len(s) - 1))
            return float(s[idx])

        return {"count": cnt, "avg_ms": avg, "p50_ms": p50, "p90_ms": pct(90.0), "p99_ms": pct(99.0)}

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            snap = dict(self._counters)
            # aggregate histograms by kind (ignore tags for top-level keys but include per-tag summaries)
            per_kind: Dict[str, Dict[str, Any]] = {}
            per_tags: Dict[str, Dict[Tuple[Tuple[str, str], ...], Dict[str, Any]]] = {}
            for (kind, tags_key), samples in self._histograms.items():
                kind_summary = per_kind.setdefault(kind, {"count": 0, "avg_ms": 0.0})
                # update aggregate count/avg incrementally (simple weighted avg)
                ksum = kind_summary.get("_sum", 0.0) + sum(samples)
                kcnt = kind_summary.get("count", 0) + len(samples)
                kind_summary["_sum"] = ksum
                kind_summary["count"] = kcnt
                kind_summary["avg_ms"] = ksum / kcnt if kcnt else 0.0
                # per tags summary
                tag_map = per_tags.setdefault(kind, {})
                tag_map[tags_key] = self._summarize_hist(samples)

            # expose summaries in snap
            for kind, vals in per_kind.items():
                snap[f"{kind}_count"] = vals.get("count", 0)
                snap[f"{kind}_avg_ms"] = float(vals.get("avg_ms", 0.0))

            # include detailed per-tag histograms under a `histograms` key
            hist_out: Dict[str, Dict[str, Any]] = {}
            for kind, tagmap in per_tags.items():
                out = {}
                for tags_key, summary in tagmap.items():
                    # convert tags_key back to dict string representation
                    if not tags_key:
                        tag_name = "_default"
                    else:
                        tag_name = ";".join(f"{k}={v}" for k, v in tags_key)
                    out[tag_name] = summary
                hist_out[kind] = out

            snap["histograms"] = hist_out
            return snap
