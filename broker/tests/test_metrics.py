import sys
from pathlib import Path
import asyncio
import tempfile
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker, pack_frame


def test_metrics_after_produce_and_fetch():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        b = Broker(data_dir=data_dir)
        # produce
        asyncio.run(b.handle_request({"type": "PRODUCE", "topic": "tm", "partition": 0, "value": "v1"}))
        # fetch
        asyncio.run(b.handle_request({"type": "FETCH", "topic": "tm", "partition": 0, "offset": 0, "max_bytes": 1024}))
        resp = asyncio.run(b.handle_request({"type": "METRICS"}))
        assert resp.get("status") == "OK"
        m = resp.get("metrics", {})
        assert m.get("messages_produced", 0) >= 1
        assert m.get("messages_fetched", 0) >= 1
        assert "produce_avg_ms" in m
        assert "fetch_avg_ms" in m
