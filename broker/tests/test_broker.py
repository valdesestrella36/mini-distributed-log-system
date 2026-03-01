import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker


def test_ping():
    b = Broker()
    resp = asyncio.run(b.handle_request({"type": "PING"}))
    assert resp.get("status") == "OK"
