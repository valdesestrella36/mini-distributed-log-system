import sys
from pathlib import Path
import asyncio
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker


async def _produce_many(b: Broker, n: int):
    async def p(i: int):
        await b.handle_request({"type": "PRODUCE", "topic": "ct", "partition": 0, "value": f"m{i}"})

    await asyncio.gather(*(p(i) for i in range(n)))


def test_concurrent_produce_creates_all_messages():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        b = Broker(data_dir=data_dir)
        asyncio.run(_produce_many(b, 50))
        msgs = asyncio.run(b.handle_request({"type": "FETCH", "topic": "ct", "partition": 0, "offset": 0, "max_bytes": 65536}))
        assert msgs.get("status") == "OK"
        collected = msgs.get("messages", [])
        assert len(collected) == 50
