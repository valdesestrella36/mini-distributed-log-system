import sys
from pathlib import Path
import asyncio
import json
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker, pack_frame


async def _run_server_and_test():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        b = Broker(data_dir=data_dir)
        server = await asyncio.start_server(b._client_handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        task = asyncio.create_task(server.serve_forever())
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            # produce 3 messages
            for i in range(3):
                req = {"type": "PRODUCE", "topic": "t", "partition": 0, "value": f"m{i}"}
                writer.write(pack_frame(req))
                await writer.drain()
                header = await reader.readexactly(4)
                size = int.from_bytes(header, "big")
                payload = await reader.readexactly(size)
                resp = json.loads(payload.decode())
                assert resp.get("status") == "OK"

            # fetch from offset 0
            req = {"type": "FETCH", "topic": "t", "partition": 0, "offset": 0, "max_bytes": 4096}
            writer.write(pack_frame(req))
            await writer.drain()
            header = await reader.readexactly(4)
            size = int.from_bytes(header, "big")
            payload = await reader.readexactly(size)
            resp = json.loads(payload.decode())
            msgs = resp.get("messages", [])
            assert len(msgs) == 3
            assert msgs[0]["value"] == "m0"

            writer.close()
            await writer.wait_closed()
        finally:
            task.cancel()
            server.close()
            await server.wait_closed()


def test_produce_fetch_e2e():
    asyncio.run(_run_server_and_test())
