import sys
from pathlib import Path
import asyncio
import json
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import Broker, pack_frame


async def _start_server(data_dir):
    b = Broker(data_dir=data_dir)
    # set strict limits for test
    b.max_message_bytes = 16
    b.rate_limit_per_sec = 1.0
    b.rate_limit_burst = 1.0
    server = await asyncio.start_server(b._client_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    task = asyncio.create_task(server.serve_forever())
    return server, task, port


def test_message_too_large():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        async def run():
            server, task, port = await _start_server(data_dir)
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                # message > 16 bytes
                req = {"type": "PRODUCE", "topic": "t", "partition": 0, "value": "this is a large message"}
                writer.write(pack_frame(req))
                await writer.drain()
                header = await reader.readexactly(4)
                size = int.from_bytes(header, "big")
                payload = await reader.readexactly(size)
                resp = json.loads(payload.decode())
                assert resp.get("status") == "ERR"
                assert resp.get("code") == "MSG_TOO_LARGE"
                writer.close()
                await writer.wait_closed()
            finally:
                task.cancel()
                server.close()
                await server.wait_closed()

        asyncio.run(run())


def test_rate_limit_rejects_excess():
    with tempfile.TemporaryDirectory() as td:
        data_dir = Path(td) / "data" / "logs"
        async def run():
            server, task, port = await _start_server(data_dir)
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                # send 3 messages quickly, only 1 token available
                errs = 0
                oks = 0
                for i in range(3):
                    req = {"type": "PRODUCE", "topic": "t", "partition": 0, "value": f"m{i}"}
                    writer.write(pack_frame(req))
                    await writer.drain()
                    header = await reader.readexactly(4)
                    size = int.from_bytes(header, "big")
                    payload = await reader.readexactly(size)
                    resp = json.loads(payload.decode())
                    if resp.get("status") == "ERR":
                        errs += 1
                    else:
                        oks += 1

                assert errs >= 1
                writer.close()
                await writer.wait_closed()
            finally:
                task.cancel()
                server.close()
                await server.wait_closed()

        asyncio.run(run())
