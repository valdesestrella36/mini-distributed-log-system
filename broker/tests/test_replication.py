import asyncio
import os
import time

from broker import Broker, pack_frame, read_frame


async def run_broker(b: Broker, host, port):
    server = await asyncio.start_server(b._client_handler, host, port)
    async with server:
        await server.serve_forever()


def test_leader_replicates_to_follower(tmp_path):
    async def body():
        host = "127.0.0.1"
        leader_port = 9101
        follower_port = 9102

        follower_dir = tmp_path / "follower"
        follower = Broker(data_dir=follower_dir)
        follower_task = asyncio.create_task(run_broker(follower, host, follower_port))
        await asyncio.sleep(0.05)

        os.environ["MDLS_PEERS"] = f"{host}:{follower_port}"
        leader_dir = tmp_path / "leader"
        leader = Broker(data_dir=leader_dir)
        leader_task = asyncio.create_task(run_broker(leader, host, leader_port))
        await asyncio.sleep(0.05)

        reader, writer = await asyncio.open_connection(host, leader_port)
        req = {"type": "PRODUCE", "topic": "rep-test", "value": "hello", "partition": 0}
        writer.write(pack_frame(req))
        await writer.drain()
        resp = await read_frame(reader)
        assert resp.get("status") == "OK"

        await asyncio.sleep(0.1)

        r2, w2 = await asyncio.open_connection(host, follower_port)
        freq = {"type": "FETCH", "topic": "rep-test", "partition": 0, "offset": 0, "max_bytes": 4096}
        w2.write(pack_frame(freq))
        await w2.drain()
        fresp = await read_frame(r2)
        assert fresp.get("status") == "OK"
        msgs = fresp.get("messages", [])
        assert any(m.get("value") == "hello" for m in msgs)

        writer.close()
        await writer.wait_closed()
        w2.close()
        await w2.wait_closed()

        follower_task.cancel()
        leader_task.cancel()

    asyncio.run(body())