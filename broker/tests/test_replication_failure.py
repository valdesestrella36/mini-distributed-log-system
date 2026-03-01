import asyncio
import os

from broker import Broker, pack_frame, read_frame


def test_replication_peer_down(tmp_path):
    async def body():
        host = "127.0.0.1"
        leader_port = 9211
        # peer port where nothing listens
        dead_peer_port = 9311

        os.environ["MDLS_PEERS"] = f"{host}:{dead_peer_port}"

        leader_dir = tmp_path / "leader"
        leader = Broker(data_dir=leader_dir)

        # start leader
        server = await asyncio.start_server(leader._client_handler, host, leader_port)
        server_task = asyncio.create_task(server.serve_forever())
        await asyncio.sleep(0.05)

        # produce to leader; replication will attempt to contact dead peer and should fail silently
        r, w = await asyncio.open_connection(host, leader_port)
        req = {"type": "PRODUCE", "topic": "rep-fail", "value": "x", "partition": 0}
        w.write(pack_frame(req))
        await w.drain()
        resp = await read_frame(r)
        assert resp.get("status") == "OK"

        # fetch from leader to ensure message persisted locally
        r2, w2 = await asyncio.open_connection(host, leader_port)
        freq = {"type": "FETCH", "topic": "rep-fail", "partition": 0, "offset": 0, "max_bytes": 4096}
        w2.write(pack_frame(freq))
        await w2.drain()
        fresp = await read_frame(r2)
        assert fresp.get("status") == "OK"
        msgs = fresp.get("messages", [])
        assert any(m.get("value") == "x" for m in msgs)

        # cleanup
        w.close()
        await w.wait_closed()
        w2.close()
        await w2.wait_closed()
        server_task.cancel()

    asyncio.run(body())
