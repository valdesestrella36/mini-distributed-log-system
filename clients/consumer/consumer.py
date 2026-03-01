import argparse
import asyncio
import json
import time


async def fetch_once(reader, writer, topic: str, partition: int, offset: int, max_bytes: int):
    req = {"type": "FETCH", "topic": topic, "partition": partition, "offset": offset, "max_bytes": max_bytes}
    payload = json.dumps(req).encode("utf-8")
    writer.write(len(payload).to_bytes(4, "big") + payload)
    await writer.drain()
    header = await reader.readexactly(4)
    size = int.from_bytes(header, "big")
    payload = await reader.readexactly(size)
    resp = json.loads(payload.decode())
    return resp


async def fetch(host: str, port: int, topic: str, partition: int, offset: int, max_bytes: int, follow: bool, poll_interval: float = 0.5):
    reader, writer = await asyncio.open_connection(host, port)
    cur_offset = offset
    try:
        while True:
            resp = await fetch_once(reader, writer, topic, partition, cur_offset, max_bytes)
            msgs = resp.get("messages", [])
            for m in msgs:
                print(m)
                cur_offset = m.get("offset", cur_offset) + 1
            if not follow:
                break
            await asyncio.sleep(poll_interval)
    finally:
        writer.close()
        await writer.wait_closed()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("topic", help="topic name")
    p.add_argument("--partition", type=int, default=0)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--max-bytes", type=int, default=4096)
    p.add_argument("--follow", action="store_true", help="keep polling for new messages")
    p.add_argument("--poll-interval", type=float, default=0.5)
    args = p.parse_args()
    asyncio.run(fetch(args.host, args.port, args.topic, args.partition, args.offset, args.max_bytes, args.follow, args.poll_interval))


if __name__ == "__main__":
    main()
