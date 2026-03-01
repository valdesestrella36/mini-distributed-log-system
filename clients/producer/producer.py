import argparse
import asyncio
import json
import sys
from typing import Any, Dict


def pack_frame(obj: Dict[str, Any]) -> bytes:
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


async def produce(host: str, port: int, topic: str, partition: int, value: str, count: int):
    reader, writer = await asyncio.open_connection(host, port)
    for i in range(count):
        # create value per message; if value contains {i} format, format it
        try:
            v = value.format(i=i)
        except Exception:
            # fallback: append index when count > 1
            v = f"{value}" if count == 1 else f"{value}-{i}"
        req = {"type": "PRODUCE", "topic": topic, "partition": partition, "value": v}
        writer.write(pack_frame(req))
        await writer.drain()
        header = await reader.readexactly(4)
        size = int.from_bytes(header, "big")
        payload = await reader.readexactly(size)
        resp = json.loads(payload.decode())
        print(resp)
    writer.close()
    await writer.wait_closed()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("topic", help="topic name")
    p.add_argument("value", help="message value (supports '{i}' for index)")
    p.add_argument("--partition", type=int, default=0)
    p.add_argument("--count", type=int, default=1, help="number of messages to send")
    args = p.parse_args()
    asyncio.run(produce(args.host, args.port, args.topic, args.partition, args.value, args.count))


if __name__ == "__main__":
    main()
