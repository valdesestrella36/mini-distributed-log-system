import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

try:
    from .metrics import Metrics
except Exception:
    from metrics import Metrics

# Configure structured JSON logging for the broker
logger = logging.getLogger("mini_distributed_log.broker")
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = lambda record: json.dumps({
        "ts": time.time(),
        "level": record.levelname,
        "module": record.name,
        "msg": record.getMessage(),
        **(getattr(record, "extra", {}) if hasattr(record, "extra") else {}),
    })
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            return fmt(record)

    h.setFormatter(JsonFormatter())
    logger.addHandler(h)
    logger.setLevel(logging.INFO)
try:
    from .metadata import Metadata
except Exception:
    # fallback when module is loaded without package context during tests
    from metadata import Metadata

DATA_DIR = Path.cwd() / "data" / "logs"


def pack_frame(obj: Dict[str, Any]) -> bytes:
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    length = len(payload).to_bytes(4, "big")
    return length + payload


async def read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    data = await reader.readexactly(n)
    return data


async def read_frame(reader: asyncio.StreamReader) -> Dict[str, Any]:
    header = await read_exact(reader, 4)
    size = int.from_bytes(header, "big")
    payload = await read_exact(reader, size)
    return json.loads(payload.decode("utf-8"))


class Broker:
    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # metadata DB sits next to logs directory
        meta_path = self.data_dir.parent / "meta.db"
        self.meta = Metadata(meta_path)
        # backpressure / rate limit settings
        self.max_message_bytes = int(os.environ.get("MDLS_MAX_MESSAGE_BYTES", 1024 * 64))
        # token bucket params: tokens/sec and burst capacity
        self.rate_limit_per_sec = float(os.environ.get("MDLS_RATE_LIMIT_PER_SEC", 50.0))
        self.rate_limit_burst = float(os.environ.get("MDLS_RATE_LIMIT_BURST", 100.0))
        # pending write thresholds for pause/resume
        self.pause_threshold = int(os.environ.get("MDLS_PAUSE_THRESHOLD", 16))
        self.resume_threshold = int(os.environ.get("MDLS_RESUME_THRESHOLD", 8))
        # metrics
        self.metrics = Metrics()
        # cluster support removed in this build (single-node MVP)

    async def handle_request(self, req: Dict[str, Any]) -> Dict[str, Any]:
        t = req.get("type")
        if t == "PING":
            return {"status": "OK", "pong": True}

        if t == "METRICS":
            return {"status": "OK", "metrics": self.metrics.snapshot()}

        # replication RPC removed (single-node)

    # replication helpers removed (single-node)

        if t == "PRODUCE":
            topic = req.get("topic")
            partition = int(req.get("partition", 0))
            value = req.get("value")
            message_id = req.get("message_id")
            if topic is None or value is None:
                return {"status": "ERR", "code": "INVALID", "message": "topic/value required"}
            start = time.monotonic()
            offset = await asyncio.to_thread(self._append_message, topic, partition, value, message_id)
            elapsed_ms = (time.monotonic() - start) * 1000.0
            self.metrics.incr("messages_produced", 1)
            self.metrics.record_latency("produce", elapsed_ms)
            logger.info("produce", extra={"topic": topic, "partition": partition, "offset": offset, "message_id": message_id, "latency_ms": elapsed_ms})
            return {"status": "OK", "offset": offset}

        if t == "FETCH":
            topic = req.get("topic")
            partition = int(req.get("partition", 0))
            offset = int(req.get("offset", 0))
            max_bytes = int(req.get("max_bytes", 4096))
            start = time.monotonic()
            msgs = await asyncio.to_thread(self._read_messages, topic, partition, offset, max_bytes)
            elapsed_ms = (time.monotonic() - start) * 1000.0
            self.metrics.incr("messages_fetched", len(msgs))
            self.metrics.record_latency("fetch", elapsed_ms)
            logger.info("fetch", extra={"topic": topic, "partition": partition, "offset": offset, "count": len(msgs), "latency_ms": elapsed_ms})
            return {"status": "OK", "messages": msgs}

        return {"status": "ERR", "code": "UNKNOWN", "message": "unknown request type"}

    def _log_path(self, topic: str, partition: int) -> Path:
        fn = f"{topic}_{partition}.log"
        return self.data_dir / fn

    def _append_message(self, topic: str, partition: int, value: Any, message_id: Optional[str] = None) -> int:
        p = self._log_path(topic, partition)
        p.parent.mkdir(parents=True, exist_ok=True)
        # ensure topic exists in metadata (auto-create with single partition if missing)
        if not self.meta.has_topic(topic):
            self.meta.add_topic(topic, partitions=1)

        # dedup: if message_id provided and already present, return existing offset
        if message_id is not None:
            existing = self.meta.lookup_message_id(topic, partition, message_id)
            if existing is not None:
                return existing
        # simple offset = number of lines before append
        offset = 0
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                for _ in f:
                    offset += 1
        with p.open("a", encoding="utf-8") as f:
            line = json.dumps({"value": value})
            f.write(line + "\n")
        # record dedup mapping if message_id provided
        if message_id is not None:
            try:
                self.meta.add_message_id(topic, partition, message_id, offset)
            except Exception:
                pass
        return offset

    def _read_messages(self, topic: str, partition: int, offset: int, max_bytes: int) -> List[Dict[str, Any]]:
        p = self._log_path(topic, partition)
        if not p.exists():
            return []
        out = []
        with p.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < offset:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                out.append({"offset": i, "value": msg.get("value")})
                # approximate size check
                if sum(len(json.dumps(m)) for m in out) >= max_bytes:
                    break
        return out

    async def _client_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # per-connection token bucket
        token_bucket = _TokenBucket(self.rate_limit_per_sec, self.rate_limit_burst)
        pending_responses = 0
        transport = writer.transport
        try:
            while True:
                req = await read_frame(reader)

                # enforce PRODUCE message size limit early (if payload contains value length)
                if req.get("type") == "PRODUCE":
                    val = req.get("value")
                    if isinstance(val, str) and len(val.encode("utf-8")) > self.max_message_bytes:
                        resp = {"status": "ERR", "code": "MSG_TOO_LARGE", "message": "message too large"}
                        writer.write(pack_frame(resp))
                        await writer.drain()
                        continue
                    # rate limit: try consume a token, otherwise return error
                    if not token_bucket.consume(1):
                        resp = {"status": "ERR", "code": "RATE_LIMIT", "message": "rate limit exceeded"}
                        writer.write(pack_frame(resp))
                        await writer.drain()
                        continue

                resp = await self.handle_request(req)  # No-op for consistency
                # support replication RPC on followers: respond to REPLICATE requests
                # handled inside handle_request if type == 'REPLICATE'
                # write response and use pending counter to apply backpressure
                pending_responses += 1
                writer.write(pack_frame(resp))
                try:
                    await writer.drain()
                finally:
                    pending_responses -= 1

                if pending_responses >= self.pause_threshold:
                    try:
                        transport.pause_reading()
                    except Exception:
                        pass
                elif pending_responses <= self.resume_threshold:
                    try:
                        transport.resume_reading()
                    except Exception:
                        pass
        except asyncio.IncompleteReadError:
            pass
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self, host: str = "127.0.0.1", port: int = 9000):
        server = await asyncio.start_server(self._client_handler, host, port)
        addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"Broker listening on {addrs}")
        async with server:
            await server.serve_forever()


class _TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last = now

    def consume(self, amount: float) -> bool:
        self._refill()
        if self._tokens >= amount:
            self._tokens -= amount
            return True
        return False



if __name__ == "__main__":
    b = Broker()
    asyncio.run(b.start())
