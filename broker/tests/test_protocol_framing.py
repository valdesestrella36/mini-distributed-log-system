import sys
from pathlib import Path
import asyncio

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from broker import pack_frame, read_frame


def test_pack_frame_header():
    payload = {"type": "PING"}
    b = pack_frame(payload)
    assert len(b) >= 4
    size = int.from_bytes(b[:4], "big")
    assert size == len(b) - 4


def test_read_frame_sync():
    payload = {"type": "PING"}
    b = pack_frame(payload)

    class DummyReader:
        def __init__(self, data: bytes):
            self._data = data

        async def readexactly(self, n: int) -> bytes:
            if len(self._data) < n:
                raise asyncio.IncompleteReadError(partial=self._data, expected=n)
            out = self._data[:n]
            self._data = self._data[n:]
            return out

    reader = DummyReader(b)
    resp = asyncio.run(read_frame(reader))
    assert resp == payload
