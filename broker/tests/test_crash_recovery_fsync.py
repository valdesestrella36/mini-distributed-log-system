import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import json

from broker import pack_frame


def read_frame_sync(f):
    hdr = f.read(4)
    if not hdr:
        raise EOFError("no header")
    size = int.from_bytes(hdr, "big")
    payload = f.read(size)
    return json.loads(payload.decode("utf-8"))


def _find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    addr, port = s.getsockname()
    s.close()
    return port


def _wait_for_port(port, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            s.close()
            return True
        except Exception:
            time.sleep(0.05)
    return False


def test_crash_recovery_with_fsync(tmp_path):
    # FSYNC enabled: messages should survive abrupt kill
    os.environ["MDLS_FSYNC"] = "1"
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    port = _find_free_port()

    # start broker subprocess
    code = (
        "import sys,asyncio,pathlib;"
        "from broker import Broker;"
        "b=Broker(data_dir=pathlib.Path(sys.argv[1]));"
        "asyncio.run(b.start(host='127.0.0.1', port=int(sys.argv[2])))"
    )

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen([sys.executable, "-u", "-c", code, str(data_dir), str(port)], env=env)
    try:
        assert _wait_for_port(port, timeout=3.0)

        # produce messages
        reader = None
        writer = None
        try:
            reader, writer = socket.create_connection(("127.0.0.1", port)), None
        except Exception:
            pass
        # use low-level socket with pack_frame/read_frame helpers
        s = socket.create_connection(("127.0.0.1", port))
        f = s.makefile('rwb')
        n = 20
        for i in range(n):
            req = {"type": "PRODUCE", "topic": "crash-topic", "value": f"m{i}", "partition": 0}
            f.write(pack_frame(req))
            f.flush()
            # read ack synchronously
            ack = read_frame_sync(f)
            assert ack.get("status") == "OK"

        # kill process abruptly
        proc.kill()
        proc.wait(timeout=2)

        # restart broker
        proc2 = subprocess.Popen([sys.executable, "-u", "-c", code, str(data_dir), str(port)], env=env)
        try:
            assert _wait_for_port(port, timeout=3.0)
            s2 = socket.create_connection(("127.0.0.1", port))
            f2 = s2.makefile('rwb')
            # fetch all messages
            freq = {"type": "FETCH", "topic": "crash-topic", "partition": 0, "offset": 0, "max_bytes": 65536}
            f2.write(pack_frame(freq))
            f2.flush()
            fresp = read_frame_sync(f2)
            assert fresp.get("status") == "OK"
            msgs = fresp.get("messages", [])
            vals = [m.get("value") for m in msgs]
            for i in range(n):
                assert f"m{i}" in vals
        finally:
            proc2.kill()
            proc2.wait(timeout=2)
    finally:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=1)
        except Exception:
            pass