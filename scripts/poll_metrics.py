#!/usr/bin/env python3
"""Poll the broker for METRICS and pretty-print the histogram summaries.

Usage: ./scripts/poll_metrics.py --host 127.0.0.1 --port 9000
"""
import argparse
import json
import socket
from typing import Any, Dict


def pack_frame(obj: Dict[str, Any]) -> bytes:
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def read_frame_from_sock(s: socket.socket) -> Dict[str, Any]:
    # read 4-byte header
    hdr = s.recv(4)
    if len(hdr) < 4:
        raise EOFError("connection closed before header")
    size = int.from_bytes(hdr, "big")
    data = b""
    while len(data) < size:
        chunk = s.recv(size - len(data))
        if not chunk:
            raise EOFError("connection closed while reading payload")
        data += chunk
    return json.loads(data.decode("utf-8"))


def pretty_print_metrics(metrics: Dict[str, Any]) -> None:
    print("--- METRICS ---")
    # print counters
    for k, v in metrics.items():
        if k == "histograms":
            continue
        print(f"{k}: {v}")

    h = metrics.get("histograms", {})
    if h:
        print("\nHistograms:")
        for kind, tagmap in h.items():
            print(f"- {kind}:")
            for tag, summary in tagmap.items():
                print(f"    {tag} -> count={summary.get('count')} avg={summary.get('avg_ms'):.3f} p50={summary.get('p50_ms'):.3f} p90={summary.get('p90_ms'):.3f} p99={summary.get('p99_ms'):.3f}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    args = p.parse_args()

    with socket.create_connection((args.host, args.port), timeout=5) as s:
        s.sendall(pack_frame({"type": "METRICS"}))
        resp = read_frame_from_sock(s)
        if resp.get("status") != "OK":
            print("Broker returned error:", resp)
            return
        metrics = resp.get("metrics", {})
        pretty_print_metrics(metrics)


if __name__ == "__main__":
    main()
