#!/usr/bin/env bash
set -euo pipefail

# Simple demo: start broker, produce messages with Python producer, fetch with Python consumer.
# Run from repo root: ./scripts/demo_python.sh

ROOT=$(cd "$(dirname "$0")/.." && pwd)
LOGS_DIR="$ROOT/tmp_demo_logs"
mkdir -p "$LOGS_DIR"

BROKER_LOG="$LOGS_DIR/broker.log"

echo "Starting broker (background)..."
python "$ROOT/broker/src/broker.py" >"$BROKER_LOG" 2>&1 &
BROKER_PID=$!
echo "broker pid=$BROKER_PID"

cleanup() {
  echo "Stopping broker..."
  kill "$BROKER_PID" 2>/dev/null || true
  wait "$BROKER_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 0.6
echo "Producing 5 messages to topic 'demo'..."
python "$ROOT/clients/producer/producer.py" demo "hello-{i}" --count 5

echo "Fetching messages from 'demo'..."
python "$ROOT/clients/consumer/consumer.py" demo --follow --offset 0 --max-bytes 4096 --poll-interval 0.2 &
PID_CONS=$!

# Let consumer run a short while to print messages
sleep 2
kill "$PID_CONS" 2>/dev/null || true

echo "Broker log tail:"
tail -n 40 "$BROKER_LOG" || true

echo "Demo complete. Logs in $LOGS_DIR"
