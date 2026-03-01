#!/usr/bin/env bash
set -euo pipefail

ROOT=$(dirname "$(dirname "$0")")
cd "$ROOT"

echo "Building C client..."
make build-c-client

LOGFILE=$(mktemp /tmp/mini-broker-log.XXXXXX)

echo "Starting broker in background... (logs -> $LOGFILE)"
python broker/src/broker.py >"$LOGFILE" 2>&1 &
BROKER_PID=$!
trap 'echo Stopping broker; kill $BROKER_PID 2>/dev/null || true; rm -f "$LOGFILE"' EXIT

# wait for broker port 9000 to be ready
echo -n "Waiting for broker on 127.0.0.1:9000"
for i in {1..50}; do
  if nc -z 127.0.0.1 9000 2>/dev/null; then
    echo " ok"
    break
  fi
  echo -n .
  sleep 0.05
done

echo "Producing message using C client..."
./clients/c_client/c_client produce --host 127.0.0.1 --port 9000 --topic demo_topic --partition 0 --value "hello-from-c"

echo "Fetching messages using C client..."
./clients/c_client/c_client fetch --host 127.0.0.1 --port 9000 --topic demo_topic --partition 0 --offset 0

echo "Demo finished. Broker logs (last 50 lines):"
tail -n 50 "$LOGFILE" || true
