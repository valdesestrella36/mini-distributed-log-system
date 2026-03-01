# mini-distributed-log-system

[![CI](https://github.com/valdesestrella36/mini-distributed-log-system/actions/workflows/ci.yml/badge.svg)](https://github.com/valdesestrella36/mini-distributed-log-system/actions)

A minimal distributed log broker (mini-Kafka) implemented as a learning / portfolio project.

Quickstart

- Build C client:

```bash
make build-c-client
```

- Run broker locally:

```bash
make run-broker
```

- Run the demo (builds C client, starts broker, produces and fetches):

```bash
./scripts/demo_c_client.sh
```

Observability
-----------

The broker exposes runtime metrics via the broker `Metrics` helper.

- To enable JSON structured logs set the environment variable `MDLS_LOG_JSON=1`.
- To set log level use `MDLS_LOG_LEVEL` (e.g. `DEBUG`, `INFO`, `WARNING`).

Metrics snapshot example (returned by the `METRICS` request):

```json
{
	"messages_produced": 10,
	"messages_fetched": 5,
	"produce_count": 10,
	"produce_avg_ms": 2.3,
	"histograms": {
		"produce": {
			"_default": { "count": 10, "avg_ms": 2.3, "p50_ms": 1.9, "p90_ms": 4.5, "p99_ms": 9.1 }
		}
	}
}
```

Recording tagged metrics in code:

```python
from broker.src.metrics import Metrics
metrics = Metrics()
metrics.incr("messages_produced", 1, tags={"topic": "t1"})
metrics.record_latency("produce", 3.2, tags={"topic": "t1", "partition": "0"})
```

Enable JSON logs and run broker (example):

```bash
export MDLS_LOG_JSON=1
export MDLS_LOG_LEVEL=INFO
make run-broker
```

Step-by-step tutorial (quick demo)
---------------------------------

From the repository root you can run a quick Python-only demo that starts the broker,
produces messages and fetches them with the included Python clients:

```bash
./scripts/demo_python.sh
```

This script prints the produced and consumed messages and shows a tail of the broker log.

If you prefer running the steps manually:

1. Start the broker (foreground):

```bash
python broker/src/broker.py
```

2. In another shell, produce messages:

```bash
# produce 10 messages containing index
python clients/producer/producer.py demo "msg-{i}" --count 10
```

3. In another shell, fetch messages (follow mode):

```bash
python clients/consumer/consumer.py demo --follow --offset 0
```


