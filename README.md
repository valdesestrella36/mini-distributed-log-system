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

