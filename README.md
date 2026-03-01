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
